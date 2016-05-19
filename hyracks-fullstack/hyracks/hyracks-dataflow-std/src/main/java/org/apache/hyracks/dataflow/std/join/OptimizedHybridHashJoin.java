/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

/**
 * This class mainly applies one level of HHJ on a pair of
 * relations. It is always called by the descriptor.
 */
public class OptimizedHybridHashJoin {

    // Used for special probe BigObject which can not be held into the Join memory
    private FrameTupleAppender bigProbeFrameAppender;

    enum SIDE {
        BUILD,
        PROBE
    }

    private IHyracksTaskContext ctx;

    private final String buildRelName;
    private final String probeRelName;

    private final int[] buildKeys;
    private final int[] probeKeys;

    private final IBinaryComparator[] comparators;

    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;

    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;

    private RunFileWriter[] buildRFWriters; //writing spilled build partitions
    private RunFileWriter[] probeRFWriters; //writing spilled probe partitions

    private final IPredicateEvaluator predEvaluator;
    private final boolean isLeftOuter;
    private final INullWriter[] nullWriters;

    private final BitSet spilledStatus; //0=resident, 1=spilled
    private final int numOfPartitions;
    private final int memForJoin;
    private InMemoryHashJoin inMemJoiner; //Used for joining resident partitions

    private IPartitionedTupleBufferManager bufferManager;
    private PreferToSpillFullyOccupiedFramePolicy spillPolicy;

    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;

    private boolean isReversed; //Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal

    // stats information
    private int[] buildPSizeInTups;
    private IFrame reloadBuffer;
    private TuplePointer tempPtr = new TuplePointer(); // this is a reusable object to store the pointer,which is not used anywhere.
                                                       // we mainly use it to match the corresponding function signature.
    private int[] probePSizeInTups;

    public OptimizedHybridHashJoin(IHyracksTaskContext ctx, int memForJoin, int numOfPartitions, String probeRelName,
            String buildRelName, int[] probeKeys, int[] buildKeys, IBinaryComparator[] comparators,
            RecordDescriptor probeRd, RecordDescriptor buildRd, ITuplePartitionComputer probeHpc,
            ITuplePartitionComputer buildHpc, IPredicateEvaluator predEval, boolean isLeftOuter,
            INullWriterFactory[] nullWriterFactories1) {
        this.ctx = ctx;
        this.memForJoin = memForJoin;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.comparators = comparators;
        this.buildRelName = buildRelName;
        this.probeRelName = probeRelName;

        this.numOfPartitions = numOfPartitions;
        this.buildRFWriters = new RunFileWriter[numOfPartitions];
        this.probeRFWriters = new RunFileWriter[numOfPartitions];

        this.accessorBuild = new FrameTupleAccessor(buildRd);
        this.accessorProbe = new FrameTupleAccessor(probeRd);

        this.predEvaluator = predEval;
        this.isLeftOuter = isLeftOuter;
        this.isReversed = false;

        this.spilledStatus = new BitSet(numOfPartitions);

        this.nullWriters = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nullWriters[i] = nullWriterFactories1[i].createNullWriter();
            }
        }
    }

    public void initBuild() throws HyracksDataException {
        bufferManager = new VPartitionTupleBufferManager(ctx,
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus),
                numOfPartitions, memForJoin * ctx.getInitialFrameSize());
        spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, spilledStatus,
                ctx.getInitialFrameSize());
        spilledStatus.clear();
        buildPSizeInTups = new int[numOfPartitions];
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();

        for (int i = 0; i < tupleCount; ++i) {
            int pid = buildHpc.partition(accessorBuild, i, numOfPartitions);
            processTuple(i, pid);
            buildPSizeInTups[pid]++;
        }

    }

    private void processTuple(int tid, int pid) throws HyracksDataException {
        while (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
            selectAndSpillVictim(pid);
        }
    }

    private void selectAndSpillVictim(int pid) throws HyracksDataException {
        int victimPartition = spillPolicy.selectVictimPartition(pid);
        if (victimPartition < 0) {
            throw new HyracksDataException(
                    "No more space left in the memory buffer, please give join more memory budgets.");
        }
        spillPartition(victimPartition);
    }

    private void spillPartition(int pid) throws HyracksDataException {
        RunFileWriter writer = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.BUILD);
        bufferManager.flushPartition(pid, writer);
        bufferManager.clearPartition(pid);
        spilledStatus.set(pid);
    }

    private RunFileWriter getSpillWriterOrCreateNewOneIfNotExist(int pid, SIDE whichSide) throws HyracksDataException {
        RunFileWriter[] runFileWriters = null;
        String refName = null;
        switch (whichSide) {
            case BUILD:
                runFileWriters = buildRFWriters;
                refName = buildRelName;
                break;
            case PROBE:
                refName = probeRelName;
                runFileWriters = probeRFWriters;
                break;
        }
        RunFileWriter writer = runFileWriters[pid];
        if (writer == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(refName);
            writer = new RunFileWriter(file, ctx.getIOManager());
            writer.open();
            runFileWriters[pid] = writer;
        }
        return writer;
    }

    public void closeBuild() throws HyracksDataException {

        closeAllSpilledPartitions(SIDE.BUILD);

        bringBackSpilledPartitionIfHasMoreMemory(); //Trying to bring back as many spilled partitions as possible, making them resident

        int inMemTupCount = 0;

        for (int i = spilledStatus.nextClearBit(0); i >= 0
                && i < numOfPartitions; i = spilledStatus.nextClearBit(i + 1)) {
            inMemTupCount += buildPSizeInTups[i];
        }

        createInMemoryJoiner(inMemTupCount);
        cacheInMemJoin();
    }

    /**
     * In case of failure happens, we need to clear up the generated temporary files.
     */
    public void clearBuildTempFiles() {
        for (int i = 0; i < buildRFWriters.length; i++) {
            if (buildRFWriters[i] != null) {
                buildRFWriters[i].getFileReference().delete();
            }
        }
    }

    private void closeAllSpilledPartitions(SIDE whichSide) throws HyracksDataException {
        RunFileWriter[] runFileWriters = null;
        switch (whichSide) {
            case BUILD:
                runFileWriters = buildRFWriters;
                break;
            case PROBE:
                runFileWriters = probeRFWriters;
                break;
        }

        for (int pid = spilledStatus.nextSetBit(0); pid >= 0; pid = spilledStatus.nextSetBit(pid + 1)) {
            if (bufferManager.getNumTuples(pid) > 0) {
                bufferManager.flushPartition(pid, getSpillWriterOrCreateNewOneIfNotExist(pid, whichSide));
                bufferManager.clearPartition(pid);
                runFileWriters[pid].close();
            }
        }
    }

    private void bringBackSpilledPartitionIfHasMoreMemory() throws HyracksDataException {
        // we need number of |spilledPartitions| buffers to store the probe data
        int freeSpace = (memForJoin - spilledStatus.cardinality()) * ctx.getInitialFrameSize();
        for (int p = spilledStatus.nextClearBit(0); p >= 0
                && p < numOfPartitions; p = spilledStatus.nextClearBit(p + 1)) {
            freeSpace -= bufferManager.getPhysicalSize(p);
        }

        int pid = 0;
        while ((pid = selectPartitionsToReload(freeSpace, pid)) >= 0) {
            if (!loadPartitionInMem(pid, buildRFWriters[pid])) {
                return;
            }
            freeSpace -= bufferManager.getPhysicalSize(pid);
        }
    }

    private boolean loadPartitionInMem(int pid, RunFileWriter wr) throws HyracksDataException {
        RunFileReader r = wr.createReader();
        r.open();
        if (reloadBuffer == null) {
            reloadBuffer = new VSizeFrame(ctx);
        }
        while (r.nextFrame(reloadBuffer)) {
            accessorBuild.reset(reloadBuffer.getBuffer());
            for (int tid = 0; tid < accessorBuild.getTupleCount(); tid++) {
                if (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                    // for some reason (e.g. due to fragmentation) if the inserting failed, we need to clear the occupied frames
                    bufferManager.clearPartition(pid);
                    r.close();
                    return false;
                }
            }
        }

        FileUtils.deleteQuietly(wr.getFileReference().getFile()); // delete the runfile if it already loaded into memory.
        r.close();
        spilledStatus.set(pid, false);
        buildRFWriters[pid] = null;
        return true;
    }

    private int selectPartitionsToReload(int freeSpace, int pid) {
        for (int i = spilledStatus.nextSetBit(pid); i >= 0; i = spilledStatus.nextSetBit(i + 1)) {
            assert buildRFWriters[i].getFileSize() > 0 : "How comes a spilled partition have size 0?";
            if (freeSpace >= buildRFWriters[i].getFileSize()) {
                return i;
            }
        }
        return -1;
    }

    private void createInMemoryJoiner(int inMemTupCount) throws HyracksDataException {
        ISerializableTable table = new SerializableHashTable(inMemTupCount, ctx);
        this.inMemJoiner = new InMemoryHashJoin(ctx, inMemTupCount, new FrameTupleAccessor(probeRd), probeHpc,
                new FrameTupleAccessor(buildRd), buildHpc,
                new FrameTuplePairComparator(probeKeys, buildKeys, comparators), isLeftOuter, nullWriters, table,
                predEvaluator, isReversed);
    }

    private void cacheInMemJoin() throws HyracksDataException {

        for (int pid = 0; pid < numOfPartitions; pid++) {
            if (!spilledStatus.get(pid)) {
                bufferManager.flushPartition(pid, new IFrameWriter() {
                    @Override
                    public void open() throws HyracksDataException {

                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        inMemJoiner.build(buffer);
                    }

                    @Override
                    public void fail() throws HyracksDataException {

                    }

                    @Override
                    public void close() throws HyracksDataException {

                    }
                });
            }
        }
    }

    public void initProbe() throws HyracksDataException {

        probePSizeInTups = new int[numOfPartitions];
        probeRFWriters = new RunFileWriter[numOfPartitions];

    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();

        if (isBuildRelAllInMemory()) {
            inMemJoiner.join(buffer, writer);
            return;
        }
        for (int i = 0; i < tupleCount; ++i) {
            int pid = probeHpc.partition(accessorProbe, i, numOfPartitions);

            if (buildPSizeInTups[pid] > 0 || isLeftOuter) { //Tuple has potential match from previous phase
                if (spilledStatus.get(pid)) { //pid is Spilled
                    while (!bufferManager.insertTuple(pid, accessorProbe, i, tempPtr)) {
                        int victim = pid;
                        if (bufferManager.getNumTuples(pid) == 0) { // current pid is empty, choose the biggest one
                            victim = spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
                        }
                        if (victim < 0) { // current tuple is too big for all the free space
                            flushBigProbeObjectToDisk(pid, accessorProbe, i);
                            break;
                        }
                        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(victim, SIDE.PROBE);
                        bufferManager.flushPartition(victim, runFileWriter);
                        bufferManager.clearPartition(victim);
                    }
                } else { //pid is Resident
                    inMemJoiner.join(accessorProbe, i, writer);
                }
                probePSizeInTups[pid]++;
            }
        }

    }

    private void flushBigProbeObjectToDisk(int pid, FrameTupleAccessor accessorProbe, int i)
            throws HyracksDataException {
        if (bigProbeFrameAppender == null) {
            bigProbeFrameAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        }
        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.PROBE);
        if (!bigProbeFrameAppender.append(accessorProbe, i)) {
            throw new HyracksDataException("The given tuple is too big");
        }
        bigProbeFrameAppender.write(runFileWriter, true);
    }

    private boolean isBuildRelAllInMemory() {
        return spilledStatus.nextSetBit(0) < 0;
    }

    public void closeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level (which join technique to use)
        inMemJoiner.closeJoin(writer);
        closeAllSpilledPartitions(SIDE.PROBE);
        bufferManager = null;
    }

    /**
     * In case of failure happens, we need to clear up the generated temporary files.
     */
    public void clearProbeTempFiles() {
        for (int i = 0; i < probeRFWriters.length; i++) {
            if (probeRFWriters[i] != null) {
                probeRFWriters[i].getFileReference().delete();
            }
        }
    }

    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return ((buildRFWriters[pid] == null) ? null : (buildRFWriters[pid]).createDeleteOnCloseReader());
    }

    public int getBuildPartitionSizeInTup(int pid) {
        return (buildPSizeInTups[pid]);
    }

    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return ((probeRFWriters[pid] == null) ? null : (probeRFWriters[pid]).createDeleteOnCloseReader());
    }

    public int getProbePartitionSizeInTup(int pid) {
        return (probePSizeInTups[pid]);
    }

    public int getMaxBuildPartitionSize() {
        int max = buildPSizeInTups[0];
        for (int i = 1; i < buildPSizeInTups.length; i++) {
            if (buildPSizeInTups[i] > max) {
                max = buildPSizeInTups[i];
            }
        }
        return max;
    }

    public int getMaxProbePartitionSize() {
        int max = probePSizeInTups[0];
        for (int i = 1; i < probePSizeInTups.length; i++) {
            if (probePSizeInTups[i] > max) {
                max = probePSizeInTups[i];
            }
        }
        return max;
    }

    public BitSet getPartitionStatus() {
        return spilledStatus;
    }

    public void setIsReversed(boolean b) {
        this.isReversed = b;
    }
}
