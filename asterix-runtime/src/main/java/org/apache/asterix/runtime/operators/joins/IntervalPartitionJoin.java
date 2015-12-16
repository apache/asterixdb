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
package org.apache.asterix.runtime.operators.joins;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VGroupTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

/**
 * This class mainly applies one level of HHJ on a pair of
 * relations. It is always called by the descriptor.
 */
public class IntervalPartitionJoin {

    // Used for special probe BigObject which can not be held into the Join memory
    private FrameTupleAppender bigProbeFrameAppender;

    enum SIDE {
        BUILD,
        PROBE
    }

    private IHyracksTaskContext ctx;

    private final String buildRelName;
    private final String probeRelName;

    private final boolean RESIDENT = false;
    private final boolean SPILLED = true;

    private final int[] buildKeys;
    private final int[] probeKeys;

    private final IBinaryComparatorFactory[] comparatorFactories;

    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;

    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;

    private RunFileWriter[] buildRFWriters; //writing spilled build partitions
    private RunFileWriter[] probeRFWriters; //writing spilled probe partitions

    private final IPredicateEvaluator predEvaluator;
    private final boolean isLeftOuter;
    private final INullWriter[] nullWriters1;

    private final BitSet spilledStatus; //0=resident, 1=spilled
    private final int memForJoin;
    private final int k;
    private final int numOfPartitions;
    private InMemoryIntervalPartitionJoin inMemJoiner[]; //Used for joining resident partitions

    private VGroupTupleBufferManager buildBufferManager;
    private VGroupTupleBufferManager probeBufferManager;

    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;

    private boolean isTableEmpty; //Added for handling the case, where build side is empty (tableSize is 0)
    private boolean isReversed; //Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionJoin.class.getName());

    // stats information
    private int[] buildPSizeInTups;
    private IFrame reloadBuffer;
    private TuplePointer tempPtr = new TuplePointer();
    private int[] probePSizeInTups;

    private IIntervalMergeJoinChecker imjc;
    private ArrayList<Integer> spillOrder;
    private ArrayList<HashSet<Integer>> buildJoinMap;
    private ArrayList<HashSet<Integer>> probeJoinMap;
    ArrayList<HashSet<Integer>> probeJoinPartitions;
    ArrayList<HashSet<Integer>> probeJoinPartitionsInMemory;
    ArrayList<HashSet<Integer>> probeJoinPartitionsSpilled;
    private BitSet probePartitionsToSpill;

    public IntervalPartitionJoin(IHyracksTaskContext ctx, int memForJoin, int k, int numOfPartitions,
            String probeRelName, String buildRelName, int[] probeKeys, int[] buildKeys, IIntervalMergeJoinChecker mjc,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor probeRd, RecordDescriptor buildRd,
            ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator predEval,
            boolean isLeftOuter, INullWriterFactory[] nullWriterFactories1) {
        this.ctx = ctx;
        this.memForJoin = memForJoin;
        this.k = k;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.comparatorFactories = comparatorFactories;
        this.imjc = mjc;
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

        this.nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
            }
        }

        buildJoinMap = IntervalPartitionUtil.getJoinPartitionListOfSets(k, imjc);
        spillOrder = IntervalPartitionUtil.getPartitionSpillOrder(buildJoinMap);

        probeJoinMap = IntervalPartitionUtil.getProbeJoinPartitionListOfSets(buildJoinMap);
    }

    public void initBuild() throws HyracksDataException {
        buildBufferManager = new VGroupTupleBufferManager(ctx, numOfPartitions, memForJoin * ctx.getInitialFrameSize());
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
        while (!buildBufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
            int victimPartition = selectPartitionToSpill();
            if (victimPartition < 0) {
                throw new HyracksDataException(
                        "No more space left in the memory buffer, please give join more memory budgets.");
            }
            spillPartition(victimPartition);
        }
    }

    private int selectPartitionToSpill() {
        int partitionToSpill = selectLargestSpilledPartition();
        int maxToSpillPartSize = 0;
        if (partitionToSpill < 0 || (maxToSpillPartSize = buildBufferManager.getPhysicalSize(partitionToSpill)) == ctx
                .getInitialFrameSize()) {
            int partitionInMem = selectNextInMemoryPartitionToSpill();
            if (partitionInMem >= 0 && buildBufferManager.getPhysicalSize(partitionInMem) > maxToSpillPartSize) {
                partitionToSpill = partitionInMem;
            }
        }
        return partitionToSpill;
    }

    private int selectNextInMemoryPartitionToSpill() {
        for (int i = 0; i < spillOrder.size(); ++i) {
            if (spilledStatus.get(i) == RESIDENT && buildBufferManager.getPhysicalSize(i) > 0) {
                return i;
            }
        }
        return -1;

    }

    private int selectLargestSpilledPartition() {
        int pid = -1;
        int max = 0;
        for (int i = spilledStatus.nextSetBit(0); i >= 0; i = spilledStatus.nextSetBit(i + 1)) {
            int partSize = buildBufferManager.getPhysicalSize(i);
            if (partSize > max) {
                max = partSize;
                pid = i;
            }
        }
        return pid;
    }

    private void spillPartition(int pid) throws HyracksDataException {
        RunFileWriter writer = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.BUILD);
        buildBufferManager.flushPartition(pid, writer);
        buildBufferManager.clearPartition(pid);
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

        flushAndClearSpilledPartition(SIDE.BUILD);

        // Trying to bring back as many spilled partitions as possible, making them resident
        bringBackSpilledPartitionIfHasMoreMemory();

        // Update build partition join map based on partitions with actual data.
        for (int i = 0; i < buildPSizeInTups.length; ++i) {
            if (buildPSizeInTups[i] == 0) {
                buildJoinMap.get(i).clear();
            }
        }

        // Set up build memory for processing joins for partitions in memory.
        int inMemTupCount = 0;
        for (int i = spilledStatus.nextClearBit(0); i >= 0
                && i < numOfPartitions; i = spilledStatus.nextClearBit(i + 1)) {
            inMemTupCount += buildPSizeInTups[i];
            createInMemoryJoiner(i);
        }
        this.isTableEmpty = (inMemTupCount == 0);
    }

    private void flushAndClearSpilledPartition(SIDE whichSide) throws HyracksDataException {
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
            if (buildBufferManager.getNumTuples(pid) > 0) {
                buildBufferManager.flushPartition(pid, getSpillWriterOrCreateNewOneIfNotExist(pid, whichSide));
                buildBufferManager.clearPartition(pid);
                runFileWriters[pid].close();
            }
        }
    }

    private void bringBackSpilledPartitionIfHasMoreMemory() throws HyracksDataException {
        // we need number of |spilledPartitions| buffers to store the probe data
        int freeSpace = (memForJoin - spilledStatus.cardinality()) * ctx.getInitialFrameSize();
        for (int p = spilledStatus.nextClearBit(0); p >= 0
                && p < numOfPartitions; p = spilledStatus.nextClearBit(p + 1)) {
            freeSpace -= buildBufferManager.getPhysicalSize(p);
        }

        int pid = 0;
        while ((pid = selectPartitionsToReload(freeSpace, pid)) >= 0) {
            if (!loadPartitionInMem(pid, buildRFWriters[pid])) {
                return;
            }
            freeSpace -= buildBufferManager.getPhysicalSize(pid);
        }
    }

    private boolean loadPartitionInMem(int pid, RunFileWriter wr) throws HyracksDataException {
        RunFileReader r = wr.createDeleteOnCloseReader();
        r.open();
        if (reloadBuffer == null) {
            reloadBuffer = new VSizeFrame(ctx);
        }
        while (r.nextFrame(reloadBuffer)) {
            accessorBuild.reset(reloadBuffer.getBuffer());
            for (int tid = 0; tid < accessorBuild.getTupleCount(); tid++) {
                if (!buildBufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                    // for some reason (e.g. due to fragmentation) if the inserting failed, we need to clear the occupied frames
                    buildBufferManager.clearPartition(pid);
                    r.close();
                    return false;
                }
            }
        }

        r.close();
        spilledStatus.set(pid, false);
        buildRFWriters[pid] = null;
        return true;
    }

    private int selectPartitionsToReload(int freeSpace, int pid) {
        for (int i = spillOrder.size() - 1; i >= 0; --i) {
            int id = spillOrder.get(i);
            if (spilledStatus.get(id) == SPILLED) {
                assert buildRFWriters[id].getFileSize() > 0 : "How comes a spilled partition have size 0?";
                if (freeSpace >= buildRFWriters[id].getFileSize()) {
                    return i;
                }
            }
        }
        return -1;
    }

    private void createInMemoryJoiner(int pid) throws HyracksDataException {
        this.inMemJoiner[pid] = new InMemoryIntervalPartitionJoin(ctx,
                buildBufferManager.getPartitionFrameBufferManager(pid), new FrameTupleAccessor(probeRd),
                new FrameTupleAccessor(buildRd), predEvaluator, isReversed, buildKeys, probeKeys, comparatorFactories,
                buildRd, buildBufferManager.getNumTuples(pid));
    }

    public void initProbe() throws HyracksDataException {
        int probeMemory = numOfPartitions > memForJoin ? memForJoin : numOfPartitions;
        probeBufferManager = new VGroupTupleBufferManager(ctx, numOfPartitions,
                (probeMemory) * ctx.getInitialFrameSize());

        probePSizeInTups = new int[numOfPartitions];
        probeRFWriters = new RunFileWriter[numOfPartitions];

        probePartitionsToSpill = IntervalPartitionUtil.getProbePartitionSpillList(buildJoinMap, spilledStatus);
        probeJoinMap = IntervalPartitionUtil.getProbeJoinPartitionListOfSets(buildJoinMap);
        probeJoinPartitionsInMemory = IntervalPartitionUtil.getProbeJoinPartitionsInMemory(probeJoinMap, spilledStatus);
        probeJoinPartitionsSpilled = IntervalPartitionUtil.getProbeJoinPartitionsSpilled(probeJoinMap, spilledStatus);
        probeRFWriters = new RunFileWriter[numOfPartitions];

    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();

        for (int i = 0; i < tupleCount; ++i) {
            int pid = probeHpc.partition(accessorProbe, i, numOfPartitions);

            if (buildPSizeInTups[pid] > 0 || isLeftOuter) { //Tuple has potential match from previous phase
                if (!isBuildRelAllInMemory() && probePartitionsToSpill.get(pid)) { //pid is Spilled
                    while (!buildBufferManager.insertTuple(pid, accessorProbe, i, tempPtr)) {
                        int victim = pid;
                        if (buildBufferManager.getNumTuples(pid) == 0) { // current pid is empty, choose the biggest one
                            victim = selectLargestSpilledPartition();
                        }
                        if (victim < 0) { // current tuple is too big for all the free space
                            flushBigProbeObjectToDisk(pid, accessorProbe, i);
                            break;
                        }
                        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(victim, SIDE.PROBE);
                        buildBufferManager.flushPartition(victim, runFileWriter);
                        buildBufferManager.clearPartition(victim);
                    }
                }
                for (Integer j : probeJoinPartitionsInMemory.get(pid)) {
                    // pid has join partitions that are Resident
                    inMemJoiner[j].join(accessorProbe, i, writer);
                }
            }
            probePSizeInTups[pid]++;
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
        bigProbeFrameAppender.flush(runFileWriter, true);
    }

    private boolean isBuildRelAllInMemory() {
        return spilledStatus.nextSetBit(0) < 0;
    }

    public void closeProbe(IFrameWriter writer) throws HyracksDataException {
        // We do NOT join the spilled partitions here, that decision is made at the descriptor level (which join technique to use)
        for (int i = spilledStatus.nextClearBit(0); i >= 0
                && i < numOfPartitions; i = spilledStatus.nextClearBit(i + 1)) {
            inMemJoiner[i].closeJoin(writer);
            inMemJoiner[i] = null;
        }
        flushAndClearSpilledPartition(SIDE.PROBE);
        buildBufferManager = null;
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

    IFrame rFrame;

    public void joinSpilledPartitions(IFrameWriter writer) throws HyracksDataException {
        BitSet partitionStatus = (BitSet) getPartitionStatus().clone();
        do {
            bringBackSpilledPartitionIfHasMoreMemory();
            partitionStatus.xor(getPartitionStatus());
            probeJoinPartitionsInMemory = IntervalPartitionUtil.getProbeJoinPartitionsInMemory(probeJoinMap,
                    spilledStatus);

            for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid = partitionStatus.nextSetBit(pid + 1)) {
                createInMemoryJoiner(pid);
            }
            for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid = partitionStatus.nextSetBit(pid + 1)) {
                RunFileReader pReader = getProbeRFReader(pid);
                while (pReader.nextFrame(rFrame)) {
                    accessorProbe.reset(rFrame.getBuffer());
                    for (int i = 0; i < accessorProbe.getTupleCount(); ++i) {
                        for (Integer j : probeJoinPartitionsInMemory.get(pid)) {
                            // pid has join partitions that are Resident
                            inMemJoiner[j].join(accessorProbe, i, writer);
                        }
                    }
                }

                inMemJoiner[pid].closeJoin(writer);
                inMemJoiner[pid] = null;
            }
        } while (true);

    }

}
