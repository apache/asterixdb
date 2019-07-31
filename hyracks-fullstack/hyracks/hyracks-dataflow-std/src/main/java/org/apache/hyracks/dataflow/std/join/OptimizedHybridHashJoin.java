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

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * This class mainly applies one level of HHJ on a pair of
 * relations. It is always called by the descriptor.
 */
public class OptimizedHybridHashJoin {

    // Used for special probe BigObject which can not be held into the Join memory
    private FrameTupleAppender bigProbeFrameAppender;

    public enum SIDE {
        BUILD,
        PROBE
    }

    private final IHyracksTaskContext ctx;

    private final String buildRelName;
    private final String probeRelName;
    private final ITuplePairComparator comparator;
    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;
    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;
    private final RunFileWriter[] buildRFWriters; //writing spilled build partitions
    private final RunFileWriter[] probeRFWriters; //writing spilled probe partitions
    private final IPredicateEvaluator predEvaluator;
    private final boolean isLeftOuter;
    private final IMissingWriter[] nonMatchWriters;
    private final BitSet spilledStatus; //0=resident, 1=spilled
    private final int numOfPartitions;
    private final int memSizeInFrames;
    private InMemoryHashJoin inMemJoiner; //Used for joining resident partitions
    private IPartitionedTupleBufferManager bufferManager;
    private PreferToSpillFullyOccupiedFramePolicy spillPolicy;
    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    private ISimpleFrameBufferManager bufferManagerForHashTable;
    // Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal.
    private boolean isReversed;
    // stats information
    private int[] buildPSizeInTups;
    private IFrame reloadBuffer;
    // this is a reusable object to store the pointer,which is not used anywhere. we mainly use it to match the
    // corresponding function signature.
    private final TuplePointer tempPtr = new TuplePointer();
    private int[] probePSizeInTups;

    public OptimizedHybridHashJoin(IHyracksTaskContext ctx, int memSizeInFrames, int numOfPartitions,
            String probeRelName, String buildRelName, ITuplePairComparator comparator, RecordDescriptor probeRd,
            RecordDescriptor buildRd, ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc,
            IPredicateEvaluator predEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        this.ctx = ctx;
        this.memSizeInFrames = memSizeInFrames;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.comparator = comparator;
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
        this.nonMatchWriters = isLeftOuter ? new IMissingWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nonMatchWriters[i] = nullWriterFactories1[i].createMissingWriter();
            }
        }
    }

    public void initBuild() throws HyracksDataException {
        IDeallocatableFramePool framePool =
                new DeallocatableFramePool(ctx, memSizeInFrames * ctx.getInitialFrameSize());
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);
        bufferManager = new VPartitionTupleBufferManager(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus),
                numOfPartitions, framePool);
        spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, spilledStatus);
        spilledStatus.clear();
        buildPSizeInTups = new int[numOfPartitions];
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            int pid = buildHpc.partition(accessorBuild, i, numOfPartitions);
            processTupleBuildPhase(i, pid);
            buildPSizeInTups[pid]++;
        }

    }

    private void processTupleBuildPhase(int tid, int pid) throws HyracksDataException {
        while (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
            int victimPartition = spillPolicy.selectVictimPartition(pid);
            if (victimPartition < 0) {
                throw new HyracksDataException(
                        "No more space left in the memory buffer, please assign more memory to hash-join.");
            }
            spillPartition(victimPartition);
        }
    }

    private void spillPartition(int pid) throws HyracksDataException {
        RunFileWriter writer = getSpillWriterOrCreateNewOneIfNotExist(buildRFWriters, buildRelName, pid);
        bufferManager.flushPartition(pid, writer);
        bufferManager.clearPartition(pid);
        spilledStatus.set(pid);
    }

    private void closeBuildPartition(int pid) throws HyracksDataException {
        if (buildRFWriters[pid] == null) {
            throw new HyracksDataException("Tried to close the non-existing file writer.");
        }
        buildRFWriters[pid].close();
    }

    private RunFileWriter getSpillWriterOrCreateNewOneIfNotExist(RunFileWriter[] runFileWriters, String refName,
            int pid) throws HyracksDataException {
        RunFileWriter writer = runFileWriters[pid];
        if (writer == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(refName);
            writer = new RunFileWriter(file, ctx.getIoManager());
            writer.open();
            runFileWriters[pid] = writer;
        }
        return writer;
    }

    public void closeBuild() throws HyracksDataException {
        // Flushes the remaining chunks of the all spilled partitions to the disk.
        closeAllSpilledPartitions(buildRFWriters, buildRelName);

        // Makes the space for the in-memory hash table (some partitions may need to be spilled to the disk
        // during this step in order to make the space.)
        // and tries to bring back as many spilled partitions as possible if there is free space.
        int inMemTupCount = makeSpaceForHashTableAndBringBackSpilledPartitions();

        ISerializableTable table = new SerializableHashTable(inMemTupCount, ctx, bufferManagerForHashTable);
        this.inMemJoiner = new InMemoryHashJoin(ctx, new FrameTupleAccessor(probeRd), probeHpc,
                new FrameTupleAccessor(buildRd), buildRd, buildHpc, comparator, isLeftOuter, nonMatchWriters, table,
                predEvaluator, isReversed, bufferManagerForHashTable);

        buildHashTable();
    }

    public void clearBuildTempFiles() throws HyracksDataException {
        clearTempFiles(buildRFWriters);
    }

    public void clearProbeTempFiles() throws HyracksDataException {
        clearTempFiles(probeRFWriters);
    }

    /**
     * In case of failure happens, we need to clear up the generated temporary files.
     */
    private void clearTempFiles(RunFileWriter[] runFileWriters) throws HyracksDataException {
        for (int i = 0; i < runFileWriters.length; i++) {
            if (runFileWriters[i] != null) {
                runFileWriters[i].erase();
            }
        }
    }

    private void closeAllSpilledPartitions(RunFileWriter[] runFileWriters, String refName) throws HyracksDataException {
        try {
            for (int pid = spilledStatus.nextSetBit(0); pid >= 0 && pid < numOfPartitions; pid =
                    spilledStatus.nextSetBit(pid + 1)) {
                if (bufferManager.getNumTuples(pid) > 0) {
                    bufferManager.flushPartition(pid,
                            getSpillWriterOrCreateNewOneIfNotExist(runFileWriters, refName, pid));
                    bufferManager.clearPartition(pid);
                }
            }
        } finally {
            // Force to close all run file writers.
            for (RunFileWriter runFileWriter : runFileWriters) {
                if (runFileWriter != null) {
                    runFileWriter.close();
                }
            }
        }
    }

    /**
     * Makes the space for the hash table. If there is no enough space, one or more partitions will be spilled
     * to the disk until the hash table can fit into the memory. After this, bring back spilled partitions
     * if there is available memory.
     *
     * @return the number of tuples in memory after this method is executed.
     * @throws HyracksDataException
     */
    private int makeSpaceForHashTableAndBringBackSpilledPartitions() throws HyracksDataException {
        int frameSize = ctx.getInitialFrameSize();
        long freeSpace = (long) (memSizeInFrames - spilledStatus.cardinality()) * frameSize;

        int inMemTupCount = 0;
        for (int p = spilledStatus.nextClearBit(0); p >= 0 && p < numOfPartitions; p =
                spilledStatus.nextClearBit(p + 1)) {
            freeSpace -= bufferManager.getPhysicalSize(p);
            inMemTupCount += buildPSizeInTups[p];
        }
        freeSpace -= SerializableHashTable.getExpectedTableByteSize(inMemTupCount, frameSize);

        return spillAndReloadPartitions(frameSize, freeSpace, inMemTupCount);
    }

    private int spillAndReloadPartitions(int frameSize, long freeSpace, int inMemTupCount) throws HyracksDataException {
        int pidToSpill, numberOfTuplesToBeSpilled;
        long expectedHashTableSizeDecrease;
        long currentFreeSpace = freeSpace;
        int currentInMemTupCount = inMemTupCount;
        // Spill some partitions if there is no free space.
        while (currentFreeSpace < 0) {
            pidToSpill = selectSinglePartitionToSpill(currentFreeSpace, currentInMemTupCount, frameSize);
            if (pidToSpill >= 0) {
                numberOfTuplesToBeSpilled = buildPSizeInTups[pidToSpill];
                expectedHashTableSizeDecrease =
                        -SerializableHashTable.calculateByteSizeDeltaForTableSizeChange(currentInMemTupCount,
                                -numberOfTuplesToBeSpilled, frameSize);
                currentInMemTupCount -= numberOfTuplesToBeSpilled;
                currentFreeSpace +=
                        bufferManager.getPhysicalSize(pidToSpill) + expectedHashTableSizeDecrease - frameSize;
                spillPartition(pidToSpill);
                closeBuildPartition(pidToSpill);
            } else {
                throw new HyracksDataException("Hash join does not have enough memory even after spilling.");
            }
        }
        // Bring some partitions back in if there is enough space.
        return bringPartitionsBack(currentFreeSpace, currentInMemTupCount, frameSize);
    }

    /**
     * Brings back some partitions if there is free memory and partitions that fit in that space.
     *
     * @param freeSpace current amount of free space in memory
     * @param inMemTupCount number of in memory tuples
     * @return number of in memory tuples after bringing some (or none) partitions in memory.
     * @throws HyracksDataException
     */
    private int bringPartitionsBack(long freeSpace, int inMemTupCount, int frameSize) throws HyracksDataException {
        int pid = 0;
        int currentMemoryTupleCount = inMemTupCount;
        long currentFreeSpace = freeSpace;
        while ((pid = selectAPartitionToReload(currentFreeSpace, pid, currentMemoryTupleCount)) >= 0
                && loadSpilledPartitionToMem(pid, buildRFWriters[pid])) {
            currentMemoryTupleCount += buildPSizeInTups[pid];
            // Reserve space for loaded data & increase in hash table (give back one frame taken by spilled partition.)
            currentFreeSpace = currentFreeSpace
                    - bufferManager.getPhysicalSize(pid) - SerializableHashTable
                            .calculateByteSizeDeltaForTableSizeChange(inMemTupCount, buildPSizeInTups[pid], frameSize)
                    + frameSize;
        }
        return currentMemoryTupleCount;
    }

    /**
     * Finds a best-fit partition that will be spilled to the disk to make enough space to accommodate the hash table.
     *
     * @return the partition id that will be spilled to the disk. Returns -1 if there is no single suitable partition.
     */
    private int selectSinglePartitionToSpill(long currentFreeSpace, int currentInMemTupCount, int frameSize) {
        long spaceAfterSpill;
        long minSpaceAfterSpill = (long) memSizeInFrames * frameSize;
        int minSpaceAfterSpillPartID = -1;
        int nextAvailablePidToSpill = -1;
        for (int p = spilledStatus.nextClearBit(0); p >= 0 && p < numOfPartitions; p =
                spilledStatus.nextClearBit(p + 1)) {
            if (buildPSizeInTups[p] == 0 || bufferManager.getPhysicalSize(p) == 0) {
                continue;
            }
            if (nextAvailablePidToSpill < 0) {
                nextAvailablePidToSpill = p;
            }
            // We put minus since the method returns a negative value to represent a newly reclaimed space.
            // One frame is deducted since a spilled partition needs one frame from free space.
            spaceAfterSpill = currentFreeSpace + bufferManager.getPhysicalSize(p) - frameSize + (-SerializableHashTable
                    .calculateByteSizeDeltaForTableSizeChange(currentInMemTupCount, -buildPSizeInTups[p], frameSize));
            if (spaceAfterSpill == 0) {
                // Found the perfect one. Just returns this partition.
                return p;
            } else if (spaceAfterSpill > 0 && spaceAfterSpill < minSpaceAfterSpill) {
                // We want to find the best-fit partition to avoid many partition spills.
                minSpaceAfterSpill = spaceAfterSpill;
                minSpaceAfterSpillPartID = p;
            }
        }

        return minSpaceAfterSpillPartID >= 0 ? minSpaceAfterSpillPartID : nextAvailablePidToSpill;
    }

    /**
     * Finds a partition that can fit in the left over memory.
     * @param freeSpace current free space
     * @param inMemTupCount number of tuples currently in memory
     * @return partition id of selected partition to reload
     */
    private int selectAPartitionToReload(long freeSpace, int pid, int inMemTupCount) {
        int frameSize = ctx.getInitialFrameSize();
        // Add one frame to freeSpace to consider the one frame reserved for the spilled partition
        long totalFreeSpace = freeSpace + frameSize;
        if (totalFreeSpace > 0) {
            for (int i = spilledStatus.nextSetBit(pid); i >= 0 && i < numOfPartitions; i =
                    spilledStatus.nextSetBit(i + 1)) {
                int spilledTupleCount = buildPSizeInTups[i];
                // Expected hash table size increase after reloading this partition
                long expectedHashTableByteSizeIncrease = SerializableHashTable
                        .calculateByteSizeDeltaForTableSizeChange(inMemTupCount, spilledTupleCount, frameSize);
                if (totalFreeSpace >= buildRFWriters[i].getFileSize() + expectedHashTableByteSizeIncrease) {
                    return i;
                }
            }
        }
        return -1;
    }

    private boolean loadSpilledPartitionToMem(int pid, RunFileWriter wr) throws HyracksDataException {
        RunFileReader r = wr.createReader();
        try {
            r.open();
            if (reloadBuffer == null) {
                reloadBuffer = new VSizeFrame(ctx);
            }
            while (r.nextFrame(reloadBuffer)) {
                accessorBuild.reset(reloadBuffer.getBuffer());
                for (int tid = 0; tid < accessorBuild.getTupleCount(); tid++) {
                    if (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                        // for some reason (e.g. fragmentation) if inserting fails, we need to clear the occupied frames
                        bufferManager.clearPartition(pid);
                        return false;
                    }
                }
            }
            // Closes and deletes the run file if it is already loaded into memory.
            r.setDeleteAfterClose(true);
        } finally {
            r.close();
        }
        spilledStatus.set(pid, false);
        buildRFWriters[pid] = null;
        return true;
    }

    private void buildHashTable() throws HyracksDataException {

        for (int pid = 0; pid < numOfPartitions; pid++) {
            if (!spilledStatus.get(pid)) {
                bufferManager.flushPartition(pid, new IFrameWriter() {
                    @Override
                    public void open() {
                        // Only nextFrame method is needed to pass the frame to the next operator.
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        inMemJoiner.build(buffer);
                    }

                    @Override
                    public void fail() {
                        // Only nextFrame method is needed to pass the frame to the next operator.
                    }

                    @Override
                    public void close() {
                        // Only nextFrame method is needed to pass the frame to the next operator.
                    }
                });
            }
        }
    }

    public void initProbe() {
        probePSizeInTups = new int[numOfPartitions];
    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();

        if (isBuildRelAllInMemory()) {
            inMemJoiner.join(buffer, writer);
            return;
        }
        inMemJoiner.resetAccessorProbe(accessorProbe);
        for (int i = 0; i < tupleCount; ++i) {
            int pid = probeHpc.partition(accessorProbe, i, numOfPartitions);

            if (buildPSizeInTups[pid] > 0 || isLeftOuter) { //Tuple has potential match from previous phase
                if (spilledStatus.get(pid)) { //pid is Spilled
                    processTupleProbePhase(i, pid);
                } else { //pid is Resident
                    inMemJoiner.join(i, writer);
                }
                probePSizeInTups[pid]++;
            }
        }
    }

    private void processTupleProbePhase(int tupleId, int pid) throws HyracksDataException {

        if (!bufferManager.insertTuple(pid, accessorProbe, tupleId, tempPtr)) {
            int recordSize =
                    VPartitionTupleBufferManager.calculateActualSize(null, accessorProbe.getTupleLength(tupleId));
            // If the partition is at least half-full and insertion fails, that partition is preferred to get
            // spilled, otherwise the biggest partition gets chosen as the victim.
            boolean modestCase = recordSize <= (ctx.getInitialFrameSize() / 2);
            int victim = (modestCase && bufferManager.getNumTuples(pid) > 0) ? pid
                    : spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
            // This method is called for the spilled partitions, so we know that this tuple is going to get written to
            // disk, sooner or later. As such, we try to reduce the number of writes that happens. So if the record is
            // larger than the size of the victim partition, we just flush it to the disk, otherwise we spill the
            // victim and this time insertion should be successful.
            //TODO:(More optimization) There might be a case where the leftover memory in the last frame of the
            // current partition + free memory in buffer manager + memory from the victim would be sufficient to hold
            // the record.
            if (victim >= 0 && bufferManager.getPhysicalSize(victim) >= recordSize) {
                RunFileWriter runFileWriter =
                        getSpillWriterOrCreateNewOneIfNotExist(probeRFWriters, probeRelName, victim);
                bufferManager.flushPartition(victim, runFileWriter);
                bufferManager.clearPartition(victim);
                if (!bufferManager.insertTuple(pid, accessorProbe, tupleId, tempPtr)) {
                    // This should not happen if the size calculations are correct, just not to let the query fail.
                    flushBigProbeObjectToDisk(pid, accessorProbe, tupleId);
                }
            } else {
                flushBigProbeObjectToDisk(pid, accessorProbe, tupleId);
            }
        }
    }

    private void flushBigProbeObjectToDisk(int pid, FrameTupleAccessor accessorProbe, int i)
            throws HyracksDataException {
        if (bigProbeFrameAppender == null) {
            bigProbeFrameAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        }
        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(probeRFWriters, probeRelName, pid);
        if (!bigProbeFrameAppender.append(accessorProbe, i)) {
            throw new HyracksDataException("The given tuple is too big");
        }
        bigProbeFrameAppender.write(runFileWriter, true);
    }

    private boolean isBuildRelAllInMemory() {
        return spilledStatus.nextSetBit(0) < 0;
    }

    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level
        //(which join technique to use)
        inMemJoiner.completeJoin(writer);
    }

    public void releaseResource() throws HyracksDataException {
        inMemJoiner.closeTable();
        closeAllSpilledPartitions(probeRFWriters, probeRelName);
        bufferManager.close();
        inMemJoiner = null;
        bufferManager = null;
        bufferManagerForHashTable = null;
    }

    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return buildRFWriters[pid] == null ? null : buildRFWriters[pid].createDeleteOnCloseReader();
    }

    public int getBuildPartitionSizeInTup(int pid) {
        return buildPSizeInTups[pid];
    }

    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return probeRFWriters[pid] == null ? null : probeRFWriters[pid].createDeleteOnCloseReader();
    }

    public int getProbePartitionSizeInTup(int pid) {
        return probePSizeInTups[pid];
    }

    public int getMaxBuildPartitionSize() {
        return getMaxPartitionSize(buildPSizeInTups);
    }

    public int getMaxProbePartitionSize() {
        return getMaxPartitionSize(probePSizeInTups);
    }

    private int getMaxPartitionSize(int[] partitions) {
        int max = partitions[0];
        for (int i = 1; i < partitions.length; i++) {
            if (partitions[i] > max) {
                max = partitions[i];
            }
        }
        return max;
    }

    public BitSet getPartitionStatus() {
        return spilledStatus;
    }

    public int getPartitionSize(int pid) {
        return bufferManager.getPhysicalSize(pid);
    }

    public void setIsReversed(boolean b) {
        this.isReversed = b;
    }
}
