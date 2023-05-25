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
import java.util.*;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
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
    private FrameTupleAppender bigFrameAppender;
    private FrameTupleAppender bigFrameAppenderProbe;
    private final IHyracksJobletContext jobletCtx;
    private final String buildRelName;
    private final String probeRelName;
    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;
    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;
    private final IPredicateEvaluator buildPredEval;
    private final IPredicateEvaluator probePredEval;
    private final boolean isLeftOuter;
    private final IMissingWriter[] nonMatchWriters;
    /**
     * During Probe Phase, a Build Partition might be brought back from disk in case of a memory expansion, or be
     * spilled due to a memory contention.
     * If this Partition at some point during Probe Phase changed from resident to spilled or vice-versa than it
     * is an inconsistent partition.
     * An inconsistent Partition is Hashed already.
     */
    private final int numOfPartitions;
    private int memSizeInFrames;
    /**
     * This is an instance of a class that implements an In Memory hash Join, it manages the Hash Table.
     */
    private InMemoryHashJoin inMemJoiner; //Used for joining resident partitions
    private IPartitionedTupleBufferManager bufferManager;
    private PreferToSpillFullyOccupiedFramePolicy spillPolicy;
    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    private ISimpleFrameBufferManager bufferManagerForHashTable;
    // Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal.
    private boolean isReversed;
    private IFrame reloadBuffer;
    /** This is a reusable object to store the pointer,which is not used anywhere. we mainly use it to match the
    corresponding function signature.**/
    private final TuplePointer tempPtr = new TuplePointer();
    private IOperatorStats stats = null;
    private MemoryChangeStats memoryStats = new MemoryChangeStats();
    private PartitionManager buildPartitionManager;
    private PartitionManager probePartitionManager;

    public OptimizedHybridHashJoin(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
            String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
            ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
            IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) throws HyracksDataException{
        this.jobletCtx = jobletCtx;
        this.memSizeInFrames = memSizeInFrames;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildRelName = buildRelName;
        this.probeRelName = probeRelName;
        this.numOfPartitions = numOfPartitions;
        this.accessorBuild = new FrameTupleAccessor(buildRd);
        this.accessorProbe = new FrameTupleAccessor(probeRd);
        this.isLeftOuter = isLeftOuter;
        if (isLeftOuter && probePredEval != null) {
            throw new IllegalStateException();
        }
        this.buildPredEval = buildPredEval;
        this.probePredEval = probePredEval;
        this.isReversed = false;
        this.nonMatchWriters = isLeftOuter ? new IMissingWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nonMatchWriters[i] = nullWriterFactories1[i].createMissingWriter();
            }
        }
    }

    //region [BUILD PHASE]
    public void initBuild() throws HyracksDataException {
        this.bigFrameAppender = new FrameTupleAppender(new VSizeFrame(jobletCtx));
        IDeallocatableFramePool framePool =
                new DeallocatableFramePool(jobletCtx, memSizeInFrames * jobletCtx.getInitialFrameSize(),false);
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);
        bufferManager = new VPartitionTupleBufferManager(null,numOfPartitions, framePool);
        buildPartitionManager = new PartitionManager(numOfPartitions,jobletCtx,bufferManager,buildHpc,accessorBuild,bigFrameAppender);
        bufferManager.setConstrain(PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(buildPartitionManager.getSpilledStatus()));
        spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, buildPartitionManager.getSpilledStatus());
    }

    /**
     * Process every tuple in buffer
     * @param buffer Buffer containing tuples
     * @throws HyracksDataException
     */
    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        for (int i = 0; i < accessorBuild.getTupleCount(); ++i) {
            if (buildPredEval == null || buildPredEval.evaluate(accessorBuild, i)) {
                int pid = buildHpc.partition(accessorBuild, i, numOfPartitions);
                processTupleBuildPhase(i, pid);
            }
        }
    }

    public void closeBuild() throws HyracksDataException {
        // Flushes the remaining chunks of the all spilled partitions to the disk.
        buildPartitionManager.closeSpilledPartitions();
        // Makes the space for the in-memory hash table (some partitions may need to be spilled to the disk
        // during this step in order to make the space.)
        // and tries to bring back as many spilled partitions as possible if there is free space.
        int inMemTupCount = makeSpaceForHashTableAndBringBackSpilledPartitions();
        ISerializableTable table = new SerializableHashTable(inMemTupCount, jobletCtx, bufferManagerForHashTable);
        this.inMemJoiner = new InMemoryHashJoin(jobletCtx, new FrameTupleAccessor(probeRd), probeHpc,
                new FrameTupleAccessor(buildRd), buildRd, buildHpc, isLeftOuter, nonMatchWriters, table, isReversed,
                bufferManagerForHashTable);
        buildHashTable();
    }

    private void processTupleBuildPhase(int tid, int pid) throws HyracksDataException {
        // insertTuple prevents the tuple to acquire a number of frames that is > the frame limit
        while (!buildPartitionManager.insertTuple(tid)) {
            int victimPartition = spillPolicy.selectVictimPartition(pid);
            buildPartitionManager.spillPartition(victimPartition);
            }
    }
    //endregion

    //region [PARTITION SPILLING AND RELOADING]

    private int spillAndReloadPartitions(int frameSize, long freeSpace, int inMemTupCount) throws HyracksDataException {
        int pidToSpill, numberOfTuplesToBeSpilled;
        long expectedHashTableSizeDecrease;
        long currentFreeSpace = freeSpace;
        int currentInMemTupCount = inMemTupCount;
        // Spill some partitions if there is no free space.
        while (currentFreeSpace < 0) {
            pidToSpill = selectSinglePartitionToSpill(currentFreeSpace, currentInMemTupCount, frameSize);
            if (pidToSpill >= 0) {
                numberOfTuplesToBeSpilled = buildPartitionManager.getTuplesInMemory(pidToSpill);
                expectedHashTableSizeDecrease =
                        -SerializableHashTable.calculateByteSizeDeltaForTableSizeChange(currentInMemTupCount,
                                -numberOfTuplesToBeSpilled, frameSize);
                currentInMemTupCount -= numberOfTuplesToBeSpilled;
                currentFreeSpace +=
                        buildPartitionManager.getPartition(pidToSpill).getMemoryUsed() + expectedHashTableSizeDecrease - frameSize;
                buildPartitionManager.spillPartition(pidToSpill);
            } else {
                throw new HyracksDataException("Hash join does not have enough memory even after spilling.");
            }
        }
        // Bring some partitions back in if there is enough space.
        return bringPartitionsBack(currentFreeSpace, currentInMemTupCount, frameSize);
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
        int frameSize = jobletCtx.getInitialFrameSize();
        long freeSpace = getFreeSpace();
        int inMemTupCount = buildPartitionManager.getTuplesInMemory();
        freeSpace -= SerializableHashTable.getExpectedTableByteSize(inMemTupCount, frameSize);
        return spillAndReloadPartitions(frameSize, freeSpace, inMemTupCount);
    }

    private int bringPartitionsBack(long freeSpace, int inMemTupCount, int frameSize) throws HyracksDataException {
        int pid = 0;
        int currentMemoryTupleCount = inMemTupCount;
        long currentFreeSpace = freeSpace;
        if(reloadBuffer == null)
            reloadBuffer = new VSizeFrame(jobletCtx);
        while ((pid = selectAPartitionToReload(currentFreeSpace, pid, currentMemoryTupleCount)) >= 0
                && buildPartitionManager.reloadPartition(pid)) {
            currentMemoryTupleCount += buildPartitionManager.getPartition(pid).getTuplesInMemory();
            // Reserve space for loaded data & increase in hash table (give back one frame taken by spilled partition.)
            currentFreeSpace = currentFreeSpace
                    - bufferManager.getPhysicalSize(pid) - SerializableHashTable
                    .calculateByteSizeDeltaForTableSizeChange(inMemTupCount, buildPartitionManager.getPartition(pid).getTuplesInMemory(), frameSize)
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
        for (int p = buildPartitionManager.getSpilledStatus().nextClearBit(0); p >= 0 && p < numOfPartitions; p =
                buildPartitionManager.getSpilledStatus().nextClearBit(p + 1)) {
            if (buildPartitionManager.getPartition(p).getTuplesInMemory() == 0 || buildPartitionManager.getPartition(p).getMemoryUsed() == 0) {
                continue;
            }
            if (nextAvailablePidToSpill < 0) {
                nextAvailablePidToSpill = p;
            }
            // We put minus since the method returns a negative value to represent a newly reclaimed space.
            // One frame is deducted since a spilled partition needs one frame from free space.
            spaceAfterSpill = currentFreeSpace + buildPartitionManager.getPartition(p).getMemoryUsed() - frameSize + (-SerializableHashTable
                    .calculateByteSizeDeltaForTableSizeChange(currentInMemTupCount, -buildPartitionManager.getPartition(p).getMemoryUsed(), frameSize));
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
        int frameSize = jobletCtx.getInitialFrameSize();
        // Add one frame to freeSpace to consider the one frame reserved for the spilled partition
        long totalFreeSpace = freeSpace + frameSize;
        if (totalFreeSpace > 0) {
            for (int i = buildPartitionManager.getSpilledStatus().nextSetBit(pid); i >= 0 && i < numOfPartitions; i =
                    buildPartitionManager.getSpilledStatus().nextSetBit(i + 1)) {
                int spilledTupleCount = buildPartitionManager.getPartition(pid).getTuplesSpilled();
                // Expected hash table size increase after reloading this partition
                long expectedHashTableByteSizeIncrease = SerializableHashTable
                        .calculateByteSizeDeltaForTableSizeChange(inMemTupCount, spilledTupleCount, frameSize);
                if (totalFreeSpace >= buildPartitionManager.getPartition(i).getFileSize() + expectedHashTableByteSizeIncrease) {
                    return i;
                }
            }
        }
        return -1;
    }
    //endregion

    //region [TEMPORARY FILES MANAGEMENT]
    public void clearBuildTempFiles() throws HyracksDataException {
        buildPartitionManager.closeSpilledPartitions();
    }

    public void clearProbeTempFiles() throws HyracksDataException {
        probePartitionManager.closeSpilledPartitions();
    }


    //endregion

    //region [HASH TABLE MANAGEMENT]

    /**
     * Create the Hashtable Entries for all Memory Resident Partitions
     * @throws HyracksDataException
     */
    private void buildHashTable() throws HyracksDataException {
        try {
            for (int pid = 0; pid < numOfPartitions; pid++) {
                if (!buildPartitionManager.getSpilledStatus().get(pid)) { //If partition is not resident and HashTable is not already Built;
                    hashPartition(pid);
                }
            }
        }
        catch (Exception ex){
            throw new HyracksDataException(ex.getMessage());
        }
    }

    /**
     * Hash Partition and insert it into the Hash Table
     * @param pid
     * @throws HyracksDataException
     */
    private void hashPartition(int pid) throws HyracksDataException {
            bufferManager.flushPartition(pid, new IFrameWriter() {
                @Override
                public void open() {
                    // Only nextFrame method is needed to pass the frame to the next operator.
                }
                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    inMemJoiner.build(buffer);  //Hash every tuple in every resident partition.
                    //Don't need to recreate the Hash table for a partition that was already been hashed.
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
    //endregion

    //region [PROBE PHASE]
    public void initProbe(ITuplePairComparator comparator) throws HyracksDataException {
        inMemJoiner.setComparator(comparator);
        bufferManager.setConstrain(VPartitionTupleBufferManager.NO_CONSTRAIN);
        this.bigFrameAppenderProbe = new FrameTupleAppender(new VSizeFrame(jobletCtx));
        probePartitionManager = new PartitionManager(numOfPartitions,jobletCtx,bufferManager,probeHpc,accessorProbe,bigFrameAppenderProbe);
    }
    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();
        inMemJoiner.resetAccessorProbe(accessorProbe);
        if (buildPartitionManager.areAllPartitionsMemoryResident()) {
            for (int i = 0; i < tupleCount; ++i) {
                // NOTE: probePredEval is guaranteed to be 'null' for outer join and in case of role reversal
                if (probePredEval == null || probePredEval.evaluate(accessorProbe, i)) {
                    inMemJoiner.join(i, writer);
                }
            }
        } else {
            for (int i = 0; i < tupleCount; ++i) {
                // NOTE: probePredEval is guaranteed to be 'null' for outer join and in case of role reversal
                if (probePredEval == null || probePredEval.evaluate(accessorProbe, i)) {
                    int pid = probeHpc.partition(accessorProbe, i, numOfPartitions); //Hash tuple to partition
                    if (buildPartitionManager.getPartition(pid).getTuplesProcessed() > 0 || isLeftOuter) { //Tuple has potential match from previous phase
                        if (buildPartitionManager.getSpilledStatus().get(pid)) { //pid is Spilled
                            processTupleProbePhase(i, pid);
                        } else { //pid is Resident
                                inMemJoiner.join(i, writer);
                        }
                    }
                }
            }
        }
    }
    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level
        //(which join technique to use)
        probePartitionManager.closeSpilledPartitions();
        inMemJoiner.completeJoin(writer);
    }

    /**
     * Move tuple to its output buffer.
     * @param tupleId Tuple Id.
     * @param pid Partition that will hold this tuple.
     * @throws HyracksDataException
     */
    private void processTupleProbePhase(int tupleId, int pid) throws HyracksDataException {
        while(!probePartitionManager.insertTuple(tupleId)) {
            int recordSize =
                    VPartitionTupleBufferManager.calculateActualSize(null, accessorProbe.getTupleLength(tupleId));
            // If the partition is at least half-full and insertion fails, that partition is preferred to get
            // spilled, otherwise the biggest partition gets chosen as the victim.
            boolean modestCase = recordSize <= (jobletCtx.getInitialFrameSize() / 2);
            int victim = (modestCase && probePartitionManager.getTuplesInMemory(pid) > 0) ? pid
                    : spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
            // This method is called for the spilled partitions, so we know that this tuple is going to get written to
            // disk, sooner or later. As such, we try to reduce the number of writes that happens. So if the record is
            // larger than the size of the victim partition, we just flush it to the disk, otherwise we spill the
            // victim and this time insertion should be successful.
            //TODO:(More optimization) There might be a case where the leftover memory in the last frame of the
            // current partition + free memory in buffer manager + memory from the victim would be sufficient to hold
            // the record.
            if (victim >= 0 && bufferManager.getPhysicalSize(victim) >= recordSize) {
                probePartitionManager.spillPartition(victim);
            }
        }
    }

    //endregion
    public void fail() throws HyracksDataException {
        buildPartitionManager.cleanUp();
        probePartitionManager.cleanUp();
    }
    public void releaseResource() throws HyracksDataException {
        inMemJoiner.closeTable();
        buildPartitionManager.closeSpilledPartitions();
        bufferManager.close();
        inMemJoiner = null;
        bufferManager = null;
        bufferManagerForHashTable = null;
    }
    //region [GETTERS AND SETTERS]

    public int getBuildPartitionSizeInTup(int pid) {
        return buildPartitionManager.getPartition(pid).getTuplesProcessed();
    }

    public int getProbePartitionSizeInTup(int pid) {
        return probePartitionManager.getPartition(pid).getTuplesProcessed();
    }

    public int getMaxBuildPartitionSize() {
        return buildPartitionManager.getNumberOfTuplesOfLargestPartition();
    }

    public int getMaxProbePartitionSize() {
        return probePartitionManager.getNumberOfTuplesOfLargestPartition();
    }

    public BitSet getPartitionStatus() {
        return buildPartitionManager.getSpilledStatus();
    }
    public int getPartitionSize(int pid) {
        return buildPartitionManager.getPartition(pid).getMemoryUsed();
    }
    public void setIsReversed(boolean reversed) {
        if (reversed && (buildPredEval != null || probePredEval != null)) {
            throw new IllegalStateException();
        }
        this.isReversed = reversed;
    }

    public void setOperatorStats(IOperatorStats stats) {
        this.stats = stats;
    }

    /**
     * Get Amount of Free Space in Memory.
     * @return
     */
    public long getFreeSpace(){
        int frameSize = jobletCtx.getInitialFrameSize();
        long freeSpace = (long) (memSizeInFrames - buildPartitionManager.getSpilledStatus().cardinality())  * frameSize; //At least one Frame for each spilled partition
        freeSpace -= buildPartitionManager.getTotalMemory();
        return freeSpace;
    }

    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException{
        return buildPartitionManager.getPartition(pid).getRfReader();
    }
    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException{
        return probePartitionManager.getPartition(pid).getRfReader();
    }
}
