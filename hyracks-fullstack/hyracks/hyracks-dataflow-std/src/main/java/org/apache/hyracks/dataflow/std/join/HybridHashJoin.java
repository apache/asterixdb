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

import org.apache.http.HttpException;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksJobletContext;
import org.apache.hyracks.api.dataflow.value.*;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.buffermanager.*;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

/**
 * This class mainly applies one level of HHJ on a pair of
 * relations. It is always called by the descriptor.
 */
public class HybridHashJoin implements IHybridHashJoin {

    private static final Logger LOGGER = LogManager.getLogger();
    // Used for special probe BigObject which can not be held into the Join memory
    private FrameTupleAppender bigFrameAppender;
    protected boolean dynamicMemory = false;
    protected final IHyracksJobletContext jobletCtx;
    private final String buildRelName;
    private final String probeRelName;
    protected final ITuplePartitionComputer buildHpc;
    protected final ITuplePartitionComputer probeHpc;
    protected final RecordDescriptor buildRd;
    protected final RecordDescriptor probeRd;
    private final IPredicateEvaluator buildPredEval;
    private final IPredicateEvaluator probePredEval;
    protected final boolean isLeftOuter;
    protected final IMissingWriter[] nonMatchWriters;
    private final BitSet spilledStatus; //0=resident, 1=spilled
    private final BitSet probeSpilledStatus; //0=resident, 1=spilled
    private final int numOfPartitions;
    protected int memSizeInFrames;
    protected InMemoryHashJoin inMemJoiner; //Used for joining resident partitions
    protected IPartitionedTupleBufferManager bufferManager;
    private PreferToSpillFullyOccupiedFramePolicy spillPolicy;
    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;
    protected ISimpleFrameBufferManager bufferManagerForHashTable;
    // Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal.
    protected boolean isReversed;
    protected PartitionManager buildPartitionManager;
    protected PartitionManager probePartitionManager;
    // this is a reusable object to store the pointer,which is not used anywhere. we mainly use it to match the
    // corresponding function signature.
    private IOperatorStats stats = null;
    ISerializableTable table;

    public HybridHashJoin(IHyracksJobletContext jobletCtx, int memSizeInFrames, int numOfPartitions,
                          String probeRelName, String buildRelName, RecordDescriptor probeRd, RecordDescriptor buildRd,
                          ITuplePartitionComputer probeHpc, ITuplePartitionComputer buildHpc, IPredicateEvaluator probePredEval,
                          IPredicateEvaluator buildPredEval, boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
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
        this.spilledStatus = new BitSet(numOfPartitions);
        this.probeSpilledStatus = new BitSet(numOfPartitions);
        this.nonMatchWriters = isLeftOuter ? new IMissingWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nonMatchWriters[i] = nullWriterFactories1[i].createMissingWriter();
            }
        }
    }

    @Override
    public void initBuild() throws HyracksDataException {
        IDeallocatableFramePool framePool =
                new DeallocatableFramePool(jobletCtx, memSizeInFrames * jobletCtx.getInitialFrameSize(), dynamicMemory);
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);
        bufferManager = new VPartitionTupleBufferManager(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus),
                numOfPartitions, framePool);

        spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, spilledStatus);
        buildPartitionManager = new PartitionManager(numOfPartitions, jobletCtx, bufferManager, buildHpc, accessorBuild, bigFrameAppender, spilledStatus, buildRelName);
    }

    @Override
    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            if (buildPredEval == null || buildPredEval.evaluate(accessorBuild, i)) {
                buildPartitionManager.insertTupleWithSpillPolicy(i, spillPolicy);
            }
        }
    }

    @Override
    public void closeBuild() throws HyracksDataException {
        // Flushes the remaining chunks of the all spilled partitions to the disk.
        buildPartitionManager.closeSpilledPartitions();
        // Makes the space for the in-memory hash table (some partitions may need to be spilled to the disk
        // during this step in order to make the space.)
        // and tries to bring back as many spilled partitions as possible if there is free space.
        spillAndReloadPartitions();
        table = new SerializableHashTable(buildPartitionManager.getTuplesInMemory(), jobletCtx, bufferManagerForHashTable);
        this.inMemJoiner = new InMemoryHashJoin(jobletCtx, new FrameTupleAccessor(probeRd), probeHpc,
                new FrameTupleAccessor(buildRd), buildRd, buildHpc, isLeftOuter, nonMatchWriters, table, isReversed,
                bufferManagerForHashTable);
        buildHashTable();
        if (stats != null) {
            LOGGER.info(String.format("Bytes Written:%d | Bytes Read: %d", buildPartitionManager.getBytesSpilled(), buildPartitionManager.getBytesReloaded()));
        }
    }

    @Override
    public void clearBuildTempFiles() throws HyracksDataException {
        buildPartitionManager.cleanUp();
    }

    @Override
    public void clearProbeTempFiles() throws HyracksDataException {
        probePartitionManager.cleanUp();
    }

    @Override
    public void fail() throws HyracksDataException {
        buildPartitionManager.cleanUp();
        if (probePartitionManager != null)
            probePartitionManager.cleanUp();
    }

    /**
     * Return the amount of Free Bytes.
     *
     * @return
     */
    private long calculateFreeSpace() {
        long freeSpace = (memSizeInFrames * jobletCtx.getInitialFrameSize()) - (long) buildPartitionManager.getTotalMemory();
        if (probePartitionManager != null) {
            freeSpace -= (long) probePartitionManager.getTotalMemory();
        }
        int inMemTupCount = buildPartitionManager.getTuplesInMemory();
        freeSpace -= SerializableHashTable.getExpectedTableByteSize(inMemTupCount, jobletCtx.getInitialFrameSize());
        return freeSpace;
    }

    private long calculateSpaceAfterSpillBuildPartition(Partition p) {
        int frameSize = jobletCtx.getInitialFrameSize();
        long spaceAfterSpill = calculateFreeSpace() + p.getMemoryUsed();
        spaceAfterSpill -= SerializableHashTable
                .calculateByteSizeDeltaForTableSizeChange(buildPartitionManager.getTuplesInMemory(), -p.getTuplesInMemory(), frameSize);
        return spaceAfterSpill;
    }

    protected void spillAndReloadPartitions() throws HyracksDataException {
        // Spill some partitions if there is no free space.
        for (Partition p : buildPartitionManager.getMemoryResidentPartitions()) {
            if (calculateFreeSpace() < 0) {
                buildPartitionManager.spillPartition(p.getId());
            } else {
                break;
            }
        }
        bringPartitionsBack();
        // Bring some partitions back in if there is enough space.
    }

    /**
     * Brings back some partitions if there is free memory and partitions that fit in that space.
     *
     * @return number of in memory tuples after bringing some (or none) partitions in memory.
     * @throws HyracksDataException
     */
    protected void bringPartitionsBack() throws HyracksDataException {
        selectAPartitionToReload();
    }

    /**
     * Finds a best-fit partition that will be spilled to the disk to make enough space to accommodate the hash table.
     *
     * @return the partition id that will be spilled to the disk. Returns -1 if there is no single suitable partition.
     */
    private Partition selectSinglePartitionToSpill() {
        int frameSize = jobletCtx.getInitialFrameSize();
        long minSpaceAfterSpill = (long) memSizeInFrames * frameSize;
        Partition minSpaceAfterSpillPartID = null, nextAvailablePidToSpill = null;
        for (Partition partition : buildPartitionManager.getMemoryResidentPartitions()) {
            if (partition.getTuplesProcessed() == 0 || partition.getMemoryUsed() == 0) {
                continue;
            } else if (nextAvailablePidToSpill == null) {
                nextAvailablePidToSpill = partition;
            }
            long spaceAfterSpill = calculateSpaceAfterSpillBuildPartition(partition);
            if (spaceAfterSpill == 0) {
                // Found the perfect one. Just returns this partition.
                return partition;
            } else if (spaceAfterSpill > 0 && spaceAfterSpill < minSpaceAfterSpill) {
                // We want to find the best-fit partition to avoid many partition spills.
                minSpaceAfterSpill = spaceAfterSpill;
                minSpaceAfterSpillPartID = partition;
            }
        }

        return minSpaceAfterSpillPartID != null ? minSpaceAfterSpillPartID : nextAvailablePidToSpill;
    }

    /**
     * Finds a partition that can fit in the left over memory.
     *
     * @return partition id of selected partition to reload
     */
    protected boolean selectAPartitionToReload() throws HyracksDataException {
        boolean reloaded = false;
        int frameSize = jobletCtx.getInitialFrameSize();
        // Add one frame to freeSpace to consider the one frame reserved for the spilled partition
        if (calculateFreeSpace() > 0) {
            for (Partition p : buildPartitionManager.getSpilledPartitions()) {
                // Expected hash table size increase after reloading this partition
                long expectedHashTableByteSizeIncrease = SerializableHashTable
                        .calculateByteSizeDeltaForTableSizeChange(buildPartitionManager.getTuplesInMemory(), p.getTuplesProcessed(), frameSize);
                if (calculateFreeSpace() >= p.getFileSize() + expectedHashTableByteSizeIncrease) {
                    LOGGER.info(String.format("Reloading Partition %d", p.getId()));
                    buildPartitionManager.reloadPartition(p.getId());
//                    table = new SerializableHashTable(buildPartitionManager.getTuplesInMemory(), jobletCtx, bufferManagerForHashTable);
//                    inMemJoiner.setTable(table);
//                    buildHashTable();
//                    reloaded = true;
                }
            }
            LOGGER.info("NO Partitions to Reload %d");
        }
        return reloaded;
    }

    protected void buildHashTable() throws HyracksDataException {
        List<Partition> resident = buildPartitionManager.getMemoryResidentPartitions();
        for (Partition p : resident) {
            buildHashTableForPartition(p.getId());
        }
    }

    protected void buildHashTableForPartition(int pid) throws HyracksDataException{
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

    @Override
    public void initProbe(ITuplePairComparator comparator) throws HyracksDataException {
        probePartitionManager = new PartitionManager(numOfPartitions, jobletCtx, bufferManager, probeHpc,
                accessorProbe, bigFrameAppender, probeSpilledStatus, probeRelName);
        if (inMemJoiner == null)
            throw new HyracksDataException("Build must be closed");
        inMemJoiner.setComparator(comparator);
        bufferManager.setConstrain(VPartitionTupleBufferManager.NO_CONSTRAIN);
    }

    @Override
    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();
        inMemJoiner.resetAccessorProbe(accessorProbe);
        if (buildPartitionManager.areAllPartitionsMemoryResident()) {
            for (int i = 0; i < tupleCount; ++i) {
                // NOTE: probePredEval is guaranteed to be 'null' for outer join and in case of role reversal
                if (probePredEval == null || probePredEval.evaluate(accessorProbe, i)) {
                    try {
                        inMemJoiner.join(i, writer);
                    } catch (Exception ex) {
                        throw new HyracksDataException("Error Probing");
                    }
                }
            }
        } else {
            for (int i = 0; i < tupleCount; ++i) {
                // NOTE: probePredEval is guaranteed to be 'null' for outer join and in case of role reversal
                if (probePredEval == null || probePredEval.evaluate(accessorProbe, i)) {
                    int pid = probeHpc.partition(accessorProbe, i, numOfPartitions);
                    Partition partition = buildPartitionManager.getPartition(pid);
                    if (partition.getTuplesProcessed() > 0 || isLeftOuter) { //Tuple has potential match from previous phase
                        if (partition.getSpilledStatus()) { //pid is Spilled
                            processTupleProbePhase(i, pid);
                        } else { //pid is Resident
                            inMemJoiner.join(i, writer);
                        }
                    }
                }
            }
        }
    }

    private void processTupleProbePhase(int tupleId, int pid) throws HyracksDataException {
        if (!probePartitionManager.insertTuple(tupleId)) {
            int recordSize = VPartitionTupleBufferManager.calculateActualSize(null, accessorProbe.getTupleLength(tupleId));
            // If the partition is at least half-full and insertion fails, that partition is preferred to get
            // spilled, otherwise the biggest partition gets chosen as the victim.
            boolean modestCase = recordSize <= (jobletCtx.getInitialFrameSize() / 2);
            Partition partition = probePartitionManager.getPartition(pid);
            int victim = (modestCase && partition.getTuplesInMemory() > 0) ? pid
                    : spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
            // This method is called for the spilled partitions, so we know that this tuple is going to get written to
            // disk, sooner or later. As such, we try to reduce the number of writes that happens. So if the record is
            // larger than the size of the victim partition, we just flush it to the disk, otherwise we spill the
            // victim and this time insertion should be successful.
            //TODO:(More optimization) There might be a case where the leftover memory in the last frame of the
            // current partition + free memory in buffer manager + memory from the victim would be sufficient to hold
            // the record.
            if (victim >= 0 && probePartitionManager.getPartition(victim).getMemoryUsed() >= recordSize) {
                probePartitionManager.spillPartition(victim);
                probePartitionManager.insertTuple(tupleId);
            } else {
                partition.insertLargeTuple(tupleId);
            }
        }
    }

    @Override
    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level
        //(which join technique to use)
        for (Partition p : buildPartitionManager.getSpilledOrInconsistentPartitions()) {
            buildPartitionManager.spillPartition(p.getId());
            probePartitionManager.spillPartition(p.getId());
        }
        inMemJoiner.completeJoin(writer);
        if (stats != null) {
            LOGGER.info(String.format("Bytes Written:%d | Bytes Read: %d", buildPartitionManager.getBytesSpilled(), buildPartitionManager.getBytesReloaded()));
        }
    }

    @Override
    public void releaseResource() throws HyracksDataException {
        inMemJoiner.closeTable();
        probePartitionManager.closeSpilledPartitions();
        bufferManager.close();
        inMemJoiner = null;
        bufferManager = null;
        bufferManagerForHashTable = null;
    }

    @Override
    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return buildPartitionManager.getPartition(pid).getRfReader();
    }

    @Override
    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return probePartitionManager.getPartition(pid).getRfReader();
    }

    @Override
    public int getBuildPartitionSizeInTup(int pid) {
        return buildPartitionManager.getPartition(pid).getTuplesProcessed();
    }

    @Override
    public int getProbePartitionSizeInTup(int pid) {
        return probePartitionManager.getPartition(pid).getTuplesProcessed();
    }

    @Override
    public int getMaxBuildPartitionSize() {
        return buildPartitionManager.getNumberOfTuplesOfLargestPartition();
    }

    @Override
    public int getMaxProbePartitionSize() {
        return probePartitionManager.getNumberOfTuplesOfLargestPartition();
    }

    @Override
    public BitSet getPartitionStatus() {
        return buildPartitionManager.getSpilledStatus();
    }

    @Override
    public BitSet getInconsistentStatus() {
        BitSet inconsistent = buildPartitionManager.getInconsistentStatus();
        inconsistent.or(probePartitionManager.getSpilledStatus());
        return inconsistent;
    }

    @Override
    public int getPartitionSize(int pid) {
        return bufferManager.getPhysicalSize(pid);
    }

    @Override
    public void setIsReversed(boolean reversed) {
        if (reversed && (buildPredEval != null || probePredEval != null)) {
            throw new IllegalStateException();
        }
        this.isReversed = reversed;
    }

    @Override
    public void setOperatorStats(IOperatorStats stats) {

        this.stats = stats;
    }

    @Override
    public int updateMemoryBudget(int newBudget) throws HyracksDataException {
        return 0;
        //Nothing to Do here
    }

    @Override
    public int updateMemoryBudgetProbe(int newBudget) throws HyracksDataException {
        return 0;
        //Do Nothing
    }

}

