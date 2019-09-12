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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author pouria
 *         This class guides the joining process, and switches between different
 *         joining techniques, w.r.t the implemented optimizations and skew in size of the
 *         partitions.
 *         - Operator overview:
 *         Assume we are trying to do (R Join S), with M buffers available, while we have an estimate on the size
 *         of R (in terms of buffers). HHJ (Hybrid Hash Join) has two main phases: Build and Probe,
 *         where in our implementation Probe phase can apply HHJ recursively, based on the value of M and size of
 *         R and S. HHJ phases proceed as follow:
 *         BUILD:
 *         Calculate number of partitions (Based on the size of R, fudge factor and M)
 *         [See Shapiro's paper for the detailed discussion].
 *         Initialize the build phase (one frame per partition, all partitions considered resident at first)
 *         Read tuples of R, frame by frame, and hash each tuple (based on a given hash function) to find
 *         its target partition and try to append it to that partition:
 *         If target partition's buffer is full, try to allocate a new buffer for it.
 *         if no free buffer is available, find the largest resident partition and spill it. Using its freed
 *         buffers after spilling, allocate a new buffer for the target partition.
 *         Being done with R, close the build phase. (During closing we write the very last buffer of each
 *         spilled partition to the disk, and we do partition tuning, where we try to bring back as many buffers,
 *         belonging to spilled partitions as possible into memory, based on the free buffers - We will stop at the
 *         point where remaining free buffers is not enough for reloading an entire partition back into memory)
 *         Create the hash table for the resident partitions (basically we create an in-memory hash join here)
 *         PROBE:
 *         Initialize the probe phase on S (mainly allocate one buffer per spilled partition, and one buffer
 *         for the whole resident partitions)
 *         Read tuples of S, frame by frame and hash each tuple T to its target partition P
 *         if P is a resident partition, pass T to the in-memory hash join and generate the output record,
 *         if any matching(s) record found
 *         if P is spilled, write T to the dedicated buffer for P (on the probe side)
 *         Once scanning of S is done, we try to join partition pairs (Ri, Si) of the spilled partitions:
 *         if any of Ri or Si is smaller than M, then we simply use an in-memory hash join to join them
 *         otherwise we apply HHJ recursively:
 *         if after applying HHJ recursively, we do not gain enough size reduction (max size of the
 *         resulting partitions were more than 80% of the initial Ri,Si size) then we switch to
 *         nested loop join for joining.
 *         (At each step of partition-pair joining, we consider role reversal, which means if size of Si were
 *         greater than Ri, then we make sure that we switch the roles of build/probe between them)
 */

public class OptimizedHybridHashJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private static final double NLJ_SWITCH_THRESHOLD = 0.8;

    private static final String PROBE_REL = "RelR";
    private static final String BUILD_REL = "RelS";

    private final int memSizeInFrames;
    private final int inputsize0;
    private final double fudgeFactor;
    private final int[] probeKeys;
    private final int[] buildKeys;
    private final IBinaryHashFunctionFamily[] propHashFunctionFactories;
    private final IBinaryHashFunctionFamily[] buildHashFunctionFactories;
    private final ITuplePairComparatorFactory tuplePairComparatorFactoryProbe2Build; //For HHJ & NLJ in probe
    private final ITuplePairComparatorFactory tuplePairComparatorFactoryBuild2Probe; //For HHJ & NLJ in probe
    private final IPredicateEvaluatorFactory predEvaluatorFactory;

    private final boolean isLeftOuter;
    private final IMissingWriterFactory[] nonMatchWriterFactories;

    //Flags added for test purpose
    private boolean skipInMemoryHJ = false;
    private boolean forceNLJ = false;
    private boolean forceRoleReversal = false;

    private static final Logger LOGGER = LogManager.getLogger();

    public OptimizedHybridHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memSizeInFrames,
            int inputsize0, double factor, int[] keys0, int[] keys1,
            IBinaryHashFunctionFamily[] propHashFunctionFactories,
            IBinaryHashFunctionFamily[] buildHashFunctionFactories, RecordDescriptor recordDescriptor,
            ITuplePairComparatorFactory tupPaircomparatorFactory01,
            ITuplePairComparatorFactory tupPaircomparatorFactory10, IPredicateEvaluatorFactory predEvaluatorFactory,
            boolean isLeftOuter, IMissingWriterFactory[] nonMatchWriterFactories) {
        super(spec, 2, 1);
        this.memSizeInFrames = memSizeInFrames;
        this.inputsize0 = inputsize0;
        this.fudgeFactor = factor;
        this.probeKeys = keys0;
        this.buildKeys = keys1;
        this.propHashFunctionFactories = propHashFunctionFactories;
        this.buildHashFunctionFactories = buildHashFunctionFactories;
        this.tuplePairComparatorFactoryProbe2Build = tupPaircomparatorFactory01;
        this.tuplePairComparatorFactoryBuild2Probe = tupPaircomparatorFactory10;
        outRecDescs[0] = recordDescriptor;
        this.predEvaluatorFactory = predEvaluatorFactory;
        this.isLeftOuter = isLeftOuter;
        this.nonMatchWriterFactories = nonMatchWriterFactories;
    }

    public OptimizedHybridHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memSizeInFrames,
            int inputsize0, double factor, int[] keys0, int[] keys1,
            IBinaryHashFunctionFamily[] propHashFunctionFactories,
            IBinaryHashFunctionFamily[] buildHashFunctionFactories, RecordDescriptor recordDescriptor,
            ITuplePairComparatorFactory tupPaircomparatorFactory01,
            ITuplePairComparatorFactory tupPaircomparatorFactory10, IPredicateEvaluatorFactory predEvaluatorFactory) {
        this(spec, memSizeInFrames, inputsize0, factor, keys0, keys1, propHashFunctionFactories,
                buildHashFunctionFactories, recordDescriptor, tupPaircomparatorFactory01, tupPaircomparatorFactory10,
                predEvaluatorFactory, false, null);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId buildAid = new ActivityId(odId, BUILD_AND_PARTITION_ACTIVITY_ID);
        ActivityId probeAid = new ActivityId(odId, PARTITION_AND_JOIN_ACTIVITY_ID);
        PartitionAndBuildActivityNode phase1 = new PartitionAndBuildActivityNode(buildAid, probeAid);
        ProbeAndJoinActivityNode phase2 = new ProbeAndJoinActivityNode(probeAid, buildAid);

        builder.addActivity(this, phase1);
        builder.addSourceEdge(1, phase1, 0);

        builder.addActivity(this, phase2);
        builder.addSourceEdge(0, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);

    }

    //memorySize is the memory for join (we have already excluded the 2 buffers for in/out)
    private int getNumberOfPartitions(int memorySize, int buildSize, double factor, int nPartitions)
            throws HyracksDataException {
        int numberOfPartitions = 0;
        if (memorySize <= 2) {
            throw new HyracksDataException("Not enough memory is available for Hybrid Hash Join.");
        }
        if (memorySize > buildSize * factor) {
            // We will switch to in-Mem HJ eventually: create two big partitions.
            // We set 2 (not 1) to avoid a corner case where the only partition may be spilled to the disk.
            // This may happen since this formula doesn't consider the hash table size. If this is the case,
            // we will do a nested loop join after some iterations. But, this is not effective.
            return 2;
        }
        numberOfPartitions = (int) (Math.ceil((buildSize * factor / nPartitions - memorySize) / (memorySize - 1)));
        numberOfPartitions = Math.max(2, numberOfPartitions);
        if (numberOfPartitions > memorySize) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(buildSize * factor / nPartitions));
            return Math.max(2, Math.min(numberOfPartitions, memorySize));
        }
        return numberOfPartitions;
    }

    public static class BuildAndPartitionTaskState extends AbstractStateObject {

        private int memForJoin;
        private int numOfPartitions;
        private OptimizedHybridHashJoin hybridHJ;

        public BuildAndPartitionTaskState() {
        }

        private BuildAndPartitionTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }

    }

    /**
     * Build phase of Hybrid Hash Join:
     * Creating an instance of Hybrid Hash Join, using Shapiro's formula to get the optimal number of partitions, build
     * relation is read and partitioned, and hybrid hash join instance gets ready for the probing.
     * (See OptimizedHybridHashJoin for the details on different steps)
     */
    private class PartitionAndBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId probeAid;

        public PartitionAndBuildActivityNode(ActivityId id, ActivityId probeAid) {
            super(id);
            this.probeAid = probeAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {

            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(probeAid, 0);
            final ITuplePairComparator probComparator =
                    tuplePairComparatorFactoryProbe2Build.createTuplePairComparator(ctx);
            final IPredicateEvaluator predEvaluator =
                    (predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator());

            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(
                        ctx.getJobletContext().getJobId(), new TaskId(getActivityId(), partition));

                ITuplePartitionComputer probeHpc =
                        new FieldHashPartitionComputerFamily(probeKeys, propHashFunctionFactories).createPartitioner(0);
                ITuplePartitionComputer buildHpc =
                        new FieldHashPartitionComputerFamily(buildKeys, buildHashFunctionFactories)
                                .createPartitioner(0);
                boolean isFailed = false;

                @Override
                public void open() throws HyracksDataException {
                    if (memSizeInFrames <= 2) { //Dedicated buffers: One buffer to read and two buffers for output
                        throw new HyracksDataException("Not enough memory is assigend for Hybrid Hash Join.");
                    }
                    state.memForJoin = memSizeInFrames - 2;
                    state.numOfPartitions =
                            getNumberOfPartitions(state.memForJoin, inputsize0, fudgeFactor, nPartitions);
                    state.hybridHJ = new OptimizedHybridHashJoin(ctx, state.memForJoin, state.numOfPartitions,
                            PROBE_REL, BUILD_REL, probComparator, probeRd, buildRd, probeHpc, buildHpc, predEvaluator,
                            isLeftOuter, nonMatchWriterFactories);

                    state.hybridHJ.initBuild();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("OptimizedHybridHashJoin is starting the build phase with " + state.numOfPartitions
                                + " partitions using " + state.memForJoin + " frames for memory.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.hybridHJ.build(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (state.hybridHJ != null) {
                        state.hybridHJ.closeBuild();
                        if (isFailed) {
                            state.hybridHJ.clearBuildTempFiles();
                        } else {
                            ctx.setStateObject(state);
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace("OptimizedHybridHashJoin closed its build phase");
                            }
                        }
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    isFailed = true;
                }

                @Override
                public String getDisplayName() {
                    return "Hybrid Hash Join: Build";
                }

            };
        }
    }

    /**
     * Probe phase of Hybrid Hash Join:
     * Reading the probe side and partitioning it, resident tuples get joined with the build side residents (through
     * formerly created HybridHashJoin in the build phase) and spilled partitions get written to run files. During
     * the close() call, pairs of spilled partition (build side spilled partition and its corresponding probe side
     * spilled partition) join, by applying Hybrid Hash Join recursively on them.
     */
    private class ProbeAndJoinActivityNode extends AbstractActivityNode {

        private static final long serialVersionUID = 1L;

        private final ActivityId buildAid;

        public ProbeAndJoinActivityNode(ActivityId id, ActivityId buildAid) {
            super(id);
            this.buildAid = buildAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {

            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(buildAid, 0);
            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final ITuplePairComparator probComp = tuplePairComparatorFactoryProbe2Build.createTuplePairComparator(ctx);
            final ITuplePairComparator buildComp = tuplePairComparatorFactoryBuild2Probe.createTuplePairComparator(ctx);
            final IPredicateEvaluator predEvaluator =
                    predEvaluatorFactory == null ? null : predEvaluatorFactory.createPredicateEvaluator();

            final IMissingWriter[] nonMatchWriter =
                    isLeftOuter ? new IMissingWriter[nonMatchWriterFactories.length] : null;
            final ArrayTupleBuilder nullTupleBuild =
                    isLeftOuter ? new ArrayTupleBuilder(buildRd.getFieldCount()) : null;
            if (isLeftOuter) {
                DataOutput out = nullTupleBuild.getDataOutput();
                for (int i = 0; i < nonMatchWriterFactories.length; i++) {
                    nonMatchWriter[i] = nonMatchWriterFactories[i].createMissingWriter();
                    nonMatchWriter[i].writeMissing(out);
                    nullTupleBuild.addFieldEndOffset();
                }
            }

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private BuildAndPartitionTaskState state;
                private IFrame rPartbuff = new VSizeFrame(ctx);

                private FrameTupleAppender nullResultAppender = null;
                private FrameTupleAccessor probeTupleAccessor;
                private boolean failed = false;

                @Override
                public void open() throws HyracksDataException {
                    state = (BuildAndPartitionTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), BUILD_AND_PARTITION_ACTIVITY_ID), partition));

                    writer.open();
                    state.hybridHJ.initProbe();

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("OptimizedHybridHashJoin is starting the probe phase.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.hybridHJ.probe(buffer, writer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    failed = true;
                    writer.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    if (failed) {
                        try {
                            // Clear temp files if fail() was called.
                            state.hybridHJ.clearBuildTempFiles();
                            state.hybridHJ.clearProbeTempFiles();
                        } finally {
                            writer.close(); // writer should always be closed.
                        }
                        logProbeComplete();
                        return;
                    }
                    try {
                        try {
                            state.hybridHJ.completeProbe(writer);
                        } finally {
                            state.hybridHJ.releaseResource();
                        }
                        BitSet partitionStatus = state.hybridHJ.getPartitionStatus();
                        rPartbuff.reset();
                        for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid =
                                partitionStatus.nextSetBit(pid + 1)) {
                            RunFileReader bReader = state.hybridHJ.getBuildRFReader(pid);
                            RunFileReader pReader = state.hybridHJ.getProbeRFReader(pid);

                            if (bReader == null || pReader == null) {
                                if (isLeftOuter && pReader != null) {
                                    appendNullToProbeTuples(pReader);
                                }
                                continue;
                            }
                            int bSize = state.hybridHJ.getBuildPartitionSizeInTup(pid);
                            int pSize = state.hybridHJ.getProbePartitionSizeInTup(pid);
                            joinPartitionPair(bReader, pReader, bSize, pSize, 1);
                        }
                    } catch (Exception e) {
                        // Since writer.nextFrame() is called in the above "try" body, we have to call writer.fail()
                        // to send the failure signal to the downstream, when there is a throwable thrown.
                        writer.fail();
                        // Clear temp files as this.fail() nor this.close() will no longer be called after close().
                        state.hybridHJ.clearBuildTempFiles();
                        state.hybridHJ.clearProbeTempFiles();
                        // Re-throw the whatever is caught.
                        throw e;
                    } finally {
                        try {
                            logProbeComplete();
                        } finally {
                            writer.close();
                        }
                    }
                }

                private void logProbeComplete() {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("OptimizedHybridHashJoin closed its probe phase");
                    }
                }

                //The buildSideReader should be always the original buildSideReader, so should the probeSideReader
                private void joinPartitionPair(RunFileReader buildSideReader, RunFileReader probeSideReader,
                        int buildSizeInTuple, int probeSizeInTuple, int level) throws HyracksDataException {
                    ITuplePartitionComputer probeHpc =
                            new FieldHashPartitionComputerFamily(probeKeys, propHashFunctionFactories)
                                    .createPartitioner(level);
                    ITuplePartitionComputer buildHpc =
                            new FieldHashPartitionComputerFamily(buildKeys, buildHashFunctionFactories)
                                    .createPartitioner(level);

                    int frameSize = ctx.getInitialFrameSize();
                    long buildPartSize = (long) Math.ceil((double) buildSideReader.getFileSize() / (double) frameSize);
                    long probePartSize = (long) Math.ceil((double) probeSideReader.getFileSize() / (double) frameSize);
                    int beforeMax = Math.max(buildSizeInTuple, probeSizeInTuple);

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("\n>>>Joining Partition Pairs (thread_id " + Thread.currentThread().getId()
                                + ") (pid " + ") - (level " + level + ")" + " - BuildSize:\t" + buildPartSize
                                + "\tProbeSize:\t" + probePartSize + " - MemForJoin " + (state.memForJoin)
                                + "  - LeftOuter is " + isLeftOuter);
                    }

                    // Calculate the expected hash table size for the both side.
                    long expectedHashTableSizeForBuildInFrame =
                            SerializableHashTable.getExpectedTableFrameCount(buildSizeInTuple, frameSize);
                    long expectedHashTableSizeForProbeInFrame =
                            SerializableHashTable.getExpectedTableFrameCount(probeSizeInTuple, frameSize);

                    //Apply in-Mem HJ if possible
                    if (!skipInMemoryHJ && ((buildPartSize + expectedHashTableSizeForBuildInFrame < state.memForJoin)
                            || (probePartSize + expectedHashTableSizeForProbeInFrame < state.memForJoin
                                    && !isLeftOuter))) {

                        int tabSize = -1;
                        if (!forceRoleReversal && (isLeftOuter || (buildPartSize < probePartSize))) {
                            //Case 1.1 - InMemHJ (without Role-Reversal)
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("\t>>>Case 1.1 (IsLeftOuter || buildSize<probe) AND ApplyInMemHJ - [Level "
                                        + level + "]");
                            }
                            tabSize = buildSizeInTuple;
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Build Side is smaller
                            applyInMemHashJoin(buildKeys, probeKeys, tabSize, buildRd, probeRd, buildHpc, probeHpc,
                                    buildSideReader, probeSideReader, probComp); // checked-confirmed
                        } else { //Case 1.2 - InMemHJ with Role Reversal
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("\t>>>Case 1.2. (NoIsLeftOuter || probe<build) AND ApplyInMemHJ"
                                        + "WITH RoleReversal - [Level " + level + "]");
                            }
                            tabSize = probeSizeInTuple;
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Probe Side is smaller
                            applyInMemHashJoin(probeKeys, buildKeys, tabSize, probeRd, buildRd, probeHpc, buildHpc,
                                    probeSideReader, buildSideReader, buildComp); // checked-confirmed
                        }
                    }
                    //Apply (Recursive) HHJ
                    else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("\t>>>Case 2. ApplyRecursiveHHJ - [Level " + level + "]");
                        }
                        if (!forceRoleReversal && (isLeftOuter || buildPartSize < probePartSize)) {
                            //Case 2.1 - Recursive HHJ (without Role-Reversal)
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(
                                        "\t\t>>>Case 2.1 - RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                                + level + "]");
                            }
                            applyHybridHashJoin((int) buildPartSize, PROBE_REL, BUILD_REL, probeKeys, buildKeys,
                                    probeRd, buildRd, probeHpc, buildHpc, probeSideReader, buildSideReader, level,
                                    beforeMax, probComp);

                        } else { //Case 2.2 - Recursive HHJ (with Role-Reversal)
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(
                                        "\t\t>>>Case 2.2. - RecursiveHHJ WITH RoleReversal - [Level " + level + "]");
                            }

                            applyHybridHashJoin((int) probePartSize, BUILD_REL, PROBE_REL, buildKeys, probeKeys,
                                    buildRd, probeRd, buildHpc, probeHpc, buildSideReader, probeSideReader, level,
                                    beforeMax, buildComp);

                        }
                    }
                }

                private void applyHybridHashJoin(int tableSize, final String PROBE_REL, final String BUILD_REL,
                        final int[] probeKeys, final int[] buildKeys, final RecordDescriptor probeRd,
                        final RecordDescriptor buildRd, final ITuplePartitionComputer probeHpc,
                        final ITuplePartitionComputer buildHpc, RunFileReader probeSideReader,
                        RunFileReader buildSideReader, final int level, final long beforeMax, ITuplePairComparator comp)
                        throws HyracksDataException {

                    boolean isReversed = probeKeys == OptimizedHybridHashJoinOperatorDescriptor.this.buildKeys
                            && buildKeys == OptimizedHybridHashJoinOperatorDescriptor.this.probeKeys;
                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";
                    OptimizedHybridHashJoin rHHj;
                    int n = getNumberOfPartitions(state.memForJoin, tableSize, fudgeFactor, nPartitions);
                    rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin, n, PROBE_REL, BUILD_REL, comp, probeRd,
                            buildRd, probeHpc, buildHpc, predEvaluator, isLeftOuter, nonMatchWriterFactories); //checked-confirmed

                    rHHj.setIsReversed(isReversed);
                    try {
                        buildSideReader.open();
                        try {
                            rHHj.initBuild();
                            rPartbuff.reset();
                            while (buildSideReader.nextFrame(rPartbuff)) {
                                rHHj.build(rPartbuff.getBuffer());
                            }
                        } finally {
                            // Makes sure that files are always properly closed.
                            rHHj.closeBuild();
                        }
                    } finally {
                        buildSideReader.close();
                    }
                    try {
                        probeSideReader.open();
                        rPartbuff.reset();
                        try {
                            rHHj.initProbe();
                            while (probeSideReader.nextFrame(rPartbuff)) {
                                rHHj.probe(rPartbuff.getBuffer(), writer);
                            }
                            rHHj.completeProbe(writer);
                        } finally {
                            rHHj.releaseResource();
                        }
                    } finally {
                        // Makes sure that files are always properly closed.
                        probeSideReader.close();
                    }

                    try {
                        int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
                        int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
                        int afterMax = Math.max(maxAfterBuildSize, maxAfterProbeSize);

                        BitSet rPStatus = rHHj.getPartitionStatus();
                        if (!forceNLJ && (afterMax < (NLJ_SWITCH_THRESHOLD * beforeMax))) {
                            //Case 2.1.1 - Keep applying HHJ
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("\t\t>>>Case 2.1.1 - KEEP APPLYING RecursiveHHJ WITH "
                                        + "(isLeftOuter || build<probe) - [Level " + level + "]");
                            }
                            for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                                RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
                                int rbSizeInTuple = rHHj.getBuildPartitionSizeInTup(rPid);
                                int rpSizeInTuple = rHHj.getProbePartitionSizeInTup(rPid);

                                if (rbrfw == null || rprfw == null) {
                                    if (isLeftOuter && rprfw != null) {
                                        // For the outer join, we don't reverse the role.
                                        appendNullToProbeTuples(rprfw);
                                    }
                                    continue;
                                }

                                if (isReversed) {
                                    joinPartitionPair(rprfw, rbrfw, rpSizeInTuple, rbSizeInTuple, level + 1);
                                } else {
                                    joinPartitionPair(rbrfw, rprfw, rbSizeInTuple, rpSizeInTuple, level + 1);
                                }
                            }

                        } else { //Case 2.1.2 - Switch to NLJ
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("\t\t>>>Case 2.1.2 - SWITCHED to NLJ RecursiveHHJ WITH "
                                        + "(isLeftOuter || build<probe) - [Level " + level + "]");
                            }
                            for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                                RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                RunFileReader rprfw = rHHj.getProbeRFReader(rPid);

                                if (rbrfw == null || rprfw == null) {
                                    if (isLeftOuter && rprfw != null) {
                                        // For the outer join, we don't reverse the role.
                                        appendNullToProbeTuples(rprfw);
                                    }
                                    continue;
                                }

                                int buildSideInTups = rHHj.getBuildPartitionSizeInTup(rPid);
                                int probeSideInTups = rHHj.getProbePartitionSizeInTup(rPid);
                                // NLJ order is outer + inner, the order is reversed from the other joins
                                if (isLeftOuter || probeSideInTups < buildSideInTups) {
                                    //checked-modified
                                    applyNestedLoopJoin(probeRd, buildRd, memSizeInFrames, rprfw, rbrfw);
                                } else {
                                    //checked-modified
                                    applyNestedLoopJoin(buildRd, probeRd, memSizeInFrames, rbrfw, rprfw);
                                }
                            }
                        }
                    } catch (Exception e) {
                        // Make sure that temporary run files generated in recursive hybrid hash joins
                        // are closed and deleted.
                        rHHj.clearBuildTempFiles();
                        rHHj.clearProbeTempFiles();
                        throw e;
                    }
                }

                private void appendNullToProbeTuples(RunFileReader probReader) throws HyracksDataException {
                    if (nullResultAppender == null) {
                        nullResultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                    }
                    if (probeTupleAccessor == null) {
                        probeTupleAccessor = new FrameTupleAccessor(probeRd);
                    }
                    try {
                        probReader.open();
                        while (probReader.nextFrame(rPartbuff)) {
                            probeTupleAccessor.reset(rPartbuff.getBuffer());
                            for (int tid = 0; tid < probeTupleAccessor.getTupleCount(); tid++) {
                                FrameUtils.appendConcatToWriter(writer, nullResultAppender, probeTupleAccessor, tid,
                                        nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0,
                                        nullTupleBuild.getSize());
                            }
                        }
                        nullResultAppender.write(writer, true);
                    } finally {
                        probReader.close();
                    }
                }

                private void applyInMemHashJoin(int[] bKeys, int[] pKeys, int tabSize, RecordDescriptor buildRDesc,
                        RecordDescriptor probeRDesc, ITuplePartitionComputer hpcRepBuild,
                        ITuplePartitionComputer hpcRepProbe, RunFileReader bReader, RunFileReader pReader,
                        ITuplePairComparator comp) throws HyracksDataException {
                    boolean isReversed = pKeys == OptimizedHybridHashJoinOperatorDescriptor.this.buildKeys
                            && bKeys == OptimizedHybridHashJoinOperatorDescriptor.this.probeKeys;
                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";
                    IDeallocatableFramePool framePool =
                            new DeallocatableFramePool(ctx, state.memForJoin * ctx.getInitialFrameSize());
                    ISimpleFrameBufferManager bufferManager = new FramePoolBackedFrameBufferManager(framePool);

                    ISerializableTable table = new SerializableHashTable(tabSize, ctx, bufferManager);
                    InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, new FrameTupleAccessor(probeRDesc), hpcRepProbe,
                            new FrameTupleAccessor(buildRDesc), buildRDesc, hpcRepBuild, comp, isLeftOuter,
                            nonMatchWriter, table, predEvaluator, isReversed, bufferManager);

                    try {
                        bReader.open();
                        rPartbuff.reset();
                        while (bReader.nextFrame(rPartbuff)) {
                            // We need to allocate a copyBuffer, because this buffer gets added to the buffers list
                            // in the InMemoryHashJoin.
                            ByteBuffer copyBuffer = bufferManager.acquireFrame(rPartbuff.getFrameSize());
                            // If a frame cannot be allocated, there may be a chance if we can compact the table,
                            // one or more frame may be reclaimed.
                            if (copyBuffer == null) {
                                if (joiner.compactHashTable() > 0) {
                                    copyBuffer = bufferManager.acquireFrame(rPartbuff.getFrameSize());
                                }
                                if (copyBuffer == null) {
                                    // Still no frame is allocated? At this point, we have no way to get a frame.
                                    throw new HyracksDataException(
                                            "Can't allocate one more frame. Assign more memory to InMemoryHashJoin.");
                                }
                            }
                            FrameUtils.copyAndFlip(rPartbuff.getBuffer(), copyBuffer);
                            joiner.build(copyBuffer);
                            rPartbuff.reset();
                        }
                    } finally {
                        bReader.close();
                    }
                    try {
                        //probe
                        pReader.open();
                        rPartbuff.reset();
                        try {
                            while (pReader.nextFrame(rPartbuff)) {
                                joiner.join(rPartbuff.getBuffer(), writer);
                                rPartbuff.reset();
                            }
                            joiner.completeJoin(writer);
                        } finally {
                            joiner.releaseMemory();
                        }
                    } finally {
                        try {
                            pReader.close();
                        } finally {
                            joiner.closeTable();
                        }
                    }
                }

                private void applyNestedLoopJoin(RecordDescriptor outerRd, RecordDescriptor innerRd, int memorySize,
                        RunFileReader outerReader, RunFileReader innerReader) throws HyracksDataException {
                    // The nested loop join result is outer + inner. All the other operator is probe + build.
                    // Hence the reverse relation is different.
                    boolean isReversed = outerRd == buildRd && innerRd == probeRd;
                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";
                    ITuplePairComparator nljComptorOuterInner = isReversed ? buildComp : probComp;
                    NestedLoopJoin nlj =
                            new NestedLoopJoin(ctx, new FrameTupleAccessor(outerRd), new FrameTupleAccessor(innerRd),
                                    nljComptorOuterInner, memorySize, predEvaluator, isLeftOuter, nonMatchWriter);
                    nlj.setIsReversed(isReversed);

                    IFrame cacheBuff = new VSizeFrame(ctx);
                    try {
                        innerReader.open();
                        while (innerReader.nextFrame(cacheBuff)) {
                            nlj.cache(cacheBuff.getBuffer());
                            cacheBuff.reset();
                        }
                    } finally {
                        try {
                            nlj.closeCache();
                        } finally {
                            innerReader.close();
                        }
                    }
                    try {
                        IFrame joinBuff = new VSizeFrame(ctx);
                        outerReader.open();
                        try {
                            while (outerReader.nextFrame(joinBuff)) {
                                nlj.join(joinBuff.getBuffer(), writer);
                                joinBuff.reset();
                            }
                            nlj.completeJoin(writer);
                        } finally {
                            nlj.releaseMemory();
                        }
                    } finally {
                        outerReader.close();
                    }
                }

                @Override
                public String getDisplayName() {
                    return "Hybrid Hash Join: Probe & Join";
                }
            };
            return op;
        }
    }
}
