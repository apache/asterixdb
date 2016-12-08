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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
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
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

/**
 * @author pouria
 *         This class guides the joining process, and switches between different
 *         joining techniques, w.r.t the implemented optimizations and skew in size of the
 *         partitions.
 *         - Operator overview:
 *         Assume we are trying to do (R Join S), with M buffers available, while we have an estimate on the size
 *         of R (in terms of buffers). HHJ (Hybrid Hash Join) has two main phases: Build and Probe, where in our implementation Probe phase
 *         can apply HHJ recursively, based on the value of M and size of R and S. HHJ phases proceed as follow:
 *         BUILD:
 *         Calculate number of partitions (Based on the size of R, fudge factor and M) [See Shapiro's paper for the detailed discussion].
 *         Initialize the build phase (one frame per partition, all partitions considered resident at first)
 *         Read tuples of R, frame by frame, and hash each tuple (based on a given hash function) to find
 *         its target partition and try to append it to that partition:
 *         If target partition's buffer is full, try to allocate a new buffer for it.
 *         if no free buffer is available, find the largest resident partition and spill it. Using its freed
 *         buffers after spilling, allocate a new buffer for the target partition.
 *         Being done with R, close the build phase. (During closing we write the very last buffer of each
 *         spilled partition to the disk, and we do partition tuning, where we try to bring back as many buffers, belonging to
 *         spilled partitions as possible into memory, based on the free buffers - We will stop at the point where remaining free buffers is not enough
 *         for reloading an entire partition back into memory)
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

    private final int frameLimit;
    private final int inputsize0;
    private final double fudgeFactor;
    private final int[] probeKeys;
    private final int[] buildKeys;
    private final IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories;
    private final IBinaryComparatorFactory[] comparatorFactories; //For in-mem HJ
    private final ITuplePairComparatorFactory tuplePairComparatorFactoryProbe2Build; //For NLJ in probe
    private final ITuplePairComparatorFactory tuplePairComparatorFactoryBuild2Probe; //For NLJ in probe
    private final IPredicateEvaluatorFactory predEvaluatorFactory;

    private final boolean isLeftOuter;
    private final IMissingWriterFactory[] nonMatchWriterFactories;

    //Flags added for test purpose
    private static boolean skipInMemoryHJ = false;
    private static boolean forceNLJ = false;
    private static boolean forceRR = false;

    private static final Logger LOGGER = Logger.getLogger(OptimizedHybridHashJoinOperatorDescriptor.class.getName());

    public OptimizedHybridHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int inputsize0,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor,
            ITuplePairComparatorFactory tupPaircomparatorFactory01,
            ITuplePairComparatorFactory tupPaircomparatorFactory10, IPredicateEvaluatorFactory predEvaluatorFactory,
            boolean isLeftOuter, IMissingWriterFactory[] nonMatchWriterFactories) throws HyracksDataException {

        super(spec, 2, 1);
        this.frameLimit = frameLimit;
        this.inputsize0 = inputsize0;
        this.fudgeFactor = factor;
        this.probeKeys = keys0;
        this.buildKeys = keys1;
        this.hashFunctionGeneratorFactories = hashFunctionGeneratorFactories;
        this.comparatorFactories = comparatorFactories;
        this.tuplePairComparatorFactoryProbe2Build = tupPaircomparatorFactory01;
        this.tuplePairComparatorFactoryBuild2Probe = tupPaircomparatorFactory10;
        recordDescriptors[0] = recordDescriptor;
        this.predEvaluatorFactory = predEvaluatorFactory;
        this.isLeftOuter = isLeftOuter;
        this.nonMatchWriterFactories = nonMatchWriterFactories;
    }

    public OptimizedHybridHashJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int inputsize0,
            double factor, int[] keys0, int[] keys1, IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor,
            ITuplePairComparatorFactory tupPaircomparatorFactory01,
            ITuplePairComparatorFactory tupPaircomparatorFactory10, IPredicateEvaluatorFactory predEvaluatorFactory)
            throws HyracksDataException {
        this(spec, frameLimit, inputsize0, factor, keys0, keys1, hashFunctionGeneratorFactories, comparatorFactories,
                recordDescriptor, tupPaircomparatorFactory01, tupPaircomparatorFactory10, predEvaluatorFactory, false,
                null);
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
        if (memorySize <= 1) {
            throw new HyracksDataException("not enough memory is available for Hybrid Hash Join");
        }
        if (memorySize > buildSize) {
            return 1; //We will switch to in-Mem HJ eventually
        }
        numberOfPartitions = (int) (Math.ceil((buildSize * factor / nPartitions - memorySize) / (memorySize - 1)));
        if (numberOfPartitions <= 0) {
            numberOfPartitions = 1; //becomes in-memory hash join
        }
        if (numberOfPartitions > memorySize) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(buildSize * factor / nPartitions));
            return (numberOfPartitions < memorySize ? numberOfPartitions : memorySize);
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

    /*
     * Build phase of Hybrid Hash Join:
     * Creating an instance of Hybrid Hash Join, using Shapiro's formula
     * to get the optimal number of partitions, build relation is read and
     * partitioned, and hybrid hash join instance gets ready for the probing.
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
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions) {

            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(probeAid, 0);

            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            for (int i = 0; i < comparatorFactories.length; i++) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            final IPredicateEvaluator predEvaluator = (predEvaluatorFactory == null ? null
                    : predEvaluatorFactory.createPredicateEvaluator());

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(
                        ctx.getJobletContext().getJobId(), new TaskId(getActivityId(), partition));

                ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerFamily(probeKeys,
                        hashFunctionGeneratorFactories).createPartitioner(0);
                ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerFamily(buildKeys,
                        hashFunctionGeneratorFactories).createPartitioner(0);
                boolean isFailed = false;

                @Override
                public void open() throws HyracksDataException {
                    if (frameLimit <= 2) { //Dedicated buffers: One buffer to read and one buffer for output
                        throw new HyracksDataException("not enough memory for Hybrid Hash Join");
                    }
                    state.memForJoin = frameLimit - 2;
                    state.numOfPartitions = getNumberOfPartitions(state.memForJoin, inputsize0, fudgeFactor,
                            nPartitions);
                    state.hybridHJ = new OptimizedHybridHashJoin(ctx, state.memForJoin, state.numOfPartitions,
                            PROBE_REL, BUILD_REL, probeKeys, buildKeys, comparators, probeRd, buildRd, probeHpc,
                            buildHpc, predEvaluator, isLeftOuter, nonMatchWriterFactories);

                    state.hybridHJ.initBuild();
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("OptimizedHybridHashJoin is starting the build phase with " + state.numOfPartitions
                                + " partitions using " + state.memForJoin + " frames for memory.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.hybridHJ.build(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.hybridHJ.closeBuild();
                    if (isFailed) {
                        state.hybridHJ.clearBuildTempFiles();
                    } else {
                        ctx.setStateObject(state);
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("OptimizedHybridHashJoin closed its build phase");
                        }
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    isFailed = true;
                }

            };
            return op;
        }
    }

    /*
     * Probe phase of Hybrid Hash Join:
     * Reading the probe side and partitioning it, resident tuples get
     * joined with the build side residents (through formerly created HybridHashJoin in the build phase)
     * and spilled partitions get written to run files. During the close() call, pairs of spilled partition
     * (build side spilled partition and its corresponding probe side spilled partition) join, by applying
     * Hybrid Hash Join recursively on them.
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
            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            final ITuplePairComparator nljComparatorProbe2Build = tuplePairComparatorFactoryProbe2Build
                    .createTuplePairComparator(ctx);
            final ITuplePairComparator nljComparatorBuild2Probe = tuplePairComparatorFactoryBuild2Probe
                    .createTuplePairComparator(ctx);
            final IPredicateEvaluator predEvaluator = predEvaluatorFactory == null ? null
                    : predEvaluatorFactory.createPredicateEvaluator();

            for (int i = 0; i < comparatorFactories.length; i++) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            final IMissingWriter[] nonMatchWriter = isLeftOuter ? new IMissingWriter[nonMatchWriterFactories.length]
                    : null;
            final ArrayTupleBuilder nullTupleBuild = isLeftOuter ? new ArrayTupleBuilder(buildRd.getFieldCount())
                    : null;
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

                @Override
                public void open() throws HyracksDataException {
                    state = (BuildAndPartitionTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), BUILD_AND_PARTITION_ACTIVITY_ID), partition));

                    writer.open();
                    state.hybridHJ.initProbe();

                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("OptimizedHybridHashJoin is starting the probe phase.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.hybridHJ.probe(buffer, writer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    try {
                        state.hybridHJ.closeProbe(writer);

                        BitSet partitionStatus = state.hybridHJ.getPartitionStatus();

                        rPartbuff.reset();
                        for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid = partitionStatus
                                .nextSetBit(pid + 1)) {

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
                    } finally {
                        writer.close();
                    }
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("OptimizedHybridHashJoin closed its probe phase");
                    }
                }

                //The buildSideReader should be always the original buildSideReader, so should the probeSideReader
                private void joinPartitionPair(RunFileReader buildSideReader, RunFileReader probeSideReader,
                        int buildSizeInTuple, int probeSizeInTuple, int level) throws HyracksDataException {
                    ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerFamily(probeKeys,
                            hashFunctionGeneratorFactories).createPartitioner(level);
                    ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerFamily(buildKeys,
                            hashFunctionGeneratorFactories).createPartitioner(level);

                    long buildPartSize = buildSideReader.getFileSize() / ctx.getInitialFrameSize();
                    long probePartSize = probeSideReader.getFileSize() / ctx.getInitialFrameSize();
                    int beforeMax = Math.max(buildSizeInTuple, probeSizeInTuple);

                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("\n>>>Joining Partition Pairs (thread_id " + Thread.currentThread().getId()
                                + ") (pid " + ") - (level " + level + ")" + " - BuildSize:\t" + buildPartSize
                                + "\tProbeSize:\t" + probePartSize + " - MemForJoin " + (state.memForJoin)
                                + "  - LeftOuter is " + isLeftOuter);
                    }

                    //Apply in-Mem HJ if possible
                    if (!skipInMemoryHJ && ((buildPartSize < state.memForJoin)
                            || (probePartSize < state.memForJoin && !isLeftOuter))) {
                        int tabSize = -1;
                        if (!forceRR && (isLeftOuter || (buildPartSize < probePartSize))) {
                            //Case 1.1 - InMemHJ (wout Role-Reversal)
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("\t>>>Case 1.1 (IsLeftOuter || buildSize<probe) AND ApplyInMemHJ - [Level "
                                        + level + "]");
                            }
                            tabSize = buildSizeInTuple;
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Build Side is smaller
                            applyInMemHashJoin(buildKeys, probeKeys, tabSize, buildRd, probeRd, buildHpc, probeHpc,
                                    buildSideReader, probeSideReader); // checked-confirmed
                        } else { //Case 1.2 - InMemHJ with Role Reversal
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine(
                                        "\t>>>Case 1.2. (NoIsLeftOuter || probe<build) AND ApplyInMemHJ WITH RoleReversal - [Level "
                                                + level + "]");
                            }
                            tabSize = probeSizeInTuple;
                            if (tabSize == 0) {
                                throw new HyracksDataException(
                                        "Trying to join an empty partition. Invalid table size for inMemoryHashJoin.");
                            }
                            //Probe Side is smaller
                            applyInMemHashJoin(probeKeys, buildKeys, tabSize, probeRd, buildRd, probeHpc, buildHpc,
                                    probeSideReader, buildSideReader); // checked-confirmed
                        }
                    }
                    //Apply (Recursive) HHJ
                    else {
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("\t>>>Case 2. ApplyRecursiveHHJ - [Level " + level + "]");
                        }
                        if (!forceRR && (isLeftOuter || buildPartSize < probePartSize)) {
                            //Case 2.1 - Recursive HHJ (wout Role-Reversal)
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("\t\t>>>Case 2.1 - RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                        + level + "]");
                            }
                            applyHybridHashJoin((int) buildPartSize, PROBE_REL, BUILD_REL, probeKeys, buildKeys,
                                    probeRd, buildRd, probeHpc, buildHpc, probeSideReader, buildSideReader, level,
                                    beforeMax);

                        } else { //Case 2.2 - Recursive HHJ (with Role-Reversal)
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine(
                                        "\t\t>>>Case 2.2. - RecursiveHHJ WITH RoleReversal - [Level " + level + "]");
                            }

                            applyHybridHashJoin((int) probePartSize, BUILD_REL, PROBE_REL, buildKeys, probeKeys,
                                    buildRd, probeRd, buildHpc, probeHpc, buildSideReader, probeSideReader, level,
                                    beforeMax);

                        }
                    }
                }

                private void applyHybridHashJoin(int tableSize, final String PROBE_REL, final String BUILD_REL,
                        final int[] probeKeys, final int[] buildKeys, final RecordDescriptor probeRd,
                        final RecordDescriptor buildRd, final ITuplePartitionComputer probeHpc,
                        final ITuplePartitionComputer buildHpc, RunFileReader probeSideReader,
                        RunFileReader buildSideReader, final int level, final long beforeMax)
                        throws HyracksDataException {

                    boolean isReversed = probeKeys == OptimizedHybridHashJoinOperatorDescriptor.this.buildKeys
                            && buildKeys == OptimizedHybridHashJoinOperatorDescriptor.this.probeKeys;

                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";

                    OptimizedHybridHashJoin rHHj;
                    int n = getNumberOfPartitions(state.memForJoin, tableSize, fudgeFactor, nPartitions);
                    rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin, n, PROBE_REL, BUILD_REL, probeKeys,
                            buildKeys, comparators, probeRd, buildRd, probeHpc, buildHpc, predEvaluator, isLeftOuter,
                            nonMatchWriterFactories); //checked-confirmed

                    rHHj.setIsReversed(isReversed);
                    buildSideReader.open();
                    rHHj.initBuild();
                    rPartbuff.reset();
                    while (buildSideReader.nextFrame(rPartbuff)) {
                        rHHj.build(rPartbuff.getBuffer());
                    }
                    rHHj.closeBuild();
                    buildSideReader.close();

                    probeSideReader.open();
                    rHHj.initProbe();
                    rPartbuff.reset();
                    while (probeSideReader.nextFrame(rPartbuff)) {
                        rHHj.probe(rPartbuff.getBuffer(), writer);
                    }
                    rHHj.closeProbe(writer);
                    probeSideReader.close();

                    int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
                    int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
                    int afterMax = Math.max(maxAfterBuildSize, maxAfterProbeSize);

                    BitSet rPStatus = rHHj.getPartitionStatus();
                    if (!forceNLJ && (afterMax < (NLJ_SWITCH_THRESHOLD * beforeMax))) { //Case 2.1.1 - Keep applying HHJ
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine(
                                    "\t\t>>>Case 2.1.1 - KEEP APPLYING RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                            + level + "]");
                        }
                        for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                            RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                            RunFileReader rprfw = rHHj.getProbeRFReader(rPid);
                            int rbSizeInTuple = rHHj.getBuildPartitionSizeInTup(rPid);
                            int rpSizeInTuple = rHHj.getProbePartitionSizeInTup(rPid);

                            if (rbrfw == null || rprfw == null) {
                                if (isLeftOuter && rprfw != null) { // if outer join, we don't reverse
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
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine(
                                    "\t\t>>>Case 2.1.2 - SWITCHED to NLJ RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                            + level + "]");
                        }
                        for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus.nextSetBit(rPid + 1)) {
                            RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                            RunFileReader rprfw = rHHj.getProbeRFReader(rPid);

                            if (rbrfw == null || rprfw == null) {
                                if (isLeftOuter && rprfw != null) { // if outer join, we don't reverse
                                    appendNullToProbeTuples(rprfw);
                                }
                                continue;
                            }

                            int buildSideInTups = rHHj.getBuildPartitionSizeInTup(rPid);
                            int probeSideInTups = rHHj.getProbePartitionSizeInTup(rPid);
                            // NLJ order is outer + inner, the order is reversed from the other joins
                            if (isLeftOuter || probeSideInTups < buildSideInTups) {
                                applyNestedLoopJoin(probeRd, buildRd, frameLimit, rprfw, rbrfw); //checked-modified
                            } else {
                                applyNestedLoopJoin(buildRd, probeRd, frameLimit, rbrfw, rprfw); //checked-modified
                            }
                        }
                    }
                }

                private void appendNullToProbeTuples(RunFileReader probReader) throws HyracksDataException {
                    if (nullResultAppender == null) {
                        nullResultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                    }
                    if (probeTupleAccessor == null) {
                        probeTupleAccessor = new FrameTupleAccessor(probeRd);
                    }
                    probReader.open();
                    while (probReader.nextFrame(rPartbuff)) {
                        probeTupleAccessor.reset(rPartbuff.getBuffer());
                        for (int tid = 0; tid < probeTupleAccessor.getTupleCount(); tid++) {
                            FrameUtils.appendConcatToWriter(writer, nullResultAppender, probeTupleAccessor, tid,
                                    nullTupleBuild.getFieldEndOffsets(), nullTupleBuild.getByteArray(), 0,
                                    nullTupleBuild.getSize());
                        }
                    }
                    probReader.close();
                    nullResultAppender.write(writer, true);
                }

                private void applyInMemHashJoin(int[] bKeys, int[] pKeys, int tabSize, RecordDescriptor buildRDesc,
                        RecordDescriptor probeRDesc, ITuplePartitionComputer hpcRepBuild,
                        ITuplePartitionComputer hpcRepProbe, RunFileReader bReader, RunFileReader pReader)
                        throws HyracksDataException {
                    boolean isReversed = pKeys == OptimizedHybridHashJoinOperatorDescriptor.this.buildKeys
                            && bKeys == OptimizedHybridHashJoinOperatorDescriptor.this.probeKeys;

                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";

                    ISerializableTable table = new SerializableHashTable(tabSize, ctx);
                    InMemoryHashJoin joiner = new InMemoryHashJoin(ctx, tabSize, new FrameTupleAccessor(probeRDesc),
                            hpcRepProbe, new FrameTupleAccessor(buildRDesc), hpcRepBuild,
                            new FrameTuplePairComparator(pKeys, bKeys, comparators), isLeftOuter, nonMatchWriter, table,
                            predEvaluator, isReversed);

                    bReader.open();
                    rPartbuff.reset();
                    while (bReader.nextFrame(rPartbuff)) {
                        //We need to allocate a copyBuffer, because this buffer gets added to the buffers list in the InMemoryHashJoin
                        ByteBuffer copyBuffer = ctx.allocateFrame(rPartbuff.getFrameSize());
                        FrameUtils.copyAndFlip(rPartbuff.getBuffer(), copyBuffer);
                        joiner.build(copyBuffer);
                        rPartbuff.reset();
                    }
                    bReader.close();
                    rPartbuff.reset();
                    //probe
                    pReader.open();
                    while (pReader.nextFrame(rPartbuff)) {
                        joiner.join(rPartbuff.getBuffer(), writer);
                        rPartbuff.reset();
                    }
                    pReader.close();
                    joiner.closeJoin(writer);
                    joiner.closeTable();
                }

                private void applyNestedLoopJoin(RecordDescriptor outerRd, RecordDescriptor innerRd, int memorySize,
                        RunFileReader outerReader, RunFileReader innerReader) throws HyracksDataException {
                    // The nested loop join result is outer + inner. All the other operator is probe + build. Hence the reverse relation is different
                    boolean isReversed = outerRd == buildRd && innerRd == probeRd;
                    assert isLeftOuter ? !isReversed : true : "LeftOut Join can not reverse roles";
                    ITuplePairComparator nljComptorOuterInner = isReversed ? nljComparatorBuild2Probe
                            : nljComparatorProbe2Build;
                    NestedLoopJoin nlj = new NestedLoopJoin(ctx, new FrameTupleAccessor(outerRd),
                            new FrameTupleAccessor(innerRd), nljComptorOuterInner, memorySize, predEvaluator,
                            isLeftOuter, nonMatchWriter);
                    nlj.setIsReversed(isReversed);

                    IFrame cacheBuff = new VSizeFrame(ctx);
                    innerReader.open();
                    while (innerReader.nextFrame(cacheBuff)) {
                        nlj.cache(cacheBuff.getBuffer());
                        cacheBuff.reset();
                    }
                    nlj.closeCache();

                    IFrame joinBuff = new VSizeFrame(ctx);
                    outerReader.open();

                    while (outerReader.nextFrame(joinBuff)) {
                        nlj.join(joinBuff.getBuffer(), writer);
                        joinBuff.reset();
                    }

                    nlj.closeJoin(writer);
                    outerReader.close();
                    innerReader.close();
                }
            };
            return op;
        }
    }

    public void setSkipInMemHJ(boolean b) {
        skipInMemoryHJ = b;
    }

    public void setForceNLJ(boolean b) {
        forceNLJ = b;
    }

    public void setForceRR(boolean b) {
        forceRR = (!isLeftOuter && b);
    }
}
