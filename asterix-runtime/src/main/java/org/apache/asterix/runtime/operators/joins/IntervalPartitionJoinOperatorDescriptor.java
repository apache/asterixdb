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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleRangePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.common.data.partition.range.FieldRangePartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.join.IMergeJoinChecker;
import org.apache.hyracks.dataflow.std.join.IMergeJoinCheckerFactory;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

public class IntervalPartitionJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;

    private static final double NLJ_SWITCH_THRESHOLD = 0.8;

    private static final String PROBE_REL = "RelR";
    private static final String BUILD_REL = "RelS";

    private final int memsize;
    private final double fudgeFactor;
    private final int[] probeKeys;
    private final int[] buildKeys;
    //    private final IBinaryHashFunctionFamily[] hashFunctionGeneratorFactories;
    //        private final IBinaryComparatorFactory[] comparatorFactories; //For in-mem HJ
    //    private final ITuplePairComparatorFactory tuplePairComparatorFactoryProbe2Build; //For NLJ in probe
    //    private final ITuplePairComparatorFactory tuplePairComparatorFactoryBuild2Probe; //For NLJ in probe
    private final IPredicateEvaluatorFactory predEvaluatorFactory;

    //    private final boolean isLeftOuter;
    //    private final INullWriterFactory[] nullWriterFactories1;

    //Flags added for test purpose
    private static boolean skipInMemoryHJ = false;
    private static boolean forceNLJ = false;
    private static boolean forceRR = false;

    private final int inputsize0;
    private final int recordsPerFrame;
    private final int probeKey;
    private final int buildKey;
    private final IIntervalMergeJoinCheckerFactory imjcf;
    private final IRangeMap rangeMap;
    private final IBinaryComparatorFactory[] comparatorFactories;

    //    private final IPredicateEvaluator predEvaluator;
    private boolean isReversed; //Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionJoinOperatorDescriptor.class.getName());

    public IntervalPartitionJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memsize, int inputsize0,
            int recordsPerFrame, double fudgeFactor, int[] keys0, int[] keys1, RecordDescriptor recordDescriptor,
            IBinaryComparatorFactory[] comparatorFactories, IIntervalMergeJoinCheckerFactory imjcf, IRangeMap rangeMap,
            IPredicateEvaluatorFactory predEvaluatorFactory) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.recordsPerFrame = recordsPerFrame;
        this.probeKey = keys0[0];
        this.buildKey = keys1[0];
        this.probeKeys = keys0;
        this.buildKeys = keys1;
        recordDescriptors[0] = recordDescriptor;
        this.imjcf = imjcf;
        this.rangeMap = rangeMap;
        this.fudgeFactor = fudgeFactor;
        this.comparatorFactories = comparatorFactories;
        this.predEvaluatorFactory = predEvaluatorFactory;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId p1Aid = new ActivityId(odId, BUILD_AND_PARTITION_ACTIVITY_ID);
        ActivityId p2Aid = new ActivityId(odId, PARTITION_AND_JOIN_ACTIVITY_ID);
        IActivity phase1 = new PartitionAndBuildActivityNode(p1Aid, p2Aid);
        IActivity phase2 = new ProbeAndJoinActivityNode(p2Aid, p1Aid);

        builder.addActivity(this, phase1);
        builder.addSourceEdge(1, phase1, 0);

        builder.addActivity(this, phase2);
        builder.addSourceEdge(0, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);
    }

    public static class BuildAndPartitionTaskState extends AbstractStateObject {
        private RunFileWriter[] fWriters;
        private IntervalPartitionJoin ipj;
        private int nPartitions;
        private int k;
        private int memoryForJoin;

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

    public class IntervalPartition {
        private int partitionI;
        private int partitionJ;

        public IntervalPartition(int i, int j) {
            reset(i, j);
        }

        public IntervalPartition(long intervalPartitionI, long intervalPartitionJ) {
            // TODO Auto-generated constructor stub
        }

        public void reset(int i, int j) {
            partitionI = i;
            partitionJ = j;
        }

        public int getI() {
            return partitionI;
        }

        public int getJ() {
            return partitionJ;
        }

        public int hashCode() {
            return (int) partitionI << 4 & (int) partitionJ;
        }

        public int partition(int k) {
            long duration = partitionJ - partitionI;
            int p;
            for (p = 0; p < duration - 1; ++p) {
                p += k - duration + 1;
            }
            p += partitionI;
            return p;
        }

    }

    private int determineK() {
        return 10;
    }

    private IntervalPartition getIntervalPartition(IFrameTupleAccessor accessorBuild, int tIndex, int fieldId,
            long partitionStart, long partitionDuration) throws HyracksDataException {
        return new IntervalPartition(
                IntervalPartitionUtil.getIntervalPartitionI(accessorBuild, tIndex, fieldId, partitionStart,
                        partitionDuration),
                IntervalPartitionUtil.getIntervalPartitionJ(accessorBuild, tIndex, fieldId, partitionStart,
                        partitionDuration));
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
        numberOfPartitions = (int) (Math
                .ceil((double) (buildSize * factor / nPartitions - memorySize) / (double) (memorySize - 1)));
        if (numberOfPartitions <= 0) {
            numberOfPartitions = 1; //becomes in-memory hash join
        }
        if (numberOfPartitions > memorySize) {
            numberOfPartitions = (int) Math.ceil(Math.sqrt(buildSize * factor / nPartitions));
            return (numberOfPartitions < memorySize ? numberOfPartitions : memorySize);
        }
        return numberOfPartitions;
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
            final long partitionStart = IntervalPartitionUtil.getStartOfPartition(rangeMap, partition);
            final long partitionEnd = IntervalPartitionUtil.getEndOfPartition(rangeMap, partition);
            final long partitionDuration = partitionEnd - partitionStart;
            final int k = determineK();

            //            final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
            //            for (int i = 0; i < comparatorFactories.length; i++) {
            //                comparators[i] = comparatorFactories[i].createBinaryComparator();
            //            }

            final IPredicateEvaluator predEvaluator = (predEvaluatorFactory == null ? null
                    : predEvaluatorFactory.createPredicateEvaluator());

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(
                        ctx.getJobletContext().getJobId(), new TaskId(getActivityId(), partition));

                ITuplePartitionComputer probeHpc = new IntervalPartitionComputerFactory(probeKey, k, partitionStart,
                        partitionDuration).createPartitioner();
                ITuplePartitionComputer buildHpc = new IntervalPartitionComputerFactory(buildKey, k, partitionStart,
                        partitionDuration).createPartitioner();

                @Override
                public void open() throws HyracksDataException {
                    if (memsize <= 2) {
                        // Dedicated buffers: One buffer to read and one buffer for output
                        throw new HyracksDataException("not enough memory for join");
                    }
                    state.k = determineK();
                    state.nPartitions = IntervalPartitionUtil.getMaxPartitions(state.k);
                    state.memoryForJoin = memsize - 2;
                    IIntervalMergeJoinChecker imjc = imjcf.createMergeJoinChecker(buildKeys, probeKeys, nPartitions);
                    state.ipj = new IntervalPartitionJoin(ctx, state.memoryForJoin, state.k, state.nPartitions,
                            PROBE_REL, BUILD_REL, probeKeys, buildKeys, imjc, comparatorFactories, probeRd, buildRd,
                            probeHpc, buildHpc, predEvaluator, isReversed, null);

                    state.ipj.initBuild();
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("IntervalPartitionJoin is starting the build phase with " + state.nPartitions
                                + " partitions using " + state.memoryForJoin + " frames for memory.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.ipj.build(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.ipj.closeBuild();
                    ctx.setStateObject(state);
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("IntervalPartitionJoin closed its build phase");
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
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
            //            final ITuplePairComparator nljComparatorProbe2Build = tuplePairComparatorFactoryProbe2Build
            //                    .createTuplePairComparator(ctx);
            //            final ITuplePairComparator nljComparatorBuild2Probe = tuplePairComparatorFactoryBuild2Probe
            //                    .createTuplePairComparator(ctx);
            final IPredicateEvaluator predEvaluator = (predEvaluatorFactory == null ? null
                    : predEvaluatorFactory.createPredicateEvaluator());

            for (int i = 0; i < comparatorFactories.length; i++) {
                comparators[i] = comparatorFactories[i].createBinaryComparator();
            }

            //            final INullWriter[] nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
            //            final ArrayTupleBuilder nullTupleBuild = isLeftOuter ? new ArrayTupleBuilder(buildRd.getFieldCount())
            //                    : null;
            //            if (isLeftOuter) {
            //                DataOutput out = nullTupleBuild.getDataOutput();
            //                for (int i = 0; i < nullWriterFactories1.length; i++) {
            //                    nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
            //                    nullWriters1[i].writeNull(out);
            //                    nullTupleBuild.addFieldEndOffset();
            //                }
            //            }

            final long partitionStart = IntervalPartitionUtil.getStartOfPartition(rangeMap, partition);
            final long partitionEnd = IntervalPartitionUtil.getEndOfPartition(rangeMap, partition);
            final long partitionDuration = partitionEnd - partitionStart;

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
                    state.ipj.initProbe();

                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("OptimizedHybridHashJoin is starting the probe phase.");
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.ipj.probe(buffer, writer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    state.ipj.closeProbe(writer);
                    state.ipj.joinSpilledPartitions(writer);
                    writer.close();
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("OptimizedHybridHashJoin closed its probe phase");
                    }
                }
            };
            return op;
        }
    }
}