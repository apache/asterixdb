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

package org.apache.asterix.runtime.operators.joins.intervalpartition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class IntervalPartitionJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;

    private static final String PROBE_REL = "RelR";
    private static final String BUILD_REL = "RelS";

    private final int memsize;
    private final int[] probeKeys;
    private final int[] buildKeys;
    private final IPredicateEvaluatorFactory predEvaluatorFactory;

    private final int inputSizeBuild;
    private final int probeKey;
    private final int buildKey;
    private final IIntervalMergeJoinCheckerFactory imjcf;
    private final IRangeMap rangeMap;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private boolean isReversed; //Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionJoinOperatorDescriptor.class.getName());

    public IntervalPartitionJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memsize, int inputSizeBuild,
            int[] leftKeys, int[] rightKeys, RecordDescriptor recordDescriptor,
            IBinaryComparatorFactory[] comparatorFactories, IIntervalMergeJoinCheckerFactory imjcf, IRangeMap rangeMap,
            IPredicateEvaluatorFactory predEvaluatorFactory) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputSizeBuild = inputSizeBuild;
        this.buildKey = leftKeys[0];
        this.probeKey = rightKeys[0];
        this.buildKeys = leftKeys;
        this.probeKeys = rightKeys;
        recordDescriptors[0] = recordDescriptor;
        this.imjcf = imjcf;
        this.rangeMap = rangeMap;
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
        builder.addSourceEdge(0, phase1, 0);

        builder.addActivity(this, phase2);
        builder.addSourceEdge(1, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);
    }

    public static class BuildAndPartitionTaskState extends AbstractStateObject {
        private IntervalPartitionJoin ipj;
        private int intervalPartitions;
        private int partition;
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
            final int k = IntervalPartitionUtil.determineK();
            final long partitionStart = IntervalPartitionUtil.getStartOfPartition(rangeMap, partition);
            final long partitionEnd = IntervalPartitionUtil.getEndOfPartition(rangeMap, partition);

            final IPredicateEvaluator predEvaluator = (predEvaluatorFactory == null ? null
                    : predEvaluatorFactory.createPredicateEvaluator());

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(
                        ctx.getJobletContext().getJobId(), new TaskId(getActivityId(), partition));

                @Override
                public void open() throws HyracksDataException {
                    if (memsize <= 2) {
                        // Dedicated buffers: One buffer to read and one buffer for output
                        throw new HyracksDataException("not enough memory for join");
                    }
                    if (k <= 2) {
                        throw new HyracksDataException("not enough partitions (k) for interval partition join");
                    }
                    ITuplePartitionComputer buildHpc = new IntervalPartitionComputerFactory(buildKey, k, partitionStart,
                            partitionEnd).createPartitioner();
                    ITuplePartitionComputer probeHpc = new IntervalPartitionComputerFactory(probeKey, k, partitionStart,
                            partitionEnd).createPartitioner();

                    state.partition = partition;
                    state.k = k;
                    state.intervalPartitions = IntervalPartitionUtil.getMaxPartitions(state.k);
                    state.memoryForJoin = memsize - 2;
                    IIntervalMergeJoinChecker imjc = imjcf.createMergeJoinChecker(buildKeys, probeKeys, partition);
                    state.ipj = new IntervalPartitionJoin(ctx, state.memoryForJoin, state.k, state.intervalPartitions,
                            BUILD_REL, PROBE_REL, buildKeys, probeKeys, imjc, comparatorFactories, buildRd, probeRd,
                            buildHpc, probeHpc, predEvaluator, isReversed, null);

                    state.ipj.initBuild();
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("IntervalPartitionJoin is starting the build phase with " + state.intervalPartitions
                                + " interval partitions using " + state.memoryForJoin + " frames for memory.");
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

    private class ProbeAndJoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public ProbeAndJoinActivityNode(ActivityId id, ActivityId buildAid) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                        throws HyracksDataException {

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private BuildAndPartitionTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = (BuildAndPartitionTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), BUILD_AND_PARTITION_ACTIVITY_ID), partition));

                    writer.open();
                    state.ipj.initProbe();

                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("IntervalPartitionJoin is starting the probe phase.");
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
                    state.ipj.closeAndDeleteRunFiles();
                    writer.close();
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine("IntervalPartitionJoin closed its probe phase");
                    }
                }
            };
            return op;
        }
    }
}