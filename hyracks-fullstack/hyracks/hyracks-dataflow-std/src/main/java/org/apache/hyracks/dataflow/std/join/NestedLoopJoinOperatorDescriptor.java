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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class NestedLoopJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int JOIN_CACHE_ACTIVITY_ID = 0;
    private static final int NL_JOIN_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private final ITuplePairComparatorFactory comparatorFactory;
    private final int memSize;
    private final IPredicateEvaluatorFactory predEvaluatorFactory;
    private final boolean isLeftOuter;
    private final IMissingWriterFactory[] nullWriterFactories1;

    public NestedLoopJoinOperatorDescriptor(IOperatorDescriptorRegistry spec,
            ITuplePairComparatorFactory comparatorFactory, RecordDescriptor recordDescriptor, int memSize,
            boolean isLeftOuter, IMissingWriterFactory[] nullWriterFactories1) {
        this(spec, comparatorFactory, recordDescriptor, memSize, null, isLeftOuter, nullWriterFactories1);
    }

    public NestedLoopJoinOperatorDescriptor(IOperatorDescriptorRegistry spec,
            ITuplePairComparatorFactory comparatorFactory, RecordDescriptor recordDescriptor, int memSize,
            IPredicateEvaluatorFactory predEvalFactory, boolean isLeftOuter,
            IMissingWriterFactory[] nullWriterFactories1) {
        super(spec, 2, 1);
        this.comparatorFactory = comparatorFactory;
        this.outRecDescs[0] = recordDescriptor;
        this.memSize = memSize;
        this.predEvaluatorFactory = predEvalFactory;
        this.isLeftOuter = isLeftOuter;
        this.nullWriterFactories1 = nullWriterFactories1;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId jcaId = new ActivityId(getOperatorId(), JOIN_CACHE_ACTIVITY_ID);
        ActivityId nljAid = new ActivityId(getOperatorId(), NL_JOIN_ACTIVITY_ID);
        JoinCacheActivityNode jc = new JoinCacheActivityNode(jcaId, nljAid);
        NestedLoopJoinActivityNode nlj = new NestedLoopJoinActivityNode(nljAid);

        builder.addActivity(this, jc);
        builder.addSourceEdge(1, jc, 0);

        builder.addActivity(this, nlj);
        builder.addSourceEdge(0, nlj, 0);

        builder.addTargetEdge(0, nlj, 0);
        builder.addBlockingEdge(jc, nlj);
    }

    public static class JoinCacheTaskState extends AbstractStateObject {
        private NestedLoopJoin joiner;

        private JoinCacheTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }
    }

    private class JoinCacheActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId nljAid;

        public JoinCacheActivityNode(ActivityId id, ActivityId nljAid) {
            super(id);
            this.nljAid = nljAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(nljAid, 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final ITuplePairComparator comparator = comparatorFactory.createTuplePairComparator(ctx);
            final IPredicateEvaluator predEvaluator =
                    (predEvaluatorFactory != null) ? predEvaluatorFactory.createPredicateEvaluator() : null;

            final IMissingWriter[] nullWriters1 = isLeftOuter ? new IMissingWriter[nullWriterFactories1.length] : null;
            if (isLeftOuter) {
                for (int i = 0; i < nullWriterFactories1.length; i++) {
                    nullWriters1[i] = nullWriterFactories1[i].createMissingWriter();
                }
            }

            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private JoinCacheTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new JoinCacheTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));

                    state.joiner = new NestedLoopJoin(ctx, new FrameTupleAccessor(rd0), new FrameTupleAccessor(rd1),
                            comparator, memSize, predEvaluator, isLeftOuter, nullWriters1);

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame(buffer.capacity());
                    FrameUtils.copyAndFlip(buffer, copyBuffer);
                    state.joiner.cache(copyBuffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.joiner.closeCache();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    // No variables to update.
                }
            };
        }
    }

    private class NestedLoopJoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public NestedLoopJoinActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private JoinCacheTaskState state;
                boolean failed = false;

                @Override
                public void open() throws HyracksDataException {
                    writer.open();
                    state = (JoinCacheTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), JOIN_CACHE_ACTIVITY_ID), partition));
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.joiner.join(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (failed) {
                        try {
                            state.joiner.closeCache();
                        } finally {
                            writer.close();
                        }
                        return;
                    }
                    try {
                        try {
                            state.joiner.completeJoin(writer);
                        } finally {
                            state.joiner.releaseMemory();
                        }
                    } catch (Exception e) {
                        state.joiner.closeCache();
                        writer.fail();
                        throw e;
                    } finally {
                        writer.close();
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    failed = true;
                    writer.fail();
                }
            };
        }
    }
}
