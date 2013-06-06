/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.std.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluator;
import edu.uci.ics.hyracks.api.dataflow.value.IPredicateEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class NestedLoopJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int JOIN_CACHE_ACTIVITY_ID = 0;
    private static final int NL_JOIN_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private final ITuplePairComparatorFactory comparatorFactory;
    private final int memSize;
    private final IPredicateEvaluatorFactory predEvaluatorFactory;
    private final boolean isLeftOuter;
    private final INullWriterFactory[] nullWriterFactories1;
    
    public NestedLoopJoinOperatorDescriptor(IOperatorDescriptorRegistry spec,
            ITuplePairComparatorFactory comparatorFactory, RecordDescriptor recordDescriptor, int memSize,
            boolean isLeftOuter, INullWriterFactory[] nullWriterFactories1) {
        this(spec, comparatorFactory, recordDescriptor, memSize, null, isLeftOuter, nullWriterFactories1);
    }
    
    public NestedLoopJoinOperatorDescriptor(IOperatorDescriptorRegistry spec,
            ITuplePairComparatorFactory comparatorFactory, RecordDescriptor recordDescriptor, int memSize,
            IPredicateEvaluatorFactory predEvalFactory, boolean isLeftOuter, INullWriterFactory[] nullWriterFactories1) {
        super(spec, 2, 1);
        this.comparatorFactory = comparatorFactory;
        this.recordDescriptors[0] = recordDescriptor;
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

        public JoinCacheTaskState() {
        }

        private JoinCacheTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

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
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(nljAid, 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final ITuplePairComparator comparator = comparatorFactory.createTuplePairComparator(ctx);
            final IPredicateEvaluator predEvaluator = ( (predEvaluatorFactory != null) ? predEvaluatorFactory.createPredicateEvaluator() : null);
            
            final INullWriter[] nullWriters1 = isLeftOuter ? new INullWriter[nullWriterFactories1.length] : null;
            if (isLeftOuter) {
                for (int i = 0; i < nullWriterFactories1.length; i++) {
                    nullWriters1[i] = nullWriterFactories1[i].createNullWriter();
                }
            }

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private JoinCacheTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new JoinCacheTaskState(ctx.getJobletContext().getJobId(), new TaskId(getActivityId(),
                            partition));

                    state.joiner = new NestedLoopJoin(ctx, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                            new FrameTupleAccessor(ctx.getFrameSize(), rd1), comparator, memSize, predEvaluator, isLeftOuter,
                            nullWriters1);

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame();
                    FrameUtils.copy(buffer, copyBuffer);
                    FrameUtils.makeReadable(copyBuffer);
                    state.joiner.cache(copyBuffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.joiner.closeCache();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
            return op;
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

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private JoinCacheTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = (JoinCacheTaskState) ctx.getStateObject(new TaskId(new ActivityId(getOperatorId(),
                            JOIN_CACHE_ACTIVITY_ID), partition));
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.joiner.join(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.joiner.closeJoin(writer);
                    writer.close();
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }
            };
            return op;
        }
    }
}