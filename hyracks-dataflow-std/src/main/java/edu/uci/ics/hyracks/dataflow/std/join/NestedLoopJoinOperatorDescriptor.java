/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class NestedLoopJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final String JOINER = "joiner";

    private static final long serialVersionUID = 1L;
    private final ITuplePairComparatorFactory comparatorFactory;
    private final int memSize;

    public NestedLoopJoinOperatorDescriptor(JobSpecification spec, ITuplePairComparatorFactory comparatorFactory,
            RecordDescriptor recordDescriptor, int memSize) {
        super(spec, 2, 1);
        this.comparatorFactory = comparatorFactory;
        this.recordDescriptors[0] = recordDescriptor;
        this.memSize = memSize;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        JoinCacheActivityNode jc = new JoinCacheActivityNode(new ActivityId(getOperatorId(), 0));
        NestedLoopJoinActivityNode nlj = new NestedLoopJoinActivityNode(new ActivityId(getOperatorId(), 1));

        builder.addActivity(jc);
        builder.addSourceEdge(1, jc, 0);

        builder.addActivity(nlj);
        builder.addSourceEdge(0, nlj, 0);

        builder.addTargetEdge(0, nlj, 0);
        builder.addBlockingEdge(jc, nlj);
    }

    private class JoinCacheActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public JoinCacheActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 1);
            final ITuplePairComparator comparator = comparatorFactory.createTuplePairComparator();

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private NestedLoopJoin joiner;

                @Override
                public void open() throws HyracksDataException {
                    joiner = new NestedLoopJoin(ctx, new FrameTupleAccessor(ctx.getFrameSize(), rd0),
                            new FrameTupleAccessor(ctx.getFrameSize(), rd1), comparator, memSize);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame();
                    FrameUtils.copy(buffer, copyBuffer);
                    FrameUtils.makeReadable(copyBuffer);
                    joiner.cache(copyBuffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    joiner.closeCache();
                    env.set(JOINER, joiner);
                }

                @Override
                public void flush() throws HyracksDataException {
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
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, final IOperatorEnvironment env,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private NestedLoopJoin joiner;

                @Override
                public void open() throws HyracksDataException {
                    joiner = (NestedLoopJoin) env.get(JOINER);
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    joiner.join(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    joiner.closeJoin(writer);
                    writer.close();
                    env.set(JOINER, null);
                }

                @Override
                public void flush() throws HyracksDataException {
                    writer.flush();
                }
            };
            return op;
        }
    }
}