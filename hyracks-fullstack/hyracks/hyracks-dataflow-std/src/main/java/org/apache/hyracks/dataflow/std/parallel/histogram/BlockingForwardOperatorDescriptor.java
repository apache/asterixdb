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

package org.apache.hyracks.dataflow.std.parallel.histogram;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.parallel.base.MaterializingSampleTaskState;

/**
 * @author michael
 */
public class BlockingForwardOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final RecordDescriptor sampleDesc;

    private final static int SAMPLED_RANGE_ACTIVITY_ID = 0;
    private final static int FRAME_FORWARD_ACTIVITY_ID = 1;

    public BlockingForwardOperatorDescriptor(IOperatorDescriptorRegistry spec, int frameLimit, int[] sampleFields,
            RecordDescriptor inSampleDesc, RecordDescriptor inDataDesc, IBinaryComparatorFactory[] compFactories) {
        super(spec, 2, 1);
        this.sampleDesc = inSampleDesc;
        this.recordDescriptors[0] = inDataDesc;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.api.dataflow.IOperatorDescriptor#contributeActivities(org.apache.hyracks.api.dataflow.IActivityGraphBuilder)
     */
    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        // Currently, the streaming materialization is enforced during the initial phase of sampling.
        RangeMapReaderActivityNode sra = new RangeMapReaderActivityNode(new ActivityId(odId, SAMPLED_RANGE_ACTIVITY_ID));
        builder.addActivity(this, sra);
        builder.addSourceEdge(0, sra, 0);
        BlockingForwardActivityNode bfa = new BlockingForwardActivityNode(new ActivityId(odId,
                FRAME_FORWARD_ACTIVITY_ID));
        builder.addActivity(this, bfa);
        builder.addSourceEdge(1, bfa, 0);
        builder.addBlockingEdge(sra, bfa);
    }

    private final class RangeMapReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public RangeMapReaderActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new ForwardOperatorNodePushable(ctx, sampleDesc, partition);
        }
    }

    private final class BlockingForwardActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public BlockingForwardActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private MaterializingSampleTaskState dataState;

                @Override
                public void open() throws HyracksDataException {
                    dataState = new MaterializingSampleTaskState(ctx.getJobletContext().getJobId(), new TaskId(
                            getActivityId(), partition));
                    dataState.open(ctx);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    dataState.appendFrame(buffer);
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                    dataState.close();
                    dataState.writeOut(writer, new VSizeFrame(ctx));
                }

            };
        }
    }
}
