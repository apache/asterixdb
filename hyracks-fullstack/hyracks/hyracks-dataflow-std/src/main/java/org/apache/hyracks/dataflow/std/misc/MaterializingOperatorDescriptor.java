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
package org.apache.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class MaterializingOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final static int MATERIALIZER_ACTIVITY_ID = 0;
    private final static int READER_ACTIVITY_ID = 1;
    private final static int MATERIALIZER_READER_ACTIVITY_ID = 2;

    private boolean isSingleActivity;

    public MaterializingOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDescriptor) {
        this(spec, recordDescriptor, false);
    }

    public MaterializingOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recordDescriptor,
            boolean isSingleActivity) {
        super(spec, 1, 1);
        outRecDescs[0] = recordDescriptor;
        this.isSingleActivity = isSingleActivity;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        if (isSingleActivity) {
            MaterializerReaderActivityNode mra =
                    new MaterializerReaderActivityNode(new ActivityId(odId, MATERIALIZER_READER_ACTIVITY_ID));

            builder.addActivity(this, mra);
            builder.addSourceEdge(0, mra, 0);
            builder.addTargetEdge(0, mra, 0);
        } else {
            MaterializerActivityNode ma = new MaterializerActivityNode(new ActivityId(odId, MATERIALIZER_ACTIVITY_ID));
            ReaderActivityNode ra = new ReaderActivityNode(new ActivityId(odId, READER_ACTIVITY_ID));

            builder.addActivity(this, ma);
            builder.addSourceEdge(0, ma, 0);

            builder.addActivity(this, ra);
            builder.addTargetEdge(0, ra, 0);

            builder.addBlockingEdge(ma, ra);
        }

    }

    private final class MaterializerReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializerReaderActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private MaterializerTaskState state;
                private boolean failed = false;

                @Override
                public void open() throws HyracksDataException {
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.open(ctx);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.appendFrame(buffer);
                }

                @Override
                public void fail() throws HyracksDataException {
                    failed = true;
                }

                @Override
                public void close() throws HyracksDataException {
                    state.close();
                    state.writeOut(writer, new VSizeFrame(ctx), failed);
                }
            };
        }
    }

    private final class MaterializerActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MaterializerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.open(ctx);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.appendFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.close();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    private final class ReaderActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public ReaderActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    MaterializerTaskState state = (MaterializerTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), MATERIALIZER_ACTIVITY_ID), partition));
                    state.writeOut(writer, new VSizeFrame(ctx), false);
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                }
            };
        }
    }
}
