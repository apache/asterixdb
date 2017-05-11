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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOpenableDataWriter;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import org.apache.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class SplitVectorOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int COLLECT_ACTIVITY_ID = 0;
    private static final int SPLIT_ACTIVITY_ID = 1;

    public static class CollectTaskState extends AbstractStateObject {
        private ArrayList<Object[]> buffer;

        public CollectTaskState() {
        }

        private CollectTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
        }
    }

    private class CollectActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public CollectActivity(ActivityId id) {
            super(id);
        }

        @Override
        public ActivityId getActivityId() {
            return id;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOpenableDataWriterOperator op = new IOpenableDataWriterOperator() {
                private CollectTaskState state;

                @Override
                public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
                    throw new IllegalArgumentException();
                }

                @Override
                public void open() throws HyracksDataException {
                    state = new CollectTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.buffer = new ArrayList<Object[]>();
                }

                @Override
                public void close() throws HyracksDataException {
                    ctx.setStateObject(state);
                }

                @Override
                public void writeData(Object[] data) throws HyracksDataException {
                    state.buffer.add(data);
                }

                @Override
                public void fail() throws HyracksDataException {

                }

                @Override
                public void flush() throws HyracksDataException {
                    // flush() is a no op since the frame writer's whole job is to write state data to a buffer
                }
            };
            return new DeserializedOperatorNodePushable(ctx, op,
                    recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
        }
    }

    private class SplitActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SplitActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOpenableDataWriterOperator op = new IOpenableDataWriterOperator() {
                private IOpenableDataWriter<Object[]> writer;

                private CollectTaskState state;

                @Override
                public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
                    if (index != 0) {
                        throw new IllegalArgumentException();
                    }
                    this.writer = writer;
                }

                @Override
                public void open() throws HyracksDataException {
                    state = (CollectTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), COLLECT_ACTIVITY_ID), partition));
                }

                @Override
                public void close() throws HyracksDataException {
                }

                @Override
                public void writeData(Object[] data) throws HyracksDataException {
                    int n = state.buffer.size();
                    int step = (int) Math.floor(n / (float) splits);
                    writer.open();
                    for (int i = 0; i < splits; ++i) {
                        writer.writeData(state.buffer.get(step * (i + 1) - 1));
                    }
                    writer.close();
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void flush() throws HyracksDataException {
                    writer.flush();
                }
            };
            return new DeserializedOperatorNodePushable(ctx, op,
                    recordDescProvider.getOutputRecordDescriptor(getActivityId(), 0));
        }
    }

    private static final long serialVersionUID = 1L;

    private final int splits;

    public SplitVectorOperatorDescriptor(IOperatorDescriptorRegistry spec, int splits,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.splits = splits;
        outRecDescs[0] = recordDescriptor;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        CollectActivity ca = new CollectActivity(new ActivityId(odId, COLLECT_ACTIVITY_ID));
        SplitActivity sa = new SplitActivity(new ActivityId(odId, SPLIT_ACTIVITY_ID));

        builder.addActivity(this, ca);
        builder.addSourceEdge(0, ca, 0);

        builder.addActivity(this, sa);
        builder.addTargetEdge(0, sa, 0);

        builder.addBlockingEdge(ca, sa);
    }
}
