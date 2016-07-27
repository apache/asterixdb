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
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
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
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.sort.ISorter;

public class RangeForwardOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int RANGE_FORWARD_ACTIVITY_ID = 0;
    private static final int RANGE_WRITER_ACTIVITY_ID = 1;

    private final IRangeMap rangeMap;

    public RangeForwardOperatorDescriptor(IOperatorDescriptorRegistry spec, IRangeMap rangeMap) {
        super(spec, 1, 1);
        this.rangeMap = rangeMap;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ForwardActivityNode fan = new ForwardActivityNode(new ActivityId(odId, RANGE_FORWARD_ACTIVITY_ID));
        builder.addActivity(this, fan);
        builder.addSourceEdge(0, fan, 0);
        builder.addTargetEdge(0, fan, 0);
    }

    public static class RangeForwardTaskState extends AbstractStateObject {
        private IRangeMap rangeMap;

        public RangeForwardTaskState(JobId jobId, TaskId taskId, IRangeMap rangeMap) {
            super(jobId, taskId);
            this.rangeMap = rangeMap;
        }

        public IRangeMap getRangeMap() {
            return rangeMap;
        }
    }

    private final class ForwardActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public ForwardActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private RangeForwardTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new RangeForwardTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition), rangeMap);
                    ctx.setStateObject(state);
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer bufferAccessor) throws HyracksDataException {
                    FrameUtils.flushFrame(bufferAccessor, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    writer.close();
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }
            };
        }
    }
}
