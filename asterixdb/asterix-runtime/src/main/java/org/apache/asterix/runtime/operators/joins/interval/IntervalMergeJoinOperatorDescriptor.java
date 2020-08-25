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

package org.apache.asterix.runtime.operators.joins.interval;

import java.nio.ByteBuffer;

import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtilFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class IntervalMergeJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int JOIN_BUILD_ACTIVITY_ID = 0;
    private static final int JOIN_PROBE_ACTIVITY_ID = 1;
    private final int buildKey;
    private final int probeKey;
    private final int memoryForJoin;
    private final IIntervalJoinUtilFactory imjcf;

    public IntervalMergeJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoin, int[] buildKey,
            int[] probeKeys, RecordDescriptor recordDescriptor, IIntervalJoinUtilFactory imjcf) {
        super(spec, 2, 1);
        outRecDescs[0] = recordDescriptor;
        this.buildKey = buildKey[0];
        this.probeKey = probeKeys[0];
        this.memoryForJoin = memoryForJoin;
        this.imjcf = imjcf;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId buildAid = new ActivityId(odId, JOIN_BUILD_ACTIVITY_ID);
        ActivityId probeAid = new ActivityId(odId, JOIN_PROBE_ACTIVITY_ID);

        IActivity probeAN = new JoinProbeActivityNode(probeAid);
        IActivity buildAN = new JoinBuildActivityNode(buildAid, probeAid);

        builder.addActivity(this, buildAN);
        builder.addSourceEdge(0, buildAN, 0);

        builder.addActivity(this, probeAN);
        builder.addSourceEdge(1, probeAN, 0);
        builder.addTargetEdge(0, probeAN, 0);
        builder.addBlockingEdge(buildAN, probeAN);
    }

    public static class JoinCacheTaskState extends AbstractStateObject {
        private IntervalMergeJoiner joiner;

        private JoinCacheTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }
    }

    private class JoinBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId nljAid;

        public JoinBuildActivityNode(ActivityId id, ActivityId nljAid) {
            super(id);
            this.nljAid = nljAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(nljAid, 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private JoinCacheTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new JoinCacheTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));

                    IIntervalJoinUtil imjc = imjcf.createIntervalMergeJoinUtil(buildKey, probeKey, ctx, nPartitions);

                    state.joiner = new IntervalMergeJoiner(ctx, memoryForJoin, imjc, buildKey, probeKey, rd0, rd1);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame(buffer.capacity());
                    FrameUtils.copyAndFlip(buffer, copyBuffer);
                    state.joiner.processBuildFrame(copyBuffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.joiner.processBuildClose();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() {
                    // No variables to update.
                }
            };
        }
    }

    private class JoinProbeActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public JoinProbeActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private JoinCacheTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    writer.open();
                    state = (JoinCacheTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), JOIN_BUILD_ACTIVITY_ID), partition));
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.joiner.processProbeFrame(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    try {
                        state.joiner.processProbeClose(writer);
                    } finally {
                        writer.close();
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }
            };
        }
    }
}
