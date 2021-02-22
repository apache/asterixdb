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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractForwardOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

import java.nio.ByteBuffer;

public class SpatialForwardOperatorDescriptor extends AbstractForwardOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    /**
     * @param spec used to create the operator id.
     * @param sideDataKey the unique key to store the range map in the shared map & transfer it to partitioner.
     * @param outputRecordDescriptor the output schema of this operator.
     */
    public SpatialForwardOperatorDescriptor(IOperatorDescriptorRegistry spec, String sideDataKey,
                                         RecordDescriptor outputRecordDescriptor) {
        super(spec, sideDataKey, outputRecordDescriptor);
    }

    @Override
    public AbstractActivityNode createForwardDataActivity() {
        return new ForwardDataActivity(new ActivityId(odId, FORWARD_DATA_ACTIVITY_ID));
    }

    @Override
    public AbstractActivityNode createSideDataActivity() {
        return null;
    }

    private class CountState extends AbstractStateObject {
        int count;

        private CountState(JobId jobId, TaskId stateObjectKey) {
            super(jobId, stateObjectKey);
        }
    }

    /**
     * Forward data activity. {@see {@link ForwardDataActivityNodePushable }}
     */
    private class ForwardDataActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private ForwardDataActivity(ActivityId activityId) {
            super(activityId);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                                                       IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
            return new ForwardDataActivityNodePushable(ctx, partition);
        }
    }

    private class ForwardDataActivityNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
        private final IHyracksTaskContext ctx;
        private final int partition;

        /**
         * @param ctx used to retrieve the range map stored by the range reader activity.
         * @param partition used to create the same task id used by the range reader activity for storing the range.
         */
        private ForwardDataActivityNodePushable(IHyracksTaskContext ctx, int partition) {
            this.ctx = ctx;
            this.partition = partition;
        }

        @Override
        public void open() throws HyracksDataException {
            // retrieve the range map from the state object (previous activity should have already stored it)
            // then deposit it into the ctx so that MToN-partition can pick it up
            Object stateObjKey = new TaskId(new ActivityId(odId, SIDE_DATA_ACTIVITY_ID), partition);
            CountState countState = (CountState) ctx.getStateObject(stateObjKey);
            TaskUtil.put(sideDataKey, countState.count, ctx);
            writer.open();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            writer.nextFrame(buffer);
        }

        @Override
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            writer.close();
        }

        @Override
        public void flush() throws HyracksDataException {
            writer.flush();
        }
    }
}
