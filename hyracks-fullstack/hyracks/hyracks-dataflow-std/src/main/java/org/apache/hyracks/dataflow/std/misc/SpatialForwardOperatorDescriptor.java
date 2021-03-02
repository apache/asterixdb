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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractForwardOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

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
        return new MBRReaderActivity(new ActivityId(odId, SIDE_DATA_ACTIVITY_ID));
    }

    private class MBRState extends AbstractStateObject {
        Double[] mbrCoordinates;

        private MBRState(JobId jobId, TaskId stateObjectKey) {
            super(jobId, stateObjectKey);
        }
    }

    private class MBRReaderActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private MBRReaderActivity(ActivityId activityId) {
            super(activityId);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            RecordDescriptor inputRecordDescriptor = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new MBRReaderActivityNodePushable(ctx, inputRecordDescriptor, getActivityId(), partition);
        }
    }

    private class MBRReaderActivityNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
        private final FrameTupleAccessor frameTupleAccessor;
        private final FrameTupleReference frameTupleReference;
        private final IHyracksTaskContext ctx;
        private final ActivityId activityId;
        private final int partition;
        private Double[] mbrCoordinates;;

        private MBRReaderActivityNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecordDescriptor,
                ActivityId activityId, int partition) {
            this.ctx = ctx;
            this.frameTupleAccessor = new FrameTupleAccessor(inputRecordDescriptor);
            this.frameTupleReference = new FrameTupleReference();
            this.activityId = activityId;
            this.partition = partition;
            this.mbrCoordinates = new Double[4];
            mbrCoordinates[0] = 0.0;
            mbrCoordinates[1] = 0.0;
            mbrCoordinates[2] = 0.0;
            mbrCoordinates[3] = 0.0;
        }

        @Override
        public void open() throws HyracksDataException {
            // this activity does not have a consumer to open (it's a sink), and nothing to initialize
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            // "buffer" contains the serialized MBR sent by a MBR computer function.
            // deserialize the MBR
            frameTupleAccessor.reset(buffer);
            if (frameTupleAccessor.getTupleCount() != 1) {
                throw HyracksDataException.create(ErrorCode.ONE_TUPLE_RANGEMAP_EXPECTED, sourceLoc);
            }
            frameTupleReference.reset(frameTupleAccessor, 0);
            byte[] mbrBytes = frameTupleReference.getFieldData(0);
            int offset = frameTupleReference.getFieldStart(0);
            int length = frameTupleReference.getFieldLength(0);
//            LongPointable mbrPointable = new LongPointable();
            ByteArrayPointable mbrPointable = new ByteArrayPointable();
            mbrPointable.set(mbrBytes, offset + 1, length - 1);
            ByteArrayInputStream mbrIn = new ByteArrayInputStream(mbrPointable.getByteArray(),
                    mbrPointable.getStartOffset(), mbrPointable.getLength());
            DataInputStream mbrDataInputStream = new DataInputStream(mbrIn);
            mbrCoordinates[0] = DoubleSerializerDeserializer.INSTANCE.deserialize(mbrDataInputStream);
            mbrCoordinates[1] = DoubleSerializerDeserializer.INSTANCE.deserialize(mbrDataInputStream);
            mbrCoordinates[2] = DoubleSerializerDeserializer.INSTANCE.deserialize(mbrDataInputStream);
            mbrCoordinates[3] = DoubleSerializerDeserializer.INSTANCE.deserialize(mbrDataInputStream);
        }

        @Override
        public void fail() throws HyracksDataException {
            // it's a sink node pushable, nothing to fail
        }

        @Override
        public void close() throws HyracksDataException {
            // expecting count > 0
            if ((mbrCoordinates[0] == 0.0) && (mbrCoordinates[1] == 0.0) && (mbrCoordinates[2] == 0.0) && (mbrCoordinates[3] == 0.0)) {
                throw HyracksDataException.create(ErrorCode.NO_RANGEMAP_PRODUCED, sourceLoc);
            }
            // store the range map in the state object of ctx so that next activity (forward) could retrieve it
            TaskId countReaderTaskId = new TaskId(activityId, partition);
            MBRState countState = new MBRState(ctx.getJobletContext().getJobId(), countReaderTaskId);
            countState.mbrCoordinates = mbrCoordinates;
            ctx.setStateObject(countState);
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
            MBRState countState = (MBRState) ctx.getStateObject(stateObjKey);
            TaskUtil.put(sideDataKey, countState.mbrCoordinates, ctx);
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
