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
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractForwardOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

// TODO(ali): forward operator should probably be moved to asterix layer
public class SortForwardOperatorDescriptor extends AbstractForwardOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    /**
     * @param spec used to create the operator id.
     * @param sideDataKey the unique key to store the range map in the shared map & transfer it to partitioner.
     * @param outputRecordDescriptor the output schema of this operator.
     */
    public SortForwardOperatorDescriptor(IOperatorDescriptorRegistry spec, String sideDataKey,
            RecordDescriptor outputRecordDescriptor) {
        super(spec, sideDataKey, outputRecordDescriptor);
    }

    @Override
    public AbstractActivityNode createForwardDataActivity() {
        return new ForwardDataActivity(new ActivityId(odId, FORWARD_DATA_ACTIVITY_ID));
    }

    @Override
    public AbstractActivityNode createSideDataActivity() {
        return new RangeMapReaderActivity(new ActivityId(odId, SIDE_DATA_ACTIVITY_ID));
    }

    /**
     * Internal class that is used to transfer the {@link RangeMap} object between activities in different ctx but in
     * the same NC, from {@link RangeMapReaderActivity} to {@link ForwardDataActivity}. These activities will share
     * the {@link org.apache.hyracks.api.job.IOperatorEnvironment} of the {@link org.apache.hyracks.control.nc.Joblet}
     * where the range map will be stored.
     */
    private class RangeMapState extends AbstractStateObject {
        RangeMap rangeMap;

        private RangeMapState(JobId jobId, TaskId stateObjectKey) {
            super(jobId, stateObjectKey);
        }
    }

    /**
     * Range map reader activity. {@see {@link RangeMapReaderActivityNodePushable}}
     */
    private class RangeMapReaderActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private RangeMapReaderActivity(ActivityId activityId) {
            super(activityId);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            RecordDescriptor inputRecordDescriptor = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new RangeMapReaderActivityNodePushable(ctx, inputRecordDescriptor, getActivityId(), partition);
        }
    }

    /**
     * Forward data activity. {@see {@link ForwardDataActivityNodePushable}}
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

    private class RangeMapReaderActivityNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
        private final FrameTupleAccessor frameTupleAccessor;
        private final FrameTupleReference frameTupleReference;
        private final IHyracksTaskContext ctx;
        private final ActivityId activityId;
        private final int partition;
        private int numFields;
        private byte[] splitValues;
        private int[] splitValuesEndOffsets;

        private RangeMapReaderActivityNodePushable(IHyracksTaskContext ctx, RecordDescriptor inputRecordDescriptor,
                ActivityId activityId, int partition) {
            this.ctx = ctx;
            this.frameTupleAccessor = new FrameTupleAccessor(inputRecordDescriptor);
            this.frameTupleReference = new FrameTupleReference();
            this.activityId = activityId;
            this.partition = partition;
        }

        @Override
        public void open() throws HyracksDataException {
            // this activity does not have a consumer to open (it's a sink), and nothing to initialize
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            // "buffer" contains the serialized range map sent by a range map computer function.
            // deserialize the range map
            frameTupleAccessor.reset(buffer);
            if (frameTupleAccessor.getTupleCount() != 1) {
                throw HyracksDataException.create(ErrorCode.ONE_TUPLE_RANGEMAP_EXPECTED, sourceLoc);
            }
            frameTupleReference.reset(frameTupleAccessor, 0);
            byte[] rangeMap = frameTupleReference.getFieldData(0);
            int offset = frameTupleReference.getFieldStart(0);
            int length = frameTupleReference.getFieldLength(0);
            ByteArrayPointable pointable = new ByteArrayPointable();
            pointable.set(rangeMap, offset + 1, length - 1);
            ByteArrayInputStream rangeMapIn = new ByteArrayInputStream(pointable.getByteArray(),
                    pointable.getContentStartOffset(), pointable.getContentLength());
            DataInputStream dataInputStream = new DataInputStream(rangeMapIn);
            numFields = IntegerSerializerDeserializer.read(dataInputStream);
            splitValues = ByteArraySerializerDeserializer.read(dataInputStream);
            splitValuesEndOffsets = IntArraySerializerDeserializer.read(dataInputStream);
        }

        @Override
        public void fail() throws HyracksDataException {
            // it's a sink node pushable, nothing to fail
        }

        @Override
        public void close() throws HyracksDataException {
            // expecting a range map
            if (numFields <= 0 || splitValues == null || splitValuesEndOffsets == null) {
                throw HyracksDataException.create(ErrorCode.NO_RANGEMAP_PRODUCED, sourceLoc);
            }
            // store the range map in the state object of ctx so that next activity (forward) could retrieve it
            TaskId rangeMapReaderTaskId = new TaskId(activityId, partition);
            RangeMapState rangeMapState = new RangeMapState(ctx.getJobletContext().getJobId(), rangeMapReaderTaskId);
            rangeMapState.rangeMap = new RangeMap(numFields, splitValues, splitValuesEndOffsets);
            ctx.setStateObject(rangeMapState);
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
            RangeMapState rangeMapState = (RangeMapState) ctx.getStateObject(stateObjKey);
            TaskUtil.put(sideDataKey, rangeMapState.rangeMap, ctx);
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
