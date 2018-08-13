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
package org.apache.hyracks.dataflow.std.group.aggregators;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptor;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;

/**
 *
 */
public class AvgFieldMergeAggregatorFactory implements IFieldAggregateDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private final int aggField;

    private final boolean useObjectState;

    public AvgFieldMergeAggregatorFactory(int aggField, boolean useObjectState) {
        this.aggField = aggField;
        this.useObjectState = useObjectState;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.hyracks.dataflow.std.aggregations.
     * IFieldAggregateDescriptorFactory
     * #createAggregator(org.apache.hyracks.api.context.IHyracksTaskContext,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor)
     */
    @Override
    public IFieldAggregateDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {

        return new IFieldAggregateDescriptor() {

            @Override
            public void reset() {
            }

            @Override
            public void outputPartialResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                int sum, count;
                if (!useObjectState) {
                    sum = IntegerPointable.getInteger(data, offset);
                    count = IntegerPointable.getInteger(data, offset + 4);
                } else {
                    Integer[] fields = (Integer[]) state.state;
                    sum = fields[0];
                    count = fields[1];
                }
                try {
                    fieldOutput.writeInt(sum);
                    fieldOutput.writeInt(count);
                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }

            @Override
            public void outputFinalResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                int sum, count;
                if (!useObjectState) {
                    sum = IntegerPointable.getInteger(data, offset);
                    count = IntegerPointable.getInteger(data, offset + 4);
                } else {
                    Integer[] fields = (Integer[]) state.state;
                    sum = fields[0];
                    count = fields[1];
                }
                try {
                    fieldOutput.writeFloat((float) sum / count);
                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub

            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset,
                    AggregateState state) throws HyracksDataException {
                int sum = 0, count = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, aggField);
                sum += IntegerPointable.getInteger(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart);
                count += 1;
                if (!useObjectState) {
                    ByteBuffer buf = ByteBuffer.wrap(data);
                    sum += buf.getInt(offset);
                    count += buf.getInt(offset + 4);
                    buf.putInt(offset, sum);
                    buf.putInt(offset + 4, count);
                } else {
                    Integer[] fields = (Integer[]) state.state;
                    sum += fields[0];
                    count += fields[1];
                    state.state = new Integer[] { sum, count };
                }
            }

            @Override
            public boolean needsObjectState() {
                return useObjectState;
            }

            @Override
            public boolean needsBinaryState() {
                return !useObjectState;
            }

            @Override
            public AggregateState createState() {
                return new AggregateState(new Integer[] { 0, 0 });
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, DataOutput fieldOutput, AggregateState state)
                    throws HyracksDataException {
                int sum = 0;
                int count = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, aggField);
                sum += IntegerPointable.getInteger(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart);
                count += IntegerPointable.getInteger(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart + 4);
                if (!useObjectState) {
                    try {
                        fieldOutput.writeInt(sum);
                        fieldOutput.writeInt(count);
                    } catch (IOException e) {
                        throw new HyracksDataException("I/O exception when initializing the aggregator.");
                    }
                } else {
                    state.state = new Integer[] { sum, count };
                }
            }
        };
    }

}
