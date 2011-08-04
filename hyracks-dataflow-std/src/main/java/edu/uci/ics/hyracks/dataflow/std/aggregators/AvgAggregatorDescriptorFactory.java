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
package edu.uci.ics.hyracks.dataflow.std.aggregators;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

/**
 * @author jarodwen
 */
public class AvgAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final int avgField;
    private int outField = -1;

    public AvgAggregatorDescriptorFactory(int avgField) {
        this.avgField = avgField;
    }

    public AvgAggregatorDescriptorFactory(int avgField, int outField) {
        this.avgField = avgField;
        this.outField = outField;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory#createAggregator(edu.uci.ics.hyracks.api.context.IHyracksStageletContext, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int[])
     */
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksStageletContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields) throws HyracksDataException {

        if (this.outField < 0)
            this.outField = keyFields.length;

        return new IAggregatorDescriptor() {

            @Override
            public void reset() {

            }

            @Override
            public void outputResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                int sum = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart);
                int count = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart + 4);

                try {
                    tupleBuilder.getDataOutput().writeInt(sum / count);
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }

            @Override
            public void outputPartialResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                int sum = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart);
                int count = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart + 4);
                try {
                    tupleBuilder.getDataOutput().writeInt(sum);
                    tupleBuilder.getDataOutput().writeInt(count);
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                // Init aggregation value
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, avgField);
                int sum = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart);
                int count = 1;

                try {
                    tupleBuilder.getDataOutput().writeInt(sum);
                    tupleBuilder.getDataOutput().writeInt(count);
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub

            }

            @Override
            public int aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                int sum1 = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart);
                int count1 = 1;

                int sum2 = IntegerSerializerDeserializer.getInt(data, offset);
                int count2 = IntegerSerializerDeserializer.getInt(data, offset + 4);

                ByteBuffer buf = ByteBuffer.wrap(data, offset, 8);
                buf.putInt(sum1 + sum2);
                buf.putInt(count1 + count2);

                return 8;
            }
        };
    }

}
