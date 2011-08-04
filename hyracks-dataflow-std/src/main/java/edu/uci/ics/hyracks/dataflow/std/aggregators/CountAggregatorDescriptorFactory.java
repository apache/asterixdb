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
public class CountAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private int outField = -1;

    public CountAggregatorDescriptorFactory() {
    }

    public CountAggregatorDescriptorFactory(int outField) {
        this.outField = outField;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory#createAggregator(edu.uci.ics.hyracks.api.context.IHyracksStageletContext, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int[])
     */
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksStageletContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int[] keyFields) throws HyracksDataException {

        if (this.outField < 0) {
            this.outField = keyFields.length;
        }
        return new IAggregatorDescriptor() {

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, 1);
            }

            @Override
            public void close() {
            }

            @Override
            public int aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length)
                    throws HyracksDataException {
                ByteBuffer buf = ByteBuffer.wrap(data);
                int count = buf.getInt(offset);
                buf.putInt(offset, count + 1);
                return 4;
            }

            @Override
            public void outputPartialResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);

                try {
                    tupleBuilder.getDataOutput().write(accessor.getBuffer().array(),
                            tupleOffset + accessor.getFieldSlotsLength() + fieldStart, 4);
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException("Failed to write int sum as a partial result.");
                }
            }

            @Override
            public void outputResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);

                try {
                    tupleBuilder.getDataOutput().write(accessor.getBuffer().array(),
                            tupleOffset + accessor.getFieldSlotsLength() + fieldStart, 4);
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException("Failed to write int sum as a partial result.");
                }
            }

            @Override
            public void reset() {
                // TODO Auto-generated method stub

            }
        };
    }

}
