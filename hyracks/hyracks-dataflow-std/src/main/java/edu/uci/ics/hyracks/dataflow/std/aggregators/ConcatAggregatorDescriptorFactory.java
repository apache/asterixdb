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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class ConcatAggregatorDescriptorFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private static final int INIT_ACCUMULATORS_SIZE = 8;
    private final int concatField;
    private int outField = -1;

    /**
     * Initialize the aggregator, with the field to be concatenated.
     * 
     * @param concatField
     */
    public ConcatAggregatorDescriptorFactory(int concatField) {
        this.concatField = concatField;
    }

    /**
     * Initialize the aggregator, with the field index to be concatenated, and
     * also the field where the aggregation result will be outputted.
     * 
     * @param concatField
     * @param outField
     */
    public ConcatAggregatorDescriptorFactory(int concatField, int outField) {
        this.concatField = concatField;
        this.outField = outField;
    }

    /**
     * Create a concatenation aggregator. A byte buffer will be allocated inside of the
     * aggregator to contain the partial aggregation results. A reference will be written
     * onto the output frame for indexing the aggregation result from the buffer.
     */
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksStageletContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int[] keyFields) throws HyracksDataException {

        if (this.outField < 0)
            this.outField = keyFields.length;

        return new IAggregatorDescriptor() {

            byte[][] buf = new byte[INIT_ACCUMULATORS_SIZE][];

            int currentAggregatorIndex = -1;
            int aggregatorCount = 0;

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                // Initialize the aggregation value
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, concatField);
                int fieldLength = accessor.getFieldLength(tIndex, concatField);
                int appendOffset = tupleOffset + accessor.getFieldSlotsLength() + fieldStart;
                // Get the initial value
                currentAggregatorIndex++;
                if (currentAggregatorIndex >= buf.length) {
                    byte[][] newBuf = new byte[buf.length * 2][];
                    for (int i = 0; i < buf.length; i++) {
                        newBuf[i] = buf[i];
                    }
                    this.buf = newBuf;
                }
                buf[currentAggregatorIndex] = new byte[fieldLength];
                System.arraycopy(accessor.getBuffer().array(), appendOffset, buf[currentAggregatorIndex], 0,
                        fieldLength);
                // Update the aggregator index
                aggregatorCount++;

                try {
                    tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, currentAggregatorIndex);
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }

            @Override
            public void reset() {
                currentAggregatorIndex = -1;
                aggregatorCount = 0;
            }

            @Override
            public void close() {
                currentAggregatorIndex = -1;
                aggregatorCount = 0;
                for (int i = 0; i < buf.length; i++) {
                    buf[i] = null;
                }
            }

            @Override
            public int aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset, int length)
                    throws HyracksDataException {
                int refIndex = IntegerSerializerDeserializer.getInt(data, offset);
                // FIXME Should be done in binary way
                StringBuilder sbder = new StringBuilder();
                sbder.append(UTF8StringSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                        new ByteArrayInputStream(buf[refIndex]))));
                // Get the new data
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, concatField);
                int fieldLength = accessor.getFieldLength(tIndex, concatField);
                sbder.append(UTF8StringSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                        new ByteArrayInputStream(accessor.getBuffer().array(), tupleOffset
                                + accessor.getFieldSlotsLength() + fieldStart, fieldLength))));

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                UTF8StringSerializerDeserializer.INSTANCE.serialize(sbder.toString(), new DataOutputStream(baos));
                buf[refIndex] = baos.toByteArray();
                return 4;
            }

            @Override
            public void outputResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, outField);
                int refIndex = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset
                        + accessor.getFieldSlotsLength() + fieldStart);

                try {
                    if (refIndex >= 0)
                        tupleBuilder.getDataOutput().write(buf[refIndex]);
                    else {
                        int fieldLength = accessor.getFieldLength(tIndex, outField);
                        tupleBuilder.getDataOutput().write(accessor.getBuffer().array(),
                                tupleOffset + accessor.getFieldSlotsLength() + fieldStart + 4, fieldLength - 4);
                    }
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }

            @Override
            public void outputPartialResult(IFrameTupleAccessor accessor, int tIndex, ArrayTupleBuilder tupleBuilder)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldOffset = accessor.getFieldStartOffset(tIndex, outField);
                int fieldLength = accessor.getFieldLength(tIndex, outField);
                int refIndex = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(), tupleOffset
                        + accessor.getFieldSlotsLength() + fieldOffset);

                try {
                    tupleBuilder.getDataOutput().writeInt(-1);
                    if (refIndex < 0) {
                        tupleBuilder.getDataOutput().write(accessor.getBuffer().array(),
                                tupleOffset + accessor.getFieldSlotsLength() + fieldOffset + 4, fieldLength - 4);
                    } else {
                        tupleBuilder.getDataOutput().write(buf[refIndex], 0, buf[refIndex].length);
                    }
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException();
                }
            }
        };
    }
}
