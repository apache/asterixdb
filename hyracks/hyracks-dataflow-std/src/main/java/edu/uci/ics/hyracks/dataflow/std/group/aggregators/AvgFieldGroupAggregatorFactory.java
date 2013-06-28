/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group.aggregators;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;

/**
 *
 */
public class AvgFieldGroupAggregatorFactory implements IFieldAggregateDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private final int aggField;

    private final boolean useObjectState;

    public AvgFieldGroupAggregatorFactory(int aggField, boolean useObjectState) {
        this.aggField = aggField;
        this.useObjectState = useObjectState;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.aggregations.
     * IFieldAggregateDescriptorFactory
     * #createAggregator(edu.uci.ics.hyracks.api.context.IHyracksTaskContext,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor)
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
                    sum = IntegerSerializerDeserializer.getInt(data, offset);
                    count = IntegerSerializerDeserializer.getInt(data, offset + 4);
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
                    sum = IntegerSerializerDeserializer.getInt(data, offset);
                    count = IntegerSerializerDeserializer.getInt(data, offset + 4);
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
            public void init(IFrameTupleAccessor accessor, int tIndex, DataOutput fieldOutput, AggregateState state)
                    throws HyracksDataException {
                int sum = 0;
                int count = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, aggField);
                sum += IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart);
                count += 1;
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
                sum += IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
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

        };
    }

}
