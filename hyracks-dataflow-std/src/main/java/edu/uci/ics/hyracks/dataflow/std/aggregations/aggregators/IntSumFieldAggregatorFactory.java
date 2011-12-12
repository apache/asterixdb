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
package edu.uci.ics.hyracks.dataflow.std.aggregations.aggregators;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.aggregations.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.aggregations.IAggregateStateFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregations.IFieldAggregateDescriptor;
import edu.uci.ics.hyracks.dataflow.std.aggregations.IFieldAggregateDescriptorFactory;

/**
 *
 */
public class IntSumFieldAggregatorFactory implements
        IFieldAggregateDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private final int aggField;
    
    private final boolean useObjectState;

    public IntSumFieldAggregatorFactory(int aggField, boolean useObjState) {
        this.aggField = aggField;
        this.useObjectState = useObjState;
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
    public IFieldAggregateDescriptor createAggregator(IHyracksTaskContext ctx,
            RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {

        return new IFieldAggregateDescriptor() {

            @Override
            public void reset() {
            }

            @Override
            public void outputPartialResult(DataOutput fieldOutput,
                    byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                int sum;
                if (!useObjectState) {
                    sum = IntegerSerializerDeserializer.getInt(data, offset);
                } else {
                    sum = (Integer) state.getState();
                }
                try {
                    fieldOutput.writeInt(sum);
                } catch (IOException e) {
                    throw new HyracksDataException(
                            "I/O exception when writing aggregation to the output buffer.");
                }
            }

            @Override
            public void outputFinalResult(DataOutput fieldOutput, byte[] data,
                    int offset, AggregateState state)
                    throws HyracksDataException {
                int sum;
                if (!useObjectState) {
                    sum = IntegerSerializerDeserializer.getInt(data, offset);
                } else {
                    sum = (Integer) state.getState();
                }
                try {
                    fieldOutput.writeInt(sum);
                } catch (IOException e) {
                    throw new HyracksDataException(
                            "I/O exception when writing aggregation to the output buffer.");
                }
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex,
                    DataOutput fieldOutput, AggregateState state)
                    throws HyracksDataException {

                int sum = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, aggField);
                
                sum += IntegerSerializerDeserializer.getInt(accessor
                        .getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength()
                                + fieldStart);
                
                if (!useObjectState) {
                    try {
                        fieldOutput.writeInt(sum);
                    } catch (IOException e) {
                        throw new HyracksDataException(
                                "I/O exception when initializing the aggregator.");
                    }
                } else {
                    state.setState(sum);
                }
            }

            @Override
            public IAggregateStateFactory getAggregateStateFactory(){
                return new IAggregateStateFactory() {
                    
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean hasObjectState() {
                        return useObjectState;
                    }
                    
                    @Override
                    public boolean hasBinaryState() {
                        return !useObjectState;
                    }
                    
                    @Override
                    public int getStateLength() {
                        return 4;
                    }
                    
                    @Override
                    public Object createState() {
                        return new Integer(0);
                    }
                };
            }

            @Override
            public void close() {

            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex,
                    byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                int sum = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, aggField);
                sum += IntegerSerializerDeserializer.getInt(accessor
                        .getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength()
                                + fieldStart);
                
                if (!useObjectState) {
                    ByteBuffer buf = ByteBuffer.wrap(data);
                    sum += buf.getInt(offset);
                    buf.putInt(offset, sum);
                } else {
                    sum += (Integer) state.getState();
                    state.setState(sum);
                }
            }

            @Override
            public void initFromPartial(IFrameTupleAccessor accessor,
                    int tIndex, DataOutput fieldOutput, AggregateState state)
                    throws HyracksDataException {
                init(accessor, tIndex, fieldOutput, state);
            }
        };
    }

}
