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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;

/**
 *
 */
public class MinMaxStringFieldAggregatorFactory implements IFieldAggregateDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private final int aggField;

    private final boolean isMax;

    private final boolean hasBinaryState;

    public MinMaxStringFieldAggregatorFactory(int aggField, boolean isMax, boolean hasBinaryState) {
        this.aggField = aggField;
        this.isMax = isMax;
        this.hasBinaryState = hasBinaryState;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory
     * #createAggregator(edu.uci.ics.hyracks.api.context.IHyracksTaskContext,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int[])
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
                try {
                    if (hasBinaryState) {
                        int stateIdx = IntegerSerializerDeserializer.getInt(data, offset);
                        Object[] storedState = (Object[]) state.state;
                        fieldOutput.writeUTF((String) storedState[stateIdx]);
                    } else {
                        fieldOutput.writeUTF((String) state.state);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(
                            "I/O exception when writing a string to the output writer in MinMaxStringAggregatorFactory.");
                }
            }

            @Override
            public void outputFinalResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                try {
                    if (hasBinaryState) {
                        int stateIdx = IntegerSerializerDeserializer.getInt(data, offset);
                        Object[] storedState = (Object[]) state.state;
                        fieldOutput.writeUTF((String) storedState[stateIdx]);
                    } else {
                        fieldOutput.writeUTF((String) state.state);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(
                            "I/O exception when writing a string to the output writer in MinMaxStringAggregatorFactory.");
                }
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, DataOutput fieldOutput, AggregateState state)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, aggField);
                int fieldLength = accessor.getFieldLength(tIndex, aggField);
                String strField = UTF8StringSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                        new ByteArrayInputStream(accessor.getBuffer().array(), tupleOffset
                                + accessor.getFieldSlotsLength() + fieldStart, fieldLength)));
                if (hasBinaryState) {
                    // Object-binary-state
                    Object[] storedState;
                    if (state.state == null) {
                        storedState = new Object[8];
                        storedState[0] = new Integer(0);
                        state.state = storedState;
                    } else {
                        storedState = (Object[]) state.state;
                    }
                    int stateCount = (Integer) (storedState[0]);
                    if (stateCount + 1 >= storedState.length) {
                        storedState = Arrays.copyOf(storedState, storedState.length * 2);
                        state.state = storedState;
                    }

                    stateCount++;
                    storedState[0] = stateCount;
                    storedState[stateCount] = strField;
                    try {
                        fieldOutput.writeInt(stateCount);
                    } catch (IOException e) {
                        throw new HyracksDataException(e.fillInStackTrace());
                    }
                } else {
                    // Only object-state
                    state.state = strField;
                }
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub

            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset,
                    AggregateState state) throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, aggField);
                int fieldLength = accessor.getFieldLength(tIndex, aggField);
                String strField = UTF8StringSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                        new ByteArrayInputStream(accessor.getBuffer().array(), tupleOffset
                                + accessor.getFieldSlotsLength() + fieldStart, fieldLength)));
                if (hasBinaryState) {
                    int stateIdx = IntegerSerializerDeserializer.getInt(data, offset);

                    Object[] storedState = (Object[]) state.state;

                    if (isMax) {
                        if (strField.length() > ((String) (storedState[stateIdx])).length()) {
                            storedState[stateIdx] = strField;
                        }
                    } else {
                        if (strField.length() < ((String) (storedState[stateIdx])).length()) {
                            storedState[stateIdx] = strField;
                        }
                    }
                } else {
                    if (isMax) {
                        if (strField.length() > ((String) (state.state)).length()) {
                            state.state = strField;
                        }
                    } else {
                        if (strField.length() < ((String) (state.state)).length()) {
                            state.state = strField;
                        }
                    }
                }
            }

            public boolean needsObjectState() {
                return true;
            }

            public boolean needsBinaryState() {
                return hasBinaryState;
            }

            public AggregateState createState() {
                return new AggregateState();
            }

        };
    }
}
