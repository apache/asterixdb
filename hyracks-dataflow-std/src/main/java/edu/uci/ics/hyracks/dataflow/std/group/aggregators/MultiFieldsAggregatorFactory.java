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
package edu.uci.ics.hyracks.dataflow.std.group.aggregators;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregateStateFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;

/**
 *
 */
public class MultiFieldsAggregatorFactory implements
        IAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private final IFieldAggregateDescriptorFactory[] aggregatorFactories;

    public MultiFieldsAggregatorFactory(
            IFieldAggregateDescriptorFactory[] aggregatorFactories) {
        this.aggregatorFactories = aggregatorFactories;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.aggregations.IAggregatorDescriptorFactory
     * #createAggregator(edu.uci.ics.hyracks.api.context.IHyracksTaskContext,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor,
     * edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor)
     */
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx,
            RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int[] keyFields, final int[] keyFieldsInPartialResults)
            throws HyracksDataException {

        final IFieldAggregateDescriptor[] aggregators = new IFieldAggregateDescriptor[aggregatorFactories.length];
        final IAggregateStateFactory[] aggregateStateFactories = new IAggregateStateFactory[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = aggregatorFactories[i].createAggregator(ctx,
                    inRecordDescriptor, outRecordDescriptor);
            aggregateStateFactories[i] = aggregators[i]
                    .getAggregateStateFactory();
        }

        int stateTupleFieldCount = keyFields.length;
        for (int i = 0; i < aggregateStateFactories.length; i++) {
            if (aggregateStateFactories[i].hasBinaryState()) {
                stateTupleFieldCount++;
            }
        }

        final ArrayTupleBuilder stateTupleBuilder = new ArrayTupleBuilder(
                stateTupleFieldCount);

        final ArrayTupleBuilder resultTupleBuilder = new ArrayTupleBuilder(
                outRecordDescriptor.getFields().length);

        return new IAggregatorDescriptor() {

            private boolean initPending, outputPending;

            @Override
            public void reset() {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].reset();
                    aggregateStateFactories[i] = aggregators[i]
                            .getAggregateStateFactory();
                }
                initPending = false;
                outputPending = false;
            }

            @Override
            public boolean outputPartialResult(FrameTupleAppender appender,
                    IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                if (!outputPending) {
                    resultTupleBuilder.reset();
                    for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
                        resultTupleBuilder.addField(accessor, tIndex,
                        		keyFieldsInPartialResults[i]);
                    }
                    DataOutput dos = resultTupleBuilder.getDataOutput();

                    int tupleOffset = accessor.getTupleStartOffset(tIndex);
                    for (int i = 0; i < aggregators.length; i++) {
                        int fieldOffset = accessor.getFieldStartOffset(tIndex,
                                keyFields.length + i);
                        aggregators[i].outputPartialResult(dos, accessor
                                .getBuffer().array(),
                                fieldOffset + accessor.getFieldSlotsLength()
                                        + tupleOffset,
                                ((AggregateState[]) state.getState())[i]);
                        resultTupleBuilder.addFieldEndOffset();
                    }
                }
                if (!appender.append(resultTupleBuilder.getFieldEndOffsets(),
                        resultTupleBuilder.getByteArray(), 0,
                        resultTupleBuilder.getSize())) {
                    outputPending = true;
                    return false;
                }
                outputPending = false;
                return true;

            }

            @Override
            public boolean outputFinalResult(FrameTupleAppender appender,
                    IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                if (!outputPending) {
                    resultTupleBuilder.reset();
                    
                    for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
                        resultTupleBuilder.addField(accessor, tIndex,
                        		keyFieldsInPartialResults[i]);
                    }
 
                    DataOutput dos = resultTupleBuilder.getDataOutput();

                    int tupleOffset = accessor.getTupleStartOffset(tIndex);
                    for (int i = 0; i < aggregators.length; i++) {
                        if (aggregateStateFactories[i].hasBinaryState()) {
                            int fieldOffset = accessor.getFieldStartOffset(
                                    tIndex, keyFields.length + i);
                            aggregators[i].outputFinalResult(dos, accessor
                                    .getBuffer().array(), tupleOffset
                                    + accessor.getFieldSlotsLength()
                                    + fieldOffset, ((AggregateState[]) state
                                    .getState())[i]);
                        } else {
                            aggregators[i].outputFinalResult(dos, null, 0,
                                    ((AggregateState[]) state.getState())[i]);
                        }
                        resultTupleBuilder.addFieldEndOffset();
                    }
                }
                if (!appender.append(resultTupleBuilder.getFieldEndOffsets(),
                        resultTupleBuilder.getByteArray(), 0,
                        resultTupleBuilder.getSize())) {
                    outputPending = true;
                    return false;
                }
                outputPending = false;
                return true;
            }

            @Override
            public boolean init(FrameTupleAppender appender,
                    IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                if (!initPending) {
                    stateTupleBuilder.reset();
                    for (int i = 0; i < keyFields.length; i++) {
                        stateTupleBuilder.addField(accessor, tIndex,
                                keyFields[i]);
                    }
                    DataOutput dos = stateTupleBuilder.getDataOutput();

                    for (int i = 0; i < aggregators.length; i++) {
                        aggregators[i].init(accessor, tIndex, dos,
                                ((AggregateState[]) state.getState())[i]);
                        if (aggregateStateFactories[i].hasBinaryState()) {
                            stateTupleBuilder.addFieldEndOffset();
                        }
                    }
                }
                // For pre-cluster: no output state is needed
                if(appender == null){
                    initPending = false;
                    return true;
                }
                if (!appender.append(stateTupleBuilder.getFieldEndOffsets(),
                        stateTupleBuilder.getByteArray(), 0,
                        stateTupleBuilder.getSize())) {
                    initPending = true;
                    return false;
                }
                initPending = false;
                return true;
            }

            @Override
            public AggregateState createAggregateStates() {
                AggregateState aggregateStates = new AggregateState();
                AggregateState[] states = new AggregateState[aggregateStateFactories.length];
                for (int i = 0; i < states.length; i++) {
                    states[i] = new AggregateState();
                    states[i]
                            .setState(aggregateStateFactories[i].createState());
                }
                aggregateStates.setState(states);
                return aggregateStates;
            }

            @Override
            public int getAggregateStatesLength() {
                int stateLength = 0;
                for (int i = 0; i < aggregateStateFactories.length; i++) {
                    stateLength += aggregateStateFactories[i].getStateLength();
                }
                return stateLength;
            }

            @Override
            public void close() {
                for(int i = 0; i < aggregators.length; i++){
                    aggregators[i].close();
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex,
                    IFrameTupleAccessor stateAccessor, int stateTupleIndex,
                    AggregateState state) throws HyracksDataException {
                if (stateAccessor != null) {
                    int stateTupleOffset = stateAccessor
                            .getTupleStartOffset(stateTupleIndex);
                    int fieldIndex = 0;
                    for (int i = 0; i < aggregators.length; i++) {
                        if (aggregateStateFactories[i].hasBinaryState()) {
                            int stateFieldOffset = stateAccessor
                                    .getFieldStartOffset(stateTupleIndex,
                                            keyFields.length + fieldIndex);
                            aggregators[i].aggregate(
                                    accessor,
                                    tIndex,
                                    stateAccessor.getBuffer().array(),
                                    stateTupleOffset
                                            + stateAccessor
                                                    .getFieldSlotsLength()
                                            + stateFieldOffset,
                                    ((AggregateState[]) state.getState())[i]);
                            fieldIndex++;
                        } else {
                            aggregators[i].aggregate(accessor, tIndex, null, 0,
                                    ((AggregateState[]) state.getState())[i]);
                        }
                    }
                } else {
                    for (int i = 0; i < aggregators.length; i++) {
                        aggregators[i].aggregate(accessor, tIndex, null, 0,
                                ((AggregateState[]) state.getState())[i]);
                    }
                }
            }
        };
    }
}
