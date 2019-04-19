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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.std.group.AbstractAccumulatingAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptor;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;

public class MultiFieldsAggregatorFactory extends AbstractAccumulatingAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private final IFieldAggregateDescriptorFactory[] aggregatorFactories;
    private int[] keys;

    public MultiFieldsAggregatorFactory(int[] keys, IFieldAggregateDescriptorFactory[] aggregatorFactories) {
        this.keys = keys;
        this.aggregatorFactories = aggregatorFactories;
    }

    public MultiFieldsAggregatorFactory(IFieldAggregateDescriptorFactory[] aggregatorFactories) {
        this.aggregatorFactories = aggregatorFactories;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hyracks.dataflow.std.aggregations.IAggregatorDescriptorFactory
     * #createAggregator(org.apache.hyracks.api.context.IHyracksTaskContext,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor,
     * org.apache.hyracks.api.dataflow.value.RecordDescriptor)
     */
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int[] keyFields, final int[] keyFieldsInPartialResults,
            long memoryBudget) throws HyracksDataException {

        final IFieldAggregateDescriptor[] aggregators = new IFieldAggregateDescriptor[aggregatorFactories.length];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = aggregatorFactories[i].createAggregator(ctx, inRecordDescriptor, outRecordDescriptor);
        }

        if (this.keys == null) {
            this.keys = keyFields;
        }

        return new IAggregatorDescriptor() {

            @Override
            public void reset() {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].reset();
                }
            }

            @Override
            public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor,
                    int tIndex, AggregateState state) throws HyracksDataException {
                DataOutput dos = tupleBuilder.getDataOutput();
                for (int i = 0; i < aggregators.length; i++) {
                    if (aggregators[i].needsBinaryState()) {
                        int tupleOffset = stateAccessor.getTupleStartOffset(tIndex);
                        int fieldOffset = stateAccessor.getFieldStartOffset(tIndex, keys.length + i);
                        aggregators[i].outputPartialResult(dos, stateAccessor.getBuffer().array(),
                                fieldOffset + stateAccessor.getFieldSlotsLength() + tupleOffset,
                                ((AggregateState[]) state.state)[i]);
                    } else {
                        aggregators[i].outputPartialResult(dos, null, 0, ((AggregateState[]) state.state)[i]);
                    }
                    tupleBuilder.addFieldEndOffset();
                }
                return true;
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor,
                    int tIndex, AggregateState state) throws HyracksDataException {
                DataOutput dos = tupleBuilder.getDataOutput();
                for (int i = 0; i < aggregators.length; i++) {
                    if (aggregators[i].needsBinaryState()) {
                        int tupleOffset = stateAccessor.getTupleStartOffset(tIndex);
                        int fieldOffset = stateAccessor.getFieldStartOffset(tIndex, keys.length + i);
                        aggregators[i].outputFinalResult(dos, stateAccessor.getBuffer().array(),
                                tupleOffset + stateAccessor.getFieldSlotsLength() + fieldOffset,
                                ((AggregateState[]) state.state)[i]);
                    } else {
                        aggregators[i].outputFinalResult(dos, null, 0, ((AggregateState[]) state.state)[i]);
                    }
                    tupleBuilder.addFieldEndOffset();
                }
                return true;
            }

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                DataOutput dos = tupleBuilder.getDataOutput();
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].init(accessor, tIndex, dos, ((AggregateState[]) state.state)[i]);
                    if (aggregators[i].needsBinaryState()) {
                        tupleBuilder.addFieldEndOffset();
                    }
                }
            }

            @Override
            public AggregateState createAggregateStates() {
                AggregateState[] states = new AggregateState[aggregators.length];
                for (int i = 0; i < states.length; i++) {
                    states[i] = aggregators[i].createState();
                }
                return new AggregateState(states);
            }

            @Override
            public void close() {
                for (int i = 0; i < aggregators.length; i++) {
                    aggregators[i].close();
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                int fieldIndex = 0;
                for (int i = 0; i < aggregators.length; i++) {
                    if (aggregators[i].needsBinaryState()) {
                        int stateTupleOffset = stateAccessor.getTupleStartOffset(stateTupleIndex);
                        int stateFieldOffset =
                                stateAccessor.getFieldStartOffset(stateTupleIndex, keys.length + fieldIndex);
                        aggregators[i].aggregate(accessor, tIndex, stateAccessor.getBuffer().array(),
                                stateTupleOffset + stateAccessor.getFieldSlotsLength() + stateFieldOffset,
                                ((AggregateState[]) state.state)[i]);
                        fieldIndex++;
                    } else {
                        aggregators[i].aggregate(accessor, tIndex, null, 0, ((AggregateState[]) state.state)[i]);
                    }
                }
            }
        };
    }
}
