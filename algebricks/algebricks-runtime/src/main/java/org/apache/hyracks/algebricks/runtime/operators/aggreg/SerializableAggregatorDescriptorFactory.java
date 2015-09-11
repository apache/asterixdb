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
package org.apache.hyracks.algebricks.runtime.operators.aggreg;

import java.io.DataOutput;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunction;
import org.apache.hyracks.algebricks.runtime.base.ICopySerializableAggregateFunctionFactory;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.group.AbstractAccumulatingAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class SerializableAggregatorDescriptorFactory extends AbstractAccumulatingAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private ICopySerializableAggregateFunctionFactory[] aggFactories;

    public SerializableAggregatorDescriptorFactory(ICopySerializableAggregateFunctionFactory[] aggFactories) {
        this.aggFactories = aggFactories;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, final int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        final int[] keys = keyFields;

        /**
         * one IAggregatorDescriptor instance per Gby operator
         */
        return new IAggregatorDescriptor() {
            private FrameTupleReference ftr = new FrameTupleReference();
            private ICopySerializableAggregateFunction[] aggs = new ICopySerializableAggregateFunction[aggFactories.length];
            private int offsetFieldIndex = keys.length;
            private int stateFieldLength[] = new int[aggFactories.length];

            @Override
            public AggregateState createAggregateStates() {
                return new AggregateState();
            }

            @Override
            public void init(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex, AggregateState state)
                    throws HyracksDataException {
                DataOutput output = tb.getDataOutput();
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        int begin = tb.getSize();
                        if (aggs[i] == null) {
                            aggs[i] = aggFactories[i].createAggregateFunction();
                        }
                        aggs[i].init(output);
                        tb.addFieldEndOffset();
                        stateFieldLength[i] = tb.getSize() - begin;
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }

                // doing initial aggregate
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        byte[] data = tb.getByteArray();
                        int prevFieldPos = i + keys.length - 1;
                        int start = prevFieldPos >= 0 ? tb.getFieldEndOffsets()[prevFieldPos] : 0;
                        aggs[i].step(ftr, data, start, stateFieldLength[i]);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                ftr.reset(accessor, tIndex);
                int stateTupleStart = stateAccessor.getTupleStartOffset(stateTupleIndex);
                int fieldSlotLength = stateAccessor.getFieldSlotsLength();
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        byte[] data = stateAccessor.getBuffer().array();
                        int start = stateAccessor.getFieldStartOffset(stateTupleIndex, i + keys.length)
                                + stateTupleStart + fieldSlotLength;
                        aggs[i].step(ftr, data, start, stateFieldLength[i]);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public boolean outputPartialResult(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int start = refOffset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].finishPartial(data, start, stateFieldLength[i], tb.getDataOutput());
                        start += stateFieldLength[i];
                        tb.addFieldEndOffset();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                return true;
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tb, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                byte[] data = accessor.getBuffer().array();
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int aggFieldOffset = accessor.getFieldStartOffset(tIndex, offsetFieldIndex);
                int refOffset = startOffset + accessor.getFieldSlotsLength() + aggFieldOffset;
                int start = refOffset;
                for (int i = 0; i < aggs.length; i++) {
                    try {
                        aggs[i].finish(data, start, stateFieldLength[i], tb.getDataOutput());
                        start += stateFieldLength[i];
                        tb.addFieldEndOffset();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                return true;
            }

            @Override
            public void reset() {

            }

            @Override
            public void close() {
                reset();
            }

        };
    }
}
