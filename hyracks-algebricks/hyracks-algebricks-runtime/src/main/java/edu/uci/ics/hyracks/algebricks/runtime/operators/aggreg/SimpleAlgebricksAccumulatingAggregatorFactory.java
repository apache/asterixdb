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
package edu.uci.ics.hyracks.algebricks.runtime.operators.aggreg;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class SimpleAlgebricksAccumulatingAggregatorFactory implements IAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private ICopyAggregateFunctionFactory[] aggFactories;

    public SimpleAlgebricksAccumulatingAggregatorFactory(ICopyAggregateFunctionFactory[] aggFactories, int[] keys,
            int[] fdColumns) {
        this.aggFactories = aggFactories;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor, int[] aggKeys, int[] partialKeys) throws HyracksDataException {

        return new IAggregatorDescriptor() {

            private FrameTupleReference ftr = new FrameTupleReference();

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                Pair<ArrayBackedValueStorage[], ICopyAggregateFunction[]> aggState = (Pair<ArrayBackedValueStorage[], ICopyAggregateFunction[]>) state.state;
                ArrayBackedValueStorage[] aggOutput = aggState.first;
                ICopyAggregateFunction[] agg = aggState.second;

                // initialize aggregate functions
                for (int i = 0; i < agg.length; i++) {
                    aggOutput[i].reset();
                    try {
                        agg[i].init();
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }

                ftr.reset(accessor, tIndex);
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i].step(ftr);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                Pair<ArrayBackedValueStorage[], ICopyAggregateFunction[]> aggState = (Pair<ArrayBackedValueStorage[], ICopyAggregateFunction[]>) state.state;
                ICopyAggregateFunction[] agg = aggState.second;
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i].step(ftr);
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public void outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                Pair<ArrayBackedValueStorage[], ICopyAggregateFunction[]> aggState = (Pair<ArrayBackedValueStorage[], ICopyAggregateFunction[]>) state.state;
                ArrayBackedValueStorage[] aggOutput = aggState.first;
                ICopyAggregateFunction[] agg = aggState.second;
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i].finish();
                        tupleBuilder.addField(aggOutput[i].getByteArray(), aggOutput[i].getStartOffset(),
                                aggOutput[i].getLength());
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
            }

            @Override
            public AggregateState createAggregateStates() {
                ICopyAggregateFunction[] agg = new ICopyAggregateFunction[aggFactories.length];
                ArrayBackedValueStorage[] aggOutput = new ArrayBackedValueStorage[aggFactories.length];
                for (int i = 0; i < agg.length; i++) {
                    aggOutput[i] = new ArrayBackedValueStorage();
                    try {
                        agg[i] = aggFactories[i].createAggregateFunction(aggOutput[i]);
                    } catch (AlgebricksException e) {
                        throw new IllegalStateException(e);
                    }
                }
                return new AggregateState(new Pair<ArrayBackedValueStorage[], ICopyAggregateFunction[]>(aggOutput, agg));
            }

            @Override
            public void reset() {

            }

            @Override
            public void outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                throw new IllegalStateException("this method should not be called");
            }

            @Override
            public void close() {

            }

        };
    }
}