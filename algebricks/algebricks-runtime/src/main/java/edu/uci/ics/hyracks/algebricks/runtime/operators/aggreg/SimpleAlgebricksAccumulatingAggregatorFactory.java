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
package edu.uci.ics.hyracks.algebricks.runtime.operators.aggreg;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.group.AbstractAccumulatingAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class SimpleAlgebricksAccumulatingAggregatorFactory extends AbstractAccumulatingAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private IAggregateEvaluatorFactory[] aggFactories;

    public SimpleAlgebricksAccumulatingAggregatorFactory(IAggregateEvaluatorFactory[] aggFactories, int[] keys) {
        this.aggFactories = aggFactories;
    }

    @Override
    public IAggregatorDescriptor createAggregator(final IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor, int[] aggKeys, int[] partialKeys) throws HyracksDataException {

        return new IAggregatorDescriptor() {

            private FrameTupleReference ftr = new FrameTupleReference();
            private IPointable p = VoidPointable.FACTORY.createPointable();

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                IAggregateEvaluator[] agg = (IAggregateEvaluator[]) state.state;

                // initialize aggregate functions
                for (int i = 0; i < agg.length; i++) {
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
                IAggregateEvaluator[] agg = (IAggregateEvaluator[]) state.state;
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
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                IAggregateEvaluator[] agg = (IAggregateEvaluator[]) state.state;
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i].finish(p);
                        tupleBuilder.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
                    } catch (AlgebricksException e) {
                        throw new HyracksDataException(e);
                    }
                }
                return true;
            }

            @Override
            public AggregateState createAggregateStates() {
                IAggregateEvaluator[] agg = new IAggregateEvaluator[aggFactories.length];
                for (int i = 0; i < agg.length; i++) {
                    try {
                        agg[i] = aggFactories[i].createAggregateEvaluator(ctx);
                    } catch (AlgebricksException e) {
                        throw new IllegalStateException(e);
                    }
                }
                return new AggregateState(agg);
            }

            @Override
            public void reset() {

            }

            @Override
            public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                throw new IllegalStateException("this method should not be called");
            }

            @Override
            public void close() {

            }

        };
    }
}