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

import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.group.AbstractAccumulatingAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class SimpleAlgebricksAccumulatingAggregatorFactory extends AbstractAccumulatingAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private IAggregateEvaluatorFactory[] aggFactories;

    public SimpleAlgebricksAccumulatingAggregatorFactory(IAggregateEvaluatorFactory[] aggFactories, int[] keys) {
        this.aggFactories = aggFactories;
    }

    @Override
    public IAggregatorDescriptor createAggregator(final IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor, int[] aggKeys, int[] partialKeys, long memoryBudget)
            throws HyracksDataException {
        IEvaluatorContext evalCtx = new EvaluatorContext(ctx);

        return new IAggregatorDescriptor() {

            private FrameTupleReference ftr = new FrameTupleReference();
            private IPointable p = VoidPointable.FACTORY.createPointable();

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                IAggregateEvaluator[] agg = (IAggregateEvaluator[]) state.state;

                // initialize aggregate functions
                for (int i = 0; i < agg.length; i++) {
                    agg[i].init();
                }

                ftr.reset(accessor, tIndex);
                for (int i = 0; i < agg.length; i++) {
                    agg[i].step(ftr);
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                IAggregateEvaluator[] agg = (IAggregateEvaluator[]) state.state;
                ftr.reset(accessor, tIndex);
                for (int i = 0; i < agg.length; i++) {
                    agg[i].step(ftr);
                }
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor,
                    int tIndex, AggregateState state) throws HyracksDataException {
                IAggregateEvaluator[] agg = (IAggregateEvaluator[]) state.state;
                for (int i = 0; i < agg.length; i++) {
                    agg[i].finish(p);
                    tupleBuilder.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
                }
                return true;
            }

            @Override
            public AggregateState createAggregateStates() throws HyracksDataException {
                IAggregateEvaluator[] agg = new IAggregateEvaluator[aggFactories.length];
                for (int i = 0; i < agg.length; i++) {
                    agg[i] = aggFactories[i].createAggregateEvaluator(evalCtx);
                }
                return new AggregateState(agg);
            }

            @Override
            public void reset() {

            }

            @Override
            public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor,
                    int tIndex, AggregateState state) throws HyracksDataException {
                IAggregateEvaluator[] agg = (IAggregateEvaluator[]) state.state;
                for (int i = 0; i < agg.length; i++) {
                    agg[i].finishPartial(p);
                    tupleBuilder.addField(p.getByteArray(), p.getStartOffset(), p.getLength());
                }
                return true;
            }

            @Override
            public void close() {

            }

        };
    }
}
