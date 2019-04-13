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

package org.apache.hyracks.algebricks.runtime.operators.win;

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.AggregatePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.NestedPlansAccumulatingAggregatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.PipelineAssembler;
import org.apache.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory.NestedTupleSourceRuntime;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.group.AbstractAccumulatingAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.AggregateState;

/**
 * Aggregator factory for window operators
 */
public final class WindowAggregatorDescriptorFactory extends AbstractAccumulatingAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private final AlgebricksPipeline[] subplans;

    private boolean partialOutputEnabled;

    public WindowAggregatorDescriptorFactory(AlgebricksPipeline[] subplans) {
        this.subplans = subplans;
    }

    public void setPartialOutputEnabled(boolean value) {
        partialOutputEnabled = value;
    }

    @Override
    public IWindowAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor, int[] keys, int[] partialKeys, long memoryBudget)
            throws HyracksDataException {
        NestedPlansAccumulatingAggregatorFactory.AggregatorOutput outputWriter =
                new NestedPlansAccumulatingAggregatorFactory.AggregatorOutput(subplans, 0);
        NestedTupleSourceRuntime[] pipelines = new NestedTupleSourceRuntime[subplans.length];

        Map<IPushRuntimeFactory, IPushRuntime> pipelineRuntimeMap = partialOutputEnabled ? new HashMap<>() : null;
        AggregatePushRuntime[] aggs = partialOutputEnabled ? new AggregatePushRuntime[subplans.length] : null;

        for (int i = 0; i < subplans.length; i++) {
            AlgebricksPipeline subplan = subplans[i];
            if (pipelineRuntimeMap != null) {
                pipelineRuntimeMap.clear();
            }
            pipelines[i] = (NestedTupleSourceRuntime) PipelineAssembler.assemblePipeline(subplan, outputWriter, ctx,
                    pipelineRuntimeMap);
            if (pipelineRuntimeMap != null) {
                IPushRuntimeFactory[] subplanFactories = subplan.getRuntimeFactories();
                IPushRuntimeFactory aggFactory = subplanFactories[subplanFactories.length - 1];
                AggregatePushRuntime agg = (AggregatePushRuntime) pipelineRuntimeMap.get(aggFactory);
                if (agg == null) {
                    throw new IllegalStateException();
                }
                aggs[i] = agg;
            }
        }

        return new IWindowAggregatorDescriptor() {

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                outputWriter.getTupleBuilder().reset();
                for (NestedTupleSourceRuntime pipeline : pipelines) {
                    pipeline.open();
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                memoryUsageCheck();
                for (NestedTupleSourceRuntime pipeline : pipelines) {
                    pipeline.writeTuple(accessor.getBuffer(), tIndex);
                }
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor,
                    int tIndex, AggregateState state) throws HyracksDataException {
                closePipelines();
                memoryUsageCheck();
                TupleUtils.addFields(outputWriter.getTupleBuilder(), tupleBuilder);
                return true;
            }

            private void closePipelines() throws HyracksDataException {
                for (int i = 0; i < pipelines.length; i++) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].close();
                }
            }

            /**
             * This method is called when evaluating accumulating frames.
             * It emits current result of the aggregates but does not close pipelines, so aggregation can continue.
             * This method may be called several times.
             * {@link #outputFinalResult(ArrayTupleBuilder, IFrameTupleAccessor, int, AggregateState)}
             * should be called at the end to emit the last value and close all pipelines
             */
            @Override
            public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                if (aggs == null) {
                    throw new UnsupportedOperationException();
                }
                for (int i = 0; i < pipelines.length; i++) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].flush();
                    aggs[i].finishAggregates(true);
                }
                memoryUsageCheck();
                TupleUtils.addFields(outputWriter.getTupleBuilder(), tupleBuilder);
                outputWriter.getTupleBuilder().reset();
                return true;
            }

            @Override
            public void discardFinalResult() throws HyracksDataException {
                closePipelines();
            }

            @Override
            public AggregateState createAggregateStates() {
                return null;
            }

            @Override
            public void reset() {
            }

            @Override
            public void close() {
            }

            private void memoryUsageCheck() {
                // TODO: implement as in NestedPlansAccumulatingAggregatorFactory.memoryUsageCheck()
            }
        };
    }
}
