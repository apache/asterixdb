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

import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
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
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;

/**
 * Aggregator factory for window operators
 */
public final class WindowAggregatorDescriptorFactory extends AbstractAccumulatingAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private AlgebricksPipeline[] subplans;

    public WindowAggregatorDescriptorFactory(AlgebricksPipeline[] subplans) {
        this.subplans = subplans;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor, int[] keys, int[] partialKeys, long memoryBudget)
            throws HyracksDataException {
        NestedPlansAccumulatingAggregatorFactory.AggregatorOutput outputWriter =
                new NestedPlansAccumulatingAggregatorFactory.AggregatorOutput(subplans, 0);
        NestedTupleSourceRuntime[] pipelines = new NestedTupleSourceRuntime[subplans.length];
        for (int i = 0; i < subplans.length; i++) {
            pipelines[i] =
                    (NestedTupleSourceRuntime) PipelineAssembler.assemblePipeline(subplans[i], outputWriter, ctx);
        }

        return new IAggregatorDescriptor() {

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
                for (int i = 0; i < pipelines.length; i++) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].close();
                }
                memoryUsageCheck();
                TupleUtils.addFields(outputWriter.getTupleBuilder(), tupleBuilder);
                return true;
            }

            @Override
            public AggregateState createAggregateStates() {
                return null;
            }

            @Override
            public void reset() {
            }

            @Override
            public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) {
                throw new UnsupportedOperationException();
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
