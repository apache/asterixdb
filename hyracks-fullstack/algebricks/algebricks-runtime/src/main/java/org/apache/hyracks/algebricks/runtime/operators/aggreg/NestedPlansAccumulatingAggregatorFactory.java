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

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.operators.meta.PipelineAssembler;
import org.apache.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory.NestedTupleSourceRuntime;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.group.AbstractAccumulatingAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class NestedPlansAccumulatingAggregatorFactory extends AbstractAccumulatingAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private AlgebricksPipeline[] subplans;
    private int[] keyFieldIdx;
    private int[] decorFieldIdx;

    public NestedPlansAccumulatingAggregatorFactory(AlgebricksPipeline[] subplans, int[] keyFieldIdx,
            int[] decorFieldIdx) {
        this.subplans = subplans;
        this.keyFieldIdx = keyFieldIdx;
        this.decorFieldIdx = decorFieldIdx;
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor, int[] keys, int[] partialKeys, long memoryBudget)
            throws HyracksDataException {
        final AggregatorOutput outputWriter = new AggregatorOutput(subplans, keyFieldIdx.length + decorFieldIdx.length);
        final NestedTupleSourceRuntime[] pipelines = new NestedTupleSourceRuntime[subplans.length];
        for (int i = 0; i < subplans.length; i++) {
            pipelines[i] =
                    (NestedTupleSourceRuntime) PipelineAssembler.assemblePipeline(subplans[i], outputWriter, ctx, null);
        }

        return new IAggregatorDescriptor() {

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                ArrayTupleBuilder tb = outputWriter.getTupleBuilder();
                tb.reset();
                for (int i = 0; i < keyFieldIdx.length; ++i) {
                    tb.addField(accessor, tIndex, keyFieldIdx[i]);
                }
                for (int i = 0; i < decorFieldIdx.length; ++i) {
                    tb.addField(accessor, tIndex, decorFieldIdx[i]);
                }
                for (int i = 0; i < pipelines.length; ++i) {
                    pipelines[i].open();
                }
                // aggregate the first tuple
                for (int i = 0; i < pipelines.length; i++) {
                    pipelines[i].writeTuple(accessor.getBuffer(), tIndex);
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                // Checks the memory usage.
                memoryUsageCheck();

                for (int i = 0; i < pipelines.length; i++) {
                    pipelines[i].writeTuple(accessor.getBuffer(), tIndex);
                }
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor stateAccessor,
                    int tIndex, AggregateState state) throws HyracksDataException {
                for (int i = 0; i < pipelines.length; i++) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].close();
                }

                // Checks the memory usage.
                memoryUsageCheck();

                tupleBuilder.reset();
                TupleUtils.addFields(outputWriter.getTupleBuilder(), tupleBuilder);

                return true;
            }

            @Override
            public AggregateState createAggregateStates() {
                return new AggregateState();
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

            // Checks the memory usage.
            private void memoryUsageCheck() throws HyracksDataException {
                if (memoryBudget > 0) {
                    ArrayTupleBuilder tb = outputWriter.getTupleBuilder();
                    byte[] data = tb.getByteArray();
                    if (data.length > memoryBudget) {
                        throw HyracksDataException.create(ErrorCode.GROUP_BY_MEMORY_BUDGET_EXCEEDS, sourceLoc,
                                data.length, memoryBudget);
                    }
                }
            }

        };
    }

    /**
     * We suppose for now, that each subplan only produces one tuple.
     */
    public static class AggregatorOutput implements IFrameWriter {

        // private ByteBuffer frame;
        private FrameTupleAccessor[] tAccess;
        private RecordDescriptor[] inputRecDesc;
        private int inputIdx;
        private ArrayTupleBuilder tb;
        private AlgebricksPipeline[] subplans;

        public AggregatorOutput(AlgebricksPipeline[] subplans, int numPropagatedFields) {
            this.subplans = subplans;
            int totalAggFields = 0;
            this.inputRecDesc = new RecordDescriptor[subplans.length];
            for (int i = 0; i < subplans.length; i++) {
                RecordDescriptor[] rd = subplans[i].getRecordDescriptors();
                this.inputRecDesc[i] = rd[rd.length - 1];
                totalAggFields += subplans[i].getOutputWidth();
            }
            tb = new ArrayTupleBuilder(numPropagatedFields + totalAggFields);

            this.tAccess = new FrameTupleAccessor[inputRecDesc.length];
            for (int i = 0; i < inputRecDesc.length; i++) {
                tAccess[i] = new FrameTupleAccessor(inputRecDesc[i]);
            }
        }

        @Override
        public void open() throws HyracksDataException {

        }

        /**
         * Since each pipeline only produces one tuple, this method is only
         * called by the close method of the pipelines.
         */
        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            int tIndex = 0;
            int w = subplans[inputIdx].getOutputWidth();
            IFrameTupleAccessor accessor = tAccess[inputIdx];
            accessor.reset(buffer);
            for (int f = 0; f < w; f++) {
                tb.addField(accessor, tIndex, f);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            // clearFrame();
        }

        public void setInputIdx(int inputIdx) {
            this.inputIdx = inputIdx;
        }

        public ArrayTupleBuilder getTupleBuilder() {
            return tb;
        }

        @Override
        public void fail() throws HyracksDataException {
        }
    }
}
