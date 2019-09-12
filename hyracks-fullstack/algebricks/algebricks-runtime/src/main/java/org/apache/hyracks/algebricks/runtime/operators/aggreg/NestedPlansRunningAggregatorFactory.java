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
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.EnforceFrameWriter;
import org.apache.hyracks.api.dataflow.TimedFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.AggregateState;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class NestedPlansRunningAggregatorFactory extends AbstractAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private final AlgebricksPipeline[] subplans;
    private final int[] keyFieldIdx;
    private final int[] decorFieldIdx;

    public NestedPlansRunningAggregatorFactory(AlgebricksPipeline[] subplans, int[] keyFieldIdx, int[] decorFieldIdx) {
        this.subplans = subplans;
        this.keyFieldIdx = keyFieldIdx;
        this.decorFieldIdx = decorFieldIdx;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.dataflow.std.group.IAggregatorDescriptorFactory#createAggregator(org.apache.hyracks.api.context.IHyracksTaskContext, org.apache.hyracks.api.dataflow.value.RecordDescriptor, org.apache.hyracks.api.dataflow.value.RecordDescriptor, int[], int[])
     */
    @Override
    public IAggregatorDescriptor createAggregator(final IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults,
            final IFrameWriter writer, long memoryBudget) throws HyracksDataException {
        final boolean enforce = ctx.getJobFlags().contains(JobFlag.ENFORCE_CONTRACT);
        final boolean profile = ctx.getJobFlags().contains(JobFlag.PROFILE_RUNTIME);
        final RunningAggregatorOutput outputWriter =
                new RunningAggregatorOutput(ctx, subplans, keyFieldIdx.length + decorFieldIdx.length, writer);
        IFrameWriter fw = outputWriter;
        if (profile) {
            fw = TimedFrameWriter.time(outputWriter, ctx, "Aggregate Writer");
        } else if (enforce) {
            fw = EnforceFrameWriter.enforce(outputWriter);
        }
        final NestedTupleSourceRuntime[] pipelines = new NestedTupleSourceRuntime[subplans.length];
        for (int i = 0; i < subplans.length; i++) {
            pipelines[i] = (NestedTupleSourceRuntime) PipelineAssembler.assemblePipeline(subplans[i], fw, ctx, null);
        }

        final ArrayTupleBuilder gbyTb = outputWriter.getGroupByTupleBuilder();

        return new IAggregatorDescriptor() {

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {

                for (int i = 0; i < pipelines.length; ++i) {
                    pipelines[i].open();
                }

                gbyTb.reset();
                for (int i = 0; i < keyFieldIdx.length; ++i) {
                    gbyTb.addField(accessor, tIndex, keyFieldIdx[i]);
                }
                for (int i = 0; i < decorFieldIdx.length; ++i) {
                    gbyTb.addField(accessor, tIndex, decorFieldIdx[i]);
                }

                // aggregate the first tuple
                for (int i = 0; i < pipelines.length; i++) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].writeTuple(accessor.getBuffer(), tIndex);
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                for (int i = 0; i < pipelines.length; i++) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].writeTuple(accessor.getBuffer(), tIndex);
                }
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                for (int i = 0; i < pipelines.length; ++i) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].close();
                }
                return false;
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
        };
    }

    private static class RunningAggregatorOutput implements IFrameWriter {

        private final FrameTupleAccessor[] tAccess;
        private final RecordDescriptor[] inputRecDesc;
        private int inputIdx;
        private final ArrayTupleBuilder tb;
        private final ArrayTupleBuilder gbyTb;
        private final AlgebricksPipeline[] subplans;
        private final IFrameWriter outputWriter;
        private final FrameTupleAppender outputAppender;

        public RunningAggregatorOutput(IHyracksTaskContext ctx, AlgebricksPipeline[] subplans, int numPropagatedFields,
                IFrameWriter outputWriter) throws HyracksDataException {
            this.subplans = subplans;
            this.outputWriter = outputWriter;

            // this.keyFieldIndexes = keyFieldIndexes;
            int totalAggFields = 0;
            this.inputRecDesc = new RecordDescriptor[subplans.length];
            for (int i = 0; i < subplans.length; i++) {
                RecordDescriptor[] rd = subplans[i].getRecordDescriptors();
                this.inputRecDesc[i] = rd[rd.length - 1];
                totalAggFields += subplans[i].getOutputWidth();
            }
            tb = new ArrayTupleBuilder(numPropagatedFields + totalAggFields);
            gbyTb = new ArrayTupleBuilder(numPropagatedFields);

            this.tAccess = new FrameTupleAccessor[inputRecDesc.length];
            for (int i = 0; i < inputRecDesc.length; i++) {
                tAccess[i] = new FrameTupleAccessor(inputRecDesc[i]);
            }

            this.outputAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        }

        @Override
        public void open() throws HyracksDataException {

        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            int w = subplans[inputIdx].getOutputWidth();
            IFrameTupleAccessor accessor = tAccess[inputIdx];
            accessor.reset(buffer);
            for (int tIndex = 0; tIndex < accessor.getTupleCount(); tIndex++) {
                tb.reset();
                TupleUtils.addFields(gbyTb, tb);
                for (int f = 0; f < w; f++) {
                    tb.addField(accessor, tIndex, f);
                }
                FrameUtils.appendToWriter(outputWriter, outputAppender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }
        }

        @Override
        public void close() throws HyracksDataException {
            outputAppender.write(outputWriter, true);
        }

        public void setInputIdx(int inputIdx) {
            this.inputIdx = inputIdx;
        }

        public ArrayTupleBuilder getGroupByTupleBuilder() {
            return gbyTb;
        }

        @Override
        public void fail() throws HyracksDataException {
        }

        @Override
        public void flush() throws HyracksDataException {
            outputAppender.flush(outputWriter);
        }

    }

}
