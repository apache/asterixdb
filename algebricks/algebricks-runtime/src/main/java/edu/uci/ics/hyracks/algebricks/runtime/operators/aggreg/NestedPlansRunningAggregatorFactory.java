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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory.NestedTupleSourceRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class NestedPlansRunningAggregatorFactory implements IAggregatorDescriptorFactory {

    private static final long serialVersionUID = 1L;
    private AlgebricksPipeline[] subplans;
    private int[] keyFieldIdx;
    private int[] decorFieldIdx;

    public NestedPlansRunningAggregatorFactory(AlgebricksPipeline[] subplans, int[] keyFieldIdx, int[] decorFieldIdx) {
        this.subplans = subplans;
        this.keyFieldIdx = keyFieldIdx;
        this.decorFieldIdx = decorFieldIdx;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory#createAggregator(edu.uci.ics.hyracks.api.context.IHyracksTaskContext, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int[], int[])
     */
    @Override
    public IAggregatorDescriptor createAggregator(final IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults,
            final IFrameWriter writer) throws HyracksDataException {
        final RunningAggregatorOutput outputWriter = new RunningAggregatorOutput(ctx, subplans, keyFieldIdx.length,
                decorFieldIdx.length, writer);
        final NestedTupleSourceRuntime[] pipelines = new NestedTupleSourceRuntime[subplans.length];
        for (int i = 0; i < subplans.length; i++) {
            try {
                pipelines[i] = (NestedTupleSourceRuntime) assemblePipeline(subplans[i], outputWriter, ctx);
            } catch (AlgebricksException e) {
                throw new HyracksDataException(e);
            }
        }

        final ArrayTupleBuilder gbyTb = outputWriter.getGroupByTupleBuilder();

        final ByteBuffer outputFrame = ctx.allocateFrame();
        final FrameTupleAppender outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputAppender.reset(outputFrame, true);

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
                    pipelines[i].forceFlush();
                }
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                for (int i = 0; i < pipelines.length; i++) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].writeTuple(accessor.getBuffer(), tIndex);
                    pipelines[i].forceFlush();
                }
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
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
            public boolean outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor,
                    int tIndex, AggregateState state) throws HyracksDataException {
                throw new IllegalStateException("this method should not be called");
            }

            @Override
            public void close() {

            }
        };
    }

    private IFrameWriter assemblePipeline(AlgebricksPipeline subplan, IFrameWriter writer, IHyracksTaskContext ctx)
            throws AlgebricksException, HyracksDataException {
        // plug the operators
        IFrameWriter start = writer;
        IPushRuntimeFactory[] runtimeFactories = subplan.getRuntimeFactories();
        RecordDescriptor[] recordDescriptors = subplan.getRecordDescriptors();
        for (int i = runtimeFactories.length - 1; i >= 0; i--) {
            IPushRuntime newRuntime = runtimeFactories[i].createPushRuntime(ctx);
            newRuntime.setFrameWriter(0, start, recordDescriptors[i]);
            if (i > 0) {
                newRuntime.setInputRecordDescriptor(0, recordDescriptors[i - 1]);
            } else {
                // the nts has the same input and output rec. desc.
                newRuntime.setInputRecordDescriptor(0, recordDescriptors[0]);
            }
            start = newRuntime;
        }
        return start;
    }

    private static class RunningAggregatorOutput implements IFrameWriter {

        private FrameTupleAccessor[] tAccess;
        private RecordDescriptor[] inputRecDesc;
        private int inputIdx;
        private ArrayTupleBuilder tb;
        private ArrayTupleBuilder gbyTb;
        private AlgebricksPipeline[] subplans;
        private IFrameWriter outputWriter;
        private ByteBuffer outputFrame;
        private FrameTupleAppender outputAppender;

        public RunningAggregatorOutput(IHyracksTaskContext ctx, AlgebricksPipeline[] subplans, int numKeys,
                int numDecors, IFrameWriter outputWriter) throws HyracksDataException {
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
            tb = new ArrayTupleBuilder(numKeys + numDecors + totalAggFields);
            gbyTb = new ArrayTupleBuilder(numKeys + numDecors);

            this.tAccess = new FrameTupleAccessor[inputRecDesc.length];
            for (int i = 0; i < inputRecDesc.length; i++) {
                tAccess[i] = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc[i]);
            }

            this.outputFrame = ctx.allocateFrame();
            this.outputAppender = new FrameTupleAppender(ctx.getFrameSize());
            this.outputAppender.reset(outputFrame, true);
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
                byte[] data = gbyTb.getByteArray();
                int[] fieldEnds = gbyTb.getFieldEndOffsets();
                int start = 0;
                int offset = 0;
                for (int i = 0; i < fieldEnds.length; i++) {
                    if (i > 0)
                        start = fieldEnds[i - 1];
                    offset = fieldEnds[i] - start;
                    tb.addField(data, start, offset);
                }
                for (int f = 0; f < w; f++) {
                    tb.addField(accessor, tIndex, f);
                }
                if (!outputAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    FrameUtils.flushFrame(outputFrame, outputWriter);
                    outputAppender.reset(outputFrame, true);
                    if (!outputAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        throw new HyracksDataException(
                                "Failed to write a running aggregation result into an empty frame: possibly the size of the result is too large.");
                    }
                }
            }
        }

        @Override
        public void close() throws HyracksDataException {
            // clearFrame();
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

    }

}
