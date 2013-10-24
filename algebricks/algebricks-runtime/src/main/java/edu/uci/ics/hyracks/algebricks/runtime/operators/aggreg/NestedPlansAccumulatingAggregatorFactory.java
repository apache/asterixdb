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
import edu.uci.ics.hyracks.dataflow.std.group.AbstractAccumulatingAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

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
            RecordDescriptor outRecordDescriptor, int[] keys, int[] partialKeys) throws HyracksDataException {

        final AggregatorOutput outputWriter = new AggregatorOutput(ctx.getFrameSize(), subplans, keyFieldIdx.length,
                decorFieldIdx.length);
        final NestedTupleSourceRuntime[] pipelines = new NestedTupleSourceRuntime[subplans.length];
        for (int i = 0; i < subplans.length; i++) {
            try {
                pipelines[i] = (NestedTupleSourceRuntime) assemblePipeline(subplans[i], outputWriter, ctx);
            } catch (AlgebricksException e) {
                throw new HyracksDataException(e);
            }
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
                // it only works if the output of the aggregator fits in one
                // frame
                for (int i = 0; i < pipelines.length; i++) {
                    pipelines[i].writeTuple(accessor.getBuffer(), tIndex);
                }
            }

            @Override
            public boolean outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                for (int i = 0; i < pipelines.length; i++) {
                    outputWriter.setInputIdx(i);
                    pipelines[i].close();
                }
                // outputWriter.writeTuple(appender);
                tupleBuilder.reset();
                ArrayTupleBuilder tb = outputWriter.getTupleBuilder();
                byte[] data = tb.getByteArray();
                int[] fieldEnds = tb.getFieldEndOffsets();
                int start = 0;
                int offset = 0;
                for (int i = 0; i < fieldEnds.length; i++) {
                    if (i > 0)
                        start = fieldEnds[i - 1];
                    offset = fieldEnds[i] - start;
                    tupleBuilder.addField(data, start, offset);
                }
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

    /**
     * We suppose for now, that each subplan only produces one tuple.
     */
    private static class AggregatorOutput implements IFrameWriter {

        // private ByteBuffer frame;
        private FrameTupleAccessor[] tAccess;
        private RecordDescriptor[] inputRecDesc;
        private int inputIdx;
        private ArrayTupleBuilder tb;
        private AlgebricksPipeline[] subplans;

        public AggregatorOutput(int frameSize, AlgebricksPipeline[] subplans, int numKeys, int numDecors) {
            this.subplans = subplans;
            // this.keyFieldIndexes = keyFieldIndexes;
            int totalAggFields = 0;
            this.inputRecDesc = new RecordDescriptor[subplans.length];
            for (int i = 0; i < subplans.length; i++) {
                RecordDescriptor[] rd = subplans[i].getRecordDescriptors();
                this.inputRecDesc[i] = rd[rd.length - 1];
                totalAggFields += subplans[i].getOutputWidth();
            }
            tb = new ArrayTupleBuilder(numKeys + numDecors + totalAggFields);

            this.tAccess = new FrameTupleAccessor[inputRecDesc.length];
            for (int i = 0; i < inputRecDesc.length; i++) {
                tAccess[i] = new FrameTupleAccessor(frameSize, inputRecDesc[i]);
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
