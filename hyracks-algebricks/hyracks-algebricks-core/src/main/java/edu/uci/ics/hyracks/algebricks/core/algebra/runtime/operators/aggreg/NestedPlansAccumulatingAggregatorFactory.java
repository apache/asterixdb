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
package edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.aggreg;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.AlgebricksPipeline;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.context.RuntimeContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.std.NestedTupleSourceRuntimeFactory.NestedTupleSourceRuntime;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.IAccumulatingAggregatorFactory;

public class NestedPlansAccumulatingAggregatorFactory implements IAccumulatingAggregatorFactory {

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
    public IAccumulatingAggregator createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDesc,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {

        final RuntimeContext rc = new RuntimeContext();
        rc.setHyracksContext(ctx);
        final AggregatorOutput outputWriter = new AggregatorOutput(ctx.getFrameSize(), subplans, keyFieldIdx.length,
                decorFieldIdx.length);
        final NestedTupleSourceRuntime[] pipelines = new NestedTupleSourceRuntime[subplans.length];
        for (int i = 0; i < subplans.length; i++) {
            try {
                pipelines[i] = (NestedTupleSourceRuntime) assemblePipeline(subplans[i], outputWriter, rc);
            } catch (AlgebricksException e) {
                throw new HyracksDataException(e);
            }
        }

        return new IAccumulatingAggregator() {

            private boolean pending;

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
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
                pending = false;
            }

            @Override
            public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                // it only works if the output of the aggregator fits in one
                // frame
                for (int i = 0; i < pipelines.length; i++) {
                    pipelines[i].writeTuple(accessor.getBuffer(), tIndex);
                }
            }

            @Override
            public boolean output(FrameTupleAppender appender, IFrameTupleAccessor accessor, int tIndex,
                    int[] keyFieldIndexes) throws HyracksDataException {
                if (!pending) {
                    for (int i = 0; i < pipelines.length; i++) {
                        outputWriter.setInputIdx(i);
                        pipelines[i].close();
                    }
                }
                if (!outputWriter.writeTuple(appender)) {
                    pending = true;
                    return false;
                } else {
                    return true;
                }
            }

        };
    }

    private IFrameWriter assemblePipeline(AlgebricksPipeline subplan, IFrameWriter writer, RuntimeContext rc)
            throws AlgebricksException {
        // plug the operators
        IFrameWriter start = writer;
        IPushRuntimeFactory[] runtimeFactories = subplan.getRuntimeFactories();
        RecordDescriptor[] recordDescriptors = subplan.getRecordDescriptors();
        for (int i = runtimeFactories.length - 1; i >= 0; i--) {
            IPushRuntime newRuntime = runtimeFactories[i].createPushRuntime(rc);
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
     * 
     * 
     * We suppose for now, that each subplan only produces one tuple.
     * 
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
         * 
         * Since each pipeline only produces one tuple, this method is only
         * called by the close method of the pipelines.
         * 
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

        public boolean writeTuple(FrameTupleAppender appender) {
            return appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }

        public ArrayTupleBuilder getTupleBuilder() {
            return tb;
        }

        @Override
        public void fail() throws HyracksDataException {
        }
    }

}
