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
package edu.uci.ics.hyracks.algebricks.runtime.operators.meta;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory.NestedTupleSourceRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class SubplanRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final AlgebricksPipeline pipeline;
    private final RecordDescriptor inputRecordDesc;
    private final INullWriterFactory[] nullWriterFactories;

    public SubplanRuntimeFactory(AlgebricksPipeline pipeline, INullWriterFactory[] nullWriterFactories,
            RecordDescriptor inputRecordDesc, int[] projectionList) {
        super(projectionList);
        this.pipeline = pipeline;
        this.nullWriterFactories = nullWriterFactories;
        this.inputRecordDesc = inputRecordDesc;
        if (projectionList != null) {
            throw new NotImplementedException();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Subplan { \n");
        for (IPushRuntimeFactory f : pipeline.getRuntimeFactories()) {
            sb.append("  " + f.toString() + ";\n");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws AlgebricksException, HyracksDataException {

        RecordDescriptor pipelineOutputRecordDescriptor = null;

        final PipelineAssembler pa = new PipelineAssembler(pipeline, 1, 1, inputRecordDesc,
                pipelineOutputRecordDescriptor);
        final INullWriter[] nullWriters = new INullWriter[nullWriterFactories.length];
        for (int i = 0; i < nullWriterFactories.length; i++) {
            nullWriters[i] = nullWriterFactories[i].createNullWriter();
        }

        return new AbstractOneInputOneOutputOneFramePushRuntime() {

            /**
             * Computes the outer product between a given tuple and the frames
             * passed.
             */
            class TupleOuterProduct implements IFrameWriter {

                private boolean smthWasWritten = false;
                private IHyracksTaskContext hCtx = ctx;
                private int frameSize = hCtx.getFrameSize();
                private FrameTupleAccessor ta = new FrameTupleAccessor(frameSize,
                        pipeline.getRecordDescriptors()[pipeline.getRecordDescriptors().length - 1]);
                private ArrayTupleBuilder tb = new ArrayTupleBuilder(nullWriters.length);

                @Override
                public void open() throws HyracksDataException {
                    smthWasWritten = false;
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ta.reset(buffer);
                    int nTuple = ta.getTupleCount();
                    for (int t = 0; t < nTuple; t++) {
                        if (!appender.appendConcat(tRef.getFrameTupleAccessor(), tRef.getTupleIndex(), ta, t)) {
                            FrameUtils.flushFrame(frame, writer);
                            appender.reset(frame, true);
                            if (!appender.appendConcat(tRef.getFrameTupleAccessor(), tRef.getTupleIndex(), ta, t)) {
                                throw new HyracksDataException(
                                        "Could not write frame: subplan result is larger than the single-frame limit.");
                            }
                        }
                    }
                    smthWasWritten = true;
                }

                @Override
                public void close() throws HyracksDataException {
                    if (!smthWasWritten) {
                        // the case when we need to write nulls
                        appendNullsToTuple();
                        appendToFrameFromTupleBuilder(tb);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                private void appendNullsToTuple() throws HyracksDataException {
                    tb.reset();
                    int n0 = tRef.getFieldCount();
                    for (int f = 0; f < n0; f++) {
                        tb.addField(tRef.getFrameTupleAccessor(), tRef.getTupleIndex(), f);
                    }
                    DataOutput dos = tb.getDataOutput();
                    for (int i = 0; i < nullWriters.length; i++) {
                        nullWriters[i].writeNull(dos);
                        tb.addFieldEndOffset();
                    }
                }

            }

            IFrameWriter endPipe = new TupleOuterProduct();

            NestedTupleSourceRuntime startOfPipeline = (NestedTupleSourceRuntime) pa.assemblePipeline(endPipe, ctx);

            boolean first = true;

            @Override
            public void open() throws HyracksDataException {
                if (first) {
                    first = false;
                    initAccessAppendRef(ctx);
                }
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tAccess.reset(buffer);
                int nTuple = tAccess.getTupleCount();
                for (int t = 0; t < nTuple; t++) {
                    tRef.reset(tAccess, t);
                    startOfPipeline.writeTuple(buffer, t);
                    startOfPipeline.open();
                    startOfPipeline.close();
                }
            }
        };
    }
}
