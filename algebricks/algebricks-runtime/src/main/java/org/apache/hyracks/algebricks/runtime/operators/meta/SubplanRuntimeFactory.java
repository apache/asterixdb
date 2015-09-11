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
package org.apache.hyracks.algebricks.runtime.operators.meta;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory.NestedTupleSourceRuntime;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.INullWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

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
                private FrameTupleAccessor ta = new FrameTupleAccessor(
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
                        appendConcat(tRef.getFrameTupleAccessor(), tRef.getTupleIndex(), ta, t);
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
