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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.NestedTupleSourceRuntimeFactory.NestedTupleSourceRuntime;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ProfiledFrameWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class SubplanRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 3L;

    private final List<AlgebricksPipeline> pipelines;

    private final RecordDescriptor inputRecordDesc;

    private final RecordDescriptor outputRecordDesc;

    private final IMissingWriterFactory[] missingWriterFactories;

    private final Map<IPushRuntimeFactory, IOperatorStats> stats;

    public SubplanRuntimeFactory(List<AlgebricksPipeline> pipelines, IMissingWriterFactory[] missingWriterFactories,
            RecordDescriptor inputRecordDesc, RecordDescriptor outputRecordDesc, int[] projectionList) {
        super(projectionList);
        this.pipelines = pipelines;
        this.missingWriterFactories = missingWriterFactories;
        this.inputRecordDesc = inputRecordDesc;
        this.outputRecordDesc = outputRecordDesc;
        if (projectionList != null) {
            throw new NotImplementedException();
        }
        this.stats = new HashMap<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Subplan { \n");
        for (AlgebricksPipeline pipeline : pipelines) {
            sb.append('{');
            for (IPushRuntimeFactory f : pipeline.getRuntimeFactories()) {
                sb.append("  ").append(f).append(";\n");
            }
            sb.append('}');
        }
        sb.append("}");
        return sb.toString();
    }

    public List<AlgebricksPipeline> getPipelines() {
        return pipelines;
    }

    public void setStats(Map<IPushRuntimeFactory, IOperatorStats> stats) {
        this.stats.putAll(stats);
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(final IHyracksTaskContext ctx)
            throws HyracksDataException {
        return new SubplanPushRuntime(ctx);
    }

    private class SubplanPushRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {

        final IHyracksTaskContext ctx;

        final NestedTupleSourceRuntime[] startOfPipelines;

        boolean first;

        boolean profile;

        SubplanPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
            this.ctx = ctx;
            this.profile = ctx.getJobFlags().contains(JobFlag.PROFILE_RUNTIME);
            this.first = true;

            IMissingWriter[] missingWriters = new IMissingWriter[missingWriterFactories.length];
            for (int i = 0; i < missingWriterFactories.length; i++) {
                missingWriters[i] = missingWriterFactories[i].createMissingWriter();
            }

            int pipelineCount = pipelines.size();
            startOfPipelines = new NestedTupleSourceRuntime[pipelineCount];
            PipelineAssembler[] pipelineAssemblers = new PipelineAssembler[pipelineCount];
            for (int i = 0; i < pipelineCount; i++) {
                AlgebricksPipeline pipeline = pipelines.get(i);
                RecordDescriptor pipelineLastRecordDescriptor =
                        pipeline.getRecordDescriptors()[pipeline.getRecordDescriptors().length - 1];

                RecordDescriptor outputRecordDescriptor;
                IFrameWriter outputWriter;
                if (i == 0) {
                    // primary pipeline
                    outputWriter = new TupleOuterProduct(pipelineLastRecordDescriptor, missingWriters);
                    //this is necessary to track the output of the last operator to the outer product,
                    //i.e. the last real operator in pipeline 0 of the subplan
                    if (profile) {
                        outputWriter = new ProfiledFrameWriter(outputWriter);
                    }
                    outputRecordDescriptor = SubplanRuntimeFactory.this.outputRecordDesc;
                } else {
                    // secondary pipeline
                    IPushRuntime outputPushRuntime = PipelineAssembler.linkPipeline(pipeline, pipelineAssemblers, i);
                    if (outputPushRuntime == null) {
                        throw new IllegalStateException("Invalid pipeline");
                    }
                    outputPushRuntime.setInputRecordDescriptor(0, pipelineLastRecordDescriptor);
                    outputWriter = outputPushRuntime;
                    outputRecordDescriptor = pipelineLastRecordDescriptor;
                }

                PipelineAssembler pa = new PipelineAssembler(pipeline, 1, 1, inputRecordDesc, outputRecordDescriptor);
                IFrameWriter head = pa.assemblePipeline(outputWriter, ctx, stats);
                startOfPipelines[i] = (NestedTupleSourceRuntime) head;
                pipelineAssemblers[i] = pa;
            }
        }

        @Override
        public void open() throws HyracksDataException {
            // writer opened many times?
            super.open();
            if (first) {
                first = false;
                initAccessAppendRef(ctx);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            tAccess.reset(buffer);
            int nTuple = tAccess.getTupleCount();
            for (int t = 0; t < nTuple; t++) {
                tRef.reset(tAccess, t);

                for (NestedTupleSourceRuntime nts : startOfPipelines) {
                    nts.writeTuple(buffer, t);
                }

                int n = 0;
                try {
                    for (; n < startOfPipelines.length; n++) {
                        NestedTupleSourceRuntime nts = startOfPipelines[n];
                        try {
                            nts.open();
                        } catch (Exception e) {
                            nts.fail();
                            throw e;
                        }
                    }
                } finally {
                    for (int i = n - 1; i >= 0; i--) {
                        startOfPipelines[i].close();
                    }
                }
            }
        }

        @Override
        public void flush() throws HyracksDataException {
            appender.flush(writer);
        }

        /**
         * Computes the outer product between a given tuple and the frames
         * passed.
         */
        class TupleOuterProduct implements IFrameWriter {

            private boolean smthWasWritten;
            private final FrameTupleAccessor ta;
            private final ArrayTupleBuilder tb;
            private final IMissingWriter[] missingWriters;

            private TupleOuterProduct(RecordDescriptor recordDescriptor, IMissingWriter[] missingWriters) {
                ta = new FrameTupleAccessor(recordDescriptor);
                tb = new ArrayTupleBuilder(
                        missingWriters.length + SubplanRuntimeFactory.this.inputRecordDesc.getFieldCount());
                this.missingWriters = missingWriters;
            }

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
                if (nTuple > 0) {
                    smthWasWritten = true;
                }
            }

            @Override
            public void close() throws HyracksDataException {
                if (!smthWasWritten && !failed) {
                    // the case when we need to write nulls
                    appendNullsToTuple();
                    appendToFrameFromTupleBuilder(tb);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                // writer.fail() is called by the outer class' writer.fail().
            }

            private void appendNullsToTuple() throws HyracksDataException {
                tb.reset();
                int n0 = tRef.getFieldCount();
                for (int f = 0; f < n0; f++) {
                    tb.addField(tRef.getFrameTupleAccessor(), tRef.getTupleIndex(), f);
                }
                DataOutput dos = tb.getDataOutput();
                for (IMissingWriter missingWriter : missingWriters) {
                    missingWriter.writeMissing(dos);
                    tb.addFieldEndOffset();
                }
            }
        }
    }
}
