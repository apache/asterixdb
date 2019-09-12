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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class AlgebricksMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    // array of factories for building the local runtime pipeline
    private final AlgebricksPipeline pipeline;

    public AlgebricksMetaOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            IPushRuntimeFactory[] runtimeFactories, RecordDescriptor[] internalRecordDescriptors) {
        this(spec, inputArity, outputArity, runtimeFactories, internalRecordDescriptors, null, null);
    }

    public AlgebricksMetaOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            IPushRuntimeFactory[] runtimeFactories, RecordDescriptor[] internalRecordDescriptors,
            IPushRuntimeFactory[] outputRuntimeFactories, int[] outputPositions) {
        super(spec, inputArity, outputArity);
        if (outputArity == 1) {
            this.outRecDescs[0] = internalRecordDescriptors[internalRecordDescriptors.length - 1];
        }
        this.pipeline = new AlgebricksPipeline(runtimeFactories, internalRecordDescriptors, outputRuntimeFactories,
                outputPositions);
    }

    public AlgebricksPipeline getPipeline() {
        return pipeline;
    }

    @Override
    public ObjectNode toJSON() {
        ObjectNode json = super.toJSON();
        json.put("micro-operators", Arrays.toString(pipeline.getRuntimeFactories()));
        return json;
    }

    @Override
    public String toString() {
        return "AlgebricksMeta " + Arrays.toString(pipeline.getRuntimeFactories());
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        if (inputArity == 0) {
            return new SourcePushRuntime(ctx);
        } else {
            return createOneInputOneOutputPushRuntime(ctx, recordDescProvider);
        }
    }

    private class SourcePushRuntime extends AbstractUnaryOutputSourceOperatorNodePushable {
        private final IHyracksTaskContext ctx;

        SourcePushRuntime(IHyracksTaskContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void initialize() throws HyracksDataException {
            IFrameWriter startOfPipeline;
            RecordDescriptor pipelineOutputRecordDescriptor =
                    outputArity > 0 ? AlgebricksMetaOperatorDescriptor.this.outRecDescs[0] : null;
            PipelineAssembler pa =
                    new PipelineAssembler(pipeline, inputArity, outputArity, null, pipelineOutputRecordDescriptor);
            startOfPipeline = pa.assemblePipeline(writer, ctx);
            HyracksDataException exception = null;
            try {
                startOfPipeline.open();
            } catch (Exception e) {
                startOfPipeline.fail();
                exception = HyracksDataException.create(e);
            } finally {
                try {
                    startOfPipeline.close();
                } catch (Exception e) {
                    if (exception == null) {
                        exception = HyracksDataException.create(e);
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }
            if (exception != null) {
                throw exception;
            }
        }

        @Override
        public String getDisplayName() {
            return "Empty Tuple Source";
        }
    }

    private IOperatorNodePushable createOneInputOneOutputPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider) {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private IFrameWriter startOfPipeline;
            private boolean opened = false;

            @Override
            public void open() throws HyracksDataException {
                if (startOfPipeline == null) {
                    RecordDescriptor pipelineOutputRecordDescriptor =
                            outputArity > 0 ? AlgebricksMetaOperatorDescriptor.this.outRecDescs[0] : null;
                    RecordDescriptor pipelineInputRecordDescriptor = recordDescProvider
                            .getInputRecordDescriptor(AlgebricksMetaOperatorDescriptor.this.getActivityId(), 0);
                    PipelineAssembler pa = new PipelineAssembler(pipeline, inputArity, outputArity,
                            pipelineInputRecordDescriptor, pipelineOutputRecordDescriptor);
                    startOfPipeline = pa.assemblePipeline(writer, ctx);
                }
                opened = true;
                startOfPipeline.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                startOfPipeline.nextFrame(buffer);
            }

            @Override
            public void close() throws HyracksDataException {
                if (opened) {
                    startOfPipeline.close();
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                if (opened) {
                    startOfPipeline.fail();
                }
            }

            @Override
            public void flush() throws HyracksDataException {
                startOfPipeline.flush();
            }

            @Override
            public String toString() {
                return AlgebricksMetaOperatorDescriptor.this.toString();
            }
        };
    }
}
