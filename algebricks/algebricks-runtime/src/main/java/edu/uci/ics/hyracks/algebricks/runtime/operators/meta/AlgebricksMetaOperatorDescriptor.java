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

import java.nio.ByteBuffer;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class AlgebricksMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    // array of factories for building the local runtime pipeline
    private final AlgebricksPipeline pipeline;

    public AlgebricksMetaOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity,
            IPushRuntimeFactory[] runtimeFactories, RecordDescriptor[] internalRecordDescriptors) {
        super(spec, inputArity, outputArity);
        if (outputArity == 1) {
            this.recordDescriptors[0] = internalRecordDescriptors[internalRecordDescriptors.length - 1];
        }
        this.pipeline = new AlgebricksPipeline(runtimeFactories, internalRecordDescriptors);
    }

    public AlgebricksPipeline getPipeline() {
        return pipeline;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = super.toJSON();
        json.put("micro-operators", pipeline.getRuntimeFactories());
        return json;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Asterix { \n");
        for (IPushRuntimeFactory f : pipeline.getRuntimeFactories()) {
            sb.append("  " + f.toString() + ";\n");
        }
        sb.append("}");
        // sb.append(super.getInputArity());
        // sb.append(";");
        // sb.append(super.getOutputArity());
        // sb.append(";");
        return sb.toString();
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        if (inputArity == 0) {
            return createSourceInputPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        } else {
            return createOneInputOneOutputPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        }
    }

    private IOperatorNodePushable createSourceInputPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {

            public void initialize() throws HyracksDataException {
                IFrameWriter startOfPipeline;
                RecordDescriptor pipelineOutputRecordDescriptor = outputArity > 0 ? AlgebricksMetaOperatorDescriptor.this.recordDescriptors[0]
                        : null;

                PipelineAssembler pa = new PipelineAssembler(pipeline, inputArity, outputArity, null,
                        pipelineOutputRecordDescriptor);
                try {
                    startOfPipeline = pa.assemblePipeline(writer, ctx);
                } catch (AlgebricksException e) {
                    throw new HyracksDataException(e);
                }
                startOfPipeline.open();
                startOfPipeline.close();
            }
        };
    }

    private IOperatorNodePushable createOneInputOneOutputPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private IFrameWriter startOfPipeline;

            @Override
            public void open() throws HyracksDataException {
                if (startOfPipeline == null) {
                    RecordDescriptor pipelineOutputRecordDescriptor = outputArity > 0 ? AlgebricksMetaOperatorDescriptor.this.recordDescriptors[0]
                            : null;
                    RecordDescriptor pipelineInputRecordDescriptor = recordDescProvider.getInputRecordDescriptor(
                            AlgebricksMetaOperatorDescriptor.this.getActivityId(), 0);
                    PipelineAssembler pa = new PipelineAssembler(pipeline, inputArity, outputArity,
                            pipelineInputRecordDescriptor, pipelineOutputRecordDescriptor);
                    try {
                        startOfPipeline = pa.assemblePipeline(writer, ctx);
                    } catch (AlgebricksException ae) {
                        throw new HyracksDataException(ae);
                    }
                }
                startOfPipeline.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                startOfPipeline.nextFrame(buffer);
            }

            @Override
            public void close() throws HyracksDataException {
                startOfPipeline.close();
            }

            @Override
            public void fail() throws HyracksDataException {
                startOfPipeline.fail();
            }
        };
    }
}
