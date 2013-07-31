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

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class PipelineAssembler {

    // array of factories for building the local runtime pipeline
    private final RecordDescriptor pipelineInputRecordDescriptor;
    private final RecordDescriptor pipelineOutputRecordDescriptor;

    private final int inputArity;
    private final int outputArity;
    private final AlgebricksPipeline pipeline;

    public PipelineAssembler(AlgebricksPipeline pipeline, int inputArity, int outputArity,
            RecordDescriptor pipelineInputRecordDescriptor, RecordDescriptor pipelineOutputRecordDescriptor) {
        this.pipeline = pipeline;
        this.pipelineInputRecordDescriptor = pipelineInputRecordDescriptor;
        this.pipelineOutputRecordDescriptor = pipelineOutputRecordDescriptor;
        this.inputArity = inputArity;
        this.outputArity = outputArity;
    }

    public IFrameWriter assemblePipeline(IFrameWriter writer, IHyracksTaskContext ctx) throws AlgebricksException,
            HyracksDataException {
        // plug the operators
        IFrameWriter start = writer;// this.writer;
        for (int i = pipeline.getRuntimeFactories().length - 1; i >= 0; i--) {
            IPushRuntime newRuntime = pipeline.getRuntimeFactories()[i].createPushRuntime(ctx);
            if (i == pipeline.getRuntimeFactories().length - 1) {
                if (outputArity == 1) {
                    newRuntime.setFrameWriter(0, start, pipelineOutputRecordDescriptor);
                }
            } else {
                newRuntime.setFrameWriter(0, start, pipeline.getRecordDescriptors()[i]);
            }
            if (i > 0) {
                newRuntime.setInputRecordDescriptor(0, pipeline.getRecordDescriptors()[i - 1]);
            } else if (inputArity > 0) {
                newRuntime.setInputRecordDescriptor(0, pipelineInputRecordDescriptor);
            }
            start = newRuntime;
        }
        return start;
    }
}
