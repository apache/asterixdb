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

import java.util.HashMap;
import java.util.Map;

import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.EnforcePushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.EnforceFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;

public class PipelineAssembler {

    // array of factories for building the local runtime pipeline
    private final RecordDescriptor pipelineInputRecordDescriptor;
    private final RecordDescriptor pipelineOutputRecordDescriptor;

    private final int inputArity;
    private final int outputArity;
    private final AlgebricksPipeline pipeline;
    private final Map<IPushRuntimeFactory, IPushRuntime[]> runtimeMap;

    public PipelineAssembler(AlgebricksPipeline pipeline, int inputArity, int outputArity,
            RecordDescriptor pipelineInputRecordDescriptor, RecordDescriptor pipelineOutputRecordDescriptor) {
        this.pipeline = pipeline;
        this.pipelineInputRecordDescriptor = pipelineInputRecordDescriptor;
        this.pipelineOutputRecordDescriptor = pipelineOutputRecordDescriptor;
        this.inputArity = inputArity;
        this.outputArity = outputArity;
        this.runtimeMap = new HashMap<>();
    }

    public IFrameWriter assemblePipeline(IFrameWriter writer, IHyracksTaskContext ctx) throws HyracksDataException {
        // should enforce protocol
        boolean enforce = ctx.getJobFlags().contains(JobFlag.ENFORCE_CONTRACT);
        boolean profile = ctx.getJobFlags().contains(JobFlag.PROFILE_RUNTIME);
        // plug the operators
        IFrameWriter start = writer;// this.writer;
        IPushRuntimeFactory[] runtimeFactories = pipeline.getRuntimeFactories();
        RecordDescriptor[] recordDescriptors = pipeline.getRecordDescriptors();
        for (int i = runtimeFactories.length - 1; i >= 0; i--) {
            start = (enforce && !profile) ? EnforceFrameWriter.enforce(start) : start;
            IPushRuntimeFactory runtimeFactory = runtimeFactories[i];
            IPushRuntime[] newRuntimes = runtimeFactory.createPushRuntime(ctx);
            for (int j = 0; j < newRuntimes.length; j++) {
                if (enforce) {
                    newRuntimes[j] = EnforcePushRuntime.enforce(newRuntimes[j]);
                }
                if (i == runtimeFactories.length - 1) {
                    if (outputArity == 1) {
                        newRuntimes[j].setOutputFrameWriter(0, start, pipelineOutputRecordDescriptor);
                    }
                } else {
                    newRuntimes[j].setOutputFrameWriter(0, start, recordDescriptors[i]);
                }
            }
            runtimeMap.put(runtimeFactory, newRuntimes);

            IPushRuntime newRuntime = newRuntimes[0];
            if (i > 0) {
                newRuntime.setInputRecordDescriptor(0, recordDescriptors[i - 1]);
            } else if (inputArity > 0) {
                newRuntime.setInputRecordDescriptor(0, pipelineInputRecordDescriptor);
            }
            start = newRuntime;
        }
        return start;
    }

    public IPushRuntime[] getPushRuntime(IPushRuntimeFactory runtimeFactory) {
        return runtimeMap.get(runtimeFactory);
    }

    //TODO: refactoring is needed
    public static IFrameWriter assemblePipeline(AlgebricksPipeline subplan, IFrameWriter writer,
            IHyracksTaskContext ctx, Map<IPushRuntimeFactory, IPushRuntime> outRuntimeMap) throws HyracksDataException {
        // should enforce protocol
        boolean enforce = ctx.getJobFlags().contains(JobFlag.ENFORCE_CONTRACT);
        boolean profile = ctx.getJobFlags().contains(JobFlag.PROFILE_RUNTIME);
        // plug the operators
        IFrameWriter start = writer;
        IPushRuntimeFactory[] runtimeFactories = subplan.getRuntimeFactories();
        RecordDescriptor[] recordDescriptors = subplan.getRecordDescriptors();
        for (int i = runtimeFactories.length - 1; i >= 0; i--) {
            start = (enforce && !profile) ? EnforceFrameWriter.enforce(start) : start;
            IPushRuntimeFactory runtimeFactory = runtimeFactories[i];
            IPushRuntime[] newRuntimes = runtimeFactory.createPushRuntime(ctx);
            IPushRuntime newRuntime = enforce ? EnforcePushRuntime.enforce(newRuntimes[0]) : newRuntimes[0];
            newRuntime.setOutputFrameWriter(0, start, recordDescriptors[i]);
            if (i > 0) {
                newRuntime.setInputRecordDescriptor(0, recordDescriptors[i - 1]);
            } else {
                // the nts has the same input and output rec. desc.
                newRuntime.setInputRecordDescriptor(0, recordDescriptors[0]);
            }
            if (outRuntimeMap != null) {
                outRuntimeMap.put(runtimeFactory, newRuntimes[0]);
            }
            start = newRuntime;
        }
        return start;
    }
}
