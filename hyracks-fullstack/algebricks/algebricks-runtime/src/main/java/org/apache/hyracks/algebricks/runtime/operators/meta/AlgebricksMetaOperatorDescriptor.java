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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.runtime.base.AlgebricksPipeline;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.ITimedWriter;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.NoOpOperatorStats;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputIntrospectingOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class AlgebricksMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 3L;

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

    private static String makeStatName(String base, String name, int pos, int input, int subPlan, int subPos) {
        StringBuilder sb = new StringBuilder();
        sb.append(base);
        sb.append(".");
        sb.append(pos);
        if (subPlan >= 0) {
            sb.append(".");
            sb.append(subPlan);
            sb.append(".");
            sb.append(subPos);
            sb.append(" - Subplan ");
        } else {
            sb.append(" - MicroOp ");
        }
        sb.append(name);
        if (input >= 0) {
            sb.append(" input [");
            sb.append(input);
            sb.append("] ");
        }
        return sb.toString();
    }

    private static String makeId(String base, int id, int subPlan, int subPos) {
        return base + "." + id + (subPlan >= 0 ? "." + subPlan : "") + (subPos >= 0 ? "." + subPos : "");
    }

    private static IOperatorStats makeStatForRuntimeFact(IPushRuntimeFactory factory, String base, String baseId,
            int pos, int subPlan, int subPos) {
        return new OperatorStats(makeStatName(base, factory.toString(), pos, -1, subPlan, subPos),
                makeId(baseId, pos, subPlan, subPos));
    }

    public static Map<IPushRuntimeFactory, IOperatorStats> makeMicroOpStats(AlgebricksPipeline pipe,
            IOperatorStats outerStats) {
        Map<IPushRuntimeFactory, IOperatorStats> microOpStats = new HashMap<>();
        String baseName = outerStats.getName().split(" - ")[0];
        String baseId = outerStats.getOperatorId();
        List<SubplanRuntimeFactory> subplans = new ArrayList<>();
        for (int i = 0; i < pipe.getRuntimeFactories().length; i++) {
            IPushRuntimeFactory fact = pipe.getRuntimeFactories()[i];
            //TODO: don't use instanceof
            if (fact instanceof SubplanRuntimeFactory) {
                SubplanRuntimeFactory subplanFact = (SubplanRuntimeFactory) fact;
                subplans.add(subplanFact);
                List<AlgebricksPipeline> pipelines = subplanFact.getPipelines();
                for (AlgebricksPipeline p : pipelines) {
                    IPushRuntimeFactory[] subplanFactories = p.getRuntimeFactories();
                    for (int j = subplanFactories.length - 1; j > 0; j--) {
                        microOpStats.put(subplanFactories[j], makeStatForRuntimeFact(subplanFactories[j], baseName,
                                baseId, i, pipelines.indexOf(p), j));
                    }
                }
            }
            microOpStats.put(fact, makeStatForRuntimeFact(fact, baseName, baseId, i, -1, -1));
        }
        for (SubplanRuntimeFactory sub : subplans) {
            sub.setStats(microOpStats);
        }
        return microOpStats;
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
            startOfPipeline = pa.assemblePipeline(writer, ctx, new HashMap<>());
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
        return new AbstractUnaryInputUnaryOutputIntrospectingOperatorNodePushable() {

            private IFrameWriter startOfPipeline;
            private boolean opened = false;
            private IOperatorStats parentStats = NoOpOperatorStats.INSTANCE;
            private Map<IPushRuntimeFactory, IOperatorStats> microOpStats = new HashMap<>();

            public void open() throws HyracksDataException {
                if (startOfPipeline == null) {
                    RecordDescriptor pipelineOutputRecordDescriptor =
                            outputArity > 0 ? AlgebricksMetaOperatorDescriptor.this.outRecDescs[0] : null;
                    RecordDescriptor pipelineInputRecordDescriptor = recordDescProvider
                            .getInputRecordDescriptor(AlgebricksMetaOperatorDescriptor.this.getActivityId(), 0);
                    PipelineAssembler pa = new PipelineAssembler(pipeline, inputArity, outputArity,
                            pipelineInputRecordDescriptor, pipelineOutputRecordDescriptor);
                    startOfPipeline = pa.assemblePipeline(writer, ctx, microOpStats);
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
            public void deinitialize() throws HyracksDataException {
                super.deinitialize();
            }

            @Override
            public String toString() {
                return AlgebricksMetaOperatorDescriptor.this.toString();
            }

            @Override
            public void addStats(IOperatorStats stats) throws HyracksDataException {
                microOpStats = makeMicroOpStats(pipeline, stats);
                for (IOperatorStats stat : microOpStats.values()) {
                    ctx.getStatsCollector().add(stat);
                }
            }

            @Override
            public void setUpstreamStats(IOperatorStats stats) {
                parentStats = stats;
            }

            @Override
            public long getTotalTime() {
                return startOfPipeline instanceof ITimedWriter ? ((ITimedWriter) startOfPipeline).getTotalTime() : 0;
            }

            @Override
            public IOperatorStats getStats() {
                IPushRuntimeFactory[] facts = pipeline.getRuntimeFactories();
                return microOpStats.getOrDefault(facts[facts.length - 1], NoOpOperatorStats.INSTANCE);
            }
        };
    }
}
