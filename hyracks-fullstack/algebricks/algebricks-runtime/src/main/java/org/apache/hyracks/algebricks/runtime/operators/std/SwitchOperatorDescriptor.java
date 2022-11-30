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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspector;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractReplicateOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;

/**
 * The switch operator propagates each tuple of the input to one or more output branches based on a given output mapping.
 * For each tuple, we peek at the value of a given field f. We look up the value of the field in the supplied
 * output mapping and propagate the tuple to all corresponding output branches.
 */
public class SwitchOperatorDescriptor extends AbstractReplicateOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    final static int SWITCHER_ACTIVITY_ID = 0;

    private final IScalarEvaluatorFactory branchingExprEvalFactory;
    private final IBinaryIntegerInspectorFactory intInspectorFactory;
    private final Map<Integer, int[]> outputMapping;

    /**
     * @param spec
     * @param rDesc
     * @param outputArity equal to the number of non-materialized outputs
     * @param branchingExprEvalFactory containing the index of the relevant field f
     * @param intInspectorFactory
     * @param outputMapping the supplied mapping from field values to arrays of output branch numbers
     */
    public SwitchOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity,
            IScalarEvaluatorFactory branchingExprEvalFactory, IBinaryIntegerInspectorFactory intInspectorFactory,
            Map<Integer, int[]> outputMapping) {
        super(spec, rDesc, outputArity);
        if (outputArity != numberOfNonMaterializedOutputs) {
            throw new IllegalArgumentException();
        }
        this.branchingExprEvalFactory = branchingExprEvalFactory;
        this.intInspectorFactory = intInspectorFactory;
        this.outputMapping = outputMapping;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SwitcherActivityNode sma = new SwitcherActivityNode(new ActivityId(odId, SWITCHER_ACTIVITY_ID));
        builder.addActivity(this, sma);
        builder.addSourceEdge(0, sma, 0);
        for (int i = 0; i < outputArity; i++) {
            builder.addTargetEdge(i, sma, i);
        }
    }

    private final class SwitcherActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SwitcherActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            final IFrameWriter[] writers = new IFrameWriter[outputArity];
            final boolean[] isOpen = new boolean[outputArity];
            final IPointable p = VoidPointable.FACTORY.createPointable();
            // To deal with each tuple in a frame
            final FrameTupleAccessor accessor = new FrameTupleAccessor(outRecDescs[0]);
            final FrameTupleAppender[] appenders = new FrameTupleAppender[outputArity];
            final FrameTupleReference tRef = new FrameTupleReference();
            final IBinaryIntegerInspector intInspector = intInspectorFactory.createBinaryIntegerInspector(ctx);
            final IEvaluatorContext evalCtx = new EvaluatorContext(ctx);
            final IScalarEvaluator eval = branchingExprEvalFactory.createScalarEvaluator(evalCtx);
            final MutableBoolean hasFailed = new MutableBoolean(false);
            for (int i = 0; i < outputArity; i++) {
                appenders[i] = new FrameTupleAppender(new VSizeFrame(ctx), true);
            }

            return new AbstractUnaryInputOperatorNodePushable() {
                @Override
                public void open() throws HyracksDataException {
                    for (int i = 0; i < outputArity; i++) {
                        isOpen[i] = true;
                        writers[i].open();
                    }
                }

                @Override
                public void nextFrame(ByteBuffer bufferAccessor) throws HyracksDataException {
                    // Tuple based access
                    accessor.reset(bufferAccessor);
                    int tupleCount = accessor.getTupleCount();

                    for (int i = 0; i < tupleCount; i++) {
                        // Get the value of the relevant field in the given tuple.
                        tRef.reset(accessor, i);
                        eval.evaluate(tRef, p);
                        int fieldValue =
                                intInspector.getIntegerValue(p.getByteArray(), p.getStartOffset(), p.getLength());

                        // Look up the corresponding output branches based on the given mapping
                        int[] outputBranches = outputMapping.get(fieldValue);

                        // Propagate to each output branch
                        for (int j : outputBranches) {
                            FrameUtils.appendToWriter(writers[j], appenders[j], accessor, i);
                        }
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    Throwable hde = null;
                    // write if hasn't failed
                    if (!hasFailed.booleanValue()) {
                        for (int i = 0; i < outputArity; i++) {
                            if (isOpen[i]) {
                                try {
                                    appenders[i].write(writers[i], true);
                                } catch (Throwable th) {
                                    hde = th;
                                    break;
                                }
                            }
                        }
                    }

                    // fail the writers
                    if (hde != null) {
                        for (int i = 0; i < outputArity; i++) {
                            if (isOpen[i]) {
                                CleanupUtils.fail(writers[i], hde);
                            }
                        }
                    }

                    // close
                    for (int i = 0; i < outputArity; i++) {
                        if (isOpen[i]) {
                            hde = CleanupUtils.close(writers[i], hde);
                        }
                    }
                    if (hde != null) {
                        throw HyracksDataException.create(hde);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    hasFailed.setTrue();
                    HyracksDataException hde = null;
                    for (int i = 0; i < outputArity; i++) {
                        if (isOpen[i]) {
                            try {
                                writers[i].fail();
                            } catch (Throwable th) {
                                if (hde == null) {
                                    hde = HyracksDataException.create(th);
                                } else {
                                    hde.addSuppressed(th);
                                }
                            }
                        }
                    }
                    if (hde != null) {
                        throw hde;
                    }
                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    writers[index] = writer;
                }

                @Override
                public void flush() throws HyracksDataException {
                    for (int i = 0; i < outputArity; i++) {
                        if (isOpen[i]) {
                            appenders[i].flush(writers[i]);
                        }
                    }
                }
            };
        }
    }
}
