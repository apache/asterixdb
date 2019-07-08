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
import org.apache.hyracks.dataflow.std.base.AbstractReplicateOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputOperatorNodePushable;

/**
 * Split operator propagates each tuple in a frame to one output branch only unlike Replicate operator.
 */
public class SplitOperatorDescriptor extends AbstractReplicateOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final IScalarEvaluatorFactory brachingExprEvalFactory;
    private final IBinaryIntegerInspectorFactory intInsepctorFactory;
    private final int defaultBranch;
    private final boolean propageToAllBranchAsDefault;

    public SplitOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity,
            IScalarEvaluatorFactory brachingExprEvalFactory, IBinaryIntegerInspectorFactory intInsepctorFactory) {
        this(spec, rDesc, outputArity, brachingExprEvalFactory, intInsepctorFactory, 0, false);
    }

    public SplitOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor rDesc, int outputArity,
            IScalarEvaluatorFactory brachingExprEvalFactory, IBinaryIntegerInspectorFactory intInsepctorFactory,
            int defaultBranch, boolean propageToAllBranchAsDefault) {
        super(spec, rDesc, outputArity);
        this.brachingExprEvalFactory = brachingExprEvalFactory;
        this.intInsepctorFactory = intInsepctorFactory;
        this.defaultBranch = defaultBranch;
        this.propageToAllBranchAsDefault = propageToAllBranchAsDefault;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SplitterMaterializerActivityNode sma =
                new SplitterMaterializerActivityNode(new ActivityId(odId, SPLITTER_MATERIALIZER_ACTIVITY_ID));
        builder.addActivity(this, sma);
        builder.addSourceEdge(0, sma, 0);
        for (int i = 0; i < outputArity; i++) {
            builder.addTargetEdge(i, sma, i);
        }
    }

    // The difference between SplitterMaterializerActivityNode and ReplicatorMaterializerActivityNode is that
    // SplitterMaterializerActivityNode propagates each tuple to one output branch only.
    private final class SplitterMaterializerActivityNode extends ReplicatorMaterializerActivityNode {
        private static final long serialVersionUID = 1L;

        public SplitterMaterializerActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            final IFrameWriter[] writers = new IFrameWriter[numberOfNonMaterializedOutputs];
            final boolean[] isOpen = new boolean[numberOfNonMaterializedOutputs];
            final IPointable p = VoidPointable.FACTORY.createPointable();;
            // To deal with each tuple in a frame
            final FrameTupleAccessor accessor = new FrameTupleAccessor(outRecDescs[0]);;
            final FrameTupleAppender[] appenders = new FrameTupleAppender[numberOfNonMaterializedOutputs];
            final FrameTupleReference tRef = new FrameTupleReference();;
            final IBinaryIntegerInspector intInsepctor = intInsepctorFactory.createBinaryIntegerInspector(ctx);
            final IEvaluatorContext evalCtx = new EvaluatorContext(ctx);
            final IScalarEvaluator eval = brachingExprEvalFactory.createScalarEvaluator(evalCtx);
            final MutableBoolean hasFailed = new MutableBoolean(false);
            for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                appenders[i] = new FrameTupleAppender(new VSizeFrame(ctx), true);
            }

            return new AbstractUnaryInputOperatorNodePushable() {
                @Override
                public void open() throws HyracksDataException {
                    for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                        isOpen[i] = true;
                        writers[i].open();
                    }
                }

                @Override
                public void nextFrame(ByteBuffer bufferAccessor) throws HyracksDataException {
                    // Tuple based access
                    accessor.reset(bufferAccessor);
                    int tupleCount = accessor.getTupleCount();
                    // The output branch number that starts from 0.
                    int outputBranch = defaultBranch;
                    boolean correctBranchValue;

                    for (int i = 0; i < tupleCount; i++) {
                        // Get the output branch number from the field in the given tuple.
                        tRef.reset(accessor, i);
                        eval.evaluate(tRef, p);
                        correctBranchValue = true;

                        try {
                            outputBranch =
                                    intInsepctor.getIntegerValue(p.getByteArray(), p.getStartOffset(), p.getLength());

                            if (outputBranch < 0 || outputBranch >= outputArity) {
                                correctBranchValue = false;
                            }
                        } catch (Exception e) {
                            correctBranchValue = false;
                        }

                        // Add this tuple to the correct output frame.
                        if (correctBranchValue) {
                            FrameUtils.appendToWriter(writers[outputBranch], appenders[outputBranch], accessor, i);
                        } else {
                            // Need to propagate to the all branches?
                            if (!propageToAllBranchAsDefault) {
                                FrameUtils.appendToWriter(writers[outputBranch], appenders[outputBranch], accessor, i);
                            } else {
                                for (int j = 0; j < outputArity; j++) {
                                    FrameUtils.appendToWriter(writers[j], appenders[j], accessor, i);
                                }
                            }
                        }

                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    Throwable hde = null;
                    // write if hasn't failed
                    if (!hasFailed.booleanValue()) {
                        for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
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
                        for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
                            if (isOpen[i]) {
                                CleanupUtils.fail(writers[i], hde);
                            }
                        }
                    }

                    // close
                    for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
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
                    for (int i = 0; i < numberOfNonMaterializedOutputs; i++) {
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
            };
        }
    }
}
