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

package org.apache.hyracks.algebricks.runtime.operators.win;

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.data.IBinaryIntegerInspector;
import org.apache.hyracks.algebricks.data.IBinaryIntegerInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.DataUtils;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.group.IAggregatorDescriptor;

/**
 * Runtime for window operators that performs partition materialization and can evaluate running aggregates
 * as well as regular aggregates (in nested plans) over window frames.
 */
public class WindowNestedPlansPushRuntime extends WindowMaterializingPushRuntime {

    private final boolean frameValueExists;

    private final IScalarEvaluatorFactory[] frameValueEvalFactories;

    private IScalarEvaluator[] frameValueEvals;

    private IPointable[] frameValuePointables;

    private final IBinaryComparatorFactory[] frameValueComparatorFactories;

    private IBinaryComparator[] frameValueComparators;

    private final boolean frameStartExists;

    private final IScalarEvaluatorFactory[] frameStartEvalFactories;

    private IScalarEvaluator[] frameStartEvals;

    private IPointable[] frameStartPointables;

    private final boolean frameEndExists;

    private final IScalarEvaluatorFactory[] frameEndEvalFactories;

    private IScalarEvaluator[] frameEndEvals;

    private IPointable[] frameEndPointables;

    private final boolean frameExcludeExists;

    private final IScalarEvaluatorFactory[] frameExcludeEvalFactories;

    private IScalarEvaluator[] frameExcludeEvals;

    private final int frameExcludeNegationStartIdx;

    private IPointable[] frameExcludePointables;

    private IPointable frameExcludePointable2;

    private final IBinaryComparatorFactory[] frameExcludeComparatorFactories;

    private IBinaryComparator[] frameExcludeComparators;

    private final boolean frameOffsetExists;

    private final IScalarEvaluatorFactory frameOffsetEvalFactory;

    private IScalarEvaluator frameOffsetEval;

    private IPointable frameOffsetPointable;

    private final IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory;

    private final int frameMaxObjects;

    private final int nestedAggOutSchemaSize;

    private final WindowAggregatorDescriptorFactory nestedAggFactory;

    private IAggregatorDescriptor nestedAgg;

    private IFrame copyFrame2;

    private IFrame runFrame;

    private FrameTupleAccessor tAccess2;

    private FrameTupleReference tRef2;

    private IBinaryIntegerInspector bii;

    WindowNestedPlansPushRuntime(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, IScalarEvaluatorFactory[] frameValueEvalFactories,
            IBinaryComparatorFactory[] frameValueComparatorFactories, IScalarEvaluatorFactory[] frameStartEvalFactories,
            IScalarEvaluatorFactory[] frameEndEvalFactories, IScalarEvaluatorFactory[] frameExcludeEvalFactories,
            int frameExcludeNegationStartIdx, IBinaryComparatorFactory[] frameExcludeComparatorFactories,
            IScalarEvaluatorFactory frameOffsetEvalFactory,
            IBinaryIntegerInspectorFactory binaryIntegerInspectorFactory, int frameMaxObjects, int[] projectionColumns,
            int[] runningAggOutColumns, IRunningAggregateEvaluatorFactory[] runningAggFactories,
            int nestedAggOutSchemaSize, WindowAggregatorDescriptorFactory nestedAggFactory, IHyracksTaskContext ctx) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories, projectionColumns,
                runningAggOutColumns, runningAggFactories, ctx);
        this.frameValueEvalFactories = frameValueEvalFactories;
        this.frameValueExists = frameValueEvalFactories != null && frameValueEvalFactories.length > 0;
        this.frameStartEvalFactories = frameStartEvalFactories;
        this.frameStartExists = frameStartEvalFactories != null && frameStartEvalFactories.length > 0;
        this.frameEndEvalFactories = frameEndEvalFactories;
        this.frameEndExists = frameEndEvalFactories != null && frameEndEvalFactories.length > 0;
        this.frameValueComparatorFactories = frameValueComparatorFactories;
        this.frameExcludeEvalFactories = frameExcludeEvalFactories;
        this.frameExcludeExists = frameExcludeEvalFactories != null && frameExcludeEvalFactories.length > 0;
        this.frameExcludeComparatorFactories = frameExcludeComparatorFactories;
        this.frameExcludeNegationStartIdx = frameExcludeNegationStartIdx;
        this.frameOffsetExists = frameOffsetEvalFactory != null;
        this.frameOffsetEvalFactory = frameOffsetEvalFactory;
        this.binaryIntegerInspectorFactory = binaryIntegerInspectorFactory;
        this.frameMaxObjects = frameMaxObjects;
        this.nestedAggFactory = nestedAggFactory;
        this.nestedAggOutSchemaSize = nestedAggOutSchemaSize;
    }

    @Override
    protected void init() throws HyracksDataException {
        super.init();

        if (frameValueExists) {
            frameValueEvals = createEvaluators(frameValueEvalFactories, ctx);
            frameValueComparators = createBinaryComparators(frameValueComparatorFactories);
            frameValuePointables = createPointables(frameValueEvalFactories.length);
        }
        if (frameStartExists) {
            frameStartEvals = createEvaluators(frameStartEvalFactories, ctx);
            frameStartPointables = createPointables(frameStartEvalFactories.length);
        }
        if (frameEndExists) {
            frameEndEvals = createEvaluators(frameEndEvalFactories, ctx);
            frameEndPointables = createPointables(frameEndEvalFactories.length);
        }
        if (frameExcludeExists) {
            frameExcludeEvals = createEvaluators(frameExcludeEvalFactories, ctx);
            frameExcludeComparators = createBinaryComparators(frameExcludeComparatorFactories);
            frameExcludePointables = createPointables(frameExcludeEvalFactories.length);
            frameExcludePointable2 = VoidPointable.FACTORY.createPointable();
        }
        if (frameOffsetExists) {
            frameOffsetEval = frameOffsetEvalFactory.createScalarEvaluator(ctx);
            frameOffsetPointable = VoidPointable.FACTORY.createPointable();
            bii = binaryIntegerInspectorFactory.createBinaryIntegerInspector(ctx);
        }

        nestedAgg = nestedAggFactory.createAggregator(ctx, null, null, null, null, null, -1);

        runFrame = new VSizeFrame(ctx);
        copyFrame2 = new VSizeFrame(ctx);
        tAccess2 = new FrameTupleAccessor(inputRecordDesc);
        tRef2 = new FrameTupleReference();
    }

    @Override
    protected void producePartitionTuples(int chunkIdx, GeneratedRunFileReader reader) throws HyracksDataException {
        long readerPos = -1;
        int nChunks = getPartitionChunkCount();
        if (nChunks > 1) {
            readerPos = reader.position();
            if (chunkIdx == 0) {
                ByteBuffer curFrameBuffer = curFrame.getBuffer();
                int nBlocks = FrameHelper.deserializeNumOfMinFrame(curFrameBuffer);
                copyFrame2.ensureFrameSize(copyFrame2.getMinSize() * nBlocks);
                int pos = curFrameBuffer.position();
                FrameUtils.copyAndFlip(curFrameBuffer, copyFrame2.getBuffer());
                curFrameBuffer.position(pos);
            }
        }

        tAccess.reset(curFrame.getBuffer());
        int tBeginIdx = getTupleBeginIdx(chunkIdx);
        int tEndIdx = getTupleEndIdx(chunkIdx);
        for (int tIdx = tBeginIdx; tIdx <= tEndIdx; tIdx++) {
            tRef.reset(tAccess, tIdx);

            // running aggregates
            produceTuple(tupleBuilder, tAccess, tIdx, tRef);

            // frame boundaries
            if (frameStartExists) {
                for (int i = 0; i < frameStartEvals.length; i++) {
                    frameStartEvals[i].evaluate(tRef, frameStartPointables[i]);
                }
            }
            if (frameEndExists) {
                for (int i = 0; i < frameEndEvals.length; i++) {
                    frameEndEvals[i].evaluate(tRef, frameEndPointables[i]);
                }
            }
            if (frameExcludeExists) {
                for (int i = 0; i < frameExcludeEvals.length; i++) {
                    frameExcludeEvals[i].evaluate(tRef, frameExcludePointables[i]);
                }
            }
            int toSkip = 0;
            if (frameOffsetExists) {
                frameOffsetEval.evaluate(tRef, frameOffsetPointable);
                toSkip = bii.getIntegerValue(frameOffsetPointable.getByteArray(), frameOffsetPointable.getStartOffset(),
                        frameOffsetPointable.getLength());
            }
            int toWrite = frameMaxObjects;

            // aggregator created by WindowAggregatorDescriptorFactory does not process argument tuple in init()
            nestedAgg.init(null, null, -1, null);

            if (nChunks > 1) {
                reader.seek(0);
            }

            frame_loop: for (int chunkIdx2 = 0; chunkIdx2 < nChunks; chunkIdx2++) {
                IFrame innerFrame;
                if (chunkIdx2 == 0) {
                    // first chunk's frame is always in memory
                    innerFrame = chunkIdx == 0 ? curFrame : copyFrame2;
                } else {
                    reader.nextFrame(runFrame);
                    innerFrame = runFrame;
                }
                tAccess2.reset(innerFrame.getBuffer());

                int tBeginIdx2 = getTupleBeginIdx(chunkIdx2);
                int tEndIdx2 = getTupleEndIdx(chunkIdx2);
                for (int tIdx2 = tBeginIdx2; tIdx2 <= tEndIdx2; tIdx2++) {
                    tRef2.reset(tAccess2, tIdx2);

                    if (frameStartExists || frameEndExists) {
                        for (int frameValueIdx = 0; frameValueIdx < frameValueEvals.length; frameValueIdx++) {
                            frameValueEvals[frameValueIdx].evaluate(tRef2, frameValuePointables[frameValueIdx]);
                        }
                        if (frameStartExists
                                && compare(frameValuePointables, frameStartPointables, frameValueComparators) < 0) {
                            // skip if value < start
                            continue;
                        }
                        if (frameEndExists
                                && compare(frameValuePointables, frameEndPointables, frameValueComparators) > 0) {
                            // skip and exit if value > end
                            break frame_loop;
                        }
                    }
                    if (frameExcludeExists && isExcluded()) {
                        // skip if excluded
                        continue;
                    }

                    if (toSkip > 0) {
                        // skip if offset hasn't been reached
                        toSkip--;
                        continue;
                    }

                    if (toWrite != 0) {
                        nestedAgg.aggregate(tAccess2, tIdx2, null, -1, null);
                    }
                    if (toWrite > 0) {
                        toWrite--;
                    }
                    if (toWrite == 0) {
                        break frame_loop;
                    }
                }
            }

            nestedAgg.outputFinalResult(tupleBuilder, null, -1, null);
            appendToFrameFromTupleBuilder(tupleBuilder);
        }

        if (nChunks > 1) {
            reader.seek(readerPos);
        }
    }

    private boolean isExcluded() throws HyracksDataException {
        for (int i = 0; i < frameExcludeEvals.length; i++) {
            frameExcludeEvals[i].evaluate(tRef2, frameExcludePointable2);
            boolean b = DataUtils.compare(frameExcludePointables[i], frameExcludePointable2,
                    frameExcludeComparators[i]) != 0;
            if (i >= frameExcludeNegationStartIdx) {
                b = !b;
            }
            if (b) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected ArrayTupleBuilder createOutputTupleBuilder(int[] projectionList) {
        return new ArrayTupleBuilder(projectionList.length + nestedAggOutSchemaSize);
    }

    private static IScalarEvaluator[] createEvaluators(IScalarEvaluatorFactory[] evalFactories, IHyracksTaskContext ctx)
            throws HyracksDataException {
        IScalarEvaluator[] evals = new IScalarEvaluator[evalFactories.length];
        for (int i = 0; i < evalFactories.length; i++) {
            evals[i] = evalFactories[i].createScalarEvaluator(ctx);
        }
        return evals;
    }

    private static IPointable[] createPointables(int ln) {
        IPointable[] pointables = new IPointable[ln];
        for (int i = 0; i < ln; i++) {
            pointables[i] = VoidPointable.FACTORY.createPointable();
        }
        return pointables;
    }

    private static int compare(IValueReference[] first, IValueReference[] second, IBinaryComparator[] comparators)
            throws HyracksDataException {
        for (int i = 0; i < first.length; i++) {
            int c = DataUtils.compare(first[i], second[i], comparators[i]);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }
}