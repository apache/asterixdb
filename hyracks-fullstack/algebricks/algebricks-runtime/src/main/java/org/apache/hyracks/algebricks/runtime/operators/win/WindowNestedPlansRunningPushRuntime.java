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

import org.apache.hyracks.algebricks.data.IBinaryBooleanInspector;
import org.apache.hyracks.algebricks.data.IBinaryBooleanInspectorFactory;
import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PointableTupleReference;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * Optimized runtime for window operators that performs partition materialization and can evaluate running aggregates
 * as well as regular aggregates (in nested plans) over accumulating window frames
 * (unbounded preceding to current row or N following).
 */
final class WindowNestedPlansRunningPushRuntime extends AbstractWindowNestedPlansPushRuntime {

    private static final int PARTITION_POSITION_SLOT = 0;

    private static final int FRAME_POSITION_SLOT = 1;

    private static final int TMP_POSITION_SLOT = 2;

    private static final int PARTITION_READER_SLOT_COUNT = TMP_POSITION_SLOT + 1;

    private final IScalarEvaluatorFactory[] frameValueEvalFactories;

    private IScalarEvaluator[] frameValueEvals;

    private PointableTupleReference frameValuePointables;

    private final IBinaryComparatorFactory[] frameValueComparatorFactories;

    private MultiComparator frameValueComparators;

    private final IScalarEvaluatorFactory[] frameEndEvalFactories;

    private IScalarEvaluator[] frameEndEvals;

    private PointableTupleReference frameEndPointables;

    private final boolean frameEndValidationExists;

    private final IScalarEvaluatorFactory[] frameEndValidationEvalFactories;

    private IScalarEvaluator[] frameEndValidationEvals;

    private PointableTupleReference frameEndValidationPointables;

    private IWindowAggregatorDescriptor nestedAggForInvalidFrame;

    private final int frameMaxObjects;

    private final IBinaryBooleanInspectorFactory booleanAccessorFactory;

    private IBinaryBooleanInspector booleanAccessor;

    private FrameTupleAccessor tAccess2;

    private FrameTupleReference tRef2;

    private int chunkIdxFrameEndGlobal;

    private int tBeginIdxFrameEndGlobal;

    private int toWrite;

    WindowNestedPlansRunningPushRuntime(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, IScalarEvaluatorFactory[] frameValueEvalFactories,
            IBinaryComparatorFactory[] frameValueComparatorFactories, IScalarEvaluatorFactory[] frameEndEvalFactories,
            IScalarEvaluatorFactory[] frameEndValidationEvalFactories, int frameMaxObjects,
            IBinaryBooleanInspectorFactory booleanAccessorFactory, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, int nestedAggOutSchemaSize,
            WindowAggregatorDescriptorFactory nestedAggFactory, IHyracksTaskContext ctx, int memSizeInFrames,
            SourceLocation sourceLoc) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories, projectionColumns,
                runningAggOutColumns, runningAggFactories, nestedAggOutSchemaSize, nestedAggFactory, ctx,
                memSizeInFrames, sourceLoc);
        this.frameValueEvalFactories = frameValueEvalFactories;
        this.frameValueComparatorFactories = frameValueComparatorFactories;
        this.frameEndEvalFactories = frameEndEvalFactories;
        this.frameEndValidationEvalFactories = frameEndValidationEvalFactories;
        this.frameEndValidationExists =
                frameEndValidationEvalFactories != null && frameEndValidationEvalFactories.length > 0;
        this.frameMaxObjects = frameMaxObjects;
        this.booleanAccessorFactory = booleanAccessorFactory;
    }

    @Override
    protected void init() throws HyracksDataException {
        super.init();
        frameValueEvals = createEvaluators(frameValueEvalFactories, ctx);
        frameValueComparators = MultiComparator.create(frameValueComparatorFactories);
        frameValuePointables = PointableTupleReference.create(frameValueEvalFactories.length, VoidPointable.FACTORY);
        frameEndEvals = createEvaluators(frameEndEvalFactories, ctx);
        frameEndPointables = PointableTupleReference.create(frameEndEvalFactories.length, VoidPointable.FACTORY);
        if (frameEndValidationExists) {
            frameEndValidationEvals = createEvaluators(frameEndValidationEvalFactories, ctx);
            frameEndValidationPointables =
                    PointableTupleReference.create(frameEndValidationEvalFactories.length, VoidPointable.FACTORY);
            booleanAccessor = booleanAccessorFactory.createBinaryBooleanInspector(ctx.getTaskContext());
            nestedAggForInvalidFrame = nestedAggCreate();
        }
        tAccess2 = new FrameTupleAccessor(inputRecordDesc);
        tRef2 = new FrameTupleReference();
    }

    @Override
    protected void beginPartitionImpl() throws HyracksDataException {
        super.beginPartitionImpl();
        nestedAggInit();
        if (frameEndValidationExists) {
            nestedAggInit(nestedAggForInvalidFrame);
        }
        chunkIdxFrameEndGlobal = 0;
        tBeginIdxFrameEndGlobal = -1;
        toWrite = frameMaxObjects;
    }

    @Override
    protected void producePartitionTuples(int chunkIdx, IFrame chunkFrame) throws HyracksDataException {
        partitionReader.savePosition(PARTITION_POSITION_SLOT);

        int nChunks = getPartitionChunkCount();
        boolean isFirstChunkInPartition = chunkIdx == 0;
        boolean isLastChunkInPartition = chunkIdx == nChunks - 1;

        tAccess.reset(chunkFrame.getBuffer());
        int tBeginIdx = getTupleBeginIdx(chunkIdx);
        int tEndIdx = getTupleEndIdx(chunkIdx);

        for (int tIdx = tBeginIdx; tIdx <= tEndIdx; tIdx++) {
            boolean isFirstTupleInPartition = isFirstChunkInPartition && tIdx == tBeginIdx;
            boolean isLastTupleInPartition = isLastChunkInPartition && tIdx == tEndIdx;

            tRef.reset(tAccess, tIdx);

            // running aggregates
            produceTuple(tupleBuilder, tAccess, tIdx, tRef);

            // frame boundary
            boolean frameEndValid = true;
            if (frameEndValidationExists) {
                evaluate(frameEndValidationEvals, tRef, frameEndValidationPointables);
                frameEndValid = allTrue(frameEndValidationPointables, booleanAccessor);
            }

            if (frameEndValid) {
                evaluate(frameEndEvals, tRef, frameEndPointables);

                int chunkIdxInnerStart = chunkIdxFrameEndGlobal;
                int tBeginIdxInnerStart = tBeginIdxFrameEndGlobal;

                if (chunkIdxInnerStart < nChunks) {
                    if (!isFirstTupleInPartition) {
                        partitionReader.restorePosition(FRAME_POSITION_SLOT);
                    } else {
                        partitionReader.rewind();
                    }
                }

                int chunkIdxFrameEndLocal = -1, tBeginIdxFrameEndLocal = -1;

                frame_loop: for (int chunkIdxInner = chunkIdxInnerStart; chunkIdxInner < nChunks; chunkIdxInner++) {
                    partitionReader.savePosition(TMP_POSITION_SLOT);
                    IFrame frameInner = partitionReader.nextFrame(false);
                    tAccess2.reset(frameInner.getBuffer());

                    int tBeginIdxInner;
                    if (tBeginIdxInnerStart >= 0) {
                        tBeginIdxInner = tBeginIdxInnerStart;
                        tBeginIdxInnerStart = -1;
                    } else {
                        tBeginIdxInner = getTupleBeginIdx(chunkIdxInner);
                    }
                    int tEndIdxInner = getTupleEndIdx(chunkIdxInner);

                    for (int tIdxInner = tBeginIdxInner; tIdxInner <= tEndIdxInner && toWrite != 0; tIdxInner++) {
                        tRef2.reset(tAccess2, tIdxInner);

                        evaluate(frameValueEvals, tRef2, frameValuePointables);

                        if (frameValueComparators.compare(frameValuePointables, frameEndPointables) > 0) {
                            // value > end => beyond the frame end
                            // save position of the current tuple, will continue from it in the next outer iteration
                            chunkIdxFrameEndLocal = chunkIdxInner;
                            tBeginIdxFrameEndLocal = tIdxInner;
                            partitionReader.copyPosition(TMP_POSITION_SLOT, FRAME_POSITION_SLOT);
                            // exit the frame loop
                            break frame_loop;
                        }

                        nestedAggAggregate(tAccess2, tIdxInner);

                        if (toWrite > 0) {
                            toWrite--;
                        }
                    }
                }

                if (chunkIdxFrameEndLocal >= 0) {
                    chunkIdxFrameEndGlobal = chunkIdxFrameEndLocal;
                    tBeginIdxFrameEndGlobal = tBeginIdxFrameEndLocal;
                } else {
                    // frame end not found, set it beyond the last chunk
                    chunkIdxFrameEndGlobal = nChunks;
                    tBeginIdxFrameEndGlobal = 0;
                }

                nestedAggOutputPartialResult(tupleBuilder);
            } else {
                nestedAggOutputPartialResult(nestedAggForInvalidFrame, tupleBuilder);
            }

            if (isLastTupleInPartition) {
                // we've already emitted accumulated partial result for this tuple, so discard it
                nestAggDiscardFinalResult();
                if (frameEndValidationExists) {
                    nestAggDiscardFinalResult(nestedAggForInvalidFrame);
                }
            }

            appendToFrameFromTupleBuilder(tupleBuilder);
        }

        partitionReader.restorePosition(PARTITION_POSITION_SLOT);
    }

    @Override
    protected int getPartitionReaderSlotCount() {
        return PARTITION_READER_SLOT_COUNT;
    }
}
