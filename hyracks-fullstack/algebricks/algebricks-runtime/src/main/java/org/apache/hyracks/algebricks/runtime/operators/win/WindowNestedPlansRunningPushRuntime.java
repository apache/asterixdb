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

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
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

    private final int frameMaxObjects;

    private FrameTupleAccessor tAccess2;

    private FrameTupleReference tRef2;

    private int chunkIdxFrameEndGlobal;

    private int tBeginIdxFrameEndGlobal;

    private int toWrite;

    WindowNestedPlansRunningPushRuntime(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, IScalarEvaluatorFactory[] frameValueEvalFactories,
            IBinaryComparatorFactory[] frameValueComparatorFactories, IScalarEvaluatorFactory[] frameEndEvalFactories,
            int frameMaxObjects, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, int nestedAggOutSchemaSize,
            WindowAggregatorDescriptorFactory nestedAggFactory, IHyracksTaskContext ctx, int memSizeInFrames,
            SourceLocation sourceLoc) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories, projectionColumns,
                runningAggOutColumns, runningAggFactories, nestedAggOutSchemaSize, nestedAggFactory, ctx,
                memSizeInFrames, sourceLoc);
        this.frameValueEvalFactories = frameValueEvalFactories;
        this.frameEndEvalFactories = frameEndEvalFactories;
        this.frameValueComparatorFactories = frameValueComparatorFactories;
        this.frameMaxObjects = frameMaxObjects;
    }

    @Override
    protected void init() throws HyracksDataException {
        super.init();
        frameValueEvals = createEvaluators(frameValueEvalFactories, ctx);
        frameValueComparators = MultiComparator.create(frameValueComparatorFactories);
        frameValuePointables = createPointables(frameValueEvalFactories.length);
        frameEndEvals = createEvaluators(frameEndEvalFactories, ctx);
        frameEndPointables = createPointables(frameEndEvalFactories.length);
        tAccess2 = new FrameTupleAccessor(inputRecordDesc);
        tRef2 = new FrameTupleReference();
    }

    @Override
    protected void beginPartitionImpl() throws HyracksDataException {
        super.beginPartitionImpl();
        nestedAggInit();
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

            // frame boundaries
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

            if (isLastTupleInPartition) {
                nestedAggOutputFinalResult(tupleBuilder);
            } else {
                nestedAggOutputPartialResult(tupleBuilder);
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
