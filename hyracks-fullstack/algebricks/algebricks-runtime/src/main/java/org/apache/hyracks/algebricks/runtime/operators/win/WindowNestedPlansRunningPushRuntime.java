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

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.PointableTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * Optimized runtime for window operators that performs partition materialization and can evaluate running aggregates
 * as well as regular aggregates (in nested plans) over accumulating window frames
 * (unbounded preceding to current row or N following).
 */
class WindowNestedPlansRunningPushRuntime extends AbstractWindowNestedPlansPushRuntime {

    private final IScalarEvaluatorFactory[] frameValueEvalFactories;

    private IScalarEvaluator[] frameValueEvals;

    private PointableTupleReference frameValuePointables;

    private final IBinaryComparatorFactory[] frameValueComparatorFactories;

    private MultiComparator frameValueComparators;

    private final IScalarEvaluatorFactory[] frameEndEvalFactories;

    private IScalarEvaluator[] frameEndEvals;

    private PointableTupleReference frameEndPointables;

    private final int frameMaxObjects;

    private IFrame copyFrame2;

    private IFrame runFrame;

    private int runFrameChunkId;

    private long runFrameSize;

    private FrameTupleAccessor tAccess2;

    private FrameTupleReference tRef2;

    private int chunkIdxFrameEndGlobal;

    private int tBeginIdxFrameEndGlobal;

    private long readerPosFrameEndGlobal;

    private int toWrite;

    WindowNestedPlansRunningPushRuntime(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, IScalarEvaluatorFactory[] frameValueEvalFactories,
            IBinaryComparatorFactory[] frameValueComparatorFactories, IScalarEvaluatorFactory[] frameEndEvalFactories,
            int frameMaxObjects, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, int nestedAggOutSchemaSize,
            WindowAggregatorDescriptorFactory nestedAggFactory, IHyracksTaskContext ctx) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories, projectionColumns,
                runningAggOutColumns, runningAggFactories, nestedAggOutSchemaSize, nestedAggFactory, ctx);
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

        runFrame = new VSizeFrame(ctx);
        copyFrame2 = new VSizeFrame(ctx);
        tAccess2 = new FrameTupleAccessor(inputRecordDesc);
        tRef2 = new FrameTupleReference();
    }

    @Override
    protected void beginPartitionImpl() throws HyracksDataException {
        super.beginPartitionImpl();
        nestedAggInit();
        chunkIdxFrameEndGlobal = 0;
        tBeginIdxFrameEndGlobal = -1;
        readerPosFrameEndGlobal = 0;
        runFrameChunkId = -1;
        toWrite = frameMaxObjects;
    }

    @Override
    protected void producePartitionTuples(int chunkIdx, GeneratedRunFileReader reader) throws HyracksDataException {
        long readerPos = -1;
        int nChunks = getPartitionChunkCount();
        if (nChunks > 1) {
            readerPos = reader.position();
            if (chunkIdx == 0) {
                ByteBuffer curFrameBuffer = curFrame.getBuffer();
                int pos = curFrameBuffer.position();
                copyFrame2.ensureFrameSize(curFrameBuffer.capacity());
                FrameUtils.copyAndFlip(curFrameBuffer, copyFrame2.getBuffer());
                curFrameBuffer.position(pos);
            }
        }

        boolean isLastChunk = chunkIdx == nChunks - 1;

        tAccess.reset(curFrame.getBuffer());
        int tBeginIdx = getTupleBeginIdx(chunkIdx);
        int tEndIdx = getTupleEndIdx(chunkIdx);
        for (int tIdx = tBeginIdx; tIdx <= tEndIdx; tIdx++) {
            tRef.reset(tAccess, tIdx);

            // running aggregates
            produceTuple(tupleBuilder, tAccess, tIdx, tRef);

            // frame boundaries
            evaluate(frameEndEvals, tRef, frameEndPointables);

            int chunkIdxInnerStart = chunkIdxFrameEndGlobal;
            int tBeginIdxInnerStart = tBeginIdxFrameEndGlobal;
            if (nChunks > 1) {
                reader.seek(readerPosFrameEndGlobal);
            }

            int chunkIdxFrameEndLocal = -1, tBeginIdxFrameEndLocal = -1;
            long readerPosFrameEndLocal = -1;

            frame_loop: for (int chunkIdxInner = chunkIdxInnerStart; chunkIdxInner < nChunks; chunkIdxInner++) {
                long readerPosFrameInner;
                IFrame frameInner;
                if (chunkIdxInner == 0) {
                    // first chunk's frame is always in memory
                    frameInner = chunkIdx == 0 ? curFrame : copyFrame2;
                    readerPosFrameInner = 0;
                } else {
                    readerPosFrameInner = reader.position();
                    if (runFrameChunkId == chunkIdxInner) {
                        // runFrame has this chunk, so just advance the reader
                        reader.seek(readerPosFrameInner + runFrameSize);
                    } else {
                        reader.nextFrame(runFrame);
                        runFrameSize = reader.position() - readerPosFrameInner;
                        runFrameChunkId = chunkIdxInner;
                    }
                    frameInner = runFrame;
                }
                tAccess2.reset(frameInner.getBuffer());

                int tBeginIdxInner;
                if (tBeginIdxInnerStart < 0) {
                    tBeginIdxInner = getTupleBeginIdx(chunkIdxInner);
                } else {
                    tBeginIdxInner = tBeginIdxInnerStart;
                    tBeginIdxInnerStart = -1;
                }
                int tEndIdxInner = getTupleEndIdx(chunkIdxInner);

                for (int tIdxInner = tBeginIdxInner; tIdxInner <= tEndIdxInner && toWrite != 0; tIdxInner++) {
                    tRef2.reset(tAccess2, tIdxInner);

                    evaluate(frameValueEvals, tRef2, frameValuePointables);
                    if (frameValueComparators.compare(frameValuePointables, frameEndPointables) > 0) {
                        // save position of the tuple that matches the frame end.
                        // we'll continue from it in the next outer iteration
                        chunkIdxFrameEndLocal = chunkIdxInner;
                        tBeginIdxFrameEndLocal = tIdxInner;
                        readerPosFrameEndLocal = readerPosFrameInner;

                        // skip and exit if value > end
                        break frame_loop;
                    }

                    nestedAggAggregate(tAccess2, tIdxInner);

                    if (toWrite > 0) {
                        toWrite--;
                    }
                }
            }

            boolean isLastTuple = isLastChunk && tIdx == tEndIdx;
            if (isLastTuple) {
                nestedAggOutputFinalResult(tupleBuilder);
            } else {
                nestedAggOutputPartialResult(tupleBuilder);
            }
            appendToFrameFromTupleBuilder(tupleBuilder);

            if (chunkIdxFrameEndLocal >= 0) {
                chunkIdxFrameEndGlobal = chunkIdxFrameEndLocal;
                tBeginIdxFrameEndGlobal = tBeginIdxFrameEndLocal;
                readerPosFrameEndGlobal = readerPosFrameEndLocal;
            } else {
                // could not find the end, set beyond the last chunk
                chunkIdxFrameEndGlobal = nChunks;
                tBeginIdxFrameEndGlobal = 0;
                readerPosFrameEndGlobal = 0;
            }
        }

        if (nChunks > 1) {
            reader.seek(readerPos);
        }
    }
}
