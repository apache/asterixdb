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
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

/**
 * Runtime for window operators that performs partition materialization and evaluates running aggregates
 * that require information about number of tuples in the partition.
 */
class WindowMaterializingPushRuntime extends AbstractWindowPushRuntime {

    private final int memSizeInFrames;

    private long partitionLength;

    private WindowPartitionWriter partitionWriter;

    WindowPartitionReader partitionReader;

    private int chunkBeginIdx;

    private IntArrayList chunkEndIdx;

    WindowMaterializingPushRuntime(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, IHyracksTaskContext ctx, int memSizeInFrames,
            SourceLocation sourceLoc) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories, projectionColumns,
                runningAggOutColumns, runningAggFactories, ctx, sourceLoc);
        this.memSizeInFrames = memSizeInFrames;
    }

    @Override
    protected void init() throws HyracksDataException {
        super.init();
        String runFilePrefix = getClass().getName();
        partitionWriter = new WindowPartitionWriter(ctx.getTaskContext(), memSizeInFrames - getReservedFrameCount(),
                runFilePrefix, getPartitionReaderSlotCount(), sourceLoc);
        partitionReader = partitionWriter.getReader();
        chunkEndIdx = new IntArrayList(128, 128);
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        if (partitionWriter != null) {
            partitionWriter.close();
        }
    }

    @Override
    protected void beginPartitionImpl() throws HyracksDataException {
        chunkEndIdx.clear();
        partitionLength = 0;
        partitionWriter.reset();
    }

    @Override
    protected void partitionChunkImpl(long frameId, ByteBuffer frameBuffer, int tBeginIdx, int tEndIdx)
            throws HyracksDataException {
        boolean isFirstChunk = chunkEndIdx.isEmpty();
        partitionWriter.nextFrame(frameId, frameBuffer);
        if (isFirstChunk) {
            chunkBeginIdx = tBeginIdx;
        }
        chunkEndIdx.add(tEndIdx);
        partitionLength += tEndIdx - tBeginIdx + 1;
    }

    @Override
    protected void endPartitionImpl() throws HyracksDataException {
        runningAggInitPartition(partitionLength);

        partitionReader.open();
        for (int chunkIdx = 0, nChunks = getPartitionChunkCount(); chunkIdx < nChunks; chunkIdx++) {
            IFrame chunkFrame = partitionReader.nextFrame(true);
            producePartitionTuples(chunkIdx, chunkFrame);
        }
        partitionReader.close();
    }

    void producePartitionTuples(int chunkIdx, IFrame chunkFrame) throws HyracksDataException {
        tAccess.reset(chunkFrame.getBuffer());
        produceTuples(tAccess, getTupleBeginIdx(chunkIdx), getTupleEndIdx(chunkIdx), tRef);
    }

    final int getPartitionChunkCount() {
        return chunkEndIdx.size();
    }

    final int getTupleBeginIdx(int chunkIdx) {
        return chunkIdx == 0 ? chunkBeginIdx : 0;
    }

    final int getTupleEndIdx(int chunkIdx) {
        return chunkEndIdx.get(chunkIdx);
    }

    int getPartitionReaderSlotCount() {
        return -1; // forward only reader by default
    }
}
