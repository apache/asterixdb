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
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

/**
 * Runtime for window operators that performs partition materialization and evaluates running aggregates
 * that require information about number of tuples in the partition.
 */
class WindowMaterializingPushRuntime extends AbstractWindowPushRuntime {

    private long partitionLength;

    IFrame curFrame;

    private long curFrameId;

    private int chunkBeginIdx;

    private IntArrayList chunkEndIdx;

    private RunFileWriter run;

    private long runLastFrameId;

    WindowMaterializingPushRuntime(int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, int[] projectionColumns, int[] runningAggOutColumns,
            IRunningAggregateEvaluatorFactory[] runningAggFactories, IHyracksTaskContext ctx) {
        super(partitionColumns, partitionComparatorFactories, orderComparatorFactories, projectionColumns,
                runningAggOutColumns, runningAggFactories, ctx);
    }

    @Override
    public void open() throws HyracksDataException {
        super.open();
        run = null;
        curFrameId = -1;
    }

    @Override
    protected void init() throws HyracksDataException {
        super.init();
        curFrame = new VSizeFrame(ctx);
        chunkEndIdx = new IntArrayList(128, 128);
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        if (run != null) {
            run.erase();
        }
    }

    @Override
    protected void beginPartitionImpl() {
        chunkEndIdx.clear();
        partitionLength = 0;
        if (run != null) {
            run.rewind();
        }
    }

    @Override
    protected void partitionChunkImpl(long frameId, ByteBuffer frameBuffer, int tBeginIdx, int tEndIdx)
            throws HyracksDataException {
        // save the frame. first one to memory, remaining ones to the run file
        boolean isFirstChunk = chunkEndIdx.isEmpty();
        if (isFirstChunk) {
            if (frameId != curFrameId) {
                int nBlocks = FrameHelper.deserializeNumOfMinFrame(frameBuffer);
                curFrame.ensureFrameSize(curFrame.getMinSize() * nBlocks);
                int pos = frameBuffer.position();
                FrameUtils.copyAndFlip(frameBuffer, curFrame.getBuffer());
                frameBuffer.position(pos);
                curFrameId = frameId;
            }
            chunkBeginIdx = tBeginIdx;
        } else {
            if (tBeginIdx != 0) {
                throw new IllegalStateException(String.valueOf(tBeginIdx));
            }
            if (run == null) {
                FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(getClass().getSimpleName());
                run = new RunFileWriter(file, ctx.getIoManager());
                run.open();
            }
            int pos = frameBuffer.position();
            frameBuffer.position(0);
            run.nextFrame(frameBuffer);
            frameBuffer.position(pos);
            runLastFrameId = frameId;
        }

        chunkEndIdx.add(tEndIdx);
        partitionLength += tEndIdx - tBeginIdx + 1;
    }

    @Override
    protected void endPartitionImpl() throws HyracksDataException {
        runningAggInitPartition(partitionLength);

        int nChunks = getPartitionChunkCount();
        if (nChunks == 1) {
            producePartitionTuples(0, null);
        } else {
            GeneratedRunFileReader reader = run.createReader();
            reader.open();
            try {
                for (int chunkIdx = 0; chunkIdx < nChunks; chunkIdx++) {
                    if (chunkIdx > 0) {
                        reader.nextFrame(curFrame);
                    }
                    producePartitionTuples(chunkIdx, reader);
                }
                curFrameId = runLastFrameId;
            } finally {
                reader.close();
            }
        }
    }

    protected void producePartitionTuples(int chunkIdx, GeneratedRunFileReader reader) throws HyracksDataException {
        tAccess.reset(curFrame.getBuffer());
        produceTuples(tAccess, getTupleBeginIdx(chunkIdx), getTupleEndIdx(chunkIdx));
    }

    int getPartitionChunkCount() {
        return chunkEndIdx.size();
    }

    int getTupleBeginIdx(int chunkIdx) {
        return chunkIdx == 0 ? chunkBeginIdx : 0;
    }

    int getTupleEndIdx(int chunkIdx) {
        return chunkEndIdx.get(chunkIdx);
    }
}
