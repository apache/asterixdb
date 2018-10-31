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

package org.apache.hyracks.algebricks.runtime.operators.aggrun;

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.runtime.base.IRunningAggregateEvaluatorFactory;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

class MaterializingWindowPushRuntime extends AbstractWindowPushRuntime {

    private RunFileWriter run;

    private IntArrayList runInfo;

    private long partitionLength;

    private IFrame curFrame;

    private long curFrameId;

    private long runLastFrameId;

    MaterializingWindowPushRuntime(int[] outColumns, IRunningAggregateEvaluatorFactory[] aggFactories,
            int[] projectionList, int[] partitionColumnList, IBinaryComparatorFactory[] partitionComparatorFactories,
            IBinaryComparatorFactory[] orderComparatorFactories, IHyracksTaskContext ctx) {
        super(outColumns, aggFactories, projectionList, partitionColumnList, partitionComparatorFactories,
                orderComparatorFactories, ctx);
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
        runInfo = new IntArrayList(128, 128);
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
        runInfo.clear();
        partitionLength = 0;
        if (run != null) {
            run.rewind();
        }
    }

    @Override
    protected void partitionChunkImpl(long frameId, ByteBuffer frameBuffer, int tBeginIdx, int tEndIdx)
            throws HyracksDataException {
        boolean firstChunk = runInfo.isEmpty();
        runInfo.add(tBeginIdx);
        runInfo.add(tEndIdx);

        // save frame. first one to memory, remaining ones to the run file
        if (firstChunk || tBeginIdx == 0) {
            int pos = frameBuffer.position();
            frameBuffer.position(0);

            if (firstChunk) {
                if (frameId != curFrameId) {
                    curFrame.resize(curFrame.getMinSize() * FrameHelper.deserializeNumOfMinFrame(frameBuffer));
                    curFrame.getBuffer().clear();
                    curFrame.getBuffer().put(frameBuffer);
                    curFrameId = frameId;
                }
            } else {
                if (run == null) {
                    FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(getClass().getSimpleName());
                    run = new RunFileWriter(file, ctx.getIoManager());
                    run.open();
                }
                run.nextFrame(frameBuffer);
                runLastFrameId = frameId;
            }

            frameBuffer.position(pos);
        }

        partitionLength += tEndIdx - tBeginIdx + 1;
    }

    @Override
    protected void endPartitionImpl() throws HyracksDataException {
        aggInitPartition(partitionLength);
        GeneratedRunFileReader reader = null;
        try {
            boolean runRead = false;
            for (int idx = 0, ln = runInfo.size(); idx < ln; idx += 2) {
                int tBeginIdx = runInfo.get(idx);
                int tEndIdx = runInfo.get(idx + 1);
                if (tBeginIdx == 0 && idx > 0) {
                    if (reader == null) {
                        reader = run.createReader();
                        reader.open();
                    }
                    reader.nextFrame(curFrame);
                    runRead = true;
                }
                tAccess.reset(curFrame.getBuffer());
                produceTuples(tAccess, tBeginIdx, tEndIdx);
            }
            if (runRead) {
                curFrameId = runLastFrameId;
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
