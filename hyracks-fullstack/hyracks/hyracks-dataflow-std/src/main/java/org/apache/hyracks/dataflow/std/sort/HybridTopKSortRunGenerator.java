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

package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.buffermanager.FrameFreeSlotPolicyFactory;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFrameMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HybridTopKSortRunGenerator extends HeapSortRunGenerator {
    private static final Logger LOG = LogManager.getLogger();

    private static final int SWITCH_TO_FRAME_SORTER_THRESHOLD = 2;
    private IFrameSorter frameSorter = null;
    private int tupleSorterFlushedTimes = 0;

    public HybridTopKSortRunGenerator(IHyracksTaskContext ctx, int frameLimit, int topK, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(ctx, frameLimit, topK, sortFields, keyNormalizerFactories, comparatorFactories, recordDescriptor);
    }

    @Override
    public ISorter getSorter() {
        if (tupleSorter != null) {
            return tupleSorter;
        } else if (frameSorter != null) {
            return frameSorter;
        }
        return null;
    }

    @Override
    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file =
                ctx.getJobletContext().createManagedWorkspaceFile(HybridTopKSortRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIoManager());
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (topK <= 0) {
            return;
        }
        inAccessor.reset(buffer);
        if (tupleSorter != null) {
            boolean isBadK = false;
            for (int i = 0; i < inAccessor.getTupleCount(); i++) {
                if (!tupleSorter.insertTuple(inAccessor, i)) {
                    flushFramesToRun();
                    isBadK = true;
                    if (!tupleSorter.insertTuple(inAccessor, i)) {
                        throw new HyracksDataException("The given tuple is too big to insert into the sorting memory.");
                    }
                }
            }
            if (isBadK) {
                tupleSorterFlushedTimes++;
                if (tupleSorterFlushedTimes > SWITCH_TO_FRAME_SORTER_THRESHOLD) {
                    if (tupleSorter.hasRemaining()) {
                        flushFramesToRun();
                    }
                    tupleSorter.close();
                    tupleSorter = null;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("clear tupleSorter");
                    }
                }
            }
        } else {
            if (frameSorter == null) {
                VariableFrameMemoryManager bufferManager = new VariableFrameMemoryManager(
                        new VariableFramePool(ctx, (frameLimit - 1) * ctx.getInitialFrameSize()),
                        FrameFreeSlotPolicyFactory.createFreeSlotPolicy(EnumFreeSlotPolicy.BIGGEST_FIT,
                                frameLimit - 1));
                frameSorter = new FrameSorterMergeSort(ctx, bufferManager, frameLimit - 1, sortFields, nmkFactories,
                        comparatorFactories, recordDescriptor, topK);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("create frameSorter");
                }
            }
            if (!frameSorter.insertFrame(buffer)) {
                flushFramesToRun();
                if (!frameSorter.insertFrame(buffer)) {
                    throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
                }
            }
        }
    }
}
