/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.FrameFreeSlotBiggestFirst;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableFrameMemoryManager;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableFramePool;

public class HybridTopKSortRunGenerator extends HeapSortRunGenerator {
    private static final Logger LOG = Logger.getLogger(HybridTopKSortRunGenerator.class.getName());

    private static final int SWITCH_TO_FRAME_SORTER_THRESHOLD = 2;
    private IFrameSorter frameSorter = null;
    private int tupleSorterFlushedTimes = 0;

    public HybridTopKSortRunGenerator(IHyracksTaskContext ctx, int frameLimit, int topK, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(ctx, frameLimit, topK, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor);
    }

    @Override
    public ISorter getSorter() throws HyracksDataException {
        if (tupleSorter != null) {
            return tupleSorter;
        } else if (frameSorter != null) {
            return frameSorter;
        }
        return null;
    }

    @Override
    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                HybridTopKSortRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIOManager());
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
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
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine("clear tupleSorter");
                    }
                }
            }
        } else {
            if (frameSorter == null) {
                VariableFrameMemoryManager bufferManager = new VariableFrameMemoryManager(
                        new VariableFramePool(ctx, (frameLimit - 1) * ctx.getInitialFrameSize()),
                        new FrameFreeSlotBiggestFirst(frameLimit - 1));
                frameSorter = new FrameSorterMergeSort(ctx, bufferManager, sortFields, nmkFactory, comparatorFactories,
                        recordDescriptor, topK);
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("create frameSorter");
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
