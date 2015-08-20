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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.EnumFreeSlotPolicy;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.FrameFreeSlotSmallestFit;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.FrameFreeSlotBiggestFirst;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.FrameFreeSlotLastFit;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.IFrameBufferManager;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.IFrameFreeSlotPolicy;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableFrameMemoryManager;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableFramePool;

public class ExternalSortRunGenerator extends AbstractSortRunGenerator {

    protected final IHyracksTaskContext ctx;
    protected final IFrameSorter frameSorter;
    protected final int maxSortFrames;

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, int framesLimit) throws HyracksDataException {
        this(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDesc, alg,
                EnumFreeSlotPolicy.LAST_FIT, framesLimit);
    }

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit)
            throws HyracksDataException {
        this(ctx, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDesc, alg, policy, framesLimit,
                Integer.MAX_VALUE);
    }

    public ExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, Algorithm alg, EnumFreeSlotPolicy policy, int framesLimit, int outputLimit)
            throws HyracksDataException {
        this.ctx = ctx;
        maxSortFrames = framesLimit - 1;

        IFrameFreeSlotPolicy freeSlotPolicy = null;
        switch (policy) {
            case SMALLEST_FIT:
                freeSlotPolicy = new FrameFreeSlotSmallestFit();
                break;
            case LAST_FIT:
                freeSlotPolicy = new FrameFreeSlotLastFit(maxSortFrames);
                break;
            case BIGGEST_FIT:
                freeSlotPolicy = new FrameFreeSlotBiggestFirst(maxSortFrames);
                break;
        }
        IFrameBufferManager bufferManager = new VariableFrameMemoryManager(
                new VariableFramePool(ctx, maxSortFrames * ctx.getInitialFrameSize()), freeSlotPolicy);
        if (alg == Algorithm.MERGE_SORT) {
            frameSorter = new FrameSorterMergeSort(ctx, bufferManager, sortFields, firstKeyNormalizerFactory,
                    comparatorFactories, recordDesc, outputLimit);
        } else {
            frameSorter = new FrameSorterQuickSort(ctx, bufferManager, sortFields, firstKeyNormalizerFactory,
                    comparatorFactories, recordDesc, outputLimit);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        if (!frameSorter.insertFrame(buffer)) {
            flushFramesToRun();
            if (!frameSorter.insertFrame(buffer)) {
                throw new HyracksDataException("The given frame is too big to insert into the sorting memory.");
            }
        }
    }

    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                ExternalSortRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIOManager());
    }

    protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException {
        return writer;
    }

    @Override
    public ISorter getSorter() {
        return frameSorter;
    }

}