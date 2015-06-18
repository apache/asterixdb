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

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.IFramePool;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.ITupleBufferManager;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableFramePool;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableTupleMemoryManager;

public class HeapSortRunGenerator extends AbstractSortRunGenerator {
    protected final IHyracksTaskContext ctx;
    protected final int frameLimit;
    protected final int topK;
    protected final int[] sortFields;
    protected final INormalizedKeyComputerFactory nmkFactory;
    protected final IBinaryComparatorFactory[] comparatorFactories;
    protected final RecordDescriptor recordDescriptor;
    protected ITupleSorter tupleSorter;
    protected IFrameTupleAccessor inAccessor;

    public HeapSortRunGenerator(IHyracksTaskContext ctx, int frameLimit, int topK, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super();
        this.ctx = ctx;
        this.frameLimit = frameLimit;
        this.topK = topK;
        this.sortFields = sortFields;
        this.nmkFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        this.inAccessor = new FrameTupleAccessor(recordDescriptor);
        this.recordDescriptor = recordDescriptor;
    }

    @Override
    public void open() throws HyracksDataException {
        IFramePool framePool = new VariableFramePool(ctx, (frameLimit - 1) * ctx.getInitialFrameSize());
        ITupleBufferManager bufferManager = new VariableTupleMemoryManager(framePool, recordDescriptor);
        tupleSorter = new TupleSorterHeapSort(ctx, bufferManager, topK, sortFields, nmkFactory,
                comparatorFactories);
        super.open();
    }

    @Override
    public ISorter getSorter() throws HyracksDataException {
        return tupleSorter;
    }

    @Override
    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                HeapSortRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIOManager());
    }

    @Override
    protected IFrameWriter getFlushableFrameWriter(RunFileWriter writer) throws HyracksDataException {
        return writer;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inAccessor.reset(buffer);
        for (int i = 0; i < inAccessor.getTupleCount(); i++) {
            if (!tupleSorter.insertTuple(inAccessor, i)) {
                flushFramesToRun();
                if (!tupleSorter.insertTuple(inAccessor, i)) {
                    throw new HyracksDataException("The given tuple is too big to insert into the sorting memory.");
                }
            }
        }
    }

}
