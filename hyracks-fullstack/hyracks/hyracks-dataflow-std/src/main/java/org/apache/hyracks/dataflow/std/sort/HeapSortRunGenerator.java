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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;

public class HeapSortRunGenerator extends AbstractSortRunGenerator {
    protected final IHyracksTaskContext ctx;
    protected final int frameLimit;
    protected final int topK;
    protected final int[] sortFields;
    protected final INormalizedKeyComputerFactory[] nmkFactories;
    protected final IBinaryComparatorFactory[] comparatorFactories;
    protected final RecordDescriptor recordDescriptor;
    protected ITupleSorter tupleSorter;
    protected final IFrameTupleAccessor inAccessor;

    public HeapSortRunGenerator(IHyracksTaskContext ctx, int frameLimit, int topK, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super();
        this.ctx = ctx;
        this.frameLimit = frameLimit;
        this.topK = topK;
        this.sortFields = sortFields;
        this.nmkFactories = keyNormalizerFactories;
        this.comparatorFactories = comparatorFactories;
        this.inAccessor = new FrameTupleAccessor(recordDescriptor);
        this.recordDescriptor = recordDescriptor;
    }

    @Override
    public void open() throws HyracksDataException {
        IFramePool framePool = new VariableFramePool(ctx, (frameLimit - 1) * ctx.getInitialFrameSize());
        IDeletableTupleBufferManager bufferManager =
                new VariableDeletableTupleMemoryManager(framePool, recordDescriptor);
        tupleSorter = new TupleSorterHeapSort(ctx, bufferManager, topK, sortFields, nmkFactories, comparatorFactories);
        super.open();
    }

    @Override
    public ISorter getSorter() {
        return tupleSorter;
    }

    @Override
    protected RunFileWriter getRunFileWriter() throws HyracksDataException {
        FileReference file =
                ctx.getJobletContext().createManagedWorkspaceFile(HeapSortRunGenerator.class.getSimpleName());
        return new RunFileWriter(file, ctx.getIoManager());
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
