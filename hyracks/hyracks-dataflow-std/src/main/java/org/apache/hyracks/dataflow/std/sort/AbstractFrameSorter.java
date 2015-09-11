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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.sort.buffermanager.IFrameBufferManager;

public abstract class AbstractFrameSorter implements IFrameSorter {

    protected Logger LOGGER = Logger.getLogger(AbstractFrameSorter.class.getName());
    static final int PTR_SIZE = 4;
    static final int ID_FRAMEID = 0;
    static final int ID_TUPLE_START = 1;
    static final int ID_TUPLE_END = 2;
    static final int ID_NORMAL_KEY = 3;

    protected final int[] sortFields;
    protected final IBinaryComparator[] comparators;
    protected final INormalizedKeyComputer nkc;
    protected final IFrameBufferManager bufferManager;
    protected final FrameTupleAccessor inputTupleAccessor;
    protected final IFrameTupleAppender outputAppender;
    protected final IFrame outputFrame;
    protected final int outputLimit;

    protected int[] tPointers;
    protected int tupleCount;

    public AbstractFrameSorter(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        this(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                Integer.MAX_VALUE);
    }

    public AbstractFrameSorter(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, int outputLimit)
            throws HyracksDataException {
        this.bufferManager = bufferManager;
        this.sortFields = sortFields;
        this.nkc = firstKeyNormalizerFactory == null ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.inputTupleAccessor = new FrameTupleAccessor(recordDescriptor);
        this.outputAppender = new FrameTupleAppender();
        this.outputFrame = new VSizeFrame(ctx);
        this.outputLimit = outputLimit;
    }

    @Override
    public void reset() throws HyracksDataException {
        this.tupleCount = 0;
        this.bufferManager.reset();
    }

    @Override
    public boolean insertFrame(ByteBuffer inputBuffer) throws HyracksDataException {
        if (bufferManager.insertFrame(inputBuffer) >= 0) {
            return true;
        }
        if (getFrameCount() == 0) {
            throw new HyracksDataException(
                    "The input frame is too big for the sorting buffer, please allocate bigger buffer size");
        }
        return false;
    }

    @Override
    public void sort() throws HyracksDataException {
        tupleCount = 0;
        for (int i = 0; i < bufferManager.getNumFrames(); ++i) {
            inputTupleAccessor
                    .reset(bufferManager.getFrame(i), bufferManager.getFrameStartOffset(i),
                            bufferManager.getFrameSize(i));
            tupleCount += inputTupleAccessor.getTupleCount();
        }
        if (tPointers == null || tPointers.length < tupleCount * PTR_SIZE) {
            tPointers = new int[tupleCount * PTR_SIZE];
        }
        int ptr = 0;
        int sfIdx = sortFields[0];
        for (int i = 0; i < bufferManager.getNumFrames(); ++i) {
            inputTupleAccessor
                    .reset(bufferManager.getFrame(i), bufferManager.getFrameStartOffset(i),
                            bufferManager.getFrameSize(i));
            int tCount = inputTupleAccessor.getTupleCount();
            byte[] array = inputTupleAccessor.getBuffer().array();
            for (int j = 0; j < tCount; ++j) {
                int tStart = inputTupleAccessor.getTupleStartOffset(j);
                int tEnd = inputTupleAccessor.getTupleEndOffset(j);
                tPointers[ptr * PTR_SIZE + ID_FRAMEID] = i;
                tPointers[ptr * PTR_SIZE + ID_TUPLE_START] = tStart;
                tPointers[ptr * PTR_SIZE + ID_TUPLE_END] = tEnd;
                int f0StartRel = inputTupleAccessor.getFieldStartOffset(j, sfIdx);
                int f0EndRel = inputTupleAccessor.getFieldEndOffset(j, sfIdx);
                int f0Start = f0StartRel + tStart + inputTupleAccessor.getFieldSlotsLength();
                tPointers[ptr * PTR_SIZE + ID_NORMAL_KEY] =
                        nkc == null ? 0 : nkc.normalize(array, f0Start, f0EndRel - f0StartRel);
                ++ptr;
            }
        }
        if (tupleCount > 0) {
            sortTupleReferences();
        }
    }

    abstract void sortTupleReferences() throws HyracksDataException;

    @Override
    public int getFrameCount() {
        return bufferManager.getNumFrames();
    }

    @Override
    public boolean hasRemaining() {
        return getFrameCount() > 0;
    }

    @Override
    public int flush(IFrameWriter writer) throws HyracksDataException {
        outputAppender.reset(outputFrame, true);
        int maxFrameSize = outputFrame.getFrameSize();
        int limit = Math.min(tupleCount, outputLimit);
        int io = 0;
        for (int ptr = 0; ptr < limit; ++ptr) {
            int i = tPointers[ptr * PTR_SIZE + ID_FRAMEID];
            int tStart = tPointers[ptr * PTR_SIZE + ID_TUPLE_START];
            int tEnd = tPointers[ptr * PTR_SIZE + ID_TUPLE_END];
            ByteBuffer buffer = bufferManager.getFrame(i);
            inputTupleAccessor.reset(buffer, bufferManager.getFrameStartOffset(i), bufferManager.getFrameSize(i));

            int flushed = FrameUtils.appendToWriter(writer, outputAppender, inputTupleAccessor, tStart, tEnd);
            if (flushed > 0) {
                maxFrameSize = Math.max(maxFrameSize, flushed);
                io++;
            }
        }
        maxFrameSize = Math.max(maxFrameSize, outputFrame.getFrameSize());
        outputAppender.flush(writer, true);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "Flushed records:" + limit + " out of " + tupleCount + "; Flushed through " + (io + 1) + " frames");
        }
        return maxFrameSize;
    }

    @Override
    public void close() {
        tupleCount = 0;
        bufferManager.close();
        tPointers = null;
    }
}
