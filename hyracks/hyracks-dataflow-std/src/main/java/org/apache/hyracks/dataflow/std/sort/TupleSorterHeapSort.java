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

import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.sort.buffermanager.ITupleBufferAccessor;
import org.apache.hyracks.dataflow.std.sort.buffermanager.ITupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.IResetableComparable;
import org.apache.hyracks.dataflow.std.structures.IResetableComparableFactory;
import org.apache.hyracks.dataflow.std.structures.MaxHeap;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class TupleSorterHeapSort implements ITupleSorter {

    private static final Logger LOGGER = Logger.getLogger(TupleSorterHeapSort.class.getName());

    class HeapEntryFactory implements IResetableComparableFactory<HeapEntry> {
        @Override
        public IResetableComparable<HeapEntry> createResetableComparable() {
            return new HeapEntry();
        }
    }

    class HeapEntry implements IResetableComparable<HeapEntry> {
        int nmk;
        TuplePointer tuplePointer;

        public HeapEntry() {
            tuplePointer = new TuplePointer();
            nmk = 0;
        }

        @Override
        public int compareTo(HeapEntry o) {
            if (nmk != o.nmk) {
                return ((((long) nmk) & 0xffffffffL) < (((long) o.nmk) & 0xffffffffL)) ? -1 : 1;
            }
            bufferAccessor1.reset(tuplePointer);
            bufferAccessor2.reset(o.tuplePointer);
            byte[] b1 = bufferAccessor1.getTupleBuffer().array();
            byte[] b2 = bufferAccessor2.getTupleBuffer().array();

            for (int f = 0; f < comparators.length; ++f) {
                int fIdx = sortFields[f];
                int s1 = bufferAccessor1.getAbsFieldStartOffset(fIdx);
                int l1 = bufferAccessor1.getFieldLength(fIdx);

                int s2 = bufferAccessor2.getAbsFieldStartOffset(fIdx);
                int l2 = bufferAccessor2.getFieldLength(fIdx);
                int c;
                try {
                    c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                } catch (HyracksDataException e) {
                    throw new IllegalStateException(e);
                }
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }

        public void reset(int nmkey) {
            nmk = nmkey;
        }

        @Override
        public void reset(HeapEntry other) {
            nmk = other.nmk;
            tuplePointer.reset(other.tuplePointer);
        }
    }

    private final ITupleBufferManager bufferManager;
    private final ITupleBufferAccessor bufferAccessor1;
    private final ITupleBufferAccessor bufferAccessor2;
    private final int topK;
    private final FrameTupleAppender outputAppender;
    private final IFrame outputFrame;
    private final int[] sortFields;
    private final INormalizedKeyComputer nkc;
    private final IBinaryComparator[] comparators;

    private HeapEntry maxEntry;
    private HeapEntry newEntry;

    private MaxHeap heap;
    private boolean isSorted;

    public TupleSorterHeapSort(IHyracksTaskContext ctx, ITupleBufferManager bufferManager, int topK, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories)
            throws HyracksDataException {
        this.bufferManager = bufferManager;
        this.bufferAccessor1 = bufferManager.getTupleAccessor();
        this.bufferAccessor2 = bufferManager.getTupleAccessor();
        this.topK = topK;
        this.outputFrame = new VSizeFrame(ctx);
        this.outputAppender = new FrameTupleAppender();
        this.sortFields = sortFields;
        this.nkc = firstKeyNormalizerFactory == null ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        this.heap = new MaxHeap(new HeapEntryFactory(), topK);
        this.maxEntry = new HeapEntry();
        this.newEntry = new HeapEntry();
        this.isSorted = false;
    }

    @Override
    public int getTupleCount() {
        return heap.getNumEntries();
    }

    @Override
    public boolean insertTuple(IFrameTupleAccessor frameTupleAccessor, int index) throws HyracksDataException {
        if (isSorted) {
            throw new HyracksDataException(
                    "The Heap haven't be reset after sorting, the order of using this class is not correct.");
        }
        int nmkey = getPNK(frameTupleAccessor, index);
        if (heap.getNumEntries() >= topK) {
            heap.peekMax(maxEntry);
            if (compareTuple(frameTupleAccessor, index, nmkey, maxEntry) >= 0) {
                return true;
            }
        }

        newEntry.reset(nmkey);
        if (!bufferManager.insertTuple(frameTupleAccessor, index, newEntry.tuplePointer)) {
            return false;
        }
        if (heap.getNumEntries() < topK) {
            heap.insert(newEntry);
        } else {
            bufferManager.deleteTuple(maxEntry.tuplePointer);
            heap.replaceMax(newEntry);
        }
        return true;
    }

    private int getPNK(IFrameTupleAccessor fta, int tIx) {
        if (nkc == null) {
            return 0;
        }
        int sfIdx = sortFields[0];
        return nkc.normalize(fta.getBuffer().array(), fta.getAbsoluteFieldStartOffset(tIx, sfIdx),
                fta.getFieldLength(tIx, sfIdx));
    }

    private int compareTuple(IFrameTupleAccessor frameTupleAccessor, int tid, int nmkey, HeapEntry maxEntry)
            throws HyracksDataException {
        if (nmkey != maxEntry.nmk) {
            return ((((long) nmkey) & 0xffffffffL) < (((long) maxEntry.nmk) & 0xffffffffL)) ? -1 : 1;
        }
        bufferAccessor2.reset(maxEntry.tuplePointer);
        byte[] b1 = frameTupleAccessor.getBuffer().array();
        byte[] b2 = bufferAccessor2.getTupleBuffer().array();

        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int s1 = frameTupleAccessor.getAbsoluteFieldStartOffset(tid, fIdx);
            int l1 = frameTupleAccessor.getFieldLength(tid, fIdx);

            int s2 = bufferAccessor2.getAbsFieldStartOffset(fIdx);
            int l2 = bufferAccessor2.getFieldLength(fIdx);
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @Override
    public boolean hasRemaining() {
        return getTupleCount() > 0;
    }

    @Override
    public void reset() throws HyracksDataException {
        bufferManager.reset();
        heap.reset();
        isSorted = false;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void sort() throws HyracksDataException {
        IResetableComparable[] entries = heap.getEntries();
        int count = heap.getNumEntries();
        Arrays.sort(entries, 0, count, entryComparator);
        isSorted = true;
    }

    private static final Comparator<IResetableComparable> entryComparator = new Comparator<IResetableComparable>() {
        @Override
        public int compare(IResetableComparable o1, IResetableComparable o2) {
            return o1.compareTo(o2);
        }
    };

    @Override
    public void close() {
        heap = null;
        bufferManager.close();
        isSorted = false;
    }

    @Override
    @SuppressWarnings("deprecation")
    public int flush(IFrameWriter writer) throws HyracksDataException {
        outputAppender.reset(outputFrame, true);
        int maxFrameSize = outputFrame.getFrameSize();
        int numEntries = heap.getNumEntries();
        IResetableComparable[] entries = heap.getEntries();
        int io = 0;
        for (int i = 0; i < numEntries; i++) {
            HeapEntry minEntry = (HeapEntry) entries[i];
            bufferAccessor1.reset(minEntry.tuplePointer);
            int flushed = FrameUtils
                    .appendToWriter(writer, outputAppender, bufferAccessor1.getTupleBuffer().array(),
                            bufferAccessor1.getTupleStartOffset(), bufferAccessor1.getTupleLength());
            if (flushed > 0) {
                maxFrameSize = Math.max(maxFrameSize, flushed);
                io++;
            }
        }
        maxFrameSize = Math.max(maxFrameSize, outputFrame.getFrameSize());
        outputAppender.flush(writer, true);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(
                    "Flushed records:" + numEntries + "; Flushed through " + (io + 1) + " frames");
        }
        return maxFrameSize;
    }

}
