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

package org.apache.hyracks.dataflow.std.sort.util;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IntSerDeUtils;

/**
 * This is a special frame which is used in TupleMemoryBuffer.
 * This frame has a special structure to organize the deleted spaces.
 * Specifically, the endOffset of the deleted tuple will be set as negative number.
 * And we add a special <code>deleted_space</code> field at the last 4 bytes to
 * remember how many bytes has been deleted.
 * The offsets also store both the start and end values because tuples may be out of
 * after several add, remove and reorganize operations.
 * A frame is formatted with tuple data concatenated starting at offset 0,
 * one tuple after another.
 * Offset FS - 4 holds an int indicating the amount of <code>deleted_space</code> in the frame.
 * Offset FS - 4 holds an int indicating the number of tuples (N) in the frame.
 * FS - ((i + 1) * (4 + 4)) for i from 0 to N - 1 holds an two ints indicating
 * the offset and length of the (i + 1)^th tuple.
 * The tuple references are organized as a sequence of ints indicating the start of the field
 * followed by the length of each tuple.
 * The offset if the start of the frame.
 * The tuple has been deleted if the length is 0.
 *
 * <pre>
 * [ *tuple_1_bytes*,
 *   *tuple_2_bytes*,
 *   ...
 *   int length, int offset, # tuple 2
 *   int length, int offset, # tuple 1
 *   int tuple_append,
 *   int next_index,
 *   int deleted_space,
 *   int index_count,
 * ]
 * </pre>
 *
 * <pre>
 * [ *tuple_1_bytes*,
 *   *tuple_2_bytes*,
 *   ...
 *   int end_offset,
 *   int offset, # tuple 2
 *   int offset, # tuple 1
 *   int deleted_space,
 *   int tuple_count,
 * ]
 * </pre>
 */
public class DeletableFrameTupleAppender implements IAppendDeletableFrameTupleAccessor {

    public static final Comparator<Pair<Integer, Integer>> INDEX_OFFSET_ASC_COMPARATOR = new Comparator<Pair<Integer, Integer>>() {
        @Override
        public int compare(Pair<Integer, Integer> p1, Pair<Integer, Integer> p2) {
            return p1.getValue() - p2.getValue();
        }

    };

    private static final int SIZE_INDEX_COUNT = 4;
    private static final int SIZE_DELETED_SPACE = 4;
    private static final int SIZE_NEXT_INDEX = 4;
    private static final int SIZE_TUPLE_APPEND = 4;

    private static final int SIZE_START_OFFSET = 4;
    private static final int SIZE_END_OFFSET = 4;
    private static final int SIZE_OFFSET_GROUP = SIZE_END_OFFSET + SIZE_START_OFFSET;

    private final RecordDescriptor recordDescriptor;
    private ByteBuffer buffer;
    private int indexSlotsOffset;
    private int indexCount;
    private int tupleAppend;
    private int deletedSpace;
    private int nextIndex;
    private byte[] array; // to speed up the array visit a little

    private final PriorityQueue<Pair<Integer, Integer>> reorganizeQueue;

    public DeletableFrameTupleAppender(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
        reorganizeQueue = new PriorityQueue<>(16, INDEX_OFFSET_ASC_COMPARATOR);
    }

    private int getIndexCount() {
        return IntSerDeUtils.getInt(array, getIndexCountOffset());
    }

    private void setIndexCount(int count) {
        IntSerDeUtils.putInt(array, getIndexCountOffset(), count);
    }

    private int getIndexCountOffset() {
        return buffer.capacity() - SIZE_INDEX_COUNT;
    }

    private int getDeletedSpace() {
        return IntSerDeUtils.getInt(array, getDeletedSpaceOffset());
    }

    private void setDeletedSpace(int space) {
        IntSerDeUtils.putInt(array, getDeletedSpaceOffset(), space);
    }

    private int getDeletedSpaceOffset() {
        return getIndexCountOffset() - SIZE_DELETED_SPACE;
    }

    private int getNextIndex() {
        return IntSerDeUtils.getInt(array, getNextIndexOffset());
    }

    private void setNextIndex(int index) {
        IntSerDeUtils.putInt(array, getNextIndexOffset(), index);
    }

    private int getNextIndexOffset() {
        return getDeletedSpaceOffset() - SIZE_NEXT_INDEX;
    }

    private int getAndUpdateNextIndex() {
        int index = nextIndex;
        nextIndex = index + 1;
        while (nextIndex < indexCount) {
            if (getTupleEndOffset(nextIndex) <= 0) {
                break;
            }
            nextIndex++;
        }
        setNextIndex(nextIndex);
        return index;
    }

    private int getTupleAppend() {
        return IntSerDeUtils.getInt(array, getTupleAppendOffset());
    }

    private void setTupleAppend(int offset) {
        IntSerDeUtils.putInt(array, getTupleAppendOffset(), offset);
    }

    private int getTupleAppendOffset() {
        return getNextIndexOffset() - SIZE_TUPLE_APPEND;
    }

    private int getIndexSlotOffset() {
        return getTupleAppendOffset();
    }

    @Override
    public void clear(ByteBuffer buffer) throws HyracksDataException {
        this.buffer = buffer;
        this.array = buffer.array();
        setIndexCount(0);
        setDeletedSpace(0);
        setNextIndex(0);
        setTupleAppend(0);
        resetCounts();
    }

    @Override
    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
        this.array = buffer.array();
        resetCounts();
    }

    private void resetCounts() {
        indexSlotsOffset = getIndexSlotOffset();
        deletedSpace = getDeletedSpace();
        indexCount = getIndexCount();
        tupleAppend = getTupleAppend();
        nextIndex = getNextIndex();
    }

    /**
     * Append the record into the frame. This method will not validate the space, please make sure space is enough
     * by calling {@link #getContiguousFreeSpace()}
     *
     * @param tupleAccessor
     * @param tIndex
     * @return
     * @throws HyracksDataException
     */
    @Override
    public int append(IFrameTupleAccessor tupleAccessor, int tIndex) throws HyracksDataException {
        byte[] src = tupleAccessor.getBuffer().array();
        int tStartOffset = tupleAccessor.getTupleStartOffset(tIndex);
        int length = tupleAccessor.getTupleLength(tIndex);
        System.arraycopy(src, tStartOffset, array, tupleAppend, length);
        int index = getAndUpdateNextIndex();
        if (index < indexCount) {
            // Don't change index count
        } else {
            // Increment count
            setIndexCount(++indexCount);
        }
        setTupleOffsets(index, tupleAppend, length);
        tupleAppend += length;
        setTupleAppend(tupleAppend);
        return index;
    }

    @Override
    public void delete(int tupleIndex) {
        int endOffset = getTupleEndOffset(tupleIndex);
        if (endOffset > 0) {
            setTupleEndOffset(tupleIndex, -endOffset);
            deletedSpace += endOffset - getTupleStartOffset(tupleIndex);
            setDeletedSpace(deletedSpace);
            if (nextIndex > tupleIndex) {
                nextIndex = tupleIndex;
                setNextIndex(nextIndex);
            }
        }
    }

    @Override
    public void reOrganizeBuffer() {
        if (deletedSpace <= 0) {
            return;
        }
        reclaimDeletedEnding();

        // Build reorganize queue
        int endOffset;
        int startOffset;
        for (int i = 0; i < indexCount; i++) {
            endOffset = getTupleEndOffset(i);
            if (endOffset > 0) {
                reorganizeQueue.add(new ImmutablePair<Integer, Integer>(i, getTupleStartOffset(i)));
            }
        }

        int index;
        tupleAppend = 0;
        while (!reorganizeQueue.isEmpty()) {
            index = reorganizeQueue.remove().getKey();
            startOffset = getTupleStartOffset(index);
            endOffset = getTupleEndOffset(index);
            if (endOffset >= 0) {
                int length = endOffset - startOffset;
                assert length >= 0;
                if (tupleAppend != startOffset) {
                    System.arraycopy(array, startOffset, array, tupleAppend, length);
                }
                setTupleOffsets(index, tupleAppend, length);
                tupleAppend += length;
            }
        }
        setTupleAppend(tupleAppend);
        deletedSpace = 0;
        setDeletedSpace(0);

        // Clean up
        reorganizeQueue.clear();
    }

    private void reclaimDeletedEnding() {
        for (int i = indexCount - 1; i >= 0; i--) {
            int endOffset = getTupleEndOffset(i);
            if (endOffset <= 0) {
                indexCount--;
            } else {
                break;
            }
        }
        setIndexCount(indexCount);
        if (nextIndex > indexCount) {
            setNextIndex(indexCount);
        }
    }

    @Override
    public int getTotalFreeSpace() {
        return getContiguousFreeSpace() + deletedSpace;
    }

    @Override
    public int getContiguousFreeSpace() {
        int slotSpace = indexCount * SIZE_OFFSET_GROUP;
        return indexSlotsOffset - tupleAppend - slotSpace;
    }

    @Override
    public int getFieldCount() {
        return recordDescriptor.getFieldCount();
    }

    @Override
    public int getFieldSlotsLength() {
        return recordDescriptor.getFieldCount() * 4;
    }

    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return IntSerDeUtils.getInt(array, getTupleStartOffset(tupleIndex) + fIdx * 4);
    }

    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return fIdx == 0 ? 0 : IntSerDeUtils.getInt(array, getTupleStartOffset(tupleIndex) + (fIdx - 1) * 4);
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + getFieldSlotsLength() + getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return getFieldEndOffset(tupleIndex, fIdx) - getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        int endOffset = getTupleEndOffset(tupleIndex);
        assert endOffset > 0;
        return endOffset - getTupleStartOffset(tupleIndex);
    }

    private void setTupleOffsets(int tupleIndex, int start, int length) {
        setTupleStartOffset(tupleIndex, start);
        setTupleEndOffset(tupleIndex, start + length);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return IntSerDeUtils.getInt(array, getTupleEndSlotOffset(tupleIndex));
    }

    private void setTupleEndOffset(int tupleIndex, int offset) {
        IntSerDeUtils.putInt(array, getTupleEndSlotOffset(tupleIndex), offset);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return IntSerDeUtils.getInt(array, getTupleStartSlotOffset(tupleIndex));
    }

    public void setTupleStartOffset(int tupleIndex, int offset) {
        IntSerDeUtils.putInt(array, getTupleStartSlotOffset(tupleIndex), offset);
    }

    public int getTupleStartSlotOffset(int tupleIndex) {
        return indexSlotsOffset - SIZE_OFFSET_GROUP * tupleIndex - SIZE_START_OFFSET;
    }

    public int getTupleEndSlotOffset(int tupleIndex) {
        return getTupleStartSlotOffset(tupleIndex) - SIZE_END_OFFSET;
    }

    @Override
    public int getTupleCount() {
        return indexCount;
    }

    private int getLiveTupleCount() {
        int live = 0;
        for (int i = 0; i < indexCount; ++i) {
            int endOffset = getTupleEndOffset(i);
            if (endOffset > 0) {
                live++;
            }
        }
        return live;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void printStats(PrintStream ps) {
        if (getLiveTupleCount() == 0) {
            ps.print("");
        }
        ps.printf("(%d, %d)", getLiveTupleCount(), getIndexCount());
    }

}
