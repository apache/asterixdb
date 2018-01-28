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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.IntSerDeUtils;

/**
 * This is a special frame which is used in TupleMemoryBuffer.
 * This frame has a special structure to organize the deleted spaces.
 * Specifically, the endOffset of the deleted tuple will be set as negative number.
 * And we add a special <code>deleted_space</code> field at the last 4 bytes to remember how many bytes has been deleted.
 */
public class DeletableFrameTupleAppender implements IAppendDeletableFrameTupleAccessor {

    private static final int SIZE_DELETED_SPACE = 4;
    private final RecordDescriptor recordDescriptor;
    private ByteBuffer buffer;
    private int tupleCountOffset;
    private int tupleCount;
    private int freeDataEndOffset;
    private int deletedSpace;
    private byte[] array; // to speed up the array visit a little

    public DeletableFrameTupleAppender(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
    }

    private int getTupleCountOffset() {
        return FrameHelper.getTupleCountOffset(buffer.capacity()) - SIZE_DELETED_SPACE;
    }

    private int getFreeDataEndOffset() {
        return tupleCount == 0 ? 0 : Math.abs(IntSerDeUtils.getInt(array, tupleCountOffset - tupleCount * 4));
    }

    private void setFreeDataEndOffset(int offset) {
        assert (offset >= 0);
        IntSerDeUtils.putInt(array, tupleCountOffset - tupleCount * 4, offset);
    }

    private void setTupleCount(int count) {
        IntSerDeUtils.putInt(array, tupleCountOffset, count);
    }

    private void setDeleteSpace(int count) {
        IntSerDeUtils.putInt(array, buffer.capacity() - SIZE_DELETED_SPACE, count);
    }

    private int getPhysicalTupleCount() {
        return IntSerDeUtils.getInt(array, tupleCountOffset);
    }

    private int getDeletedSpace() {
        return IntSerDeUtils.getInt(array, buffer.capacity() - SIZE_DELETED_SPACE);
    }

    @Override
    public void clear(ByteBuffer buffer) throws HyracksDataException {
        this.buffer = buffer;
        this.array = buffer.array();
        tupleCountOffset = getTupleCountOffset();
        setTupleCount(0);
        setDeleteSpace(0);
        resetCounts();
    }

    @Override
    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
        this.array = buffer.array();
        tupleCountOffset = getTupleCountOffset();
        resetCounts();
    }

    private void resetCounts() {
        deletedSpace = getDeletedSpace();
        tupleCount = getPhysicalTupleCount();
        freeDataEndOffset = getFreeDataEndOffset();
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
        System.arraycopy(src, tStartOffset, array, freeDataEndOffset, length);
        setTupleCount(++tupleCount);
        freeDataEndOffset += length;
        setFreeDataEndOffset(freeDataEndOffset);
        return tupleCount - 1;
    }

    @Override
    public void delete(int tupleIndex) {
        int endOffset = getTupleEndOffset(tupleIndex);
        if (endOffset > 0) {
            setTupleEndOffset(tupleIndex, -endOffset);
            deletedSpace += endOffset - getTupleStartOffset(tupleIndex);
            setDeleteSpace(deletedSpace);
        }
    }

    @Override
    public void reOrganizeBuffer() {
        if (deletedSpace <= 0) {
            return;
        }
        reclaimDeletedEnding();

        freeDataEndOffset = 0;
        int endOffset = 0;
        for (int i = 0; i < tupleCount; i++) {
            int startOffset = Math.abs(endOffset);
            endOffset = getTupleEndOffset(i);
            if (endOffset >= 0) {
                int length = endOffset - startOffset;
                assert (length >= 0);
                if (freeDataEndOffset != startOffset) {
                    System.arraycopy(array, startOffset, array, freeDataEndOffset, length);
                }
                freeDataEndOffset += length;
            }
            setTupleEndOffset(i, freeDataEndOffset);
        }
        setFreeDataEndOffset(freeDataEndOffset);
        deletedSpace = 0;
        setDeleteSpace(0);
    }

    private void reclaimDeletedEnding() {
        for (int i = tupleCount - 1; i >= 0; i--) {
            int endOffset = getTupleEndOffset(i);
            if (endOffset < 0) {
                tupleCount--;
            } else {
                break;
            }
        }
        setTupleCount(tupleCount);
    }

    @Override
    public int getTotalFreeSpace() {
        return getContiguousFreeSpace() + deletedSpace;
    }

    @Override
    public int getContiguousFreeSpace() {
        return getTupleCountOffset() - tupleCount * 4 - freeDataEndOffset;
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
    public int getFieldLength(int tupleIndex, int fIdx) {
        return getFieldEndOffset(tupleIndex, fIdx) - getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleLength(int tupleIndex) {
        int endOffset = getTupleEndOffset(tupleIndex);
        if (endOffset < 0) {
            return endOffset + getTupleStartOffset(tupleIndex);
        }
        return endOffset - getTupleStartOffset(tupleIndex);
    }

    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return IntSerDeUtils.getInt(array, tupleCountOffset - 4 * (tupleIndex + 1));
    }

    private void setTupleEndOffset(int tupleIndex, int offset) {
        IntSerDeUtils.putInt(array, tupleCountOffset - 4 * (tupleIndex + 1), offset);
    }

    @Override
    public int getTupleStartOffset(int tupleIndex) {
        int offset = tupleIndex == 0 ? 0 : IntSerDeUtils.getInt(array, tupleCountOffset - 4 * tupleIndex);
        return Math.abs(offset);
    }

    @Override
    public int getAbsoluteFieldStartOffset(int tupleIndex, int fIdx) {
        return getTupleStartOffset(tupleIndex) + getFieldSlotsLength() + getFieldStartOffset(tupleIndex, fIdx);
    }

    @Override
    public int getTupleCount() {
        return tupleCount;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

}
