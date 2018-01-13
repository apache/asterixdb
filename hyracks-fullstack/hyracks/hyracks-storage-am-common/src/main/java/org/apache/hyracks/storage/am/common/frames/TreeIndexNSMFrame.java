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

package org.apache.hyracks.storage.am.common.frames;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.ophelpers.SlotOffTupleOff;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public abstract class TreeIndexNSMFrame implements ITreeIndexFrame {

    protected static final int PAGE_LSN_OFFSET = Constants.RESERVED_HEADER_SIZE;
    protected static final int TOTAL_FREE_SPACE_OFFSET = PAGE_LSN_OFFSET + 8;
    protected static final int FLAG_OFFSET = TOTAL_FREE_SPACE_OFFSET + 4;
    protected static final int RESERVED_HEADER_SIZE = FLAG_OFFSET + 1;
    protected static final byte SMALL_FLAG_BIT = 0x1;
    protected static final byte LARGE_FLAG_BIT = 0x2;

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;
    protected ISlotManager slotManager;

    protected ITreeIndexTupleWriter tupleWriter;
    protected ITreeIndexTupleReference frameTuple;

    public TreeIndexNSMFrame(ITreeIndexTupleWriter tupleWriter, ISlotManager slotManager) {
        this.tupleWriter = tupleWriter;
        this.frameTuple = tupleWriter.createTupleReference();
        this.slotManager = slotManager;
        this.slotManager.setFrame(this);
    }

    @Override
    public void initBuffer(byte level) {
        buf.putLong(PAGE_LSN_OFFSET, 0); // TODO: might to set to a different lsn
        // during creation
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, 0);
        resetSpaceParams();
        buf.put(Constants.LEVEL_OFFSET, level);
        buf.put(FLAG_OFFSET, (byte) 0);
    }

    @Override
    public int getMaxTupleSize(int pageSize) {
        return (pageSize - getPageHeaderSize()) / 2;
    }

    @Override
    public boolean isLeaf() {
        return buf.get(Constants.LEVEL_OFFSET) == 0;
    }

    public boolean getSmFlag() {
        return (buf.get(FLAG_OFFSET) & SMALL_FLAG_BIT) != 0;
    }

    public void setSmFlag(boolean smFlag) {
        if (smFlag) {
            buf.put(FLAG_OFFSET, (byte) (buf.get(FLAG_OFFSET) | SMALL_FLAG_BIT));
        } else {
            buf.put(FLAG_OFFSET, (byte) (buf.get(FLAG_OFFSET) & ~SMALL_FLAG_BIT));
        }
    }

    public void setLargeFlag(boolean largeFlag) {
        if (largeFlag) {
            buf.put(FLAG_OFFSET, (byte) (buf.get(FLAG_OFFSET) | LARGE_FLAG_BIT));
        } else {
            buf.put(FLAG_OFFSET, (byte) (buf.get(FLAG_OFFSET) & ~LARGE_FLAG_BIT));
        }
    }

    public boolean getLargeFlag() {
        return (buf.get(FLAG_OFFSET) & LARGE_FLAG_BIT) != 0;
    }

    @Override
    public boolean isInterior() {
        return buf.get(Constants.LEVEL_OFFSET) > 0;
    }

    @Override
    public byte getLevel() {
        return buf.get(Constants.LEVEL_OFFSET);
    }

    @Override
    public void setLevel(byte level) {
        buf.put(Constants.LEVEL_OFFSET, level);
    }

    @Override
    public int getFreeSpaceOff() {
        return buf.getInt(Constants.FREE_SPACE_OFFSET);
    }

    @Override
    public void setFreeSpaceOff(int freeSpace) {
        buf.putInt(Constants.FREE_SPACE_OFFSET, freeSpace);
    }

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();
    }

    @Override
    public ByteBuffer getBuffer() {
        return page.getBuffer();
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    @Override
    public boolean compact() {
        resetSpaceParams();
        int tupleCount = buf.getInt(Constants.TUPLE_COUNT_OFFSET);
        int freeSpace = buf.getInt(Constants.FREE_SPACE_OFFSET);
        // Sort the slots by the tuple offset they point to.
        ArrayList<SlotOffTupleOff> sortedTupleOffs = new ArrayList<>();
        sortedTupleOffs.ensureCapacity(tupleCount);
        for (int i = 0; i < tupleCount; i++) {
            int slotOff = slotManager.getSlotOff(i);
            int tupleOff = slotManager.getTupleOff(slotOff);
            sortedTupleOffs.add(new SlotOffTupleOff(i, slotOff, tupleOff));
        }
        Collections.sort(sortedTupleOffs);
        // Iterate over the sorted slots, and move their corresponding tuples to
        // the left, reclaiming free space.
        for (int i = 0; i < sortedTupleOffs.size(); i++) {
            int tupleOff = sortedTupleOffs.get(i).tupleOff;
            frameTuple.resetByTupleOffset(buf.array(), tupleOff);
            int tupleEndOff = frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                    + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1);
            int tupleLength = tupleEndOff - tupleOff;
            System.arraycopy(buf.array(), tupleOff, buf.array(), freeSpace, tupleLength);
            slotManager.setSlot(sortedTupleOffs.get(i).slotOff, freeSpace);
            freeSpace += tupleLength;
        }
        // Update contiguous free space pointer and total free space indicator.
        buf.putInt(Constants.FREE_SPACE_OFFSET, freeSpace);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET, buf.capacity() - freeSpace - tupleCount * slotManager.getSlotSize());
        return false;
    }

    @Override
    public void delete(ITupleReference tuple, int tupleIndex) {
        int slotOff = slotManager.getSlotOff(tupleIndex);
        int tupleOff = slotManager.getTupleOff(slotOff);
        frameTuple.resetByTupleOffset(buf.array(), tupleOff);
        int tupleSize = tupleWriter.bytesRequired(frameTuple);

        // perform deletion (we just do a memcpy to overwrite the slot)
        int slotStartOff = slotManager.getSlotEndOff();
        int length = slotOff - slotStartOff;
        System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);

        // maintain space information
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) - 1);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) + tupleSize + slotManager.getSlotSize());
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException {
        int bytesRequired = tupleWriter.bytesRequired(tuple);
        // Enough space in the contiguous space region?
        if (bytesRequired + slotManager.getSlotSize() <= buf.capacity() - buf.getInt(Constants.FREE_SPACE_OFFSET)
                - (buf.getInt(Constants.TUPLE_COUNT_OFFSET) * slotManager.getSlotSize())) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }
        // Enough space after compaction?
        if (bytesRequired + slotManager.getSlotSize() <= buf.getInt(TOTAL_FREE_SPACE_OFFSET)) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        }
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    @Override
    public FrameOpSpaceStatus hasSpaceUpdate(ITupleReference newTuple, int oldTupleIndex) {
        frameTuple.resetByTupleIndex(this, oldTupleIndex);
        int oldTupleBytes = frameTuple.getTupleSize();
        int newTupleBytes = tupleWriter.bytesRequired(newTuple);
        return hasSpaceUpdate(oldTupleBytes, newTupleBytes);
    }

    protected FrameOpSpaceStatus hasSpaceUpdate(int oldTupleBytes, int newTupleBytes) {
        int additionalBytesRequired = newTupleBytes - oldTupleBytes;
        // Enough space for an in-place update?
        if (additionalBytesRequired <= 0) {
            return FrameOpSpaceStatus.SUFFICIENT_INPLACE_SPACE;
        }
        // Enough space if we delete the old tuple and insert the new one
        // without compaction?
        if (newTupleBytes <= buf.capacity() - buf.getInt(Constants.FREE_SPACE_OFFSET)
                - (buf.getInt(Constants.TUPLE_COUNT_OFFSET) * slotManager.getSlotSize())) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }
        // Enough space if we delete the old tuple and compact?
        if (additionalBytesRequired <= buf.getInt(TOTAL_FREE_SPACE_OFFSET)) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        }
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    protected void resetSpaceParams() {
        buf.putInt(Constants.FREE_SPACE_OFFSET, getPageHeaderSize());
        buf.putInt(TOTAL_FREE_SPACE_OFFSET, buf.capacity() - getPageHeaderSize());
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        slotManager.insertSlot(tupleIndex, buf.getInt(Constants.FREE_SPACE_OFFSET));
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), buf.getInt(Constants.FREE_SPACE_OFFSET));
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) + 1);
        buf.putInt(Constants.FREE_SPACE_OFFSET, buf.getInt(Constants.FREE_SPACE_OFFSET) + bytesWritten);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void update(ITupleReference newTuple, int oldTupleIndex, boolean inPlace) {
        frameTuple.resetByTupleIndex(this, oldTupleIndex);
        int oldTupleBytes = frameTuple.getTupleSize();
        int slotOff = slotManager.getSlotOff(oldTupleIndex);
        int bytesWritten = 0;
        if (inPlace) {
            // Overwrite the old tuple in place.
            bytesWritten = tupleWriter.writeTuple(newTuple, buf.array(), buf.getInt(slotOff));
        } else {
            // Insert the new tuple at the end of the free space, and change the
            // slot value (effectively "deleting" the old tuple).
            int newTupleOff = buf.getInt(Constants.FREE_SPACE_OFFSET);
            bytesWritten = tupleWriter.writeTuple(newTuple, buf.array(), newTupleOff);
            // Update slot value.
            buf.putInt(slotOff, newTupleOff);
            // Update contiguous free space pointer.
            buf.putInt(Constants.FREE_SPACE_OFFSET, newTupleOff + bytesWritten);
        }
        buf.putInt(TOTAL_FREE_SPACE_OFFSET, buf.getInt(TOTAL_FREE_SPACE_OFFSET) + oldTupleBytes - bytesWritten);
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("pageLsnOff:        " + PAGE_LSN_OFFSET + "\n");
        strBuilder.append("tupleCountOff:     " + Constants.TUPLE_COUNT_OFFSET + "\n");
        strBuilder.append("freeSpaceOff:      " + Constants.FREE_SPACE_OFFSET + "\n");
        strBuilder.append("totalFreeSpaceOff: " + TOTAL_FREE_SPACE_OFFSET + "\n");
        strBuilder.append("levelOff:          " + Constants.LEVEL_OFFSET + "\n");
        strBuilder.append("flagOff:           " + FLAG_OFFSET + "\n");
        return strBuilder.toString();
    }

    @Override
    public int getTupleCount() {
        return buf.getInt(Constants.TUPLE_COUNT_OFFSET);
    }

    @Override
    public ISlotManager getSlotManager() {
        return slotManager;
    }

    @Override
    public int getTupleOffset(int slotNum) {
        return slotManager.getTupleOff(slotManager.getSlotStartOff() - slotNum * slotManager.getSlotSize());
    }

    @Override
    public long getPageLsn() {
        return buf.getLong(PAGE_LSN_OFFSET);
    }

    @Override
    public void setPageLsn(long pageLsn) {
        buf.putLong(PAGE_LSN_OFFSET, pageLsn);
    }

    @Override
    public int getTotalFreeSpace() {
        return buf.getInt(TOTAL_FREE_SPACE_OFFSET);
    }

    @Override
    public boolean compress() {
        return false;
    }

    @Override
    public int getSlotSize() {
        return slotManager.getSlotSize();
    }

    @Override
    public ITreeIndexTupleWriter getTupleWriter() {
        return tupleWriter;
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    public int getFreeContiguousSpace() {
        return buf.capacity() - getFreeSpaceOff() - (getTupleCount() * slotManager.getSlotSize());
    }

    public ITupleReference getLeftmostTuple() {
        int tupleCount = getTupleCount();
        if (tupleCount == 0) {
            return null;
        } else {
            frameTuple.resetByTupleIndex(this, 0);
            return frameTuple;
        }
    }

    public ITupleReference getRightmostTuple() {
        int tupleCount = getTupleCount();
        if (tupleCount == 0) {
            return null;
        } else {
            frameTuple.resetByTupleIndex(this, tupleCount - 1);
            return frameTuple;
        }
    }
}
