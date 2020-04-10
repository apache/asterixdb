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

package org.apache.hyracks.storage.am.btree.frames;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

public class BTreeNSMLeafFrame extends TreeIndexNSMFrame implements IBTreeLeafFrame {
    protected static final int NEXT_LEAF_OFFSET = TreeIndexNSMFrame.RESERVED_HEADER_SIZE;

    private MultiComparator cmp;

    private final ITreeIndexTupleReference previousFt;

    public BTreeNSMLeafFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
        previousFt = tupleWriter.createTupleReference();
    }

    @Override
    public int getPageHeaderSize() {
        return NEXT_LEAF_OFFSET + 4;
    }

    @Override
    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + slotManager.getSlotSize();
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(NEXT_LEAF_OFFSET, -1);
    }

    @Override
    public void setNextLeaf(int page) {
        buf.putInt(NEXT_LEAF_OFFSET, page);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(NEXT_LEAF_OFFSET);
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        int tupleIndex;
        tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXCLUSIVE_ERROR_IF_EXISTS,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        // Error indicator is set if there is an exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
        }
        return tupleIndex;
    }

    @Override
    public int findUpdateTupleIndex(ITupleReference tuple) throws HyracksDataException {
        int tupleIndex;
        tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXACT,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator() || tupleIndex == slotManager.getGreatestKeyIndicator()) {
            throw HyracksDataException.create(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY);
        }
        return tupleIndex;
    }

    @Override
    public int findUpsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        int tupleIndex;
        tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.INCLUSIVE,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        // Just return the found tupleIndex. The caller will make the final
        // decision whether to insert or update.
        return tupleIndex;
    }

    @Override
    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple, int targetTupleIndex)
            throws HyracksDataException {
        // Examine the tuple index to determine whether it is valid or not.
        if (targetTupleIndex != slotManager.getGreatestKeyIndicator()) {
            // We need to check the key to determine whether it's an insert or
            // an update/delete
            frameTuple.resetByTupleIndex(this, targetTupleIndex);
            if (cmp.compare(searchTuple, frameTuple) == 0) {
                // The keys match, it's an update/delete
                return frameTuple;
            }
        }
        // Either the tuple index is a special indicator, or the keys don't
        // match.
        // In those cases, we are definitely dealing with an insert.
        return null;
    }

    @Override
    public int findDeleteTupleIndex(ITupleReference tuple) throws HyracksDataException {
        int tupleIndex;
        tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXACT,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator() || tupleIndex == slotManager.getGreatestKeyIndicator()) {
            throw HyracksDataException.create(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY);
        }
        return tupleIndex;
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        int freeSpace = buf.getInt(Constants.FREE_SPACE_OFFSET);
        slotManager.insertSlot(tupleIndex, freeSpace);
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), freeSpace);
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) + 1);
        buf.putInt(Constants.FREE_SPACE_OFFSET, buf.getInt(Constants.FREE_SPACE_OFFSET) + bytesWritten);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void insertSorted(ITupleReference tuple) {
        insert(tuple, slotManager.getGreatestKeyIndicator());
    }

    boolean isLargeTuple(int tupleSize) {
        return tupleSize > getMaxTupleSize(page.getPageSize());
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException {
        int tupleSize = getBytesRequiredToWriteTuple(tuple);

        if (isLargeTuple(tupleSize)) {
            // when do we want to overload this frame instead of creating a new one?
            // If we have fewer than two tuples in the frame, grow the current page
            return getTupleCount() < 2 ? FrameOpSpaceStatus.EXPAND : FrameOpSpaceStatus.INSUFFICIENT_SPACE;
        } else {
            return super.hasSpaceInsert(tuple);
        }
    }

    @Override
    public FrameOpSpaceStatus hasSpaceUpdate(ITupleReference newTuple, int oldTupleIndex) {
        frameTuple.resetByTupleIndex(this, oldTupleIndex);
        int oldTupleBytes = frameTuple.getTupleSize();
        int newTupleBytes = tupleWriter.bytesRequired(newTuple);
        FrameOpSpaceStatus status = hasSpaceUpdate(oldTupleBytes, newTupleBytes);
        if (status == FrameOpSpaceStatus.INSUFFICIENT_SPACE && (getLargeFlag() || getTupleCount() == 1)
                && isLargeTuple(newTupleBytes)) {
            return FrameOpSpaceStatus.EXPAND;
        }
        return status;
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException {

        int tupleSize = getBytesRequiredToWriteTuple(tuple);

        boolean tupleLarge = isLargeTuple(tupleSize);

        // normal case.
        int tupleCount = getTupleCount();

        // Find split point, and determine into which frame the new tuple should
        // be inserted into.
        BTreeNSMLeafFrame targetFrame;
        frameTuple.resetByTupleIndex(this, tupleCount - 1);
        if (cmp.compare(tuple, frameTuple) > 0) {
            // This is a special optimization case when the tuple to be inserted is the largest key on the page.
            targetFrame = (BTreeNSMLeafFrame) rightFrame;
        } else {
            int tuplesToLeft;
            int totalSize = 0;
            int halfPageSize = (buf.capacity() - getPageHeaderSize()) / 2;
            int i;
            for (i = 0; i < tupleCount; ++i) {
                frameTuple.resetByTupleIndex(this, i);
                totalSize += tupleWriter.getCopySpaceRequired(frameTuple) + slotManager.getSlotSize();
                if (totalSize >= halfPageSize) {
                    break;
                }
            }

            if (cmp.compare(tuple, frameTuple) >= 0) {
                tuplesToLeft = i + 1;
                targetFrame = (BTreeNSMLeafFrame) rightFrame;
            } else {
                tuplesToLeft = i;
                targetFrame = this;
            }
            int tuplesToRight = tupleCount - tuplesToLeft;

            ((BTreeNSMLeafFrame) rightFrame).setLargeFlag(getLargeFlag());
            int deltaPages = page.getFrameSizeMultiplier() - rightFrame.getPage().getFrameSizeMultiplier();
            if (deltaPages > 0) {
                ((BTreeNSMLeafFrame) rightFrame).growCapacity(extraPageBlockHelper, bufferCache, deltaPages);
            }

            ByteBuffer right = rightFrame.getBuffer();
            // Copy entire page.
            System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());

            // On the right page we need to copy rightmost slots to the left.
            int src = rightFrame.getSlotManager().getSlotEndOff();
            int dest = rightFrame.getSlotManager().getSlotEndOff()
                    + tuplesToLeft * rightFrame.getSlotManager().getSlotSize();
            int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
            System.arraycopy(right.array(), src, right.array(), dest, length);
            right.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToRight);

            // On left page only change the tupleCount indicator.
            buf.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToLeft);

            // Compact both pages.
            rightFrame.compact();
            compact();
        }

        if (tupleLarge) {
            targetFrame.ensureCapacity(bufferCache, tuple, extraPageBlockHelper);
        }

        // Insert the new tuple.
        int targetTupleIndex;
        // it's safe to catch this exception since it will have been caught
        // before reaching here
        targetTupleIndex = targetFrame.findInsertTupleIndex(tuple);
        targetFrame.insert(tuple, targetTupleIndex);

        // Set the split key to be highest key in the left page.
        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf.array(), tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(frameTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);
    }

    @Override
    public void ensureCapacity(IBufferCache bufferCache, ITupleReference tuple,
            IExtraPageBlockHelper extraPageBlockHelper) throws HyracksDataException {
        // we call ensureCapacity() for large tuples- ensure large flag is set
        setLargeFlag(true);
        int gapBytes = getBytesRequiredToWriteTuple(tuple) - getFreeContiguousSpace();
        if (gapBytes > 0) {
            int deltaPages = (int) Math.ceil((double) gapBytes / bufferCache.getPageSize());
            growCapacity(extraPageBlockHelper, bufferCache, deltaPages);
        }
    }

    private void growCapacity(IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache, int deltaPages)
            throws HyracksDataException {
        int framePagesOld = page.getFrameSizeMultiplier();
        int newMultiplier = framePagesOld + deltaPages;

        // we need to get the old slot offsets before we grow
        int oldSlotEnd = slotManager.getSlotEndOff();
        int oldSlotStart = slotManager.getSlotStartOff() + slotManager.getSlotSize();

        bufferCache.resizePage(getPage(), newMultiplier, extraPageBlockHelper);

        buf = getPage().getBuffer();

        // fixup the slots
        System.arraycopy(buf.array(), oldSlotEnd, buf.array(), slotManager.getSlotEndOff(), oldSlotStart - oldSlotEnd);

        // fixup total free space counter
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) + (bufferCache.getPageSize() * deltaPages));
    }

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference pageTuple, MultiComparator cmp,
            FindTupleMode ftm, FindTupleNoExactMatchPolicy ftp) throws HyracksDataException {
        return slotManager.findTupleIndex(searchKey, pageTuple, cmp, ftm, ftp);
    }

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference pageTuple, MultiComparator cmp,
            int startIndex) throws HyracksDataException {
        return slotManager.findTupleIndex(searchKey, pageTuple, cmp, startIndex);
    }

    @Override
    public void setMultiComparator(MultiComparator cmp) {
        this.cmp = cmp;
    }

    @Override
    public void validate(PageValidationInfo pvi) throws HyracksDataException {
        int tupleCount = getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            frameTuple.resetByTupleIndex(this, i);
            if (!pvi.isLowRangeNull) {
                assert cmp.compare(pvi.lowRangeTuple, frameTuple) < 0;
            }

            if (!pvi.isHighRangeNull) {
                assert cmp.compare(pvi.highRangeTuple, frameTuple) >= 0;
            }

            if (i > 0) {
                previousFt.resetByTupleIndex(this, i - 1);
                assert cmp.compare(previousFt, frameTuple) < 0;
            }
        }
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder(super.printHeader());
        strBuilder.append("nextLeafOff:       " + NEXT_LEAF_OFFSET + "\n");
        return strBuilder.toString();
    }
}
