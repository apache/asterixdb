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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import org.apache.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ILargePageHelper;

public class BTreeNSMLeafFrame extends TreeIndexNSMFrame implements IBTreeLeafFrame {
    protected static final int nextLeafOff = flagOff + 1; // 22
    protected static final int supplementalNumPagesOff = nextLeafOff + 4; // 26
    protected static final int supplementalPageIdOff = supplementalNumPagesOff + 4; // 30

    private MultiComparator cmp;

    private final ITreeIndexTupleReference previousFt;

    public BTreeNSMLeafFrame(ITreeIndexTupleWriter tupleWriter, ILargePageHelper largePageHelper) {
        super(tupleWriter, new OrderedSlotManager(), largePageHelper);
        previousFt = tupleWriter.createTupleReference();
    }

    @Override
    public int getPageHeaderSize() {
        return supplementalPageIdOff + 4;
    }

    @Override
    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + slotManager.getSlotSize();
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(nextLeafOff, -1);
        buf.putInt(supplementalNumPagesOff, 0);
        buf.putInt(supplementalPageIdOff, -1);
    }

    @Override
    public void setNextLeaf(int page) {
        buf.putInt(nextLeafOff, page);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(nextLeafOff);
    }

    public static int getSupplementalNumPages(ByteBuffer buf) {
        return buf.getInt(supplementalNumPagesOff);
    }

    public int getSupplementalNumPages() {
        return getSupplementalNumPages(buf);
    }

    public static int getSupplementalPageId(ByteBuffer buf) {
        return buf.getInt(supplementalPageIdOff);
    }

    public int getSupplementalPageId() {
        return getSupplementalPageId(buf);
    }

    public void configureLargePage(int supplementalPages, int supplementalBlockPageId) {
        setLargeFlag(true);
        buf.putInt(supplementalNumPagesOff, supplementalPages);
        buf.putInt(supplementalPageIdOff, supplementalBlockPageId);
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int tupleIndex;
        try {
            tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXCLUSIVE_ERROR_IF_EXISTS,
                    FindTupleNoExactMatchPolicy.HIGHER_KEY);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
        // Error indicator is set if there is an exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw new TreeIndexDuplicateKeyException("Trying to insert duplicate key into leaf node.");
        }
        return tupleIndex;
    }

    @Override
    public int findUpdateTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int tupleIndex;
        try {
            tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXACT,
                    FindTupleNoExactMatchPolicy.HIGHER_KEY);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator() || tupleIndex == slotManager.getGreatestKeyIndicator()) {
            throw new TreeIndexNonExistentKeyException("Trying to update a tuple with a nonexistent key in leaf node.");
        }
        return tupleIndex;
    }

    @Override
    public int findUpsertTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int tupleIndex;
        try {
            tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.INCLUSIVE,
                    FindTupleNoExactMatchPolicy.HIGHER_KEY);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
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
    public int findDeleteTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int tupleIndex;
        try {
            tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXACT,
                    FindTupleNoExactMatchPolicy.HIGHER_KEY);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator() || tupleIndex == slotManager.getGreatestKeyIndicator()) {
            throw new TreeIndexNonExistentKeyException("Trying to delete a tuple with a nonexistent key in leaf node.");
        }
        return tupleIndex;
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        int freeSpace = buf.getInt(freeSpaceOff);
        slotManager.insertSlot(tupleIndex, freeSpace);
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), freeSpace);
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void insertSorted(ITupleReference tuple) {
        insert(tuple, slotManager.getGreatestKeyIndicator());
    }

    boolean isLargeTuple(int tupleSize) {
        // TODO(mblow): make page size available to avoid calculating it
        int pageSize = isLargePage() ? buf.capacity() / (getSupplementalNumPages() + 1) : buf.capacity();

        return tupleSize > getMaxTupleSize(pageSize);
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
        if (status == FrameOpSpaceStatus.INSUFFICIENT_SPACE && (isLargePage() || getTupleCount() == 1)
                && isLargeTuple(newTupleBytes)) {
            return FrameOpSpaceStatus.EXPAND;
        }
        return status;
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IMetaDataPageManager freePageManager, ITreeIndexMetaDataFrame metaFrame, IBufferCache bufferCache)
                    throws HyracksDataException {

        int tupleSize = getBytesRequiredToWriteTuple(tuple);

        boolean tupleLarge = isLargeTuple(tupleSize);

        // normal case.
        int tupleCount = getTupleCount();

        // Find split point, and determine into which frame the new tuple should
        // be inserted into.
        BTreeNSMLeafFrame targetFrame = null;
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
            int supplementalPages = 0;
            int supplementalPageId = -1;
            if (isLargePage()) {
                ((BTreeNSMLeafFrame) rightFrame).growCapacity(freePageManager, metaFrame, bufferCache,
                        buf.capacity() - rightFrame.getBuffer().capacity());
                supplementalPages = ((BTreeNSMLeafFrame) rightFrame).getSupplementalNumPages();
                supplementalPageId = ((BTreeNSMLeafFrame) rightFrame).getSupplementalPageId();
            }

            ByteBuffer right = rightFrame.getBuffer();
            // Copy entire page.
            System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());
            if (isLargePage()) {
                // restore the supplemental page metadata
                ((BTreeNSMLeafFrame) rightFrame).configureLargePage(supplementalPages, supplementalPageId);
            }

            // On the right page we need to copy rightmost slots to the left.
            int src = rightFrame.getSlotManager().getSlotEndOff();
            int dest = rightFrame.getSlotManager().getSlotEndOff()
                    + tuplesToLeft * rightFrame.getSlotManager().getSlotSize();
            int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
            System.arraycopy(right.array(), src, right.array(), dest, length);
            right.putInt(tupleCountOff, tuplesToRight);

            // On left page only change the tupleCount indicator.
            buf.putInt(tupleCountOff, tuplesToLeft);

            // Compact both pages.
            rightFrame.compact();
            compact();
        }

        if (tupleLarge) {
            targetFrame.ensureCapacity(freePageManager, metaFrame, bufferCache, tuple);
        }

        // Insert the new tuple.
        int targetTupleIndex;
        // it's safe to catch this exception since it will have been caught
        // before reaching here
        try {
            targetTupleIndex = ((BTreeNSMLeafFrame) targetFrame).findInsertTupleIndex(tuple);
        } catch (TreeIndexException e) {
            throw new IllegalStateException(e);
        }
        targetFrame.insert(tuple, targetTupleIndex);

        // Set the split key to be highest key in the left page.
        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(frameTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);
    }

    public void ensureCapacity(IMetaDataPageManager freePageManager, ITreeIndexMetaDataFrame metaFrame,
            IBufferCache bufferCache, ITupleReference tuple) throws HyracksDataException {
        int gapBytes = getBytesRequiredToWriteTuple(tuple) - getFreeContiguousSpace();
        growCapacity(freePageManager, metaFrame, bufferCache, gapBytes);
    }

    public void growCapacity(IMetaDataPageManager freePageManager, ITreeIndexMetaDataFrame metaFrame,
            IBufferCache bufferCache, int delta) throws HyracksDataException {
        if (delta <= 0) {
            setLargeFlag(true);
            return;
        }
        int deltaPages = (int) Math.ceil((double) delta / bufferCache.getPageSize());
        int framePagesOld = getBuffer().capacity() / bufferCache.getPageSize();
        int oldSupplementalPages = 0;
        int oldSupplementalPageId = -1;
        if (isLargePage()) {
            oldSupplementalPages = getSupplementalNumPages();
            oldSupplementalPageId = getSupplementalPageId();
        }

        configureLargePage(framePagesOld + deltaPages - 1,
                freePageManager.getFreePageBlock(metaFrame, framePagesOld + deltaPages - 1));

        int pageDelta = (framePagesOld + deltaPages) - 1 - oldSupplementalPages;

        // we need to get the old slot offsets before we grow
        int oldSlotEnd = slotManager.getSlotEndOff();
        int oldSlotStart = slotManager.getSlotStartOff() + slotManager.getSlotSize();

        bufferCache.resizePage(getPage(), framePagesOld + deltaPages);
        buf = getPage().getBuffer();

        // return the dropped supplemental pages to the page manager...
        if (oldSupplementalPages > 0) {
            freePageManager.addFreePageBlock(metaFrame, oldSupplementalPageId, oldSupplementalPages);
        }

        // fixup the slots
        System.arraycopy(buf.array(), oldSlotEnd, buf.array(), slotManager.getSlotEndOff(), oldSlotStart - oldSlotEnd);

        // fixup total free space counter
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + (bufferCache.getPageSize() * pageDelta));
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference pageTuple, MultiComparator cmp,
            FindTupleMode ftm, FindTupleNoExactMatchPolicy ftp) throws HyracksDataException {
        return slotManager.findTupleIndex(searchKey, pageTuple, cmp, ftm, ftp);
    }

    @Override
    public void setMultiComparator(MultiComparator cmp) {
        this.cmp = cmp;
    }

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
        strBuilder.append("nextLeafOff:       " + nextLeafOff + "\n");
        strBuilder.append("supplementalNumPagesOff: " + supplementalNumPagesOff + "\n");
        strBuilder.append("supplementalPageIdOff: " + supplementalPageIdOff + "\n");
        return strBuilder.toString();
    }
}
