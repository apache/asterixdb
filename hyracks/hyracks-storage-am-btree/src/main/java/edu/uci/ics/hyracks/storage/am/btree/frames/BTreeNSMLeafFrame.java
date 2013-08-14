/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.btree.frames;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeNSMLeafFrame extends TreeIndexNSMFrame implements IBTreeLeafFrame {
    protected static final int nextLeafOff = smFlagOff + 1;

    private MultiComparator cmp;

    private final ITreeIndexTupleReference previousFt;

    public BTreeNSMLeafFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
        previousFt = tupleWriter.createTupleReference();
    }

    @Override
    public int getBytesRequriedToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + slotManager.getSlotSize();
    }
    
    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(nextLeafOff, -1);
    }

    @Override
    public void setNextLeaf(int page) {
        buf.putInt(nextLeafOff, page);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(nextLeafOff);
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXCLUSIVE_ERROR_IF_EXISTS,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        // Error indicator is set if there is an exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw new TreeIndexDuplicateKeyException("Trying to insert duplicate key into leaf node.");
        }
        return tupleIndex;
    }

    @Override
    public int findUpdateTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXACT,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator() || tupleIndex == slotManager.getGreatestKeyIndicator()) {
            throw new TreeIndexNonExistentKeyException("Trying to update a tuple with a nonexistent key in leaf node.");
        }
        return tupleIndex;
    }

    @Override
    public int findUpsertTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.INCLUSIVE,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        // Just return the found tupleIndex. The caller will make the final
        // decision whether to insert or update.
        return tupleIndex;
    }

    @Override
    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple, int targetTupleIndex) {
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
        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.EXACT,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
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

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey) {
        ByteBuffer right = rightFrame.getBuffer();
        int tupleCount = getTupleCount();

        // Find split point, and determine into which frame the new tuple should
        // be inserted into.
        int tuplesToLeft;
        ITreeIndexFrame targetFrame = null;
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
            targetFrame = rightFrame;
        } else {
            tuplesToLeft = i;
            targetFrame = this;
        }
        int tuplesToRight = tupleCount - tuplesToLeft;

        // Copy entire page.
        System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());

        // On the right page we need to copy rightmost slots to the left.
        int src = rightFrame.getSlotManager().getSlotEndOff();
        int dest = rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft
                * rightFrame.getSlotManager().getSlotSize();
        int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
        System.arraycopy(right.array(), src, right.array(), dest, length);
        right.putInt(tupleCountOff, tuplesToRight);

        // On left page only change the tupleCount indicator.
        buf.putInt(tupleCountOff, tuplesToLeft);

        // Compact both pages.
        rightFrame.compact();
        compact();

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

    @Override
    protected void resetSpaceParams() {
        buf.putInt(freeSpaceOff, nextLeafOff + 4);
        buf.putInt(totalFreeSpaceOff, buf.capacity() - (nextLeafOff + 4));
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference pageTuple, MultiComparator cmp,
            FindTupleMode ftm, FindTupleNoExactMatchPolicy ftp) {
        return slotManager.findTupleIndex(searchKey, pageTuple, cmp, ftm, ftp);
    }

    @Override
    public int getPageHeaderSize() {
        return nextLeafOff + 4;
    }

    @Override
    public boolean getSmFlag() {
        return buf.get(smFlagOff) != 0;
    }

    @Override
    public void setSmFlag(boolean smFlag) {
        if (smFlag) {
            buf.put(smFlagOff, (byte) 1);
        } else {
            buf.put(smFlagOff, (byte) 0);
        }
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
}