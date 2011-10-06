/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class BTreeNSMLeafFrame extends TreeIndexNSMFrame implements IBTreeLeafFrame {
    protected static final int prevLeafOff = smFlagOff + 1;
    protected static final int nextLeafOff = prevLeafOff + 4;

    public BTreeNSMLeafFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(prevLeafOff, -1);
        buf.putInt(nextLeafOff, -1);
    }

    @Override
    public void setNextLeaf(int page) {
        buf.putInt(nextLeafOff, page);
    }

    @Override
    public void setPrevLeaf(int page) {
        buf.putInt(prevLeafOff, page);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(nextLeafOff);
    }

    @Override
    public int getPrevLeaf() {
        return buf.getInt(prevLeafOff);
    }

    @Override
    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp, boolean throwIfKeyExists) throws Exception {
        frameTuple.setFieldCount(cmp.getFieldCount());
        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.FTM_INCLUSIVE,
                FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY);
        if (!throwIfKeyExists) {
            return tupleIndex;
        }
        int slotOff = slotManager.getSlotOff(tupleIndex);
        boolean isDuplicate = true;
        if (tupleIndex < 0) {
            // Greater than all existing keys.
            isDuplicate = false;
        }
        else {
            frameTuple.resetByTupleOffset(buf, slotManager.getTupleOff(slotOff));
            if (cmp.compare(tuple, frameTuple) != 0) {
                isDuplicate = false;
            }
        }
        if (isDuplicate) {
            throw new BTreeDuplicateKeyException("Trying to insert duplicate key into leaf node.");
        }
        return tupleIndex;
    }

    @Override
    public void insert(ITupleReference tuple, MultiComparator cmp, int tupleIndex) throws Exception {
        slotManager.insertSlot(tupleIndex, buf.getInt(freeSpaceOff));
        int freeSpace = buf.getInt(freeSpaceOff);
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), freeSpace);
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
        int freeSpace = buf.getInt(freeSpaceOff);
        slotManager.insertSlot(-1, freeSpace);
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), freeSpace);
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey)
            throws Exception {

        frameTuple.setFieldCount(cmp.getFieldCount());

        ByteBuffer right = rightFrame.getBuffer();
        int tupleCount = getTupleCount();

        int tuplesToLeft;
        int mid = tupleCount / 2;
        ITreeIndexFrame targetFrame = null;
        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff() + slotManager.getSlotSize() * mid);
        frameTuple.resetByTupleOffset(buf, tupleOff);
        if (cmp.compare(tuple, frameTuple) >= 0) {
            tuplesToLeft = mid + (tupleCount % 2);
            targetFrame = rightFrame;
        } else {
            tuplesToLeft = mid;
            targetFrame = this;
        }
        int tuplesToRight = tupleCount - tuplesToLeft;

        // copy entire page
        System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());

        // on right page we need to copy rightmost slots to left
        int src = rightFrame.getSlotManager().getSlotEndOff();
        int dest = rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft
                * rightFrame.getSlotManager().getSlotSize();
        int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
        System.arraycopy(right.array(), src, right.array(), dest, length);
        right.putInt(tupleCountOff, tuplesToRight);

        // on left page only change the tupleCount indicator
        buf.putInt(tupleCountOff, tuplesToLeft);

        // compact both pages
        rightFrame.compact(cmp);
        compact(cmp);

        // insert last key
        // TODO: can we avoid checking for duplicates here?
        int targetTupleIndex = targetFrame.findTupleIndex(tuple, cmp, true);
        targetFrame.insert(tuple, cmp, targetTupleIndex);

        // set split key to be highest value in left page
        tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);

        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(frameTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);

        return 0;
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
        return nextLeafOff;
    }
}
