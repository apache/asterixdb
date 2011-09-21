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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.SlotOffTupleOff;

public class BTreeNSMInteriorFrame extends TreeIndexNSMFrame implements IBTreeInteriorFrame {

    private static final int rightLeafOff = smFlagOff + 1;

    private static final int childPtrSize = 4;

    // private SimpleTupleReference cmpFrameTuple = new SimpleTupleReference();
    private ITreeIndexTupleReference cmpFrameTuple;

    public BTreeNSMInteriorFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
        cmpFrameTuple = tupleWriter.createTupleReference();

    }

    private int getLeftChildPageOff(ITupleReference tuple, MultiComparator cmp) {
        return tuple.getFieldStart(cmp.getKeyFieldCount() - 1) + tuple.getFieldLength(cmp.getKeyFieldCount() - 1);
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(rightLeafOff, -1);
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple, MultiComparator cmp) {
        int bytesRequired = tupleWriter.bytesRequired(tuple) + 8; // for the two
        // childpointers
        if (bytesRequired + slotManager.getSlotSize() <= buf.capacity() - buf.getInt(freeSpaceOff)
                - (buf.getInt(tupleCountOff) * slotManager.getSlotSize()))
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        else if (bytesRequired + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff))
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        else
            return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp) throws Exception {
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.FTM_INCLUSIVE,
                FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY);
        int slotOff = slotManager.getSlotOff(tupleIndex);
        boolean isDuplicate = true;

        if (tupleIndex < 0)
            isDuplicate = false; // greater than all existing keys
        else {
            frameTuple.resetByTupleOffset(buf, slotManager.getTupleOff(slotOff));
            if (cmp.compare(tuple, frameTuple) != 0)
                isDuplicate = false;
        }
        if (isDuplicate) {
            throw new BTreeDuplicateKeyException("Trying to insert duplicate key into interior node.");
        }
        return tupleIndex;
    }

    @Override
    public void insert(ITupleReference tuple, MultiComparator cmp, int tupleIndex) throws Exception {
        int slotOff = slotManager.insertSlot(tupleIndex, buf.getInt(freeSpaceOff));
        int freeSpace = buf.getInt(freeSpaceOff);
        int bytesWritten = tupleWriter.writeTupleFields(tuple, 0, cmp.getKeyFieldCount(), buf, freeSpace);
        System.arraycopy(tuple.getFieldData(cmp.getKeyFieldCount() - 1), getLeftChildPageOff(tuple, cmp), buf.array(),
                freeSpace + bytesWritten, childPtrSize);
        int tupleSize = bytesWritten + childPtrSize;

        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + tupleSize);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - tupleSize - slotManager.getSlotSize());

        // did insert into the rightmost slot?
        if (slotOff == slotManager.getSlotEndOff()) {
            System.arraycopy(tuple.getFieldData(cmp.getKeyFieldCount() - 1), getLeftChildPageOff(tuple, cmp)
                    + childPtrSize, buf.array(), rightLeafOff, childPtrSize);
        } else {
            // if slotOff has a right (slot-)neighbor then update its child
            // pointer
            // the only time when this is NOT the case, is when this is the
            // first tuple
            // (or when the splitkey goes into the rightmost slot but that
            // case was handled in the if above)
            if (buf.getInt(tupleCountOff) > 1) {
                int rightNeighborOff = slotOff - slotManager.getSlotSize();
                frameTuple.resetByTupleOffset(buf, slotManager.getTupleOff(rightNeighborOff));
                System.arraycopy(tuple.getFieldData(0), getLeftChildPageOff(tuple, cmp) + childPtrSize, buf.array(),
                        getLeftChildPageOff(frameTuple, cmp), childPtrSize);
            }
        }
    }

    @Override
    public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
        int freeSpace = buf.getInt(freeSpaceOff);
        slotManager.insertSlot(-1, freeSpace);
        int bytesWritten = tupleWriter.writeTupleFields(tuple, 0, cmp.getKeyFieldCount(), buf, freeSpace);
        System.arraycopy(tuple.getFieldData(cmp.getKeyFieldCount() - 1), getLeftChildPageOff(tuple, cmp), buf.array(),
                freeSpace + bytesWritten, childPtrSize);
        int tupleSize = bytesWritten + childPtrSize;
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + tupleSize);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - tupleSize - slotManager.getSlotSize());
        System.arraycopy(tuple.getFieldData(0), getLeftChildPageOff(tuple, cmp) + childPtrSize, buf.array(),
                rightLeafOff, childPtrSize);
    }

    @Override
    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey)
            throws Exception {
        // before doing anything check if key already exists
        frameTuple.setFieldCount(cmp.getKeyFieldCount());

        ByteBuffer right = rightFrame.getBuffer();
        int tupleCount = buf.getInt(tupleCountOff);

        int tuplesToLeft = (tupleCount / 2) + (tupleCount % 2);
        ITreeIndexFrame targetFrame = null;
        frameTuple.resetByTupleOffset(buf, getTupleOffset(tuplesToLeft - 1));
        if (cmp.compare(tuple, frameTuple) <= 0) {
            targetFrame = this;
        } else {
            targetFrame = rightFrame;
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

        // on left page, remove highest key and make its childpointer the
        // rightmost childpointer
        buf.putInt(tupleCountOff, tuplesToLeft);

        // copy data to be inserted, we need this because creating the splitkey
        // will overwrite the data param (data points to same memory as
        // splitKey.getData())
        ISplitKey savedSplitKey = splitKey.duplicate(tupleWriter.createTupleReference());

        // set split key to be highest value in left page
        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(frameTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);

        int deleteTupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, deleteTupleOff);
        buf.putInt(rightLeafOff, buf.getInt(getLeftChildPageOff(frameTuple, cmp)));
        buf.putInt(tupleCountOff, tuplesToLeft - 1);

        // compact both pages
        rightFrame.compact(cmp);
        compact(cmp);

        // insert key
        int targetTupleIndex = targetFrame.findTupleIndex(savedSplitKey.getTuple(), cmp);
        targetFrame.insert(savedSplitKey.getTuple(), cmp, targetTupleIndex);

        return 0;
    }

    @Override
    public boolean compact(MultiComparator cmp) {
        resetSpaceParams();

        frameTuple.setFieldCount(cmp.getKeyFieldCount());

        int tupleCount = buf.getInt(tupleCountOff);
        int freeSpace = buf.getInt(freeSpaceOff);

        ArrayList<SlotOffTupleOff> sortedTupleOffs = new ArrayList<SlotOffTupleOff>();
        sortedTupleOffs.ensureCapacity(tupleCount);
        for (int i = 0; i < tupleCount; i++) {
            int slotOff = slotManager.getSlotOff(i);
            int tupleOff = slotManager.getTupleOff(slotOff);
            sortedTupleOffs.add(new SlotOffTupleOff(i, slotOff, tupleOff));
        }
        Collections.sort(sortedTupleOffs);

        for (int i = 0; i < sortedTupleOffs.size(); i++) {
            int tupleOff = sortedTupleOffs.get(i).tupleOff;
            frameTuple.resetByTupleOffset(buf, tupleOff);

            int tupleEndOff = frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                    + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1);
            int tupleLength = tupleEndOff - tupleOff + childPtrSize;
            System.arraycopy(buf.array(), tupleOff, buf.array(), freeSpace, tupleLength);

            slotManager.setSlot(sortedTupleOffs.get(i).slotOff, freeSpace);
            freeSpace += tupleLength;
        }

        buf.putInt(freeSpaceOff, freeSpace);
        buf.putInt(totalFreeSpaceOff, buf.capacity() - freeSpace - tupleCount * slotManager.getSlotSize());

        return false;
    }

    @Override
    public int getChildPageId(RangePredicate pred, MultiComparator srcCmp) {
        // check for trivial case where there is only a child pointer (and no
        // key)
        if (buf.getInt(tupleCountOff) == 0) {
            return buf.getInt(rightLeafOff);
        }

        cmpFrameTuple.setFieldCount(srcCmp.getKeyFieldCount());
        frameTuple.setFieldCount(srcCmp.getKeyFieldCount());

        // check for trivial cases where no low key or high key exists (e.g.
        // during an index scan)
        ITupleReference tuple = null;
        FindTupleMode fsm = null;
        MultiComparator targetCmp = null;
        if (pred.isForward()) {
            tuple = pred.getLowKey();
            if (tuple == null) {
                return getLeftmostChildPageId(srcCmp);
            }
            if (pred.isLowKeyInclusive())
                fsm = FindTupleMode.FTM_INCLUSIVE;
            else
                fsm = FindTupleMode.FTM_EXCLUSIVE;
            targetCmp = pred.getLowKeyComparator();
        } else {
            tuple = pred.getHighKey();
            if (tuple == null) {
                return getRightmostChildPageId(srcCmp);
            }
            if (pred.isHighKeyInclusive())
                fsm = FindTupleMode.FTM_EXCLUSIVE;
            else
                fsm = FindTupleMode.FTM_INCLUSIVE;
            targetCmp = pred.getHighKeyComparator();
        }

        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, targetCmp, fsm,
                FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY);
        int slotOff = slotManager.getSlotOff(tupleIndex);
        if (tupleIndex < 0) {
            return buf.getInt(rightLeafOff);
        } else {
            int origTupleOff = slotManager.getTupleOff(slotOff);
            cmpFrameTuple.resetByTupleOffset(buf, origTupleOff);
            int cmpTupleOff = origTupleOff;
            if (pred.isForward()) {
                int maxSlotOff = buf.capacity();
                slotOff += slotManager.getSlotSize();
                while (slotOff < maxSlotOff) {
                    cmpTupleOff = slotManager.getTupleOff(slotOff);
                    frameTuple.resetByTupleOffset(buf, cmpTupleOff);
                    if (targetCmp.compare(cmpFrameTuple, frameTuple) != 0)
                        break;
                    slotOff += slotManager.getSlotSize();
                }
                slotOff -= slotManager.getSlotSize();
            } else {
                int minSlotOff = slotManager.getSlotEndOff() - slotManager.getSlotSize();
                slotOff -= slotManager.getSlotSize();
                while (slotOff > minSlotOff) {
                    cmpTupleOff = slotManager.getTupleOff(slotOff);
                    frameTuple.resetByTupleOffset(buf, cmpTupleOff);
                    if (targetCmp.compare(cmpFrameTuple, frameTuple) != 0)
                        break;
                    slotOff -= slotManager.getSlotSize();
                }
                slotOff += slotManager.getSlotSize();
            }

            frameTuple.resetByTupleOffset(buf, slotManager.getTupleOff(slotOff));
            int childPageOff = getLeftChildPageOff(frameTuple, srcCmp);
            return buf.getInt(childPageOff);
        }
    }

    @Override
    public void delete(ITupleReference tuple, MultiComparator cmp, boolean exactDelete) throws Exception {
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.FTM_INCLUSIVE,
                FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY);
        int slotOff = slotManager.getSlotOff(tupleIndex);
        int tupleOff;
        int keySize;

        if (tupleIndex < 0) {
            tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
            frameTuple.resetByTupleOffset(buf, tupleOff);
            keySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());

            // copy new rightmost pointer
            System.arraycopy(buf.array(), tupleOff + keySize, buf.array(), rightLeafOff, childPtrSize);
        } else {
            tupleOff = slotManager.getTupleOff(slotOff);
            frameTuple.resetByTupleOffset(buf, tupleOff);
            keySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
            // perform deletion (we just do a memcpy to overwrite the slot)
            int slotStartOff = slotManager.getSlotEndOff();
            int length = slotOff - slotStartOff;
            System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);
        }

        // maintain space information
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) - 1);
        buf.putInt(totalFreeSpaceOff,
                buf.getInt(totalFreeSpaceOff) + keySize + childPtrSize + slotManager.getSlotSize());
    }

    @Override
    protected void resetSpaceParams() {
        buf.putInt(freeSpaceOff, rightLeafOff + childPtrSize);
        buf.putInt(totalFreeSpaceOff, buf.capacity() - (rightLeafOff + childPtrSize));
    }

    @Override
    public int getLeftmostChildPageId(MultiComparator cmp) {
        int tupleOff = slotManager.getTupleOff(slotManager.getSlotStartOff());
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int childPageOff = getLeftChildPageOff(frameTuple, cmp);
        return buf.getInt(childPageOff);
    }

    @Override
    public int getRightmostChildPageId(MultiComparator cmp) {
        return buf.getInt(rightLeafOff);
    }

    @Override
    public void setRightmostChildPageId(int pageId) {
        buf.putInt(rightLeafOff, pageId);
    }

    // for debugging
    public ArrayList<Integer> getChildren(MultiComparator cmp) {
        ArrayList<Integer> ret = new ArrayList<Integer>();
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        int tupleCount = buf.getInt(tupleCountOff);
        for (int i = 0; i < tupleCount; i++) {
            int tupleOff = slotManager.getTupleOff(slotManager.getSlotOff(i));
            frameTuple.resetByTupleOffset(buf, tupleOff);
            int intVal = getInt(
                    buf.array(),
                    frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                            + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1));
            ret.add(intVal);
        }
        if (!isLeaf()) {
            int rightLeaf = buf.getInt(rightLeafOff);
            if (rightLeaf > 0)
                ret.add(buf.getInt(rightLeafOff));
        }
        return ret;
    }

    @Override
    public void deleteGreatest(MultiComparator cmp) {
        int slotOff = slotManager.getSlotEndOff();
        int tupleOff = slotManager.getTupleOff(slotOff);
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int keySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        System.arraycopy(buf.array(), tupleOff + keySize, buf.array(), rightLeafOff, childPtrSize);

        // maintain space information
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) - 1);
        buf.putInt(totalFreeSpaceOff,
                buf.getInt(totalFreeSpaceOff) + keySize + childPtrSize + slotManager.getSlotSize());

        int freeSpace = buf.getInt(freeSpaceOff);
        if (freeSpace == tupleOff + keySize + childPtrSize) {
            buf.putInt(freeSpace, freeSpace - (keySize + childPtrSize));
        }
    }

    private int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }

    @Override
    public String printKeys(MultiComparator cmp, ISerializerDeserializer[] fields) throws HyracksDataException {
        StringBuilder strBuilder = new StringBuilder();
        int tupleCount = buf.getInt(tupleCountOff);
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        for (int i = 0; i < tupleCount; i++) {
            frameTuple.resetByTupleIndex(this, i);
            for (int j = 0; j < cmp.getKeyFieldCount(); j++) {
                ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(j),
                        frameTuple.getFieldStart(j), frameTuple.getFieldLength(j));
                DataInput dataIn = new DataInputStream(inStream);
                Object o = fields[j].deserialize(dataIn);
                strBuilder.append(o.toString() + " ");
            }
            strBuilder.append(" | ");
        }
        strBuilder.append("\n");
        return strBuilder.toString();
    }

    @Override
    public int getPageHeaderSize() {
        return rightLeafOff;
    }
}
