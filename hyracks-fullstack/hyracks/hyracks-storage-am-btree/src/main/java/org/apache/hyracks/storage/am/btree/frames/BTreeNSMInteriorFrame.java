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
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.common.ophelpers.SlotOffTupleOff;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

public class BTreeNSMInteriorFrame extends TreeIndexNSMFrame implements IBTreeInteriorFrame {

    private static final int RIGHT_LEAF_OFFSET = TreeIndexNSMFrame.RESERVED_HEADER_SIZE;
    private static final int CHILD_PTR_SIZE = 4;

    private final ITreeIndexTupleReference cmpFrameTuple;
    private final ITreeIndexTupleReference previousFt;

    private MultiComparator cmp;

    public BTreeNSMInteriorFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
        cmpFrameTuple = tupleWriter.createTupleReference();
        previousFt = tupleWriter.createTupleReference();
    }

    @Override
    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + CHILD_PTR_SIZE + slotManager.getSlotSize();
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(RIGHT_LEAF_OFFSET, -1);
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.INCLUSIVE,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException {
        int tupleSize = tupleWriter.bytesRequired(tuple) + CHILD_PTR_SIZE;
        if (tupleSize > getMaxTupleSize(buf.capacity())) {
            return FrameOpSpaceStatus.TOO_LARGE;
        }
        // Tuple bytes + child pointer + slot.
        int bytesRequired = tupleSize + slotManager.getSlotSize();
        if (bytesRequired <= getFreeContiguousSpace()) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }
        if (bytesRequired <= getTotalFreeSpace()) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        }
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        int slotOff = slotManager.insertSlot(tupleIndex, buf.getInt(Constants.FREE_SPACE_OFFSET));
        int freeSpace = buf.getInt(Constants.FREE_SPACE_OFFSET);
        int bytesWritten = tupleWriter.writeTupleFields(tuple, 0, tuple.getFieldCount(), buf.array(), freeSpace);
        System.arraycopy(tuple.getFieldData(tuple.getFieldCount() - 1), getLeftChildPageOff(tuple), buf.array(),
                freeSpace + bytesWritten, CHILD_PTR_SIZE);
        int tupleSize = bytesWritten + CHILD_PTR_SIZE;
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) + 1);
        buf.putInt(Constants.FREE_SPACE_OFFSET, buf.getInt(Constants.FREE_SPACE_OFFSET) + tupleSize);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - tupleSize - slotManager.getSlotSize());
        // Did we insert into the rightmost slot?
        if (slotOff == slotManager.getSlotEndOff()) {
            System.arraycopy(tuple.getFieldData(tuple.getFieldCount() - 1), getLeftChildPageOff(tuple) + CHILD_PTR_SIZE,
                    buf.array(), RIGHT_LEAF_OFFSET, CHILD_PTR_SIZE);
        } else {
            // If slotOff has a right (slot-)neighbor then update its child pointer.
            // The only time when this is NOT the case, is when this is the very first tuple
            // (or when the splitkey goes into the rightmost slot but that case is handled in the if above).
            if (buf.getInt(Constants.TUPLE_COUNT_OFFSET) > 1) {
                int rightNeighborOff = slotOff - slotManager.getSlotSize();
                frameTuple.resetByTupleOffset(buf.array(), slotManager.getTupleOff(rightNeighborOff));
                System.arraycopy(tuple.getFieldData(0), getLeftChildPageOff(tuple) + CHILD_PTR_SIZE, buf.array(),
                        getLeftChildPageOff(frameTuple), CHILD_PTR_SIZE);
            }
        }
    }

    @Override
    public int findDeleteTupleIndex(ITupleReference tuple) throws HyracksDataException {
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.INCLUSIVE,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
    }

    @Override
    public void delete(ITupleReference tuple, int tupleIndex) {
        int slotOff = slotManager.getSlotOff(tupleIndex);
        int tupleOff;
        int keySize;
        if (tupleIndex == slotManager.getGreatestKeyIndicator()) {
            tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
            frameTuple.resetByTupleOffset(buf.array(), tupleOff);
            keySize = frameTuple.getTupleSize();
            // Copy new rightmost pointer.
            System.arraycopy(buf.array(), tupleOff + keySize, buf.array(), RIGHT_LEAF_OFFSET, CHILD_PTR_SIZE);
        } else {
            tupleOff = slotManager.getTupleOff(slotOff);
            frameTuple.resetByTupleOffset(buf.array(), tupleOff);
            keySize = frameTuple.getTupleSize();
            // Perform deletion (we just do a memcpy to overwrite the slot).
            int slotStartOff = slotManager.getSlotEndOff();
            int length = slotOff - slotStartOff;
            System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);
        }
        // Maintain space information.
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) - 1);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) + keySize + CHILD_PTR_SIZE + slotManager.getSlotSize());
    }

    @Override
    public void deleteGreatest() {
        int slotOff = slotManager.getSlotEndOff();
        int tupleOff = slotManager.getTupleOff(slotOff);
        frameTuple.resetByTupleOffset(buf.array(), tupleOff);
        int keySize = tupleWriter.bytesRequired(frameTuple);
        System.arraycopy(buf.array(), tupleOff + keySize, buf.array(), RIGHT_LEAF_OFFSET, CHILD_PTR_SIZE);
        // Maintain space information.
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) - 1);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) + keySize + CHILD_PTR_SIZE + slotManager.getSlotSize());
        int freeSpace = buf.getInt(Constants.FREE_SPACE_OFFSET);
        if (freeSpace == tupleOff + keySize + CHILD_PTR_SIZE) {
            buf.putInt(freeSpace, freeSpace - (keySize + CHILD_PTR_SIZE));
        }
    }

    @Override
    public FrameOpSpaceStatus hasSpaceUpdate(ITupleReference tuple, int oldTupleIndex) {
        throw new UnsupportedOperationException("Cannot update tuples in interior node.");
    }

    @Override
    public void insertSorted(ITupleReference tuple) {
        int freeSpace = buf.getInt(Constants.FREE_SPACE_OFFSET);
        slotManager.insertSlot(slotManager.getGreatestKeyIndicator(), freeSpace);
        int bytesWritten = tupleWriter.writeTuple(tuple, buf, freeSpace);
        System.arraycopy(tuple.getFieldData(tuple.getFieldCount() - 1), getLeftChildPageOff(tuple), buf.array(),
                freeSpace + bytesWritten, CHILD_PTR_SIZE);
        int tupleSize = bytesWritten + CHILD_PTR_SIZE;
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) + 1);
        buf.putInt(Constants.FREE_SPACE_OFFSET, buf.getInt(Constants.FREE_SPACE_OFFSET) + tupleSize);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - tupleSize - slotManager.getSlotSize());
        System.arraycopy(tuple.getFieldData(0), getLeftChildPageOff(tuple) + CHILD_PTR_SIZE, buf.array(),
                RIGHT_LEAF_OFFSET, CHILD_PTR_SIZE);
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException {
        ByteBuffer right = rightFrame.getBuffer();
        int tupleCount = getTupleCount();

        // Find split point, and determine into which frame the new tuple should be inserted into.
        ITreeIndexFrame targetFrame = null;
        frameTuple.resetByTupleIndex(this, tupleCount - 1);
        int tuplesToLeft;
        if (cmp.compare(tuple, frameTuple) > 0) {
            // This is a special optimization case when the tuple to be inserted is the largest key on the page.
            targetFrame = rightFrame;
            tuplesToLeft = tupleCount;
        } else {
            int totalSize = 0;
            int halfPageSize = (buf.capacity() - getPageHeaderSize()) / 2;
            int i;
            for (i = 0; i < tupleCount; ++i) {
                frameTuple.resetByTupleIndex(this, i);
                totalSize += tupleWriter.bytesRequired(frameTuple) + CHILD_PTR_SIZE + slotManager.getSlotSize();
                if (totalSize >= halfPageSize) {
                    break;
                }
            }

            if (cmp.compare(tuple, frameTuple) > 0) {
                tuplesToLeft = i;
                targetFrame = rightFrame;
            } else {
                tuplesToLeft = i + 1;
                targetFrame = this;
            }
            int tuplesToRight = tupleCount - tuplesToLeft;

            // Copy entire page.
            System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());

            // On the right page we need to copy rightmost slots to left.
            int src = rightFrame.getSlotManager().getSlotEndOff();
            int dest = rightFrame.getSlotManager().getSlotEndOff()
                    + tuplesToLeft * rightFrame.getSlotManager().getSlotSize();
            int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
            System.arraycopy(right.array(), src, right.array(), dest, length);
            right.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToRight);

            // On the left page, remove the highest key and make its child pointer
            // the rightmost child pointer.
            buf.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToLeft);
        }
        // Copy the split key to be inserted.
        // We must do so because setting the new split key will overwrite the
        // old split key, and we cannot insert the existing split key at this point.
        ISplitKey savedSplitKey = splitKey.duplicate(tupleWriter.createTupleReference());

        // Set split key to be highest value in left page.
        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf.array(), tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTuple(frameTuple, splitKey.getBuffer(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);

        int deleteTupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf.array(), deleteTupleOff);
        buf.putInt(RIGHT_LEAF_OFFSET, buf.getInt(getLeftChildPageOff(frameTuple)));
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, tuplesToLeft - 1);

        // Compact both pages.
        rightFrame.compact();
        compact();
        // Insert the saved split key.
        int targetTupleIndex;
        // it's safe to catch this exception since it will have been caught before reaching here
        targetTupleIndex = ((BTreeNSMInteriorFrame) targetFrame).findInsertTupleIndex(savedSplitKey.getTuple());
        targetFrame.insert(savedSplitKey.getTuple(), targetTupleIndex);
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
            int tupleLength = tupleEndOff - tupleOff + CHILD_PTR_SIZE;
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
    public int getChildPageId(RangePredicate pred) throws HyracksDataException {
        // Trivial case where there is only a child pointer (and no key).
        if (buf.getInt(Constants.TUPLE_COUNT_OFFSET) == 0) {
            return buf.getInt(RIGHT_LEAF_OFFSET);
        }
        // Trivial cases where no low key or high key was given (e.g. during an
        // index scan).
        ITupleReference tuple = null;
        FindTupleMode fsm = null;
        // The target comparator may be on a prefix of the BTree key fields.
        MultiComparator targetCmp = pred.getLowKeyComparator();
        tuple = pred.getLowKey();
        if (tuple == null) {
            return getLeftmostChildPageId();
        }
        if (pred.isLowKeyInclusive()) {
            fsm = FindTupleMode.INCLUSIVE;
        } else {
            fsm = FindTupleMode.EXCLUSIVE;
        }
        // Search for a matching key.
        int tupleIndex =
                slotManager.findTupleIndex(tuple, frameTuple, targetCmp, fsm, FindTupleNoExactMatchPolicy.HIGHER_KEY);
        int slotOff = slotManager.getSlotOff(tupleIndex);
        // Follow the rightmost (greatest) child pointer.
        if (tupleIndex == slotManager.getGreatestKeyIndicator()) {
            return buf.getInt(RIGHT_LEAF_OFFSET);
        }
        // Deal with prefix searches.
        // slotManager.findTupleIndex() will return an arbitrary tuple matching
        // the given field prefix (according to the target comparator).
        // To make sure we traverse the right path, we must find the
        // leftmost or rightmost tuple that matches the prefix.
        int origTupleOff = slotManager.getTupleOff(slotOff);
        cmpFrameTuple.resetByTupleOffset(buf.array(), origTupleOff);
        int cmpTupleOff = origTupleOff;
        // The answer set begins with the lowest key matching the prefix.
        // We must follow the child pointer of the lowest (leftmost) key
        // matching the given prefix.
        int maxSlotOff = buf.capacity();
        slotOff += slotManager.getSlotSize();
        while (slotOff < maxSlotOff) {
            cmpTupleOff = slotManager.getTupleOff(slotOff);
            frameTuple.resetByTupleOffset(buf.array(), cmpTupleOff);
            if (targetCmp.compare(cmpFrameTuple, frameTuple) != 0) {
                break;
            }
            slotOff += slotManager.getSlotSize();
        }
        slotOff -= slotManager.getSlotSize();
        frameTuple.resetByTupleOffset(buf.array(), slotManager.getTupleOff(slotOff));
        int childPageOff = getLeftChildPageOff(frameTuple);

        return buf.getInt(childPageOff);
    }

    @Override
    public int getLeftmostChildPageId() {
        int tupleOff = slotManager.getTupleOff(slotManager.getSlotStartOff());
        frameTuple.resetByTupleOffset(buf.array(), tupleOff);
        int childPageOff = getLeftChildPageOff(frameTuple);
        return buf.getInt(childPageOff);
    }

    @Override
    public int getRightmostChildPageId() {
        return buf.getInt(RIGHT_LEAF_OFFSET);
    }

    @Override
    public void setRightmostChildPageId(int pageId) {
        buf.putInt(RIGHT_LEAF_OFFSET, pageId);
    }

    @Override
    public int getPageHeaderSize() {
        return RIGHT_LEAF_OFFSET + 4;
    }

    private int getLeftChildPageOff(ITupleReference tuple) {
        return tuple.getFieldStart(tuple.getFieldCount() - 1) + tuple.getFieldLength(tuple.getFieldCount() - 1);
    }

    @Override
    public void setMultiComparator(MultiComparator cmp) {
        this.cmp = cmp;
        cmpFrameTuple.setFieldCount(cmp.getKeyFieldCount());
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        previousFt.setFieldCount(cmp.getKeyFieldCount());
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        ITreeIndexTupleReference tuple = tupleWriter.createTupleReference();
        tuple.setFieldCount(cmp.getKeyFieldCount());
        return tuple;
    }

    // For debugging.
    public ArrayList<Integer> getChildren(MultiComparator cmp) {
        ArrayList<Integer> ret = new ArrayList<>();
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        int tupleCount = buf.getInt(Constants.TUPLE_COUNT_OFFSET);
        for (int i = 0; i < tupleCount; i++) {
            int tupleOff = slotManager.getTupleOff(slotManager.getSlotOff(i));
            frameTuple.resetByTupleOffset(buf.array(), tupleOff);
            int intVal =
                    IntegerPointable.getInteger(buf.array(), frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                            + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1));
            ret.add(intVal);
        }
        if (!isLeaf()) {
            int rightLeaf = buf.getInt(RIGHT_LEAF_OFFSET);
            if (rightLeaf > 0) {
                ret.add(buf.getInt(RIGHT_LEAF_OFFSET));
            }
        }
        return ret;
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
}
