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

package edu.uci.ics.hyracks.storage.am.rtree.frames;

import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.SlotOffTupleOff;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.PathList;

public class RTreeNSMInteriorFrame extends RTreeNSMFrame implements IRTreeInteriorFrame {

    private static final int childPtrSize = 4;
    private IBinaryComparator childPtrCmp = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY)
            .createBinaryComparator();
    private final int keyFieldCount;

    public RTreeNSMInteriorFrame(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders,
            RTreePolicyType rtreePolicyType) {
        super(tupleWriter, keyValueProviders, rtreePolicyType);
        keyFieldCount = keyValueProviders.length;
        frameTuple.setFieldCount(keyFieldCount);
    }

    @Override
    public int getBytesRequriedToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + childPtrSize + slotManager.getSlotSize();
    }

    @Override
    public int findBestChild(ITupleReference tuple, MultiComparator cmp) {
        int bestChild = rtreePolicy.findBestChildPosition(this, tuple, frameTuple, cmp);
        frameTuple.resetByTupleIndex(this, bestChild);
        return buf.getInt(getChildPointerOff(frameTuple));
    }

    // frameTuple is assumed to have the tuple to be tested against.
    @Override
    public boolean checkIfEnlarementIsNeeded(ITupleReference tuple, MultiComparator cmp) {
        return !RTreeComputationUtils.containsRegion(frameTuple, tuple, cmp, keyValueProviders);
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        ITreeIndexTupleReference tuple = tupleWriter.createTupleReference();
        tuple.setFieldCount(keyFieldCount);
        return tuple;
    }

    @Override
    public int findTupleByPointer(ITupleReference tuple, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        for (int i = 0; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            int c = pointerCmp(frameTuple, tuple, cmp);
            if (c == 0) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int getChildPageId(int tupleIndex) {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        return buf.getInt(getChildPointerOff(frameTuple));
    }

    @Override
    public int getChildPageIdIfIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j));
            if (c > 0) {
                return -1;
            }
            c = cmp.getComparators()[i].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
            if (c < 0) {
                return -1;
            }
        }
        return buf.getInt(getChildPointerOff(frameTuple));
    }

    @Override
    public int findTupleByPointer(ITupleReference tuple, PathList traverseList, int parentIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        for (int i = 0; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);

            int c = pointerCmp(frameTuple, tuple, cmp);
            if (c == 0) {
                return i;
            } else {
                int pageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(cmp.getKeyFieldCount() - 1),
                        getChildPointerOff(frameTuple));
                traverseList.add(pageId, -1, parentIndex);
            }
        }
        return -1;
    }

    @Override
    public boolean compact() {
        resetSpaceParams();

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
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) {
        int bytesRequired = tupleWriter.bytesRequired(tuple) + childPtrSize;
        if (bytesRequired + slotManager.getSlotSize() <= buf.capacity() - buf.getInt(freeSpaceOff)
                - (buf.getInt(tupleCountOff) * slotManager.getSlotSize()))
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        else if (bytesRequired + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff))
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        else
            return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    @Override
    public void adjustKey(ITupleReference tuple, int tupleIndex, MultiComparator cmp) throws TreeIndexException {
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        if (tupleIndex == -1) {
            tupleIndex = findTupleByPointer(tuple, cmp);
        }
        if (tupleIndex != -1) {
            tupleWriter.writeTuple(tuple, buf.array(), getTupleOffset(tupleIndex));
        } else {
            throw new TreeIndexException("Error: Faild to find a tuple in a page");

        }

    }

    protected int pointerCmp(ITupleReference tupleA, ITupleReference tupleB, MultiComparator cmp) {
        return childPtrCmp
                .compare(tupleA.getFieldData(cmp.getKeyFieldCount() - 1), getChildPointerOff(tupleA), childPtrSize,
                        tupleB.getFieldData(cmp.getKeyFieldCount() - 1), getChildPointerOff(tupleB), childPtrSize);
    }

    public int getTupleSize(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + childPtrSize;
    }

    private int getChildPointerOff(ITupleReference tuple) {
        return tuple.getFieldStart(tuple.getFieldCount() - 1) + tuple.getFieldLength(tuple.getFieldCount() - 1);
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        frameTuple.setFieldCount(tuple.getFieldCount());
        slotManager.insertSlot(-1, buf.getInt(freeSpaceOff));
        int freeSpace = buf.getInt(freeSpaceOff);
        int bytesWritten = tupleWriter.writeTupleFields(tuple, 0, tuple.getFieldCount(), buf.array(), freeSpace);
        System.arraycopy(tuple.getFieldData(tuple.getFieldCount() - 1), getChildPointerOff(tuple), buf.array(),
                freeSpace + bytesWritten, childPtrSize);
        int tupleSize = bytesWritten + childPtrSize;

        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + tupleSize);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - tupleSize - slotManager.getSlotSize());

    }

    @Override
    public void delete(int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        int slotOff = slotManager.getSlotOff(tupleIndex);

        int tupleOff = slotManager.getTupleOff(slotOff);
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int tupleSize = tupleWriter.bytesRequired(frameTuple);

        // perform deletion (we just do a memcpy to overwrite the slot)
        int slotStartOff = slotManager.getSlotEndOff();
        int length = slotOff - slotStartOff;
        System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);

        // maintain space information
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) - 1);
        buf.putInt(totalFreeSpaceOff,
                buf.getInt(totalFreeSpaceOff) + tupleSize + childPtrSize + slotManager.getSlotSize());
    }

    @Override
    public void enlarge(ITupleReference tuple, MultiComparator cmp) {
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(frameTuple.getFieldData(i), frameTuple.getFieldStart(i),
                    frameTuple.getFieldLength(i), tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i));
            if (c > 0) {
                System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), frameTuple.getFieldData(i),
                        frameTuple.getFieldStart(i), tuple.getFieldLength(i));
            }
            c = cmp.getComparators()[j].compare(frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j), tuple.getFieldData(j), tuple.getFieldStart(j),
                    tuple.getFieldLength(j));
            if (c < 0) {
                System.arraycopy(tuple.getFieldData(j), tuple.getFieldStart(j), frameTuple.getFieldData(j),
                        frameTuple.getFieldStart(j), tuple.getFieldLength(j));
            }
        }
    }

    // For debugging.
    public ArrayList<Integer> getChildren(MultiComparator cmp) {
        ArrayList<Integer> ret = new ArrayList<Integer>();
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        int tupleCount = buf.getInt(tupleCountOff);
        for (int i = 0; i < tupleCount; i++) {
            int tupleOff = slotManager.getTupleOff(slotManager.getSlotOff(i));
            frameTuple.resetByTupleOffset(buf, tupleOff);
            int intVal = IntegerSerializerDeserializer.getInt(
                    buf.array(),
                    frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                            + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1));
            ret.add(intVal);
        }
        return ret;
    }

    @Override
    public int getFieldCount() {
        return keyValueProviders.length;
    }

    public int getChildPointerSize() {
        return childPtrSize;
    }
}
