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
import edu.uci.ics.hyracks.storage.am.rtree.impls.EntriesOrder;
import edu.uci.ics.hyracks.storage.am.rtree.impls.PathList;

public class RTreeNSMInteriorFrame extends RTreeNSMFrame implements IRTreeInteriorFrame {

    private static final int childPtrSize = 4;
    private IBinaryComparator childPtrCmp = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY)
            .createBinaryComparator();
    private final int keyFieldCount;

    public RTreeNSMInteriorFrame(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders) {
        super(tupleWriter, keyValueProviders);
        keyFieldCount = keyValueProviders.length;
        frameTuple.setFieldCount(keyFieldCount);
    }

    @Override
    public boolean findBestChild(ITupleReference tuple, MultiComparator cmp) {
        cmpFrameTuple.setFieldCount(cmp.getKeyFieldCount());
        frameTuple.setFieldCount(cmp.getKeyFieldCount());

        int bestChild = 0;
        double minEnlargedArea = Double.MAX_VALUE;

        // the children pointers in the node point to leaves
        if (getLevel() == 1) {
            // find least overlap enlargement, use minimum enlarged area to
            // break tie, if tie still exists use minimum area to break it
            for (int i = 0; i < getTupleCount(); ++i) {
                frameTuple.resetByTupleIndex(this, i);
                double enlargedArea = enlargedArea(frameTuple, tuple, cmp);
                tupleEntries1.add(i, enlargedArea);
                if (enlargedArea < minEnlargedArea) {
                    minEnlargedArea = enlargedArea;
                    bestChild = i;
                }
            }
            if (minEnlargedArea < RTreeNSMFrame.doubleEpsilon() || minEnlargedArea > RTreeNSMFrame.doubleEpsilon()) {
                minEnlargedArea = Double.MAX_VALUE;
                int k;
                if (getTupleCount() > nearMinimumOverlapFactor) {
                    // sort the entries based on their area enlargement needed
                    // to include the object
                    tupleEntries1.sort(EntriesOrder.ASCENDING, getTupleCount());
                    k = nearMinimumOverlapFactor;
                } else {
                    k = getTupleCount();
                }

                double minOverlap = Double.MAX_VALUE;
                int id = 0;
                for (int i = 0; i < k; ++i) {
                    double difference = 0.0;
                    for (int j = 0; j < getTupleCount(); ++j) {
                        frameTuple.resetByTupleIndex(this, j);
                        cmpFrameTuple.resetByTupleIndex(this, tupleEntries1.get(i).getTupleIndex());

                        int c = pointerCmp(frameTuple, cmpFrameTuple, cmp);
                        if (c != 0) {
                            double intersection = overlappedArea(frameTuple, tuple, cmpFrameTuple, cmp);
                            if (intersection != 0.0) {
                                difference += intersection - overlappedArea(frameTuple, null, cmpFrameTuple, cmp);
                            }
                        } else {
                            id = j;
                        }
                    }

                    double enlargedArea = enlargedArea(cmpFrameTuple, tuple, cmp);
                    if (difference < minOverlap) {
                        minOverlap = difference;
                        minEnlargedArea = enlargedArea;
                        bestChild = id;
                    } else if (difference == minOverlap) {
                        if (enlargedArea < minEnlargedArea) {
                            minEnlargedArea = enlargedArea;
                            bestChild = id;
                        } else if (enlargedArea == minEnlargedArea) {
                            double area = area(cmpFrameTuple, cmp);
                            frameTuple.resetByTupleIndex(this, bestChild);
                            double minArea = area(frameTuple, cmp);
                            if (area < minArea) {
                                bestChild = id;
                            }
                        }
                    }
                }
            }
        } else { // find minimum enlarged area, use minimum area to break tie
            for (int i = 0; i < getTupleCount(); i++) {
                frameTuple.resetByTupleIndex(this, i);
                double enlargedArea = enlargedArea(frameTuple, tuple, cmp);
                if (enlargedArea < minEnlargedArea) {
                    minEnlargedArea = enlargedArea;
                    bestChild = i;
                } else if (enlargedArea == minEnlargedArea) {
                    double area = area(frameTuple, cmp);
                    frameTuple.resetByTupleIndex(this, bestChild);
                    double minArea = area(frameTuple, cmp);
                    if (area < minArea) {
                        bestChild = i;
                    }
                }
            }
        }
        tupleEntries1.clear();

        frameTuple.resetByTupleIndex(this, bestChild);
        if (minEnlargedArea > 0.0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        ITreeIndexTupleReference tuple = tupleWriter.createTupleReference();
        tuple.setFieldCount(keyFieldCount);
        return tuple;
    }

    @Override
    public int getBestChildPageId() {
        return buf.getInt(getChildPointerOff(frameTuple));
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

    private int pointerCmp(ITupleReference tupleA, ITupleReference tupleB, MultiComparator cmp) {
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
    public boolean recomputeMBR(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        frameTuple.resetByTupleIndex(this, tupleIndex);

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(frameTuple.getFieldData(i), frameTuple.getFieldStart(i),
                    frameTuple.getFieldLength(i), tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i));
            if (c != 0) {
                return true;
            }
            c = cmp.getComparators()[j].compare(frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j), tuple.getFieldData(j), tuple.getFieldStart(j),
                    tuple.getFieldLength(j));

            if (c != 0) {
                return true;
            }
        }
        return false;
    }

    private double overlappedArea(ITupleReference tuple1, ITupleReference tupleToBeInserted, ITupleReference tuple2,
            MultiComparator cmp) {
        double area = 1.0;
        double f1, f2;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            double pHigh1, pLow1;
            if (tupleToBeInserted != null) {
                int c = cmp.getComparators()[i].compare(tuple1.getFieldData(i), tuple1.getFieldStart(i),
                        tuple1.getFieldLength(i), tupleToBeInserted.getFieldData(i),
                        tupleToBeInserted.getFieldStart(i), tupleToBeInserted.getFieldLength(i));
                if (c < 0) {
                    pLow1 = keyValueProviders[i].getValue(tuple1.getFieldData(i), tuple1.getFieldStart(i));
                } else {
                    pLow1 = keyValueProviders[i].getValue(tupleToBeInserted.getFieldData(i),
                            tupleToBeInserted.getFieldStart(i));
                }

                c = cmp.getComparators()[j].compare(tuple1.getFieldData(j), tuple1.getFieldStart(j),
                        tuple1.getFieldLength(j), tupleToBeInserted.getFieldData(j),
                        tupleToBeInserted.getFieldStart(j), tupleToBeInserted.getFieldLength(j));
                if (c > 0) {
                    pHigh1 = keyValueProviders[j].getValue(tuple1.getFieldData(j), tuple1.getFieldStart(j));
                } else {
                    pHigh1 = keyValueProviders[j].getValue(tupleToBeInserted.getFieldData(j),
                            tupleToBeInserted.getFieldStart(j));
                }
            } else {
                pLow1 = keyValueProviders[i].getValue(tuple1.getFieldData(i), tuple1.getFieldStart(i));
                pHigh1 = keyValueProviders[j].getValue(tuple1.getFieldData(j), tuple1.getFieldStart(j));
            }

            double pLow2 = keyValueProviders[i].getValue(tuple2.getFieldData(i), tuple2.getFieldStart(i));
            double pHigh2 = keyValueProviders[j].getValue(tuple2.getFieldData(j), tuple2.getFieldStart(j));

            if (pLow1 > pHigh2 || pHigh1 < pLow2) {
                return 0.0;
            }

            f1 = Math.max(pLow1, pLow2);
            f2 = Math.min(pHigh1, pHigh2);
            area *= f2 - f1;
        }
        return area;
    }

    private double enlargedArea(ITupleReference tuple, ITupleReference tupleToBeInserted, MultiComparator cmp) {
        double areaBeforeEnlarge = area(tuple, cmp);
        double areaAfterEnlarge = 1.0;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            double pHigh, pLow;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                    tupleToBeInserted.getFieldLength(i));
            if (c < 0) {
                pLow = keyValueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i));
            } else {
                pLow = keyValueProviders[i].getValue(tupleToBeInserted.getFieldData(i),
                        tupleToBeInserted.getFieldStart(i));
            }

            c = cmp.getComparators()[j].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                    tupleToBeInserted.getFieldLength(j));
            if (c > 0) {
                pHigh = keyValueProviders[j].getValue(tuple.getFieldData(j), tuple.getFieldStart(j));
            } else {
                pHigh = keyValueProviders[j].getValue(tupleToBeInserted.getFieldData(j),
                        tupleToBeInserted.getFieldStart(j));
            }
            areaAfterEnlarge *= pHigh - pLow;
        }
        return areaAfterEnlarge - areaBeforeEnlarge;
    }

    private double area(ITupleReference tuple, MultiComparator cmp) {
        double area = 1.0;
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            area *= keyValueProviders[j].getValue(tuple.getFieldData(j), tuple.getFieldStart(j))
                    - keyValueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i));
        }
        return area;
    }

    @Override
    public boolean checkEnlargement(ITupleReference tuple, MultiComparator cmp) {
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(frameTuple.getFieldData(i), frameTuple.getFieldStart(i),
                    frameTuple.getFieldLength(i), tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i));
            if (c > 0) {
                return true;
            }
            c = cmp.getComparators()[j].compare(frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j), tuple.getFieldData(j), tuple.getFieldStart(j),
                    tuple.getFieldLength(j));
            if (c < 0) {
                return true;
            }
        }
        return false;
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
}
