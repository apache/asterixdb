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

package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.frames.AbstractSlotManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMFrame;

public class UnorderedSlotManager extends AbstractSlotManager {

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference frameTuple, MultiComparator multiCmp,
            FindTupleMode mode, FindTupleNoExactMatchPolicy matchPolicy) {
        if (searchKey.getFieldCount() == frameTuple.getFieldCount()) {
            int maxFieldPos = multiCmp.getKeyFieldCount() / 2;
            for (int i = 0; i < frame.getTupleCount(); i++) {
                frameTuple.resetByTupleIndex(frame, i);

                boolean foundTuple = true;
                for (int j = 0; j < maxFieldPos; j++) {
                    int k = maxFieldPos + j;
                    int c1 = multiCmp.getComparators()[j].compare(frameTuple.getFieldData(j),
                            frameTuple.getFieldStart(j), frameTuple.getFieldLength(j), searchKey.getFieldData(j),
                            searchKey.getFieldStart(j), searchKey.getFieldLength(j));

                    if (c1 != 0) {
                        foundTuple = false;
                        break;
                    }
                    int c2 = multiCmp.getComparators()[k].compare(frameTuple.getFieldData(k),
                            frameTuple.getFieldStart(k), frameTuple.getFieldLength(k), searchKey.getFieldData(k),
                            searchKey.getFieldStart(k), searchKey.getFieldLength(k));
                    if (c2 != 0) {
                        foundTuple = false;
                        break;
                    }
                }
                int remainingFieldCount = frameTuple.getFieldCount() - multiCmp.getKeyFieldCount();
                for (int j = multiCmp.getKeyFieldCount(); j < multiCmp.getKeyFieldCount() + remainingFieldCount; j++) {
                    if (!compareField(searchKey, frameTuple, j)) {
                        foundTuple = false;
                        break;
                    }
                }
                if (foundTuple) {
                    return i;
                }
            }
        }
        return -1;
    }

    private boolean compareField(ITupleReference searchKey, ITreeIndexTupleReference frameTuple, int fIdx) {
        int searchKeyFieldLength = searchKey.getFieldLength(fIdx);
        int frameTupleFieldLength = frameTuple.getFieldLength(fIdx);

        if (searchKeyFieldLength != frameTupleFieldLength) {
            return false;
        }

        for (int i = 0; i < searchKeyFieldLength; i++) {
            if (searchKey.getFieldData(fIdx)[i + searchKey.getFieldStart(fIdx)] != frameTuple.getFieldData(fIdx)[i
                    + frameTuple.getFieldStart(fIdx)]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int insertSlot(int tupleIndex, int tupleOff) {
        int slotOff = getSlotEndOff() - slotSize;
        setSlot(slotOff, tupleOff);
        return slotOff;
    }

    public void modifySlot(int slotOff, int tupleOff) {
        setSlot(slotOff, tupleOff);
    }

    public void deleteSlot(int slotOff) {
        System.arraycopy(frame.getBuffer().array(), getSlotEndOff(), frame.getBuffer().array(), slotOff + slotSize,
                slotSize);
    }

    public void deleteEmptySlots() {
        int slotOff = getSlotStartOff();
        while (frame.getTupleCount() > 0) {
            while (frame.getBuffer().getInt(getSlotEndOff()) == -1) {
                ((RTreeNSMFrame) frame).setTupleCount(frame.getTupleCount() - 1);
                if (frame.getTupleCount() == 0) {
                    break;
                }
            }
            if (frame.getTupleCount() == 0 || slotOff <= getSlotEndOff()) {
                break;
            }
            if (frame.getBuffer().getInt(slotOff) == -1) {
                modifySlot(slotOff, frame.getBuffer().getInt(getSlotEndOff()));
                ((RTreeNSMFrame) frame).setTupleCount(frame.getTupleCount() - 1);
            }
            slotOff -= slotSize;

        }
    }
}
