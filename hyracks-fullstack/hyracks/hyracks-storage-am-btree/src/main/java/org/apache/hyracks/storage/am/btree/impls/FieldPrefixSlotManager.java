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

package org.apache.hyracks.storage.am.btree.impls;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IPrefixSlotManager;
import org.apache.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.common.MultiComparator;

public class FieldPrefixSlotManager implements IPrefixSlotManager {

    public static final int TUPLE_UNCOMPRESSED = 0xFF;
    public static final int MAX_PREFIX_SLOTS = 0xFE;
    public static final int GREATEST_KEY_INDICATOR = 0x00FFFFFF;
    public static final int ERROR_INDICATOR = 0x00FFFFFE;

    private static final int slotSize = 4;

    private ByteBuffer buf;
    private BTreeFieldPrefixNSMLeafFrame frame;
    private MultiComparator cmp;

    public int decodeFirstSlotField(int slot) {
        return (slot & 0xFF000000) >>> 24;
    }

    public int decodeSecondSlotField(int slot) {
        return slot & 0x00FFFFFF;
    }

    public int encodeSlotFields(int firstField, int secondField) {
        return ((firstField & 0x000000FF) << 24) | (secondField & 0x00FFFFFF);
    }

    // returns prefix slot number, or TUPLE_UNCOMPRESSED of no match was found
    public int findPrefix(ITupleReference tuple, ITreeIndexTupleReference framePrefixTuple)
            throws HyracksDataException {
        int prefixMid;
        int prefixBegin = 0;
        int prefixEnd = frame.getPrefixTupleCount() - 1;

        if (frame.getPrefixTupleCount() > 0) {
            while (prefixBegin <= prefixEnd) {
                prefixMid = (prefixBegin + prefixEnd) / 2;
                framePrefixTuple.resetByTupleIndex(frame, prefixMid);
                int cmpVal = cmp.fieldRangeCompare(tuple, framePrefixTuple, 0, framePrefixTuple.getFieldCount());
                if (cmpVal < 0)
                    prefixEnd = prefixMid - 1;
                else if (cmpVal > 0)
                    prefixBegin = prefixMid + 1;
                else
                    return prefixMid;
            }
        }

        return FieldPrefixSlotManager.TUPLE_UNCOMPRESSED;
    }

    @Override
    public int findSlot(ITupleReference searchKey, ITreeIndexTupleReference frameTuple,
            ITreeIndexTupleReference framePrefixTuple, MultiComparator multiCmp, FindTupleMode mode,
            FindTupleNoExactMatchPolicy matchPolicy) throws HyracksDataException {
        if (frame.getTupleCount() <= 0)
            encodeSlotFields(TUPLE_UNCOMPRESSED, GREATEST_KEY_INDICATOR);

        int prefixMid;
        int prefixBegin = 0;
        int prefixEnd = frame.getPrefixTupleCount() - 1;
        int prefixMatch = TUPLE_UNCOMPRESSED;

        // bounds are inclusive on both ends
        int tuplePrefixSlotNumLbound = prefixBegin;
        int tuplePrefixSlotNumUbound = prefixEnd;

        // binary search on the prefix slots to determine upper and lower bounds
        // for the prefixSlotNums in tuple slots
        while (prefixBegin <= prefixEnd) {
            prefixMid = (prefixBegin + prefixEnd) / 2;
            framePrefixTuple.resetByTupleIndex(frame, prefixMid);
            int cmp = multiCmp.fieldRangeCompare(searchKey, framePrefixTuple, 0, framePrefixTuple.getFieldCount());
            if (cmp < 0) {
                prefixEnd = prefixMid - 1;
                tuplePrefixSlotNumLbound = prefixMid - 1;
            } else if (cmp > 0) {
                prefixBegin = prefixMid + 1;
                tuplePrefixSlotNumUbound = prefixMid + 1;
            } else {
                if (mode == FindTupleMode.EXCLUSIVE) {
                    if (matchPolicy == FindTupleNoExactMatchPolicy.HIGHER_KEY)
                        prefixBegin = prefixMid + 1;
                    else
                        prefixEnd = prefixMid - 1;
                } else {
                    tuplePrefixSlotNumLbound = prefixMid;
                    tuplePrefixSlotNumUbound = prefixMid;
                    prefixMatch = prefixMid;
                }

                break;
            }
        }

        int tupleMid = -1;
        int tupleBegin = 0;
        int tupleEnd = frame.getTupleCount() - 1;

        // binary search on tuples, guided by the lower and upper bounds on prefixSlotNum
        while (tupleBegin <= tupleEnd) {
            tupleMid = (tupleBegin + tupleEnd) / 2;
            int tupleSlotOff = getTupleSlotOff(tupleMid);
            int tupleSlot = buf.getInt(tupleSlotOff);
            int prefixSlotNum = decodeFirstSlotField(tupleSlot);

            int cmp = 0;
            if (prefixSlotNum == TUPLE_UNCOMPRESSED) {
                frameTuple.resetByTupleIndex(frame, tupleMid);
                cmp = multiCmp.compare(searchKey, frameTuple);
            } else {
                if (prefixSlotNum < tuplePrefixSlotNumLbound)
                    cmp = 1;
                else if (prefixSlotNum > tuplePrefixSlotNumUbound)
                    cmp = -1;
                else {
                    frameTuple.resetByTupleIndex(frame, tupleMid);
                    cmp = multiCmp.compare(searchKey, frameTuple);
                }
            }

            if (cmp < 0)
                tupleEnd = tupleMid - 1;
            else if (cmp > 0)
                tupleBegin = tupleMid + 1;
            else {
                if (mode == FindTupleMode.EXCLUSIVE) {
                    if (matchPolicy == FindTupleNoExactMatchPolicy.HIGHER_KEY)
                        tupleBegin = tupleMid + 1;
                    else
                        tupleEnd = tupleMid - 1;
                } else {
                    if (mode == FindTupleMode.EXCLUSIVE_ERROR_IF_EXISTS) {
                        return encodeSlotFields(prefixMatch, ERROR_INDICATOR);
                    } else {
                        return encodeSlotFields(prefixMatch, tupleMid);
                    }
                }
            }
        }

        if (mode == FindTupleMode.EXACT)
            return encodeSlotFields(prefixMatch, ERROR_INDICATOR);

        // do final comparison to determine whether the search key is greater
        // than all keys or in between some existing keys
        if (matchPolicy == FindTupleNoExactMatchPolicy.HIGHER_KEY) {
            if (tupleBegin > frame.getTupleCount() - 1)
                return encodeSlotFields(prefixMatch, GREATEST_KEY_INDICATOR);
            frameTuple.resetByTupleIndex(frame, tupleBegin);
            if (multiCmp.compare(searchKey, frameTuple) < 0)
                return encodeSlotFields(prefixMatch, tupleBegin);
            else
                return encodeSlotFields(prefixMatch, GREATEST_KEY_INDICATOR);
        } else {
            if (tupleEnd < 0)
                return encodeSlotFields(prefixMatch, GREATEST_KEY_INDICATOR);
            frameTuple.resetByTupleIndex(frame, tupleEnd);
            if (multiCmp.compare(searchKey, frameTuple) > 0)
                return encodeSlotFields(prefixMatch, tupleEnd);
            else
                return encodeSlotFields(prefixMatch, GREATEST_KEY_INDICATOR);
        }
    }

    public int getPrefixSlotStartOff() {
        return buf.capacity() - slotSize;
    }

    public int getPrefixSlotEndOff() {
        return buf.capacity() - slotSize * frame.getPrefixTupleCount();
    }

    public int getTupleSlotStartOff() {
        return getPrefixSlotEndOff() - slotSize;
    }

    public int getTupleSlotEndOff() {
        return buf.capacity() - slotSize * (frame.getPrefixTupleCount() + frame.getTupleCount());
    }

    public int getSlotSize() {
        return slotSize;
    }

    public void setSlot(int offset, int value) {
        frame.getBuffer().putInt(offset, value);
    }

    public int insertSlot(int slot, int tupleOff) {
        int slotNum = decodeSecondSlotField(slot);
        if (slotNum == ERROR_INDICATOR) {
            System.out.println("WOW BIG PROBLEM!");
        }
        if (slotNum == GREATEST_KEY_INDICATOR) {
            int slotOff = getTupleSlotEndOff() - slotSize;
            int newSlot = encodeSlotFields(decodeFirstSlotField(slot), tupleOff);
            setSlot(slotOff, newSlot);
            return newSlot;
        } else {
            int slotEndOff = getTupleSlotEndOff();
            int slotOff = getTupleSlotOff(slotNum);
            int length = (slotOff - slotEndOff) + slotSize;
            System.arraycopy(frame.getBuffer().array(), slotEndOff, frame.getBuffer().array(), slotEndOff - slotSize,
                    length);

            int newSlot = encodeSlotFields(decodeFirstSlotField(slot), tupleOff);
            setSlot(slotOff, newSlot);
            return newSlot;
        }
    }

    public int getPrefixSlotOff(int tupleIndex) {
        return getPrefixSlotStartOff() - tupleIndex * slotSize;
    }

    public int getTupleSlotOff(int tupleIndex) {
        return getTupleSlotStartOff() - tupleIndex * slotSize;
    }

    public void setPrefixSlot(int tupleIndex, int slot) {
        buf.putInt(getPrefixSlotOff(tupleIndex), slot);
    }

    @Override
    public int getGreatestKeyIndicator() {
        return GREATEST_KEY_INDICATOR;
    }

    @Override
    public int getErrorIndicator() {
        return ERROR_INDICATOR;
    }

    @Override
    public void setFrame(ITreeIndexFrame frame) {
        this.frame = (BTreeFieldPrefixNSMLeafFrame) frame;
        this.buf = frame.getBuffer();
    }

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference frameTuple, MultiComparator multiCmp,
            FindTupleMode mode, FindTupleNoExactMatchPolicy matchPolicy) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public int getSlotStartOff() {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public int getSlotEndOff() {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public int getTupleOff(int slotOff) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    @Override
    public int getSlotOff(int tupleIndex) {
        throw new UnsupportedOperationException("Not implemented.");
    }

    public void setMultiComparator(MultiComparator cmp) {
        this.cmp = cmp;
    }
}
