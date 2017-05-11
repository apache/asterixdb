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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.frames.AbstractSlotManager;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.common.MultiComparator;

public class OrderedSlotManager extends AbstractSlotManager {

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference frameTuple, MultiComparator multiCmp,
            FindTupleMode mode, FindTupleNoExactMatchPolicy matchPolicy) throws HyracksDataException {
        int tupleCount = frame.getTupleCount();
        if (tupleCount <= 0) {
            return GREATEST_KEY_INDICATOR;
        }

        int mid;
        int begin;
        int end = tupleCount - 1;

        frameTuple.resetByTupleIndex(frame, end);
        int cmp = multiCmp.compare(searchKey, frameTuple);
        if (cmp > 0) {
            // This is a special optimization case when the tuple to be searched is larger than all the keys on the page.
            begin = tupleCount;
        } else {
            begin = 0;
        }

        while (begin <= end) {
            mid = (begin + end) / 2;
            frameTuple.resetByTupleIndex(frame, mid);

            cmp = multiCmp.compare(searchKey, frameTuple);
            if (cmp < 0) {
                end = mid - 1;
            } else if (cmp > 0) {
                begin = mid + 1;
            } else {
                if (mode == FindTupleMode.EXCLUSIVE) {
                    if (matchPolicy == FindTupleNoExactMatchPolicy.HIGHER_KEY) {
                        begin = mid + 1;
                    } else {
                        end = mid - 1;
                    }
                } else {
                    if (mode == FindTupleMode.EXCLUSIVE_ERROR_IF_EXISTS) {
                        return ERROR_INDICATOR;
                    } else {
                        return mid;
                    }
                }
            }
        }

        if (mode == FindTupleMode.EXACT) {
            return ERROR_INDICATOR;
        }

        if (matchPolicy == FindTupleNoExactMatchPolicy.HIGHER_KEY) {
            if (begin > tupleCount - 1) {
                return GREATEST_KEY_INDICATOR;
            }
            frameTuple.resetByTupleIndex(frame, begin);
            if (multiCmp.compare(searchKey, frameTuple) < 0) {
                return begin;
            } else {
                return GREATEST_KEY_INDICATOR;
            }
        } else {
            if (end < 0) {
                return GREATEST_KEY_INDICATOR;
            }
            frameTuple.resetByTupleIndex(frame, end);
            if (multiCmp.compare(searchKey, frameTuple) > 0) {
                return end;
            } else {
                return GREATEST_KEY_INDICATOR;
            }
        }
    }

    @Override
    public int insertSlot(int tupleIndex, int tupleOff) {
        int slotOff = getSlotOff(tupleIndex);
        if (tupleIndex == GREATEST_KEY_INDICATOR) {
            slotOff = getSlotEndOff() - slotSize;
            setSlot(slotOff, tupleOff);
            return slotOff;
        } else {
            int slotEndOff = getSlotEndOff();
            int length = (slotOff - slotEndOff) + slotSize;
            System.arraycopy(frame.getBuffer().array(), slotEndOff, frame.getBuffer().array(), slotEndOff - slotSize,
                    length);
            setSlot(slotOff, tupleOff);
            return slotOff;
        }
    }
}
