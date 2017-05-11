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

package org.apache.hyracks.storage.am.btree.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * A slot consists of two fields. The first field is 1 byte and it indicates the slot number of
 * a prefix tuple that is called the first field prefixSlotOff. The second field is 3 bytes and
 * it points to the start offset of a tuple that is called the second field tupleOff.
 * We distinguish between two slot types:
 * 1) prefix slots that point to prefix tuples (a frame is assumed to have a field numPrefixTuples)
 * 2) tuple slots that point to data tuples (a frame is assumed to have a field numTuples)
 * A tuple slot contains a tuple pointer and a pointer to a prefix slot (prefix slot number).
 * INSERT procedure:
 * - A tuple insertion may use an existing prefix tuple
 * - A tuple insertion may never create a new prefix tuple
 * Modifying the prefix slots would be extremely expensive because potentially all tuples slots
 * would have to change their prefix slot pointers. All prefixes are recomputed during a reorg
 * or compaction.
 */
public interface IPrefixSlotManager extends ISlotManager {
    // TODO: Clean up interface after extending ISlotManager.

    public int decodeFirstSlotField(int slot);

    public int decodeSecondSlotField(int slot);

    public int encodeSlotFields(int firstField, int secondField);

    public int findSlot(ITupleReference searchKey, ITreeIndexTupleReference frameTuple,
            ITreeIndexTupleReference framePrefixTuple, MultiComparator multiCmp, FindTupleMode mode,
            FindTupleNoExactMatchPolicy matchPolicy) throws HyracksDataException;

    public int insertSlot(int slot, int tupleOff);

    /**
     * @return the prefix slot number or FieldPrefixSlotManager.TUPLE_UNCOMPRESSED if none found
     * @throws HyracksDataException
     */
    public int findPrefix(ITupleReference tuple, ITreeIndexTupleReference framePrefixTuple) throws HyracksDataException;

    public int getTupleSlotStartOff();

    public int getTupleSlotEndOff();

    public int getPrefixSlotStartOff();

    public int getPrefixSlotEndOff();

    public int getTupleSlotOff(int tupleIndex);

    public int getPrefixSlotOff(int tupleIndex);

    public int getSlotSize();

    public void setSlot(int offset, int value);

    // functions for testing
    public void setPrefixSlot(int tupleIndex, int slot);

    public void setMultiComparator(MultiComparator cmp);
}
