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

package org.apache.hyracks.storage.am.vector.frames;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.btree.frames.OrderedSlotManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;

/**
 * VTree interior (root and intermediate) frame.
 * <p>
 * Page layout: base header (from {@link VTreeNSMFrame}) followed by a 4-byte next-overflow-page pointer and a
 * 1-byte overflow flag. Tuples have the format
 * {@code <cid, full_precision_centroid, child_page_pointer>}; the child pointer is read from the last field.
 */
public class VTreeInteriorFrame extends VTreeNSMFrame implements IVTreeInteriorFrame {

    // Offset (bytes from page start) of the 4-byte next-overflow-page pointer.
    protected static final int NEXT_PAGE_OFFSET = CENTROID_ID_OFFSET + Integer.BYTES;
    // Offset of the 1-byte overflow flag (0 = no overflow, non-zero = chained).
    protected static final int OVERFLOW_FLAG_OFFSET = NEXT_PAGE_OFFSET + Integer.BYTES;

    public VTreeInteriorFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new OrderedSlotManager());
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(NEXT_PAGE_OFFSET, -1);
        buf.put(OVERFLOW_FLAG_OFFSET, (byte) 0);
    }

    @Override
    public int getPageHeaderSize() {
        return OVERFLOW_FLAG_OFFSET + Byte.BYTES;
    }

    @Override
    public void setOverflowFlagBit(boolean overflowFlag) {
        buf.put(OVERFLOW_FLAG_OFFSET, (byte) (overflowFlag ? 1 : 0));
    }

    @Override
    public boolean getOverflowFlagBit() {
        return buf.get(OVERFLOW_FLAG_OFFSET) != 0;
    }

    @Override
    public void setNextPage(int nextPageId) {
        buf.putInt(NEXT_PAGE_OFFSET, nextPageId);
    }

    @Override
    public int getNextPage() {
        return buf.getInt(NEXT_PAGE_OFFSET);
    }

    @Override
    public int getChildPageId(int tupleIndex) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        // Child page pointer is stored as the last field of the tuple.
        int childPtrFieldIndex = frameTuple.getFieldCount() - 1;
        return IntegerPointable.getInteger(frameTuple.getFieldData(childPtrFieldIndex),
                frameTuple.getFieldStart(childPtrFieldIndex));
    }

    @Override
    public void setChildPageId(int tupleIndex, int childPageId) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int childPtrFieldIndex = frameTuple.getFieldCount() - 1;
        IntegerPointable.setInteger(frameTuple.getFieldData(childPtrFieldIndex),
                frameTuple.getFieldStart(childPtrFieldIndex), childPageId);
    }
}
