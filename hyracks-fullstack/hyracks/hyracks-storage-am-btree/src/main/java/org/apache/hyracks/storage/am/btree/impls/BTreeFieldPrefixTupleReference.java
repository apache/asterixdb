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

import org.apache.hyracks.storage.am.btree.api.IPrefixSlotManager;
import org.apache.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrame;
import org.apache.hyracks.storage.am.common.api.IBTreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;

public class BTreeFieldPrefixTupleReference implements IBTreeIndexTupleReference {

    private final IBTreeIndexTupleReference helperTuple;

    private BTreeFieldPrefixNSMLeafFrame frame;
    private int prefixTupleStartOff;
    private int suffixTupleStartOff;
    private int numPrefixFields;
    private int fieldCount;

    public BTreeFieldPrefixTupleReference(IBTreeIndexTupleReference helperTuple) {
        this.helperTuple = helperTuple;
        this.fieldCount = helperTuple.getFieldCount();
    }

    @Override
    public void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        this.frame = (BTreeFieldPrefixNSMLeafFrame) frame;
        IPrefixSlotManager slotManager = this.frame.getSlotManager();
        int tupleSlotOff = slotManager.getTupleSlotOff(tupleIndex);
        int tupleSlot = this.frame.getBuffer().getInt(tupleSlotOff);
        int prefixSlotNum = slotManager.decodeFirstSlotField(tupleSlot);
        suffixTupleStartOff = slotManager.decodeSecondSlotField(tupleSlot);

        if (prefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);
            int prefixSlot = this.frame.getBuffer().getInt(prefixSlotOff);
            numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
            prefixTupleStartOff = slotManager.decodeSecondSlotField(prefixSlot);
        } else {
            numPrefixFields = 0;
            prefixTupleStartOff = -1;
        }
    }

    @Override
    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
    }

    @Override
    public void setFieldCount(int fieldStartIndex, int fieldCount) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return frame.getBuffer().array();
    }

    @Override
    public int getFieldLength(int fIdx) {
        if (fIdx < numPrefixFields) {
            helperTuple.setFieldCount(numPrefixFields);
            helperTuple.resetByTupleOffset(frame.getBuffer().array(), prefixTupleStartOff);
            return helperTuple.getFieldLength(fIdx);
        } else {
            helperTuple.setFieldCount(numPrefixFields, fieldCount - numPrefixFields);
            helperTuple.resetByTupleOffset(frame.getBuffer().array(), suffixTupleStartOff);
            return helperTuple.getFieldLength(fIdx - numPrefixFields);
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        if (fIdx < numPrefixFields) {
            helperTuple.setFieldCount(numPrefixFields);
            helperTuple.resetByTupleOffset(frame.getBuffer().array(), prefixTupleStartOff);
            return helperTuple.getFieldStart(fIdx);
        } else {
            helperTuple.setFieldCount(numPrefixFields, fieldCount - numPrefixFields);
            helperTuple.resetByTupleOffset(frame.getBuffer().array(), suffixTupleStartOff);
            return helperTuple.getFieldStart(fIdx - numPrefixFields);
        }
    }

    // unsupported operation
    @Override
    public void resetByTupleOffset(byte[] buf, int tupleStartOffset) {
        throw new UnsupportedOperationException("Resetting this type of frame by offset is not supported.");
    }

    @Override
    public int getTupleSize() {
        return getSuffixTupleSize() + getPrefixTupleSize();
    }

    public int getSuffixTupleSize() {
        helperTuple.setFieldCount(numPrefixFields, fieldCount - numPrefixFields);
        helperTuple.resetByTupleOffset(frame.getBuffer().array(), suffixTupleStartOff);
        return helperTuple.getTupleSize();
    }

    public int getPrefixTupleSize() {
        if (numPrefixFields == 0) {
            return 0;
        }
        helperTuple.setFieldCount(numPrefixFields);
        helperTuple.resetByTupleOffset(frame.getBuffer().array(), prefixTupleStartOff);
        return helperTuple.getTupleSize();
    }

    public int getNumPrefixFields() {
        return numPrefixFields;
    }

    @Override
    public boolean flipUpdated() {
        return helperTuple.flipUpdated();
    }

    @Override
    public boolean isUpdated() {
        return helperTuple.isUpdated();
    }
}
