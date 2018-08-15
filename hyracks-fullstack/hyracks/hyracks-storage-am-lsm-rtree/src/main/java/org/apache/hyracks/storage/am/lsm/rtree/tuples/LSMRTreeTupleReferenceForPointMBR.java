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

package org.apache.hyracks.storage.am.lsm.rtree.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleReference;

public class LSMRTreeTupleReferenceForPointMBR extends RTreeTypeAwareTupleReference implements ILSMTreeTupleReference {
    private final int inputKeyFieldCount; //double field count for mbr secondary key of an input tuple
    private final int inputTotalFieldCount; //total field count (key + value fields) of an input tuple.
    private final int storedKeyFieldCount; //double field count to be stored for the mbr secondary key

    private final boolean antimatterAware;

    public LSMRTreeTupleReferenceForPointMBR(ITypeTraits[] typeTraits, int keyFieldCount, int valueFieldCount,
            boolean antimatterAware) {
        super(typeTraits);
        this.inputKeyFieldCount = keyFieldCount;
        this.inputTotalFieldCount = keyFieldCount + valueFieldCount;
        this.storedKeyFieldCount = keyFieldCount / 2;

        this.nullFlagsBytes = getNullFlagsBytes();
        this.decodedFieldSlots = new int[inputTotalFieldCount];
        this.antimatterAware = antimatterAware;
    }

    @Override
    public void resetByTupleOffset(byte[] buf, int tupleStartOff) {
        this.buf = buf;
        this.tupleStartOff = tupleStartOff;

        // decode field slots in three steps
        int field = 0;
        int cumul = 0;
        //step1. decode field slots for stored key
        for (int i = 0; i < storedKeyFieldCount; i++) {
            //key or value fields
            cumul += typeTraits[i].getFixedLength();
            decodedFieldSlots[field++] = cumul;
        }
        //step2. decode field slots for non-stored (duplicated point) key
        // this simply copies the field slots for stored key.
        for (int i = 0; i < storedKeyFieldCount; i++) {
            decodedFieldSlots[field++] = decodedFieldSlots[i];
        }
        //step3. decode field slots for value field
        encDec.reset(buf, tupleStartOff + nullFlagsBytes);
        for (int i = inputKeyFieldCount; i < inputTotalFieldCount; i++) {
            if (!typeTraits[i].isFixedLength()) {
                //value fields
                cumul += encDec.decode();
                decodedFieldSlots[field++] = cumul;
            } else {
                //key or value fields
                cumul += typeTraits[i].getFixedLength();
                decodedFieldSlots[field++] = cumul;
            }
        }

        dataStartOff = encDec.getPos();
    }

    @Override
    public void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        resetByTupleOffset(frame.getBuffer().array(), frame.getTupleOffset(tupleIndex));
    }

    @Override
    public void setFieldCount(int fieldCount) {
        //no op
    }

    @Override
    public void setFieldCount(int fieldStartIndex, int fieldCount) {
        //no op
    }

    @Override
    public int getFieldCount() {
        return inputTotalFieldCount;
    }

    @Override
    public int getFieldLength(int fIdx) {
        if (getInternalFieldIdx(fIdx) == 0) {
            return decodedFieldSlots[0];
        } else {
            return decodedFieldSlots[getInternalFieldIdx(fIdx)] - decodedFieldSlots[getInternalFieldIdx(fIdx) - 1];
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        if (getInternalFieldIdx(fIdx) == 0) {
            return dataStartOff;
        } else {
            return dataStartOff + decodedFieldSlots[getInternalFieldIdx(fIdx) - 1];
        }
    }

    private int getInternalFieldIdx(int fIdx) {
        if (fIdx >= storedKeyFieldCount && fIdx < inputKeyFieldCount) {
            return fIdx % storedKeyFieldCount;
        } else {
            return fIdx;
        }
    }

    @Override
    protected int getNullFlagsBytes() {
        // stored key field count + value field count
        return BitOperationUtils.getFlagBytes(
                storedKeyFieldCount + inputTotalFieldCount - inputKeyFieldCount + (antimatterAware ? 1 : 0));
    }

    @Override
    public int getTupleSize() {
        return dataStartOff - tupleStartOff + decodedFieldSlots[inputTotalFieldCount - 1];
    }

    @Override
    public boolean isAntimatter() {
        // Check antimatter bit.
        return BitOperationUtils.getBit(buf, tupleStartOff, ANTIMATTER_BIT_OFFSET);
    }
}
