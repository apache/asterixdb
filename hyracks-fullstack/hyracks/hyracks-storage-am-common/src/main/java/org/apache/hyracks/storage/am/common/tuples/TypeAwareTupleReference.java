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

package org.apache.hyracks.storage.am.common.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.VarLenIntDecoder;

public class TypeAwareTupleReference implements ITreeIndexTupleReference {
    protected byte[] buf;
    protected int fieldStartIndex;
    protected int fieldCount;
    protected int tupleStartOff;
    protected int nullFlagsBytes;
    protected int dataStartOff;

    protected final ITypeTraits[] typeTraits;
    protected VarLenIntDecoder encDec = VarLenIntEncoderDecoder.createDecoder();
    protected int[] decodedFieldSlots;

    public TypeAwareTupleReference(ITypeTraits[] typeTraits) {
        this.typeTraits = typeTraits;
        this.fieldStartIndex = 0;
        setFieldCount(typeTraits.length);
    }

    @Override
    public void resetByTupleOffset(byte[] buf, int tupleStartOff) {
        this.buf = buf;
        this.tupleStartOff = tupleStartOff;

        // decode field slots
        int field = 0;
        int cumul = 0;
        int end = fieldStartIndex + fieldCount;
        encDec.reset(buf, tupleStartOff + nullFlagsBytes);
        for (int i = fieldStartIndex; i < end; i++) {
            if (!typeTraits[i].isFixedLength()) {
                cumul += encDec.decode();
                decodedFieldSlots[field++] = cumul;
            } else {
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
        this.fieldCount = fieldCount;
        if (decodedFieldSlots == null) {
            decodedFieldSlots = new int[fieldCount];
        } else {
            if (fieldCount > decodedFieldSlots.length) {
                decodedFieldSlots = new int[fieldCount];
            }
        }
        nullFlagsBytes = getNullFlagsBytes();
        this.fieldStartIndex = 0;
    }

    @Override
    public void setFieldCount(int fieldStartIndex, int fieldCount) {
        setFieldCount(fieldCount);
        this.fieldStartIndex = fieldStartIndex;
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return buf;
    }

    @Override
    public int getFieldLength(int fIdx) {
        if (fIdx == 0) {
            return decodedFieldSlots[0];
        } else {
            return decodedFieldSlots[fIdx] - decodedFieldSlots[fIdx - 1];
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        if (fIdx == 0) {
            return dataStartOff;
        } else {
            return dataStartOff + decodedFieldSlots[fIdx - 1];
        }
    }

    protected int getNullFlagsBytes() {
        return BitOperationUtils.getFlagBytes(fieldCount);
    }

    @Override
    public int getTupleSize() {
        return dataStartOff - tupleStartOff + decodedFieldSlots[fieldCount - 1];
    }
}
