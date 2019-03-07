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

import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;

public class SimpleTupleReference implements ITreeIndexTupleReference {

    protected byte[] buf;
    protected int fieldStartIndex;
    protected int fieldCount;
    protected int tupleStartOff;
    protected int nullFlagsBytes;
    protected int fieldSlotsBytes;

    @Override
    public void resetByTupleOffset(byte[] buf, int tupleStartOff) {
        this.buf = buf;
        this.tupleStartOff = tupleStartOff;
    }

    @Override
    public void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        resetByTupleOffset(frame.getBuffer().array(), frame.getTupleOffset(tupleIndex));
    }

    @Override
    public void setFieldCount(int fieldCount) {
        this.fieldCount = fieldCount;
        nullFlagsBytes = getNullFlagsBytes();
        fieldSlotsBytes = getFieldSlotsBytes();
        fieldStartIndex = 0;
    }

    @Override
    public void setFieldCount(int fieldStartIndex, int fieldCount) {
        this.fieldCount = fieldCount;
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
            return IntegerPointable.getInteger(buf, tupleStartOff + nullFlagsBytes);
        } else {
            return IntegerPointable.getInteger(buf, tupleStartOff + nullFlagsBytes + fIdx * Integer.BYTES)
                    - IntegerPointable.getInteger(buf, tupleStartOff + nullFlagsBytes + ((fIdx - 1) * Integer.BYTES));
        }
    }

    @Override
    public int getFieldStart(int fIdx) {
        if (fIdx == 0) {
            return tupleStartOff + nullFlagsBytes + fieldSlotsBytes;
        } else {
            return tupleStartOff + nullFlagsBytes + fieldSlotsBytes
                    + IntegerPointable.getInteger(buf, tupleStartOff + nullFlagsBytes + ((fIdx - 1) * Integer.BYTES));
        }
    }

    protected int getNullFlagsBytes() {
        return BitOperationUtils.getFlagBytes(fieldCount);
    }

    protected int getFieldSlotsBytes() {
        return fieldCount * Integer.BYTES;
    }

    @Override
    public int getTupleSize() {
        return nullFlagsBytes + fieldSlotsBytes
                + IntegerPointable.getInteger(buf, tupleStartOff + nullFlagsBytes + (fieldCount - 1) * Integer.BYTES);
    }
}
