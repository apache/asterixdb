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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;

public class TypeAwareTupleWriter implements ITreeIndexTupleWriter {

    protected final ITypeTraits[] typeTraits;
    protected final ITypeTraits nullTypeTraits; // can be null
    protected final INullIntrospector nullIntrospector; // can be null

    public TypeAwareTupleWriter(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        this.typeTraits = typeTraits;
        this.nullTypeTraits = nullTypeTraits;
        this.nullIntrospector = nullIntrospector;
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        int bytes = getNullFlagsBytes(tuple) + getFieldSlotsBytes(tuple);
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            bytes += tuple.getFieldLength(i);
        }
        return bytes;
    }

    @Override
    public int bytesRequired(ITupleReference tuple, int startField, int numFields) {
        int bytes = getNullFlagsBytes(numFields) + getFieldSlotsBytes(tuple, startField, numFields);
        for (int i = startField; i < startField + numFields; i++) {
            bytes += tuple.getFieldLength(i);
        }
        return bytes;
    }

    @Override
    public TypeAwareTupleReference createTupleReference() {
        return new TypeAwareTupleReference(typeTraits, nullTypeTraits);
    }

    @Override
    public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff) {
        return writeTuple(tuple, targetBuf.array(), targetOff);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(tuple);
        // reset null indicator bits
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf[runner++] = (byte) 0;
        }

        // write field slots for variable length fields
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            if (!typeTraits[i].isFixedLength()) {
                runner += VarLenIntEncoderDecoder.encode(tuple.getFieldLength(i), targetBuf, runner);
            }
        }

        // write data fields and set null indicator bits
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            byte[] fieldData = tuple.getFieldData(i);
            int fieldOffset = tuple.getFieldStart(i);
            int fieldLength = tuple.getFieldLength(i);
            if (nullIntrospector != null && nullIntrospector.isNull(fieldData, fieldOffset, fieldLength)) {
                setNullFlag(targetBuf, targetOff, i);
            }
            System.arraycopy(fieldData, fieldOffset, targetBuf, runner, fieldLength);
            runner += fieldLength;
        }

        return runner - targetOff;
    }

    @Override
    public int writeTupleFields(ITupleReference tuple, int startField, int numFields, byte[] targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(numFields);
        // reset null indicator bits
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf[runner++] = (byte) 0;
        }

        // write field slots for variable length fields
        for (int i = startField; i < startField + numFields; i++) {
            if (!typeTraits[i].isFixedLength()) {
                runner += VarLenIntEncoderDecoder.encode(tuple.getFieldLength(i), targetBuf, runner);
            }
        }

        for (int i = startField, targetField = 0; i < startField + numFields; i++, targetField++) {
            byte[] fieldData = tuple.getFieldData(i);
            int fieldOffset = tuple.getFieldStart(i);
            int fieldLength = tuple.getFieldLength(i);
            if (nullIntrospector != null && nullIntrospector.isNull(fieldData, fieldOffset, fieldLength)) {
                setNullFlag(targetBuf, targetOff, targetField);
            }
            System.arraycopy(fieldData, fieldOffset, targetBuf, runner, fieldLength);
            runner += fieldLength;
        }

        return runner - targetOff;
    }

    protected int getNullFlagsBytes(ITupleReference tuple) {
        return BitOperationUtils.getFlagBytes(tuple.getFieldCount());
    }

    protected int getFieldSlotsBytes(ITupleReference tuple) {
        int fieldSlotBytes = 0;
        for (int i = 0; i < tuple.getFieldCount(); i++) {
            if (!typeTraits[i].isFixedLength()) {
                fieldSlotBytes += VarLenIntEncoderDecoder.getBytesRequired(tuple.getFieldLength(i));
            }
        }
        return fieldSlotBytes;
    }

    protected int getNullFlagsBytes(int numFields) {
        return BitOperationUtils.getFlagBytes(numFields);
    }

    protected int getFieldSlotsBytes(ITupleReference tuple, int startField, int numFields) {
        int fieldSlotBytes = 0;
        for (int i = startField; i < startField + numFields; i++) {
            if (!typeTraits[i].isFixedLength()) {
                fieldSlotBytes += VarLenIntEncoderDecoder.getBytesRequired(tuple.getFieldLength(i));
            }
        }
        return fieldSlotBytes;
    }

    public ITypeTraits[] getTypeTraits() {
        return typeTraits;
    }

    public ITypeTraits getNullTypeTraits() {
        return nullTypeTraits;
    }

    public INullIntrospector getNullIntrospector() {
        return nullIntrospector;
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return bytesRequired(tuple);
    }

    /**
     * Given a field index, this method finds its corresponding bit in the null flags section and sets it.
     *
     * @param flags data
     * @param flagsOffset start of the null flags data
     * @param fieldIdx logical field index
     */
    protected void setNullFlag(byte[] flags, int flagsOffset, int fieldIdx) {
        int adjustedFieldIdx = getAdjustedFieldIdx(fieldIdx);
        int flagByteIdx = adjustedFieldIdx / 8;
        int flagBitIdx = 7 - (adjustedFieldIdx % 8);
        BitOperationUtils.setBit(flags, flagsOffset + flagByteIdx, (byte) flagBitIdx);
    }

    /**
     * Adjusts the field index in case the null flags section starts with some other special-purpose fields.
     *
     * @param fieldIdx logical field index
     * @return adjusted field index
     */
    protected int getAdjustedFieldIdx(int fieldIdx) {
        return fieldIdx;
    }
}
