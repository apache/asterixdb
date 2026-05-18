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

package org.apache.hyracks.storage.am.lsm.vector.tuples;

import static org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference.ANTIMATTER_BIT_OFFSET;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;

/**
 * Tuple writer for LSM VTree data frames.
 *
 * On-disk layout produced: [null/antimatter flag bytes][field slots][fields...].
 * Bit 7 (the MSB, {@link org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference#ANTIMATTER_BIT_OFFSET})
 * of null-flag byte 0 is reserved for the antimatter (deletion) marker; user fields occupy the
 * remaining bits. Matter and anti-matter tuples share the same physical layout and size.
 */
public class LSMVTreeDataTupleWriter extends TypeAwareTupleWriter implements ILSMTreeTupleWriter {

    private boolean isAntimatter;

    public LSMVTreeDataTupleWriter(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        super(typeTraits, nullTypeTraits, nullIntrospector);
        this.isAntimatter = false;
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return super.bytesRequired(tuple);
    }

    @Override
    public LSMVTreeDataTupleReference createTupleReference() {
        return new LSMVTreeDataTupleReference(typeTraits, nullTypeTraits);
    }

    @Override
    protected int getNullFlagsBytes(int numFields) {
        // user fields + 1 antimatter bit
        int numBits = numFields + 1;
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    protected int getNullFlagsBytes(ITupleReference tuple) {
        // user fields + 1 antimatter bit
        int numBits = tuple.getFieldCount() + 1;
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        // The null-flag byte count is derived from the field count here (getNullFlagsBytes(tuple)) but from
        // typeTraits.length on the read side (LSMVTreeDataTupleReference). A mismatch shifts every field
        // offset and mis-locates the antimatter bit — silent corruption, not a crash. Guard the invariant.
        assert tuple.getFieldCount() == typeTraits.length : "tuple field count " + tuple.getFieldCount()
                + " != configured typeTraits length " + typeTraits.length;
        int bytesWritten = super.writeTuple(tuple, targetBuf, targetOff);
        if (isAntimatter) {
            BitOperationUtils.setBit(targetBuf, targetOff, ANTIMATTER_BIT_OFFSET);
        }
        return bytesWritten;
    }

    @Override
    public void setAntimatter(boolean isDelete) {
        this.isAntimatter = isDelete;
    }

    @Override
    protected int getAdjustedFieldIdx(int fieldIdx) {
        // skip the antimatter bit at index 0
        return fieldIdx + 1;
    }
}