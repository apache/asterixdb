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

package org.apache.hyracks.storage.am.lsm.btree.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;

public class LSMBTreeTupleReference extends BTreeTypeAwareTupleReference implements ILSMTreeTupleReference {

    // Indicates whether the last call to setFieldCount() was initiated by
    // by the outside or whether it was called internally to set up an
    // antimatter tuple.
    private boolean resetFieldCount = false;
    private final int numKeyFields;

    public LSMBTreeTupleReference(ITypeTraits[] typeTraits, int numKeyFields, boolean updateAware) {
        super(typeTraits, updateAware);
        this.numKeyFields = numKeyFields;
    }

    @Override
    public void setFieldCount(int fieldCount) {
        super.setFieldCount(fieldCount);
        // Don't change the fieldCount in reset calls.
        resetFieldCount = false;
    }

    @Override
    public void setFieldCount(int fieldStartIndex, int fieldCount) {
        super.setFieldCount(fieldStartIndex, fieldCount);
        // Don't change the fieldCount in reset calls.
        resetFieldCount = false;
    }

    @Override
    public void resetByTupleOffset(byte[] buf, int tupleStartOff) {
        this.buf = buf;
        this.tupleStartOff = tupleStartOff;
        if (numKeyFields != typeTraits.length) {
            if (isAntimatter()) {
                setFieldCount(numKeyFields);
                // Reset the original field count for matter tuples.
                resetFieldCount = true;
            } else {
                if (resetFieldCount) {
                    setFieldCount(typeTraits.length);
                }
            }
        }
        super.resetByTupleOffset(buf, tupleStartOff);
    }

    @Override
    public void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        resetByTupleOffset(frame.getBuffer().array(), frame.getTupleOffset(tupleIndex));
    }

    @Override
    protected int getNullFlagsBytes() {
        // number of fields + matter/antimatter bit.
        int numBits = fieldCount + 1;
        if (updateAware) {
            numBits++;
        }
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    public boolean isAntimatter() {
        // Check antimatter bit.
        return BitOperationUtils.getBit(buf, tupleStartOff, ANTIMATTER_BIT_OFFSET);
    }

    public int getTupleStart() {
        return tupleStartOff;
    }
}
