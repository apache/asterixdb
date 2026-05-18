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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;

/**
 * Tuple reference for LSM VTree data frames.
 *
 * On-disk layout: [null/antimatter flag bytes][field slots][fields...]
 * Field 0 of the null-flag byte is reserved for the antimatter bit (see
 * {@link ILSMTreeTupleReference#ANTIMATTER_BIT_OFFSET}); user fields start at logical index 1
 * via {@link #getAdjustedFieldIdx(int)}.
 */
public class LSMVTreeDataTupleReference extends TypeAwareTupleReference implements ILSMTreeTupleReference {

    public LSMVTreeDataTupleReference(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits) {
        super(typeTraits, nullTypeTraits);
    }

    @Override
    public void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        resetByTupleOffset(frame.getBuffer().array(), frame.getTupleOffset(tupleIndex));
    }

    @Override
    protected int getNullFlagsBytes() {
        // user fields + 1 antimatter bit
        int numBits = fieldCount + 1;
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    public boolean isAntimatter() {
        return BitOperationUtils.getBit(buf, tupleStartOff, ILSMTreeTupleReference.ANTIMATTER_BIT_OFFSET);
    }

    public int getTupleStart() {
        return tupleStartOff;
    }

    @Override
    protected int getAdjustedFieldIdx(int fieldIdx) {
        // skip the antimatter bit at index 0
        return fieldIdx + 1;
    }
}