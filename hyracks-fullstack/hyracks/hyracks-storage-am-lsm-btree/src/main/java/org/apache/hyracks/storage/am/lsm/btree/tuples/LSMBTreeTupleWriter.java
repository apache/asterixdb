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

import static org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleReference.UPDATE_BIT_OFFSET;
import static org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference.ANTIMATTER_BIT_OFFSET;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriter;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;

public class LSMBTreeTupleWriter extends BTreeTypeAwareTupleWriter implements ILSMTreeTupleWriter {

    private boolean isAntimatter;
    private final int numKeyFields;

    public LSMBTreeTupleWriter(ITypeTraits[] typeTraits, int numKeyFields, boolean isAntimatter, boolean updateAware) {
        super(typeTraits, updateAware);
        this.numKeyFields = numKeyFields;
        this.isAntimatter = isAntimatter;
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        if (isAntimatter) {
            // Only requires space for the key fields.
            return super.bytesRequired(tuple, 0, numKeyFields);
        } else {
            return super.bytesRequired(tuple);
        }
    }

    @Override
    public int getCopySpaceRequired(ITupleReference tuple) {
        return super.bytesRequired(tuple);
    }

    @Override
    public LSMBTreeTupleReference createTupleReference() {
        return new LSMBTreeTupleReference(typeTraits, numKeyFields, updateAware);
    }

    @Override
    protected int getNullFlagsBytes(int numFields) {
        // numFields + matter/antimatter bit + updated bit (optional).
        int numBits = numFields + 1;
        if (updateAware) {
            numBits++;
        }
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    protected int getNullFlagsBytes(ITupleReference tuple) {
        // # of fields + matter/antimatter bit + updated bit (optional).
        int numBits = tuple.getFieldCount() + 1;
        if (updateAware) {
            numBits++;
        }
        return BitOperationUtils.getFlagBytes(numBits);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        int bytesWritten = -1;
        if (isAntimatter) {
            bytesWritten = super.writeTupleFields(tuple, 0, numKeyFields, targetBuf, targetOff);
            // Set antimatter bit to 1.
            BitOperationUtils.setBit(targetBuf, targetOff, ANTIMATTER_BIT_OFFSET);
        } else {
            bytesWritten = super.writeTuple(tuple, targetBuf, targetOff);
        }
        if (updateAware && isUpdated) {
            // Set update-in-place bit to 1.
            BitOperationUtils.setBit(targetBuf, targetOff, UPDATE_BIT_OFFSET);
        }
        return bytesWritten;
    }

    @Override
    public void setAntimatter(boolean isDelete) {
        this.isAntimatter = isDelete;
    }
}
