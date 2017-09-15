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

import static org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference.ANTIMATTER_BIT_OFFSET;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public class LSMRTreeTupleWriter extends RTreeTypeAwareTupleWriter implements ILSMTreeTupleWriter {
    private boolean isAntimatter;

    public LSMRTreeTupleWriter(ITypeTraits[] typeTraits, boolean isAntimatter) {
        super(typeTraits);
        this.isAntimatter = isAntimatter;
    }

    @Override
    public LSMRTreeTupleReference createTupleReference() {
        return new LSMRTreeTupleReference(typeTraits);
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        return super.bytesRequired(tuple);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        int bytesWritten = super.writeTuple(tuple, targetBuf, targetOff);
        if (isAntimatter) {
            // Set antimatter bit to 1.
            BitOperationUtils.setBit(targetBuf, targetOff, ANTIMATTER_BIT_OFFSET);
        }
        return bytesWritten;
    }

    @Override
    protected int getNullFlagsBytes(int numFields) {
        // +1.0 is for matter/antimatter bit.
        return BitOperationUtils.getFlagBytes(numFields + 1);
    }

    @Override
    protected int getNullFlagsBytes(ITupleReference tuple) {
        // +1.0 is for matter/antimatter bit.
        return BitOperationUtils.getFlagBytes(tuple.getFieldCount() + 1);
    }

    @Override
    public void setAntimatter(boolean isAntimatter) {
        this.isAntimatter = isAntimatter;
    }

}
