/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.rtree.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public class LSMRTreeTupleWriter extends RTreeTypeAwareTupleWriter {
    private final boolean isAntimatter;

    public LSMRTreeTupleWriter(ITypeTraits[] typeTraits, boolean isAntimatter) {
        super(typeTraits);
        this.isAntimatter = isAntimatter;
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
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
            setAntimatterBit(targetBuf, targetOff);
        }
        return bytesWritten;
    }

    @Override
    protected int getNullFlagsBytes(int numFields) {
        // +1.0 is for matter/antimatter bit.
        return (int) Math.ceil(((double) numFields + 1.0) / 8.0);
    }

    @Override
    protected int getNullFlagsBytes(ITupleReference tuple) {
        // +1.0 is for matter/antimatter bit.
        return (int) Math.ceil(((double) tuple.getFieldCount() + 1.0) / 8.0);
    }

    protected void setAntimatterBit(byte[] targetBuf, int targetOff) {
        // Set leftmost bit to 1.
        targetBuf[targetOff] = (byte) (targetBuf[targetOff] | (1 << 7));
    }

}
