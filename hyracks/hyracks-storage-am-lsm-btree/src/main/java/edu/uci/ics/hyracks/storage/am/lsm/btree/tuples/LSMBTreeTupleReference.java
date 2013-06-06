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

package edu.uci.ics.hyracks.storage.am.lsm.btree.tuples;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;

public class LSMBTreeTupleReference extends TypeAwareTupleReference implements ILSMTreeTupleReference {

    // Indicates whether the last call to setFieldCount() was initiated by
    // by the outside or whether it was called internally to set up an
    // antimatter tuple.
    private boolean resetFieldCount = false;
    private final int numKeyFields;
    
    public LSMBTreeTupleReference(ITypeTraits[] typeTraits, int numKeyFields) {
		super(typeTraits);
		this.numKeyFields = numKeyFields;
	}

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
    public void resetByTupleOffset(ByteBuffer buf, int tupleStartOff) {
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
        resetByTupleOffset(frame.getBuffer(), frame.getTupleOffset(tupleIndex));
    }
    
	@Override
	protected int getNullFlagsBytes() {
		// +1.0 is for matter/antimatter bit.
		return (int) Math.ceil((fieldCount + 1.0) / 8.0);
    }

	@Override
	public boolean isAntimatter() {
	      // Check if the leftmost bit is 0 or 1.
		final byte mask = (byte) (1 << 7);
		if ((buf.array()[tupleStartOff] & mask) != 0) {
		    return true;
		}
		return false;
	}
	
    public int getTupleStart() {
    	return tupleStartOff;
    }
}
