package edu.uci.ics.hyracks.storage.am.lsm.tuples;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleReference;

public class LSMTypeAwareTupleReference extends TypeAwareTupleReference implements ILSMTreeTupleReference {

    // Indicates whether the last call to setFieldCount() was initiated by
    // called by the outside or whether it was called internally to set up an
    // antimatter tuple.
    private boolean resetFieldCount = false;
    private final int numKeyFields;
    
    public LSMTypeAwareTupleReference(ITypeTraits[] typeTraits, int numKeyFields) {
		super(typeTraits);
		this.numKeyFields = numKeyFields;
	}

    public void setFieldCount(int fieldCount) {
        super.setFieldCount(fieldCount);
        // Don't change the fieldCount in resetTuple*() calls.
        resetFieldCount = false;
    }

    @Override
    public void setFieldCount(int fieldStartIndex, int fieldCount) {
        super.setFieldCount(fieldStartIndex, fieldCount);
        // Don't change the fieldCount in resetTuple*() calls.
        resetFieldCount = false;
    }
    
    @Override
    public void resetByTupleOffset(ByteBuffer buf, int tupleStartOff) {
        this.buf = buf;
        this.tupleStartOff = tupleStartOff;
        if (numKeyFields != typeTraits.length) {
            if (isDelete()) {
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
		// +1.0 is for insert/delete tuple checking.
		return (int) Math.ceil((fieldCount + 1.0) / 8.0);
    }

	@Override
	public boolean isDelete() {
		// TODO: Alex. Rewrite this to be more efficient...
	    byte[] temp = buf.array();
		byte firstByte = temp[tupleStartOff];
		final byte mask = (byte) (1 << 7);
		final byte compare = (byte) (1 << 7);
		// Check the first bit is 0 or 1.
		if((byte)(firstByte & mask) == compare) {
			return true;
		}
		else {
			return false;
		}
	}
	
    public int getTupleStart() {
    	return tupleStartOff;
    }
}
