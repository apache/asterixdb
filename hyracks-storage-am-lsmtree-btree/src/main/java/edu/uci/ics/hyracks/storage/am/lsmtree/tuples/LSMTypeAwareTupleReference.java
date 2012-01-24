package edu.uci.ics.hyracks.storage.am.lsmtree.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleReference;

public class LSMTypeAwareTupleReference extends TypeAwareTupleReference implements ILSMTreeTupleReference {

	public LSMTypeAwareTupleReference(ITypeTraits[] typeTraits) {
		super(typeTraits);
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
