package edu.uci.ics.hyracks.storage.am.lsmtree.tuples;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;

public class LSMTypeAwareTupleWriter extends TypeAwareTupleWriter {
	private final boolean isDelete;
	
	public LSMTypeAwareTupleWriter(ITypeTraits[] typeTraits, boolean isDelete) {
		super(typeTraits);
		this.isDelete = isDelete;
	}

	@Override
    public ITreeIndexTupleReference createTupleReference() {
        return new LSMTypeAwareTupleReference(typeTraits);
    }
	
	@Override
	protected int getNullFlagsBytes(int numFields) {
		//+1.0 is for insert/delete tuple checking
		return (int) Math.ceil(((double) numFields + 1.0)/ 8.0);
    }
	
	@Override
    protected int getNullFlagsBytes(ITupleReference tuple) {
		//+1.0 is for insert/delete tuple checking
        return (int) Math.ceil(((double) tuple.getFieldCount() + 1.0) / 8.0);
    }
	
	@Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
		int bytesWritten = super.writeTuple(tuple, targetBuf, targetOff);
        if(isDelete) {
        	setDeleteBit(targetBuf, targetOff);
        }
        return bytesWritten;
    }
	
	private void setDeleteBit(byte[] targetBuf, int targetOff) {
		byte firstByte = targetBuf[targetOff];
		firstByte = (byte) (firstByte | (1 << 7));
		targetBuf[targetOff] = firstByte;
	}
}
