package edu.uci.ics.hyracks.storage.am.btree.tuples;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleReference;

public class SimpleTupleReference implements IBTreeTupleReference {
	
	protected ByteBuffer buf;
	protected int fieldStartIndex;
	protected int fieldCount;	
	protected int tupleStartOff;
	protected int nullFlagsBytes;
	protected int fieldSlotsBytes;
	
	@Override
	public void resetByOffset(ByteBuffer buf, int tupleStartOff) {
		this.buf = buf;
		this.tupleStartOff = tupleStartOff;
	}
	
	@Override
	public void resetByTupleIndex(IBTreeFrame frame, int tupleIndex) {
		resetByOffset(frame.getBuffer(), frame.getTupleOffset(tupleIndex));		
	}	
	
	@Override
	public void setFieldCount(int fieldCount) {
		this.fieldCount = fieldCount;
		nullFlagsBytes = getNullFlagsBytes();
		fieldSlotsBytes = getFieldSlotsBytes();
		fieldStartIndex = 0;
	}
	
	@Override
	public void setFieldCount(int fieldStartIndex, int fieldCount) {
		this.fieldCount = fieldCount;
		this.fieldStartIndex = fieldStartIndex;
	}
	
	@Override
	public int getFieldCount() {
		return fieldCount;
	}

	@Override
	public byte[] getFieldData(int fIdx) {
		return buf.array();
	}

	@Override
	public int getFieldLength(int fIdx) {
		if(fIdx == 0) {
			return buf.getShort(tupleStartOff + nullFlagsBytes);			
		}
		else {
			return buf.getShort(tupleStartOff + nullFlagsBytes + fIdx * 2) - buf.getShort(tupleStartOff + nullFlagsBytes + ((fIdx-1) * 2));
		}
	}

	@Override
	public int getFieldStart(int fIdx) {				
		if(fIdx == 0) {			
			return tupleStartOff + nullFlagsBytes + fieldSlotsBytes;
		}
		else {			
			return tupleStartOff + nullFlagsBytes + fieldSlotsBytes + buf.getShort(tupleStartOff + nullFlagsBytes + ((fIdx-1) * 2));
		}		
	}
	
	protected int getNullFlagsBytes() {
		return (int)Math.ceil(fieldCount / 8.0);
	}
	
	protected int getFieldSlotsBytes() {
		return fieldCount * 2;
	}	
}