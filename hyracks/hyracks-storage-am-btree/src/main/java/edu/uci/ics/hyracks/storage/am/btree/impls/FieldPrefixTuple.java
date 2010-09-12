package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FieldPrefixTuple extends FieldPrefixFieldIterator implements ITupleReference {

	@Override
	public int getFieldCount() {
		return fields.length;
	}

	@Override
	public byte[] getFieldData(int fIdx) {
		return frame.getBuffer().array();
	}

	@Override
	public int getFieldLength(int fIdx) {
		reset();
		for(int i = 0; i < fIdx; i++) {
			nextField();
		}						
		return getFieldSize();
	}
	
	@Override
	public int getFieldStart(int fIdx) {
		reset();
		for(int i = 0; i < fIdx; i++) {
			nextField();
		}						
		return recOffRunner;
	}
}
