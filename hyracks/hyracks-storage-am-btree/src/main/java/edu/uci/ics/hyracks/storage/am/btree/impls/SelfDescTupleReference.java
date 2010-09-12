package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;

public class SelfDescTupleReference implements ITupleReference {

	private ByteBuffer buf;
	private int startOff;
	private IFieldAccessor[] fields;
	
	public SelfDescTupleReference(IFieldAccessor[] fields) {
		this.fields = fields;
	}
	
	public SelfDescTupleReference() {
	}
	
	public IFieldAccessor[] getFields() {
		return fields;
	}
	
	public void reset(ByteBuffer buf, int startOff) {
		this.buf = buf;
		this.startOff = startOff;
	}
	
	public void setFields(IFieldAccessor[] fields) {
		this.fields = fields;
	}
	
	@Override
	public int getFieldCount() {
		return fields.length;
	}

	@Override
	public byte[] getFieldData(int fIdx) {
		return buf.array();
	}

	@Override
	public int getFieldLength(int fIdx) {				
		int fieldStart = getFieldStart(fIdx);
		return fields[fIdx].getLength(buf.array(), fieldStart);
	}

	@Override
	public int getFieldStart(int fIdx) {
		int runner = startOff;
		for(int i = 0; i < fIdx; i++) {
			runner += fields[i].getLength(buf.array(), runner);
		}
		return runner;
	}
	
}
