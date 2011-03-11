package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListBuilder;

public class FixedSizeElementInvertedListBuilder implements IInvertedListBuilder {	
	private final int listElementSize;
	private int listSize = 0;	
	
	private byte[] targetBuf;
	private int pos;
	
	public FixedSizeElementInvertedListBuilder(int listElementSize) {
		this.listElementSize = listElementSize;
	}
		
	@Override
	public boolean startNewList(ITupleReference tuple, int tokenField) {
		if(pos + listElementSize >= targetBuf.length) return false;
		else {
			listSize = 0;
			return true;
		}
	}		
	
	@Override
	public boolean appendElement(ITupleReference tuple, int[] elementFields) {		
		if(pos + listElementSize >= targetBuf.length) return false;
		
		for(int i = 0; i < elementFields.length; i++) {
			int field = elementFields[i];
			System.arraycopy(tuple.getFieldData(field), tuple.getFieldStart(field), targetBuf, pos, tuple.getFieldLength(field));			
		}
		
		listSize++;
		
		return true;
	}
	
	@Override
	public void setTargetBuffer(byte[] targetBuf, int startPos) {
		this.pos = startPos;
		this.targetBuf = targetBuf;
	}

	@Override
	public int getListSize() {
		return listSize;
	}

	@Override
	public int getPos() {
		return pos;
	}	
}
