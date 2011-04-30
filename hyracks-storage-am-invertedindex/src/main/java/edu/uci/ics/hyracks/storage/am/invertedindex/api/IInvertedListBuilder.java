package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IInvertedListBuilder {	
	public boolean startNewList(ITupleReference tuple, int numTokenFields);
	
	// returns true if successfully appended
	// returns false if not enough space in targetBuf	
	public boolean appendElement(ITupleReference tuple, int numTokenFields, int numElementFields);		
	
	public void setTargetBuffer(byte[] targetBuf, int startPos);
	
	public int getListSize();
	
	public int getPos();
}
