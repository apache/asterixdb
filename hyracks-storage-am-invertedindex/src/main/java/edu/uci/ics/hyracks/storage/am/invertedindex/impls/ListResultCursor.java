package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;

public class ListResultCursor implements IInvertedIndexResultCursor {

	private List<ByteBuffer> resultBuffers;
	private int numResultBuffers;
	private int currentPos = -1;
	
	public void setResults(List<ByteBuffer> resultBuffers, int numResultBuffers) {
		this.resultBuffers = resultBuffers;
		this.numResultBuffers = numResultBuffers;
		reset();
	}	
		
	@Override
	public boolean hasNext() {
		if(currentPos < numResultBuffers) return true;
		else return false;		
	}

	@Override
	public void next() {		
		currentPos++;
	}
	
	@Override
	public ByteBuffer getBuffer() {
		return resultBuffers.get(currentPos);
	}
	
	@Override
	public void reset() {
		currentPos = -1;		
	}
}
