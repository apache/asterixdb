package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import java.nio.ByteBuffer;

public interface IInvertedIndexResultCursor {		
	public boolean hasNext();
	public void next();	
	public ByteBuffer getBuffer();
	public void reset();
}
