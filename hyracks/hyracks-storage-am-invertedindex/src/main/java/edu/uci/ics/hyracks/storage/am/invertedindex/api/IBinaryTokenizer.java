package edu.uci.ics.hyracks.storage.am.invertedindex.api;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public interface IBinaryTokenizer {
	
	public void reset(byte[] data, int start, int length);	
	public boolean hasNext();	
	public void next();
	
	public int getTokenStartOff();
	public int getTokenLength();
	
	public void writeToken(DataOutput dos) throws IOException;
	
	public RecordDescriptor getTokenSchema();
}
