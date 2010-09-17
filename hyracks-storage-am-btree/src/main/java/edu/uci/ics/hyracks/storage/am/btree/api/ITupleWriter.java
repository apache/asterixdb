package edu.uci.ics.hyracks.storage.am.btree.api;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface ITupleWriter {	
	public int writeTuple(ITupleReference tuple, ByteBuffer targetBuf, int targetOff);
	public int bytesRequired(ITupleReference tuple);
	
	public int writeTupleFields(ITupleReference tuple, int startField, int numFields, ByteBuffer targetBuf, int targetOff);
	public int bytesRequired(ITupleReference tuple, int startField, int numFields);
	
	// return a tuplereference instance that can read the tuple written by this writer
	// the main idea is that the format of the written tuple may not be the same as the format written by this writer
	public ITupleReference createTupleReference();
}
