package edu.uci.ics.hyracks.storage.am.btree.api;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public interface IBTreeTupleReference extends ITupleReference {
	public void setFieldCount(int fieldCount);	
	public void resetByOffset(ByteBuffer buf, int tupleStartOffset);
	public void resetByTupleIndex(IBTreeFrame frame, int tupleIndex);
}
