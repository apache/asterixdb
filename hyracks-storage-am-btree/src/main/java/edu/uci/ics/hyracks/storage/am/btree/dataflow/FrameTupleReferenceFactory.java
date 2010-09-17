package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class FrameTupleReferenceFactory implements ITupleReferenceFactory {
	
	private static final long serialVersionUID = 1L;

	private byte[] frame;
	private int tupleIndex;
	private RecordDescriptor recDesc;
	
	public FrameTupleReferenceFactory(byte[] frame, int tupleIndex, RecordDescriptor recDesc) {
		this.frame = frame;
		this.tupleIndex = tupleIndex;		
		this.recDesc = recDesc;
	}
	
	// TODO: lots of object creation, fix later
	@Override
	public ITupleReference createTuple(IHyracksContext ctx) {				
		IFrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recDesc);
		accessor.reset(ByteBuffer.wrap(frame));
		FrameTupleReference tuple = new FrameTupleReference();
		tuple.reset(accessor, tupleIndex);
		return tuple;
	}
}
