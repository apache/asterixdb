package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class ConstantTupleSourceOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
	
	private IHyracksContext ctx;
	
	private int[] fieldSlots;
	private byte[] tupleData;	
	private int tupleSize;	
	
	
	public ConstantTupleSourceOperatorNodePushable(IHyracksContext ctx, int[] fieldSlots, byte[] tupleData, int tupleSize) {
		super();
		this.fieldSlots = fieldSlots;
		this.tupleData = tupleData;		
		this.tupleSize = tupleSize;
		this.ctx = ctx;
	}
	
	@Override
    public void initialize() throws HyracksDataException {		
		ByteBuffer writeBuffer = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);
		appender.reset(writeBuffer, true);
		appender.append(fieldSlots, tupleData, 0, tupleSize);		
		FrameUtils.flushFrame(writeBuffer, writer);
		writer.close();
	}
}
