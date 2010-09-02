package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;

public class BTreeSearchOperatorNodePushable extends AbstractBTreeOperatorNodePushable {
	
	public BTreeSearchOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx) {
		super(opDesc, ctx);
	}
		
	@Override
	public void open() throws HyracksDataException {		
		
		IBTreeLeafFrame cursorFrame = opDesc.getLeafFactory().getFrame();
		IBTreeCursor cursor = new RangeSearchCursor(cursorFrame);
		
		try {
			init();				
			fill();
			btree.search(cursor, opDesc.getRangePredicate(), leafFrame, interiorFrame);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		MultiComparator cmp = opDesc.getMultiComparator();
		ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);
		appender.reset(frame, true);
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFields().length);
		DataOutput dos = tb.getDataOutput();
		
		try {
			while(cursor.hasNext()) {
				tb.reset();                		
				cursor.next();

				int recRunner = cursor.getOffset();
				byte[] array = cursor.getPage().getBuffer().array();
				for(int i = 0; i < cmp.getFields().length; i++) {
					int fieldLen = cmp.getFields()[i].getLength(array, recRunner);
					dos.write(array, recRunner, fieldLen);                			
					recRunner += fieldLen;
					tb.addFieldEndOffset();
				}

				if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
					FrameUtils.flushFrame(frame, writer);
					appender.reset(frame, true);
					if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
						throw new IllegalStateException();
					}
				}

				//int recOffset = cursor.getOffset();                
				//String rec = cmp.printRecord(array, recOffset);
				//System.out.println(rec);
			}

			if (appender.getTupleCount() > 0) {
				FrameUtils.flushFrame(frame, writer);
			}
			writer.close();

		} catch (Exception e) {					
			e.printStackTrace();
		}
	}
	
	@Override
    public final void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }
	
	@Override
	public void close() throws HyracksDataException {            	
	}			
}
