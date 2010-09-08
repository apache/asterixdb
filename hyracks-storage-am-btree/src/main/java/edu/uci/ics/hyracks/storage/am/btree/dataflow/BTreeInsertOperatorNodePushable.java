package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;

public class BTreeInsertOperatorNodePushable extends AbstractBTreeOperatorNodePushable {
	
	private final int[] keyFields;
	private final int[] payloadFields;
	    
    private FrameTupleAccessor accessor;
    
    private IRecordDescriptorProvider recordDescProvider;
    
    private IBTreeMetaDataFrame metaFrame;
    
	public BTreeInsertOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx, int[] keyFields, int[] payloadFields, IRecordDescriptorProvider recordDescProvider) {
		super(opDesc, ctx);
		this.keyFields = keyFields;
		this.payloadFields = payloadFields;
		this.recordDescProvider = recordDescProvider;
	}
	
	@Override
	public void close() throws HyracksDataException {
		writer.close();		
	}
	
	@Override
	public void nextFrame(ByteBuffer buffer) throws HyracksDataException {		
		accessor.reset(buffer);
		
		int tupleCount = accessor.getTupleCount();
		for(int i = 0; i < tupleCount; i++) {
			byte[] btreeRecord = buildBTreeRecordFromHyraxRecord(accessor, i, keyFields, payloadFields);
			try {
				btree.insert(btreeRecord, leafFrame, interiorFrame, metaFrame);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// pass a copy of the frame to next op
		FrameUtils.flushFrame(buffer.duplicate(), writer);
	}
	
	@Override
	public void open() throws HyracksDataException {		
		RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);		
		accessor = new FrameTupleAccessor(ctx, recDesc);		
		try {
			init();
			btree.open(opDesc.getBtreeFileId());
			metaFrame = new MetaDataFrame();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
