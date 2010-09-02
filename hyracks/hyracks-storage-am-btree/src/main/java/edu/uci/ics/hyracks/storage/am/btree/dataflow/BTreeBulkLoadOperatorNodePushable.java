package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;

public class BTreeBulkLoadOperatorNodePushable extends AbstractBTreeOperatorNodePushable {
	
	private final int[] keyFields;
	private final int[] payloadFields;
	
    private float fillFactor;
    
    private FrameTupleAccessor accessor;
    private BTree.BulkLoadContext bulkLoadCtx;
    
    private IRecordDescriptorProvider recordDescProvider;
    
	public BTreeBulkLoadOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx, int[] keyFields, int[] payloadFields, float fillFactor, IRecordDescriptorProvider recordDescProvider) {
		super(opDesc, ctx);
		this.keyFields = keyFields;
		this.payloadFields = payloadFields;
		this.fillFactor = fillFactor;
		this.recordDescProvider = recordDescProvider;
	}
	
	@Override
	public void close() throws HyracksDataException {
		try {
			btree.endBulkLoad(bulkLoadCtx);
		} catch (Exception e) {
			e.printStackTrace();
		}			
	}
	
	@Override
	public void nextFrame(ByteBuffer buffer) throws HyracksDataException {		
		accessor.reset(buffer);
				
        // build record for insertion into btree               
		int tupleCount = accessor.getTupleCount();
		for(int i = 0; i < tupleCount; i++) {						
			// determine size of record
			int btreeRecordSize = 0;			
			for(int j = 0; j < keyFields.length; j++) {
				btreeRecordSize += accessor.getFieldLength(i, keyFields[j]);
			}
			for(int j = 0; j < payloadFields.length; j++) {
				btreeRecordSize += accessor.getFieldLength(i, payloadFields[j]);
			}			
			
			MultiComparator cmp = opDesc.getMultiComparator();
			
			// allocate record and copy fields
			byte[] btreeRecord = new byte[btreeRecordSize];
			int recRunner = 0;
			for(int j = 0; j < keyFields.length; j++) {
				int fieldStartOff = accessor.getTupleStartOffset(i) + + accessor.getFieldSlotsLength() + accessor.getFieldStartOffset(i, keyFields[j]);				
				int fieldLength = accessor.getFieldLength(i, keyFields[j]);								
				
				String rec = cmp.printKey(buffer.array(), fieldStartOff);
				System.out.println("REC: " + rec);
				
				System.arraycopy(buffer.array(), fieldStartOff, btreeRecord, recRunner, fieldLength);				
				recRunner += fieldLength;
			}
			for(int j = 0; j < payloadFields.length; j++) {
				int fieldStartOff = accessor.getTupleStartOffset(i) + + accessor.getFieldSlotsLength() + accessor.getFieldStartOffset(i, payloadFields[j]);
				int fieldLength = accessor.getFieldLength(i, payloadFields[j]);
				System.arraycopy(buffer.array(), fieldStartOff, btreeRecord, recRunner, fieldLength);
				recRunner += fieldLength;								
			}			
											
			// append to btree
			try {
				btree.bulkLoadAddRecord(bulkLoadCtx, btreeRecord);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void open() throws HyracksDataException {		
		RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);		
		accessor = new FrameTupleAccessor(ctx, recDesc);
		IBTreeMetaDataFrame metaFrame = new MetaDataFrame();		
		try {
			init();
			btree.open(opDesc.getBtreeFileId());
			bulkLoadCtx = btree.beginBulkLoad(fillFactor, leafFrame, interiorFrame, metaFrame);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
