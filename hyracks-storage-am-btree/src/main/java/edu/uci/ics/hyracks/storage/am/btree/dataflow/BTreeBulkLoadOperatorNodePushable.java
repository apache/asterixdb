/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

public class BTreeBulkLoadOperatorNodePushable extends AbstractBTreeOperatorNodePushable {
	
	private final int[] keyFields;
	private final int[] payloadFields;
	
    private float fillFactor;
    
    private FrameTupleAccessor accessor;
    private BTree.BulkLoadContext bulkLoadCtx;
    
    private IRecordDescriptorProvider recordDescProvider;
    
	public BTreeBulkLoadOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx, int[] keyFields, int[] payloadFields, float fillFactor, IRecordDescriptorProvider recordDescProvider) {
		super(opDesc, ctx, true);
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
		                      
		int tupleCount = accessor.getTupleCount();
		for(int i = 0; i < tupleCount; i++) {
			byte[] btreeRecord = buildBTreeRecordFromHyraxRecord(accessor, i, keyFields, payloadFields);
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

    @Override
    public void flush() throws HyracksDataException {
    }	
}
