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
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOp;

public class BTreeInsertUpdateDeleteOperatorNodePushable extends AbstractBTreeOperatorNodePushable {
	
	private final int[] keyFields;
	private final int[] payloadFields;
	    
    private FrameTupleAccessor accessor;
    
    private IRecordDescriptorProvider recordDescProvider;
    
    private IBTreeMetaDataFrame metaFrame;
    
    private BTreeOp op;
    
	public BTreeInsertUpdateDeleteOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx, int[] keyFields, int[] payloadFields, IRecordDescriptorProvider recordDescProvider, BTreeOp op) {
		super(opDesc, ctx, false);
		this.keyFields = keyFields;
		this.payloadFields = payloadFields;
		this.recordDescProvider = recordDescProvider;
		this.op = op;
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
				
				switch(op) {
				
				case BTO_INSERT: {
					btree.insert(btreeRecord, leafFrame, interiorFrame, metaFrame);				
				} break;
				
				case BTO_DELETE: {
					btree.delete(btreeRecord, leafFrame, interiorFrame, metaFrame);				
				} break;
				
				default: {
					throw new HyracksDataException("Unsupported operation " + op + " in BTree InsertUpdateDelete operator");
				}
				
				}
				
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

    @Override
    public void flush() throws HyracksDataException {
    }	
}
