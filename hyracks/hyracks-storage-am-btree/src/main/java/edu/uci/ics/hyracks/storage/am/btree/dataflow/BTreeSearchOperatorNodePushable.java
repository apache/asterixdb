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

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;

public class BTreeSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
	private BTreeOpHelper btreeOpHelper;
    private boolean isForward;    
	private FrameTupleAccessor accessor;
        
    private ByteBuffer writeBuffer;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder tb;
    private DataOutput dos;
    
    private BTree btree;    
    private PermutingFrameTupleReference lowKey; 
    private PermutingFrameTupleReference highKey;
    private RangePredicate rangePred;
    private MultiComparator searchCmp;
    private IBTreeCursor cursor;
    private IBTreeLeafFrame leafFrame;
    private IBTreeLeafFrame cursorFrame;
    private IBTreeInteriorFrame interiorFrame;        
    
    private RecordDescriptor recDesc;       
        
    public BTreeSearchOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, boolean isForward, int[] lowKeyFields, int[] highKeyFields) {
        btreeOpHelper = new BTreeOpHelper(opDesc, ctx, partition, BTreeOpHelper.BTreeMode.OPEN_BTREE);
        this.isForward = isForward;        
        this.recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);        
        if(lowKeyFields != null && lowKeyFields.length > 0) {
        	lowKey = new PermutingFrameTupleReference();
        	lowKey.setFieldPermutation(lowKeyFields);
        }
        if(highKeyFields != null && highKeyFields.length > 0) {
        	highKey = new PermutingFrameTupleReference();
        	highKey.setFieldPermutation(highKeyFields);
        }
    }
    
    @Override
	public void open() throws HyracksDataException {
		AbstractBTreeOperatorDescriptor opDesc = btreeOpHelper.getOperatorDescriptor();          
        accessor = new FrameTupleAccessor(btreeOpHelper.getHyracksContext(), recDesc);
        
        cursorFrame = opDesc.getLeafFactory().getFrame();
        cursor = new RangeSearchCursor(cursorFrame);
        
        try {
			btreeOpHelper.init();
		} catch (Exception e) {
			throw new HyracksDataException(e.getMessage());
		}
        btree = btreeOpHelper.getBTree();

        leafFrame = btreeOpHelper.getLeafFrame();
        interiorFrame = btreeOpHelper.getInteriorFrame();
        
        // construct range predicate
        
        int numSearchFields = btree.getMultiComparator().getComparators().length;
        if(lowKey != null) numSearchFields = lowKey.getFieldCount();        
        IBinaryComparator[] searchComparators = new IBinaryComparator[numSearchFields];
        for (int i = 0; i < numSearchFields; i++) {
        	searchComparators[i] = btree.getMultiComparator().getComparators()[i];
        }
        searchCmp = new MultiComparator(btree.getMultiComparator().getFieldCount(), searchComparators);
        
        rangePred = new RangePredicate(isForward, null, null, searchCmp);
                
        accessor = new FrameTupleAccessor(btreeOpHelper.getHyracksContext(), recDesc);
        
        writeBuffer = btreeOpHelper.getHyracksContext().getResourceManager().allocateFrame();
        tb = new ArrayTupleBuilder(btree.getMultiComparator().getFieldCount());
        dos = tb.getDataOutput();
        appender = new FrameTupleAppender(btreeOpHelper.getHyracksContext());                
    }
    	
    private void writeSearchResults() throws Exception {
    	while (cursor.hasNext()) {
    		tb.reset();
    		cursor.next();

    		ITupleReference frameTuple = cursor.getTuple();
    		for (int i = 0; i < frameTuple.getFieldCount(); i++) {
    			dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
    			tb.addFieldEndOffset();
    		}

    		if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
    			FrameUtils.flushFrame(writeBuffer, writer);
    			appender.reset(writeBuffer, true);
    			if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
    				throw new IllegalStateException();
    			}
    		}             
    	}    	
    }
    
	@Override
	public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
		accessor.reset(buffer);
		
        int tupleCount = accessor.getTupleCount();        
        try {        	
        	for (int i = 0; i < tupleCount; i++) {
                if(lowKey != null) lowKey.reset(accessor, i);
                if(highKey != null) highKey.reset(accessor, i);
                rangePred.setLowKey(lowKey);
                rangePred.setHighKey(highKey);
                
                cursor.reset();
                btree.search(cursor, rangePred, leafFrame, interiorFrame);
                appender.reset(writeBuffer, true);
                writeSearchResults();                                                         
            }
        	
        	if (appender.getTupleCount() > 0) {
    			FrameUtils.flushFrame(writeBuffer, writer);
    		}
        	
        } catch (Exception e) {
        	throw new HyracksDataException(e.getMessage());
        }
	}	
	
	@Override
	public void close() throws HyracksDataException {
		writer.close();
		try {
			cursor.close();
		} catch (Exception e) {
			throw new HyracksDataException(e.getMessage());
		}    	
	}

	@Override
	public void flush() throws HyracksDataException {		
	}	
}