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
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldIterator;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;

public class BTreeSearchOperatorNodePushable extends AbstractBTreeOperatorNodePushable {

	private boolean isForward;
	private ITupleReference lowKey;
	private ITupleReference highKey;
	private int searchKeyFields;
	
	public BTreeSearchOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx, boolean isForward, ITupleReference lowKey, ITupleReference highKey, int searchKeyFields) {
		super(opDesc, ctx, false);
		this.isForward = isForward;
		this.lowKey = lowKey;
		this.highKey = highKey;
		this.searchKeyFields = searchKeyFields;
	}
	
	@Override
	public void open() throws HyracksDataException {		
		
		IBTreeLeafFrame cursorFrame = opDesc.getLeafFactory().getFrame();
		IBTreeCursor cursor = new RangeSearchCursor(cursorFrame);
						
		try {
			init();										
			fill();
			
			// construct range predicate
			assert(searchKeyFields <= btree.getMultiComparator().getKeyLength());
			IBinaryComparator[] searchComparators = new IBinaryComparator[searchKeyFields];
			for(int i = 0; i < searchKeyFields; i++) {
				searchComparators[i] = btree.getMultiComparator().getComparators()[i];
			}			
			MultiComparator searchCmp = new MultiComparator(searchComparators, btree.getMultiComparator().getFields());
			RangePredicate rangePred = new RangePredicate(isForward, lowKey, highKey, searchCmp);
			
			btree.search(cursor, rangePred, leafFrame, interiorFrame);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		MultiComparator cmp = btree.getMultiComparator();
		ByteBuffer frame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);
		appender.reset(frame, true);
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getFields().length);
		DataOutput dos = tb.getDataOutput();
		
		try {
			while(cursor.hasNext()) {
				tb.reset();                		
				cursor.next();
				
				IFieldIterator fieldIter = cursor.getFieldIterator();
				for(int i = 0; i < cmp.getFields().length; i++) {					
					int fieldLen = fieldIter.getFieldSize();
					dos.write(fieldIter.getBuffer().array(), fieldIter.getFieldOff(), fieldLen);
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

    @Override
    public void flush() throws HyracksDataException {
    }
}