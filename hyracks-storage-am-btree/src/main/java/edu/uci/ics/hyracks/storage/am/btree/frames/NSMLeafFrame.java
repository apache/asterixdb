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

package edu.uci.ics.hyracks.storage.am.btree.frames;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleWriter;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.FindSlotMode;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.SplitKey;

public class NSMLeafFrame extends NSMFrame implements IBTreeLeafFrame {    
	protected static final int prevLeafOff = smFlagOff + 1;
	protected static final int nextLeafOff = prevLeafOff + 4;
	
	public NSMLeafFrame(IBTreeTupleWriter tupleWriter) {
		super(tupleWriter);
	}
	
	@Override
	public void initBuffer(byte level) {
		super.initBuffer(level);
		buf.putInt(prevLeafOff, -1);
		buf.putInt(nextLeafOff, -1);
	}

	@Override
	public void setNextLeaf(int page) {
		buf.putInt(nextLeafOff, page);
	}

	@Override
	public void setPrevLeaf(int page) {
		buf.putInt(prevLeafOff, page);
	}

	@Override
	public int getNextLeaf() {
		return buf.getInt(nextLeafOff);
	}

	@Override
	public int getPrevLeaf() {
		return buf.getInt(prevLeafOff);
	}

	@Override
	public void insert(ITupleReference tuple, MultiComparator cmp) throws Exception {		
		frameTuple.setFieldCount(cmp.getFieldCount());
		int slotOff = slotManager.findSlot(tuple, frameTuple, cmp, FindSlotMode.FSM_INCLUSIVE);
		boolean isDuplicate = true;
				
		if (slotOff < 0) isDuplicate = false; // greater than all existing keys
		else {
			frameTuple.resetByOffset(buf, slotManager.getTupleOff(slotOff));				
			if (cmp.compare(tuple, frameTuple) != 0) isDuplicate = false;
		}
				
		if (isDuplicate) {
			throw new BTreeException("Trying to insert duplicate value into leaf of unique index");
		} 
		else {
			slotOff = slotManager.insertSlot(slotOff, buf.getInt(freeSpaceOff));
			
			int freeSpace = buf.getInt(freeSpaceOff);			
			int bytesWritten = tupleWriter.writeTuple(tuple, buf, freeSpace);			
			
			buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
			buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
			buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());			
		}	
	}
	
	@Override
	public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws Exception {		
		int freeSpace = buf.getInt(freeSpaceOff);
		slotManager.insertSlot(-1, freeSpace);		
		int bytesWritten = tupleWriter.writeTuple(tuple, buf, freeSpace);	
		buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
		buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
		buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
	}
	
	@Override
	public int split(IBTreeFrame rightFrame, ITupleReference tuple, MultiComparator cmp, SplitKey splitKey) throws Exception {
				
		frameTuple.setFieldCount(cmp.getFieldCount());
		
		// before doing anything check if key already exists
		int slotOff = slotManager.findSlot(tuple, frameTuple, cmp, FindSlotMode.FSM_EXACT);
		if (slotOff >= 0) {						
			frameTuple.resetByOffset(buf, slotManager.getTupleOff(slotOff));		
			if (cmp.compare(tuple, frameTuple) == 0) {
				throw new BTreeException("Inserting duplicate key into unique index");
			}
		}
		
		ByteBuffer right = rightFrame.getBuffer();
		int tupleCount = getTupleCount();

		int tuplesToLeft;
		int mid = tupleCount / 2;
		IBTreeFrame targetFrame = null;
		int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff() + slotManager.getSlotSize() * mid);
		frameTuple.resetByOffset(buf, tupleOff);	
		if (cmp.compare(tuple, frameTuple) >= 0) {
			tuplesToLeft = mid + (tupleCount % 2);
			targetFrame = rightFrame;
		} else {
			tuplesToLeft = mid;
			targetFrame = this;
		}
		int tuplesToRight = tupleCount - tuplesToLeft;
				
		// copy entire page
		System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());
		
		// on right page we need to copy rightmost slots to left
		int src = rightFrame.getSlotManager().getSlotEndOff();
		int dest = rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft * rightFrame.getSlotManager().getSlotSize();
		int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight; 
		System.arraycopy(right.array(), src, right.array(), dest, length);
		right.putInt(tupleCountOff, tuplesToRight);
		
		// on left page only change the tupleCount indicator
		buf.putInt(tupleCountOff, tuplesToLeft);				
			
		// compact both pages
		rightFrame.compact(cmp);		
		compact(cmp);		
						
		// insert last key
		targetFrame.insert(tuple, cmp);	
		
		// set split key to be highest value in left page
		tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
		frameTuple.resetByOffset(buf, tupleOff);
		
		int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
		splitKey.initData(splitKeySize);				
		tupleWriter.writeTupleFields(frameTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer(), 0);		
		splitKey.getTuple().resetByOffset(splitKey.getBuffer(), 0);
		
		return 0;
	}

	@Override
	protected void resetSpaceParams() {
		buf.putInt(freeSpaceOff, nextLeafOff + 4);
		buf.putInt(totalFreeSpaceOff, buf.capacity() - (nextLeafOff + 4));
	}
	
	@Override
	public IBTreeTupleReference createTupleReference() {
		return tupleWriter.createTupleReference();
	}
}
