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
import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.SlotOffRecOff;
import edu.uci.ics.hyracks.storage.am.btree.impls.SplitKey;

public class BTreeNSMInterior extends BTreeNSM implements IBTreeInteriorFrame {
		
	private static final int rightLeafOff = smFlagOff + 1;
	
	private static final int childPtrSize = 4;
	
	public BTreeNSMInterior() {
		super();
	}
	
	private int getLeftChildPageOff(int recOff, MultiComparator cmp) {
		for(int i = 0; i < cmp.size(); i++) {
			recOff += cmp.getFields()[i].getLength(buf.array(), recOff);
		}		
		return recOff;
	}	
	
	@Override
	public void initBuffer(byte level) {
		super.initBuffer(level);
		buf.putInt(rightLeafOff, -1);
	}
	
	@Override
	public void insert(byte[] data, MultiComparator cmp) throws Exception {
		
		int slotOff = slotManager.findSlot(data, cmp, false);
		boolean isDuplicate = true;
		
		if(slotOff < 0) isDuplicate = false; // greater than all existing keys
		else if(cmp.compare(data, 0, buf.array(), slotManager.getRecOff(slotOff)) != 0) isDuplicate = false;
		
		if(isDuplicate) {			
			throw new BTreeException("Trying to insert duplicate value into interior node.");
		}
		else {			
			slotOff = slotManager.insertSlot(slotOff, buf.getInt(freeSpaceOff));			
						
			int recOff = buf.getInt(freeSpaceOff);
			int recSize = cmp.getKeySize(data, 0) + childPtrSize;
			System.arraycopy(data, 0, buf.array(), recOff, recSize);		
			
			buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) + 1);
			buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + recSize);
			buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - recSize - slotManager.getSlotSize());
			
			// did insert into the rightmost slot?
			if(slotOff == slotManager.getSlotEndOff()) { 
				System.arraycopy(data, recSize, buf.array(), rightLeafOff, childPtrSize);
			}
			else {
				// if slotOff has a right (slot-)neighbor then update its child pointer
				// the only time when this is NOT the case, is when this is the first record 
				// (or when the splitkey goes into the rightmost slot but that case was handled in the if above)
				
				if(buf.getInt(numRecordsOff) > 1) {					
					int rightNeighborOff = slotOff - slotManager.getSlotSize();
					int keySize = cmp.getKeySize(buf.array(), slotManager.getRecOff(rightNeighborOff));
					System.arraycopy(data, recSize, buf.array(), slotManager.getRecOff(rightNeighborOff) + keySize, childPtrSize);
				}				
			}			
		}
		
		//System.out.println("INSSPACEA: " + (buf.capacity() - buf.getInt(freeSpaceOff) - (buf.getInt(numRecordsOff) * slotManager.getSlotSize())));
		//System.out.println("INSSPACEB: " + (buf.getInt(totalFreeSpaceOff)));
	}
	
	@Override
	public void insertSorted(byte[] data, MultiComparator cmp) throws Exception {
		int recOff = buf.getInt(freeSpaceOff);
		slotManager.insertSlot(-1, buf.getInt(freeSpaceOff));
		int recSize = cmp.getKeySize(data, 0) + childPtrSize;
		System.arraycopy(data, 0, buf.array(), recOff, recSize);
		buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) + 1);
		buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + recSize);
		buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - recSize - slotManager.getSlotSize());
		System.arraycopy(data, recSize, buf.array(), rightLeafOff, childPtrSize);			
	}
		
	@Override
	public int split(IBTreeFrame rightFrame, byte[] data, MultiComparator cmp, SplitKey splitKey) throws Exception {		
		// before doing anything check if key already exists
		int slotOff = slotManager.findSlot(data, cmp, true);
		if(slotOff >= 0) {
			if(cmp.compare(data, 0, buf.array(), slotManager.getRecOff(slotOff)) == 0) {				
				throw new BTreeException("Inserting duplicate key in interior node during split");				
			}
		}
		
		ByteBuffer right = rightFrame.getBuffer();
		int numRecords = buf.getInt(numRecordsOff);
		
		int recordsToLeft = (numRecords / 2) + (numRecords % 2);
		IBTreeFrame targetFrame = null;
		if(cmp.compare(data, 0, buf.array(), getRecordOffset(recordsToLeft-1)) <= 0) {
			targetFrame = this;
		}
		else {
			targetFrame = rightFrame;
		}			
		int recordsToRight = numRecords - recordsToLeft;		
				
		// copy entire page
		System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());
		
		// on right page we need to copy rightmost slots to left		
		int src = rightFrame.getSlotManager().getSlotEndOff();
		int dest = rightFrame.getSlotManager().getSlotEndOff() + recordsToLeft * rightFrame.getSlotManager().getSlotSize();
		int length = rightFrame.getSlotManager().getSlotSize() * recordsToRight;
		System.arraycopy(right.array(), src, right.array(), dest, length);				
		right.putInt(numRecordsOff, recordsToRight);
				
		// on left page, remove highest key and make its childpointer the rightmost childpointer
		buf.putInt(numRecordsOff, recordsToLeft);
				
		// copy data to be inserted, we need this because creating the splitkey will overwrite the data param (data points to same memory as splitKey.getData())
		byte[] savedData = new byte[data.length];
		System.arraycopy(data, 0, savedData, 0, data.length);
		
		// set split key to be highest value in left page	
		int recOff = slotManager.getRecOff(slotManager.getSlotEndOff());
		int splitKeySize = cmp.getKeySize(buf.array(), recOff);
		splitKey.initData(splitKeySize);
		System.arraycopy(buf.array(), recOff, splitKey.getData(), 0, splitKeySize);
		
		int deleteRecOff = slotManager.getRecOff(slotManager.getSlotEndOff());
		int deleteKeySize = cmp.getKeySize(buf.array(), deleteRecOff); 		
		buf.putInt(rightLeafOff, buf.getInt(deleteRecOff + deleteKeySize));
		buf.putInt(numRecordsOff, recordsToLeft - 1);
				
		// compact both pages
		rightFrame.compact(cmp);
		compact(cmp);
			
		// insert key
		targetFrame.insert(savedData, cmp);
			
		return 0;
	}	
	
	@Override
	public void compact(MultiComparator cmp) {		
		resetSpaceParams();		
		
		int numRecords = buf.getInt(numRecordsOff);
		int freeSpace = buf.getInt(freeSpaceOff);
		byte[] data = buf.array();
		
		ArrayList<SlotOffRecOff> sortedRecOffs = new ArrayList<SlotOffRecOff>();
		sortedRecOffs.ensureCapacity(numRecords);
		for(int i = 0; i < numRecords; i++) {			
			int slotOff = slotManager.getSlotOff(i);
			int recOff = slotManager.getRecOff(slotOff);
			sortedRecOffs.add(new SlotOffRecOff(slotOff, recOff));
		}
		Collections.sort(sortedRecOffs);
		
		for(int i = 0; i < sortedRecOffs.size(); i++) {
			int recOff = sortedRecOffs.get(i).recOff;
			int recSize = cmp.getKeySize(data, recOff) + childPtrSize; // only difference from compact in NSMFrame
			System.arraycopy(data, recOff, data, freeSpace, recSize);
			slotManager.setSlot(sortedRecOffs.get(i).slotOff, freeSpace);		
			freeSpace += recSize;
		}
		
		buf.putInt(freeSpaceOff, freeSpace);
		buf.putInt(totalFreeSpaceOff, buf.capacity() - freeSpace - numRecords * slotManager.getSlotSize());
	}
		
	@Override
	public int getChildPageId(RangePredicate pred, MultiComparator srcCmp) {				
		// check for trivial case where there is only a child pointer (and no key)
		if(buf.getInt(numRecordsOff) == 0) {			
			return buf.getInt(rightLeafOff);
		}
		
		// check for trivial cases where no low key or high key exists (e.g. during an index scan)
		byte[] data = null;
		if(pred.isForward()) {						
			data = pred.getLowKeys();
			if(data == null) {
				return getLeftmostChildPageId(srcCmp);
			}			
		}
		else {
			data = pred.getHighKeys();
			if(data == null) {
				return getRightmostChildPageId(srcCmp);				
			}								
		}
		
		MultiComparator targetCmp = pred.getComparator();
		int slotOff = slotManager.findSlot(data, targetCmp, false);
		if(slotOff < 0) {
			return buf.getInt(rightLeafOff);
		}
		else {
			int origRecOff = slotManager.getRecOff(slotOff);
			int cmpRecOff = origRecOff;
			if(pred.isForward()) {
				int maxSlotOff = buf.capacity();
				slotOff += slotManager.getSlotSize();		
				while(slotOff < maxSlotOff) {
					cmpRecOff = slotManager.getRecOff(slotOff);
					if(targetCmp.compare(buf.array(), origRecOff, buf.array(), cmpRecOff) != 0) break;					
					slotOff += slotManager.getSlotSize();			
				}
				slotOff -= slotManager.getSlotSize();
			}
			else {
				int minSlotOff = slotManager.getSlotEndOff() - slotManager.getSlotSize();
				slotOff -= slotManager.getSlotSize();
				while(slotOff > minSlotOff) {
					cmpRecOff = slotManager.getRecOff(slotOff);
					if(targetCmp.compare(buf.array(), origRecOff, buf.array(), cmpRecOff) != 0) break;
					slotOff -= slotManager.getSlotSize();										
				}
				slotOff += slotManager.getSlotSize();
			}
			
			int childPageOff = getLeftChildPageOff(slotManager.getRecOff(slotOff), srcCmp);			
			return buf.getInt(childPageOff);
		}				
	}	
	
	@Override
	public void delete(byte[] data, MultiComparator cmp, boolean exactDelete) throws Exception {
		int slotOff = slotManager.findSlot(data, cmp, false);
		int recOff;
		int keySize;
		
		if(slotOff < 0) {						
			recOff = slotManager.getRecOff(slotManager.getSlotEndOff());
			keySize = cmp.getKeySize(buf.array(), recOff);
			// copy new rightmost pointer
			System.arraycopy(buf.array(), recOff + keySize, buf.array(), rightLeafOff, childPtrSize);						
		}
		else {						
			recOff = slotManager.getRecOff(slotOff);
			keySize = cmp.getKeySize(buf.array(), recOff);	
			// perform deletion (we just do a memcpy to overwrite the slot)
			int slotStartOff = slotManager.getSlotEndOff();
			int length = slotOff - slotStartOff;
			System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);						
		}
		
		// maintain space information
		buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) - 1);
		buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + keySize + childPtrSize + slotManager.getSlotSize());
	}
	
	@Override
	protected void resetSpaceParams() {
		buf.putInt(freeSpaceOff, rightLeafOff + childPtrSize);
		buf.putInt(totalFreeSpaceOff, buf.capacity() - (rightLeafOff + childPtrSize));
	}
		
	@Override
	public int getLeftmostChildPageId(MultiComparator cmp) {						
		int recOff = slotManager.getRecOff(slotManager.getSlotStartOff());
		int childPageOff = getLeftChildPageOff(recOff, cmp);		
		return buf.getInt(childPageOff);
	}

	@Override
	public int getRightmostChildPageId(MultiComparator cmp) {
		return buf.getInt(rightLeafOff);
	}

	@Override
	public void setRightmostChildPageId(int pageId) {
		buf.putInt(rightLeafOff, pageId);
	}		
	
	// for debugging
	public ArrayList<Integer> getChildren(MultiComparator cmp) {		
		ArrayList<Integer> ret = new ArrayList<Integer>();		
		int numRecords = buf.getInt(numRecordsOff);
		for(int i = 0; i < numRecords; i++) {
			int recOff = slotManager.getRecOff(slotManager.getSlotOff(i));
			int keySize = cmp.getKeySize(buf.array(), recOff);			
			int intVal = getInt(buf.array(), recOff + keySize);			
			ret.add(intVal);
		}
		if(!isLeaf()) {
			int rightLeaf = buf.getInt(rightLeafOff);
			if(rightLeaf > 0) ret.add(buf.getInt(rightLeafOff));
		}
		return ret;
	}

	@Override
	public void deleteGreatest(MultiComparator cmp) {
		int slotOff = slotManager.getSlotEndOff();
		int recOff = slotManager.getRecOff(slotOff);
		int keySize = cmp.getKeySize(buf.array(), recOff); 
		System.arraycopy(buf.array(), recOff + keySize, buf.array(), rightLeafOff, childPtrSize);
				
		// maintain space information
		buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) - 1);
		buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + keySize + childPtrSize + slotManager.getSlotSize());
		
		int freeSpace = buf.getInt(freeSpaceOff);
		if(freeSpace == recOff + keySize + childPtrSize) {
			buf.putInt(freeSpace, freeSpace - (keySize + childPtrSize));
		}		
	}		
	
	private int getInt(byte[] bytes, int offset) {
		return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8) + ((bytes[offset + 3] & 0xff) << 0);
	}
}
