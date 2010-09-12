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

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldIterator;
import edu.uci.ics.hyracks.storage.am.btree.api.ISlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.NSMFieldIterator;
import edu.uci.ics.hyracks.storage.am.btree.impls.OrderedSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.SelfDescTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.SlotOffRecOff;
import edu.uci.ics.hyracks.storage.am.btree.impls.SpaceStatus;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public abstract class NSMFrame implements IBTreeFrame {
	
	protected static final int pageLsnOff = 0;								// 0
	protected static final int numRecordsOff = pageLsnOff + 4;				// 4
	protected static final int freeSpaceOff = numRecordsOff + 4;			// 8
	protected static final int totalFreeSpaceOff = freeSpaceOff + 4;		// 16
	protected static final byte levelOff = totalFreeSpaceOff + 4; 	
	protected static final byte smFlagOff = levelOff + 1;	
	
	protected ICachedPage page = null;
	protected ByteBuffer buf = null;
	protected ISlotManager slotManager;
	
	protected SelfDescTupleReference pageTuple = new SelfDescTupleReference();
	
	public NSMFrame() {
		this.slotManager = new OrderedSlotManager();
	}
	
	@Override
	public void initBuffer(byte level) {
		buf.putInt(pageLsnOff, 0); // TODO: might to set to a different lsn during creation
		buf.putInt(numRecordsOff, 0);	
		resetSpaceParams();
		buf.put(levelOff, level);
		buf.put(smFlagOff, (byte)0);
	}
		
	@Override
	public boolean isLeaf() {
		return buf.get(levelOff) == 0;
	}
			
	@Override
	public byte getLevel() {		
		return buf.get(levelOff);
	}
	
	@Override
	public void setLevel(byte level) {
		buf.put(levelOff, level);				
	}
	
	@Override
	public boolean getSmFlag() {
		return buf.get(smFlagOff) != 0;
	}

	@Override
	public void setSmFlag(boolean smFlag) {
		if(smFlag)buf.put(smFlagOff, (byte)1);		
		else buf.put(smFlagOff, (byte)0);			
	}		
	
	@Override
	public int getFreeSpaceOff() {
		return buf.getInt(freeSpaceOff);
	}

	@Override
	public void setFreeSpaceOff(int freeSpace) {
		buf.putInt(freeSpaceOff, freeSpace);		
	}		
	
	@Override
	public void setPage(ICachedPage page) {
		this.page = page;
		this.buf = page.getBuffer();
		slotManager.setFrame(this);		
	}
	
	@Override
	public ByteBuffer getBuffer() {
		return page.getBuffer();
	}
	
	@Override
	public ICachedPage getPage() {
		return page;
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
			int recSize = cmp.getRecordSize(data, recOff);
			System.arraycopy(data, recOff, data, freeSpace, recSize);
			slotManager.setSlot(sortedRecOffs.get(i).slotOff, freeSpace);
			freeSpace += recSize;
		}
		
		buf.putInt(freeSpaceOff, freeSpace);
		buf.putInt(totalFreeSpaceOff, buf.capacity() - freeSpace - numRecords * slotManager.getSlotSize());				
	}

	@Override
	public void delete(ITupleReference tuple, MultiComparator cmp, boolean exactDelete) throws Exception {		
		int slotOff = slotManager.findSlot(tuple, cmp, true);
		if(slotOff < 0) {
			throw new BTreeException("Key to be deleted does not exist.");
		}
		else {
			if(exactDelete) {
				// check the non-key columns for equality by byte-by-byte comparison
				int storedRecOff = slotManager.getRecOff(slotOff);			
				pageTuple.setFields(cmp.getFields());
				pageTuple.reset(buf, storedRecOff);
				
				int comparison = cmp.fieldRangeCompare(tuple, pageTuple, cmp.getKeyLength()-1, cmp.getFields().length - cmp.getKeyLength());
                if(comparison != 0) {
                	throw new BTreeException("Cannot delete record. Byte-by-byte comparison failed to prove equality.");
                }										
			}
			
			int recOff = slotManager.getRecOff(slotOff);
			int recSize = cmp.getRecordSize(buf.array(), recOff);
			
			// perform deletion (we just do a memcpy to overwrite the slot)
			int slotStartOff = slotManager.getSlotEndOff();
			int length = slotOff - slotStartOff;
			System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);
						
			// maintain space information
			buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) - 1);
			buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + recSize + slotManager.getSlotSize());			
		}
	}
	
	@Override
	public SpaceStatus hasSpaceInsert(ITupleReference tuple, MultiComparator cmp) {				
		int tupleSpace = spaceNeededForTuple(tuple);		
		if(tupleSpace + slotManager.getSlotSize() <= buf.capacity() - buf.getInt(freeSpaceOff) - (buf.getInt(numRecordsOff) * slotManager.getSlotSize()) ) return SpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
		else if(tupleSpace + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff)) return SpaceStatus.SUFFICIENT_SPACE;
		else return SpaceStatus.INSUFFICIENT_SPACE;
	}
	
	@Override
	public SpaceStatus hasSpaceUpdate(int rid, ITupleReference tuple, MultiComparator cmp) {
		// TODO Auto-generated method stub
		return SpaceStatus.INSUFFICIENT_SPACE;
	}

	protected void resetSpaceParams() {
		buf.putInt(freeSpaceOff, totalFreeSpaceOff + 4);
		buf.putInt(totalFreeSpaceOff, buf.capacity() - (totalFreeSpaceOff + 4));
	}
	
	@Override
	public void insert(ITupleReference tuple, MultiComparator cmp) throws Exception {
		int slotOff = slotManager.findSlot(tuple, cmp, false);		
		slotOff = slotManager.insertSlot(slotOff, buf.getInt(freeSpaceOff));				
		int bytesWritten = writeTupleFields(tuple, tuple.getFieldCount(), buf, buf.getInt(freeSpaceOff));
		
		buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) + 1);
		buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
		buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());				
	}
	
	// TODO: need to change these methods
	protected int writeTupleFields(ITupleReference src, int numFields, ByteBuffer targetBuf, int targetOff) {
		int runner = targetOff;		
		for(int i = 0; i < numFields; i++) {
			System.arraycopy(src.getFieldData(i), src.getFieldStart(i), targetBuf.array(), runner, src.getFieldLength(i));
			runner += src.getFieldLength(i);
		}
		return runner - targetOff;
	}
	
	protected int spaceNeededForTuple(ITupleReference src) {
		int space = 0;
		for(int i = 0; i < src.getFieldCount(); i++) {
			space += src.getFieldLength(i);
		}
		return space;
	}
	
	@Override
	public void update(int rid, ITupleReference tuple) throws Exception {
		// TODO Auto-generated method stub
		
	}	
	
	@Override
	public void printHeader() {
		// TODO Auto-generated method stub
		
	}		
		
	@Override
	public int getNumRecords() {
		return buf.getInt(numRecordsOff);
	}

	public ISlotManager getSlotManager() {
		return slotManager;
	}
	
	@Override
	public String printKeys(MultiComparator cmp) {		
		StringBuilder strBuilder = new StringBuilder();		
		int numRecords = buf.getInt(numRecordsOff);
		for(int i = 0; i < numRecords; i++) {			
			int recOff = slotManager.getRecOff(slotManager.getSlotOff(i));
			for(int j = 0; j < cmp.size(); j++) {				
				strBuilder.append(cmp.getFields()[j].print(buf.array(), recOff) + " ");
				recOff += cmp.getFields()[j].getLength(buf.array(), recOff);
			}
			strBuilder.append(" | ");				
		}
		strBuilder.append("\n");
		return strBuilder.toString();
	}
	
	@Override
	public int getRecordOffset(int slotNum) {
		return slotManager.getRecOff(slotManager.getSlotStartOff() - slotNum * slotManager.getSlotSize());
	}

	@Override
	public int getPageLsn() {
		return buf.getInt(pageLsnOff);		
	}

	@Override
	public void setPageLsn(int pageLsn) {
		buf.putInt(pageLsnOff, pageLsn);		
	}

	@Override
	public int getTotalFreeSpace() {
		return buf.getInt(totalFreeSpaceOff);
	}
	
    @Override
    public boolean compress(MultiComparator cmp) {
        return false;
    }
    
    @Override
    public int getSlotSize() {
    	return slotManager.getSlotSize();
    }
    
    @Override
    public IFieldIterator createFieldIterator() {
    	return new NSMFieldIterator();
    }
    
    @Override
	public void setPageTupleFields(IFieldAccessor[] fields) {
		pageTuple.setFields(fields);
	}
}
