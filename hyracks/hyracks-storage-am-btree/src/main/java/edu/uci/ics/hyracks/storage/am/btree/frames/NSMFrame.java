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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleWriter;
import edu.uci.ics.hyracks.storage.am.btree.api.ISlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.OrderedSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.SlotOffTupleOff;
import edu.uci.ics.hyracks.storage.am.btree.impls.SpaceStatus;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public abstract class NSMFrame implements IBTreeFrame {
	
	protected static final int pageLsnOff = 0;								// 0
	protected static final int tupleCountOff = pageLsnOff + 4;				// 4
	protected static final int freeSpaceOff = tupleCountOff + 4;			// 8
	protected static final int totalFreeSpaceOff = freeSpaceOff + 4;		// 16
	protected static final byte levelOff = totalFreeSpaceOff + 4; 	
	protected static final byte smFlagOff = levelOff + 1;	
	
	protected ICachedPage page = null;
	protected ByteBuffer buf = null;
	protected ISlotManager slotManager;
		
	protected IBTreeTupleWriter tupleWriter;
	protected IBTreeTupleReference frameTuple;
	
	public NSMFrame(IBTreeTupleWriter tupleWriter) {
		this.tupleWriter = tupleWriter;
		this.frameTuple = tupleWriter.createTupleReference();
		this.slotManager = new OrderedSlotManager();		
	}
	
	@Override
	public void initBuffer(byte level) {
		buf.putInt(pageLsnOff, 0); // TODO: might to set to a different lsn during creation
		buf.putInt(tupleCountOff, 0);	
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
		frameTuple.setFieldCount(cmp.getFieldCount());	
		
		int tupleCount = buf.getInt(tupleCountOff);
		int freeSpace = buf.getInt(freeSpaceOff);		
		
		ArrayList<SlotOffTupleOff> sortedTupleOffs = new ArrayList<SlotOffTupleOff>();
		sortedTupleOffs.ensureCapacity(tupleCount);
		for(int i = 0; i < tupleCount; i++) {			
			int slotOff = slotManager.getSlotOff(i);
			int tupleOff = slotManager.getTupleOff(slotOff);
			sortedTupleOffs.add(new SlotOffTupleOff(i, slotOff, tupleOff));
		}
		Collections.sort(sortedTupleOffs);
		
		for(int i = 0; i < sortedTupleOffs.size(); i++) {
			int tupleOff = sortedTupleOffs.get(i).tupleOff;
			frameTuple.resetByOffset(buf, tupleOff);
			
            int tupleEndOff = frameTuple.getFieldStart(frameTuple.getFieldCount()-1) + frameTuple.getFieldLength(frameTuple.getFieldCount()-1);
            int tupleLength = tupleEndOff - tupleOff;
            System.arraycopy(buf.array(), tupleOff, buf.array(), freeSpace, tupleLength);
						
			slotManager.setSlot(sortedTupleOffs.get(i).slotOff, freeSpace);
			freeSpace += tupleLength;
		}
		
		buf.putInt(freeSpaceOff, freeSpace);
		buf.putInt(totalFreeSpaceOff, buf.capacity() - freeSpace - tupleCount * slotManager.getSlotSize());				
	}

	@Override
	public void delete(ITupleReference tuple, MultiComparator cmp, boolean exactDelete) throws Exception {		
		frameTuple.setFieldCount(cmp.getFieldCount());
		int slotOff = slotManager.findSlot(tuple, frameTuple, cmp, true);
		if(slotOff < 0) {
			throw new BTreeException("Key to be deleted does not exist.");
		}
		else {
			if(exactDelete) {
				// check the non-key columns for equality by byte-by-byte comparison
				int tupleOff = slotManager.getTupleOff(slotOff);
				frameTuple.resetByOffset(buf, tupleOff);
				
				int comparison = cmp.fieldRangeCompare(tuple, frameTuple, cmp.getKeyFieldCount()-1, cmp.getFieldCount() - cmp.getKeyFieldCount());
                if(comparison != 0) {
                	throw new BTreeException("Cannot delete tuple. Byte-by-byte comparison failed to prove equality.");
                }										
			}
			
			int tupleOff = slotManager.getTupleOff(slotOff);
			frameTuple.resetByOffset(buf, tupleOff);
			int tupleSize = tupleWriter.bytesRequired(frameTuple);
			
			// perform deletion (we just do a memcpy to overwrite the slot)
			int slotStartOff = slotManager.getSlotEndOff();
			int length = slotOff - slotStartOff;
			System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);
						
			// maintain space information
			buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) - 1);
			buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + tupleSize + slotManager.getSlotSize());			
		}
	}
	
	@Override
	public SpaceStatus hasSpaceInsert(ITupleReference tuple, MultiComparator cmp) {				
		int bytesRequired = tupleWriter.bytesRequired(tuple);
		if(bytesRequired + slotManager.getSlotSize() <= buf.capacity() - buf.getInt(freeSpaceOff) - (buf.getInt(tupleCountOff) * slotManager.getSlotSize()) ) return SpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
		else if(bytesRequired + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff)) return SpaceStatus.SUFFICIENT_SPACE;
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
		frameTuple.setFieldCount(cmp.getFieldCount());
		int slotOff = slotManager.findSlot(tuple, frameTuple, cmp, false);		
		slotOff = slotManager.insertSlot(slotOff, buf.getInt(freeSpaceOff));				
		int bytesWritten = tupleWriter.writeTuple(tuple, buf, buf.getInt(freeSpaceOff));
		
		buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
		buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
		buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());				
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
	public int getTupleCount() {
		return buf.getInt(tupleCountOff);
	}

	public ISlotManager getSlotManager() {
		return slotManager;
	}
	
	@Override
	public String printKeys(MultiComparator cmp, ISerializerDeserializer[] fields) throws HyracksDataException {		
		StringBuilder strBuilder = new StringBuilder();		
		int tupleCount = buf.getInt(tupleCountOff);
		frameTuple.setFieldCount(fields.length);	
		for(int i = 0; i < tupleCount; i++) {						
			frameTuple.resetByTupleIndex(this, i);												
			for(int j = 0; j < cmp.getKeyFieldCount(); j++) {				
				ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(j), frameTuple.getFieldStart(j), frameTuple.getFieldLength(j));
				DataInput dataIn = new DataInputStream(inStream);
				Object o = fields[j].deserialize(dataIn);
				strBuilder.append(o.toString() + " ");				
			}
			strBuilder.append(" | ");				
		}
		strBuilder.append("\n");
		return strBuilder.toString();
	}
	
	@Override
	public int getTupleOffset(int slotNum) {
		return slotManager.getTupleOff(slotManager.getSlotStartOff() - slotNum * slotManager.getSlotSize());
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
	public void setPageTupleFieldCount(int fieldCount) {
		frameTuple.setFieldCount(fieldCount);
	}
    
    public IBTreeTupleWriter getTupleWriter() {
    	return tupleWriter;
    }
}
