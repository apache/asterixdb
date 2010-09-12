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
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldIterator;
import edu.uci.ics.hyracks.storage.am.btree.api.IFrameCompressor;
import edu.uci.ics.hyracks.storage.am.btree.api.IPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.api.ISlotManager;
import edu.uci.ics.hyracks.storage.am.btree.compressors.FieldPrefixCompressor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixFieldIterator;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixPrefixTuple;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixTuple;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.SlotOffRecOff;
import edu.uci.ics.hyracks.storage.am.btree.impls.SpaceStatus;
import edu.uci.ics.hyracks.storage.am.btree.impls.SplitKey;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class FieldPrefixNSMLeafFrame implements IBTreeLeafFrame {
	
    protected static final int pageLsnOff = 0;                              // 0
    protected static final int numRecordsOff = pageLsnOff + 4;              // 4    
    protected static final int freeSpaceOff = numRecordsOff + 4;      		// 8
    protected static final int totalFreeSpaceOff = freeSpaceOff + 4;        // 12	
	protected static final int levelOff = totalFreeSpaceOff + 4;         	// 16
	protected static final int smFlagOff = levelOff + 1;                   	// 17
	protected static final int numUncompressedRecordsOff = smFlagOff + 1;				// 18
	protected static final int numPrefixRecordsOff = numUncompressedRecordsOff + 4; 		// 21
	
	protected static final int prevLeafOff = numPrefixRecordsOff + 4;		// 22
	protected static final int nextLeafOff = prevLeafOff + 4;				// 26
	
	protected ICachedPage page = null;
    protected ByteBuffer buf = null;
    public IFrameCompressor compressor;
    public IPrefixSlotManager slotManager; // TODO: should be protected, but will trigger some refactoring
    
    private FieldPrefixTuple pageTuple = new FieldPrefixTuple();
    private FieldPrefixPrefixTuple pagePrefixTuple = new FieldPrefixPrefixTuple(); 
    
    public FieldPrefixNSMLeafFrame() {
        this.slotManager = new FieldPrefixSlotManager();
        this.compressor = new FieldPrefixCompressor(0.001f, 2);        
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
    public boolean compress(MultiComparator cmp) throws Exception {
        return compressor.compress(this, cmp);
    }
    
    // assumptions: 
    // 1. prefix records are stored contiguously
    // 2. prefix records are located before records (physically on the page)
    // this procedure will not move prefix records
    @Override
    public void compact(MultiComparator cmp) {
        resetSpaceParams();
                        
        int numRecords = buf.getInt(numRecordsOff);
        byte[] data = buf.array();
        
        // determine start of target free space (depends on assumptions stated above)
        int freeSpace = buf.getInt(freeSpaceOff);        
        int numPrefixRecords = buf.getInt(numPrefixRecordsOff);
        if(numPrefixRecords > 0) {
        	int prefixFields = 0;
        	for(int i = 0; i < numPrefixRecords; i++) {
        		int prefixSlotOff = slotManager.getPrefixSlotOff(i);
        		int prefixSlot = buf.getInt(prefixSlotOff);
        		int prefixRecOff = slotManager.decodeSecondSlotField(prefixSlot);
        		if(prefixRecOff >= freeSpace) {
        			freeSpace = prefixRecOff;
        			prefixFields = slotManager.decodeFirstSlotField(prefixSlot);
        		}
        	}
        	for(int i = 0; i < prefixFields; i++) {
        		freeSpace += cmp.getFields()[i].getLength(data, freeSpace);
        	}
        }
        
        ArrayList<SlotOffRecOff> sortedRecOffs = new ArrayList<SlotOffRecOff>();
        sortedRecOffs.ensureCapacity(numRecords);
        for(int i = 0; i < numRecords; i++) {           
            int recSlotOff = slotManager.getRecSlotOff(i);
            int recSlot = buf.getInt(recSlotOff);
            int recOff = slotManager.decodeSecondSlotField(recSlot);
            sortedRecOffs.add(new SlotOffRecOff(recSlotOff, recOff));
        }
        Collections.sort(sortedRecOffs);
        
        for(int i = 0; i < sortedRecOffs.size(); i++) {                    	
        	int recOff = sortedRecOffs.get(i).recOff;
            int recSlot = buf.getInt(sortedRecOffs.get(i).slotOff);
            int prefixSlotNum = slotManager.decodeFirstSlotField(recSlot);            
            
            int fieldStart = 0;
            if(prefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
            	int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);
                int prefixSlot = buf.getInt(prefixSlotOff);
                fieldStart = slotManager.decodeFirstSlotField(prefixSlot);                                
            }
            
            int recRunner = recOff;
            for(int j = fieldStart; j < cmp.getFields().length; j++) {
            	recRunner += cmp.getFields()[j].getLength(data, recRunner);
            }
            int recSize = recRunner - recOff;
                        
            System.arraycopy(data, recOff, data, freeSpace, recSize);
            slotManager.setSlot(sortedRecOffs.get(i).slotOff, slotManager.encodeSlotFields(prefixSlotNum, freeSpace));
            freeSpace += recSize;
        }
        
        buf.putInt(freeSpaceOff, freeSpace);
        int totalFreeSpace = buf.capacity() - buf.getInt(freeSpaceOff) - ((buf.getInt(numRecordsOff) + buf.getInt(numPrefixRecordsOff)) * slotManager.getSlotSize());        
        buf.putInt(totalFreeSpaceOff, totalFreeSpace);
    }
    
    @Override
    public void delete(ITupleReference tuple, MultiComparator cmp, boolean exactDelete) throws Exception {        
        int slot = slotManager.findSlot(tuple, cmp, true);
        int recSlotNum = slotManager.decodeSecondSlotField(slot);
        if(recSlotNum == FieldPrefixSlotManager.GREATEST_SLOT) {
            throw new BTreeException("Key to be deleted does not exist.");   
        }
        else {
            int prefixSlotNum = slotManager.decodeFirstSlotField(slot);            
            int recSlotOff = slotManager.getRecSlotOff(recSlotNum);
            
            if(exactDelete) {                                    
                pageTuple.setFields(cmp.getFields());
                pageTuple.setFrame(this);
                pageTuple.openRecSlotNum(recSlotNum);
                
                int comparison = cmp.fieldRangeCompare(tuple, pageTuple, cmp.getKeyLength()-1, cmp.getFields().length - cmp.getKeyLength());
                if(comparison != 0) {
                	throw new BTreeException("Cannot delete record. Byte-by-byte comparison failed to prove equality.");
                }                                  
            }
            
            // perform deletion (we just do a memcpy to overwrite the slot)
            int slotEndOff = slotManager.getRecSlotEndOff();
            int length = recSlotOff - slotEndOff;
            System.arraycopy(buf.array(), slotEndOff, buf.array(), slotEndOff + slotManager.getSlotSize(), length);
            
            // maintain space information, get size of record suffix (suffix could be entire record)
            int recSize = 0;                        
            int suffixFieldStart = 0;
            FieldPrefixFieldIterator fieldIter = new FieldPrefixFieldIterator(cmp.getFields(), this);                 
            fieldIter.openRecSlotOff(recSlotOff);
            if(prefixSlotNum == FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
                suffixFieldStart = 0;
                buf.putInt(numUncompressedRecordsOff, buf.getInt(numUncompressedRecordsOff) - 1);
            }
            else {                
                int prefixSlot = buf.getInt(slotManager.getPrefixSlotOff(prefixSlotNum));
                suffixFieldStart = slotManager.decodeFirstSlotField(prefixSlot); 
            }
            
            for(int i = 0; i < suffixFieldStart; i++) {
                fieldIter.nextField();
            }
            for(int i = suffixFieldStart; i < cmp.getFields().length; i++) {
                recSize += cmp.getFields()[i].getLength(buf.array(), fieldIter.getFieldOff());
                fieldIter.nextField();
            }
            
            buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) - 1);
            buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + recSize + slotManager.getSlotSize());            
        }    
    }
    
    @Override
    public SpaceStatus hasSpaceInsert(ITupleReference tuple, MultiComparator cmp) {                    	
    	int freeContiguous = buf.capacity() - buf.getInt(freeSpaceOff) - ((buf.getInt(numRecordsOff) + buf.getInt(numPrefixRecordsOff)) * slotManager.getSlotSize());     	    	                        
        
    	int tupleSpace = spaceNeededForTuple(tuple);
    	
        // see if the record would fit uncompressed
        if(tupleSpace + slotManager.getSlotSize() <= freeContiguous) return SpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        
        // see if record would fit into remaining space after compaction
        if(tupleSpace + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff)) return SpaceStatus.SUFFICIENT_SPACE;
        
        // see if the record matches a prefix and will fit after truncating the prefix
        int prefixSlotNum = slotManager.findPrefix(tuple, cmp);
        if(prefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
        	int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);
        	int prefixSlot = buf.getInt(prefixSlotOff);
        	int numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
        	
        	// TODO relies on length being stored in serialized field
        	int recRunner = 0;
        	for(int i = 0; i < numPrefixFields; i++) {
        		recRunner += cmp.getFields()[i].getLength(tuple.getFieldData(i), recRunner);
        	}
        	int compressedSize = tupleSpace - recRunner;
        	if(compressedSize + slotManager.getSlotSize() <= freeContiguous) return SpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;        	
        }
        
        return SpaceStatus.INSUFFICIENT_SPACE;
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
    public SpaceStatus hasSpaceUpdate(int rid, ITupleReference tuple, MultiComparator cmp) {
        // TODO Auto-generated method stub
        return SpaceStatus.INSUFFICIENT_SPACE;
    }

    protected void resetSpaceParams() {    	
    	buf.putInt(freeSpaceOff, getOrigFreeSpaceOff());
        buf.putInt(totalFreeSpaceOff, getOrigTotalFreeSpace());
    }
    
    @Override
    public void initBuffer(byte level) {        
        buf.putInt(pageLsnOff, 0); // TODO: might to set to a different lsn during creation
        buf.putInt(numRecordsOff, 0);   
        resetSpaceParams();
        buf.putInt(numUncompressedRecordsOff, 0);
        buf.putInt(numPrefixRecordsOff, 0);
        buf.put(levelOff, level);
        buf.put(smFlagOff, (byte)0);
        buf.putInt(prevLeafOff, -1);
		buf.putInt(nextLeafOff, -1);
    }
    
    public void setTotalFreeSpace(int totalFreeSpace) {
        buf.putInt(totalFreeSpaceOff, totalFreeSpace);
    }
    
    public int getOrigTotalFreeSpace() {
        return buf.capacity() - (nextLeafOff + 4);
    }
    
    @Override
    public void insert(ITupleReference tuple, MultiComparator cmp) throws Exception {    	
    	int slot = slotManager.findSlot(tuple, cmp, false);        
        slot = slotManager.insertSlot(slot, buf.getInt(freeSpaceOff));
        
        int suffixSize = spaceNeededForTuple(tuple);
        int suffixStartOff = tuple.getFieldStart(0);
        int prefixSlotNum = slotManager.decodeFirstSlotField(slot);
        
        if(prefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
        	
        	// TODO relies on length being stored in serialized field
        	
            int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);            
            int prefixSlot = buf.getInt(prefixSlotOff);
            int numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
                                    
            // skip prefix fields
            for(int i = 0; i < numPrefixFields; i++) {
                suffixStartOff += cmp.getFields()[i].getLength(tuple.getFieldData(i), suffixStartOff);
            }
            
            // compute suffix size
            suffixSize = suffixStartOff; 
            for(int i = numPrefixFields; i < cmp.getFields().length; i++) {
                suffixSize += cmp.getFields()[i].getLength(tuple.getFieldData(i), suffixSize);
            }
            suffixSize -= suffixStartOff;                  
        }
        else {
        	buf.putInt(numUncompressedRecordsOff, buf.getInt(numUncompressedRecordsOff) + 1);
        }
        
    	// TODO relies on length being stored in serialized field        
        int freeSpace = buf.getInt(freeSpaceOff);
        System.arraycopy(tuple.getFieldData(0), suffixStartOff, buf.array(), freeSpace, suffixSize);                    
        buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + suffixSize);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - suffixSize - slotManager.getSlotSize());
               
        //System.out.println(buf.getInt(totalFreeSpaceOff) + " " + buf.getInt(freeSpaceOff) + " " + buf.getInt(numRecordsOff));        
        //System.out.println("COPIED: " + suffixSize + " / " + data.length);      
    }
    
    public void verifyPrefixes(MultiComparator cmp) {
    	int numPrefixes = buf.getInt(numPrefixRecordsOff);
    	int totalPrefixBytes = 0;
    	for(int i = 0; i < numPrefixes; i++) {
    		int prefixSlotOff = slotManager.getPrefixSlotOff(i);
    		int prefixSlot = buf.getInt(prefixSlotOff);
    		
    		int numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
    		int prefixRecOff = slotManager.decodeSecondSlotField(prefixSlot);
    		
    		System.out.print("VERIFYING " + i + " : " + numPrefixFields + " " + prefixRecOff + " ");
    		int recOffRunner = prefixRecOff;
    		for(int j = 0; j < numPrefixFields; j++) {
    			System.out.print(buf.getInt(prefixRecOff+j*4) + " ");
    			int length = cmp.getFields()[j].getLength(buf.array(), recOffRunner);
    			recOffRunner += length;
    			totalPrefixBytes += length;    			
    		}
    		System.out.println();
    	}    	        
    	System.out.println("VER TOTALPREFIXBYTES: " + totalPrefixBytes + " " + numPrefixes);
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
        return null;
    }
    
    @Override
    public String printKeys(MultiComparator cmp) {      
        StringBuilder strBuilder = new StringBuilder();
        FieldPrefixFieldIterator rec = new FieldPrefixFieldIterator(cmp.getFields(), this);      
        int numRecords = buf.getInt(numRecordsOff);
        for(int i = 0; i < numRecords; i++) {                               	
        	rec.openRecSlotNum(i);        	
        	//strBuilder.append(String.format("RECORD %5d: ", i));
        	for(int j = 0; j < cmp.size(); j++) {               
                strBuilder.append(cmp.getFields()[j].print(buf.array(), rec.getFieldOff()) + " ");
                rec.nextField();
            }
            strBuilder.append(" | ");        	        	                           
        }
        strBuilder.append("\n");
        return strBuilder.toString();
    }
    
    @Override
    public int getRecordOffset(int slotNum) {        
    	int recSlotOff = slotManager.getRecSlotOff(slotNum);
    	int recSlot = buf.getInt(recSlotOff);
    	return slotManager.decodeSecondSlotField(recSlot);    	
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
		if(smFlag)			
			buf.put(smFlagOff, (byte)1);		
		else
			buf.put(smFlagOff, (byte)0);			
	}		
	
	public int getNumPrefixRecords() {
		return buf.getInt(numPrefixRecordsOff);
	}
	
	public void setNumPrefixRecords(int numPrefixRecords) {
		buf.putInt(numPrefixRecordsOff, numPrefixRecords);
	}

    @Override
    public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws Exception {    	
    	int freeSpace = buf.getInt(freeSpaceOff);				
		int fieldsToTruncate = 0;
		
		pageTuple.setFrame(this);
		pageTuple.setFields(cmp.getFields());
		
		if(buf.getInt(numPrefixRecordsOff) > 0) {
			// check if record matches last prefix record
			int prefixSlotOff = slotManager.getPrefixSlotOff(buf.getInt(numPrefixRecordsOff)-1);
			int prefixSlot = buf.getInt(prefixSlotOff);
			int numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
			
			pagePrefixTuple.setFrame(this);
			pagePrefixTuple.setFields(cmp.getFields());
			pagePrefixTuple.openPrefixSlotOff(prefixSlotOff);
			
			if(cmp.fieldRangeCompare(tuple, pagePrefixTuple, 0, numPrefixFields) == 0) {
				fieldsToTruncate = numPrefixFields;													
			}			
		}
				
		// copy truncated record
    	// TODO relies on length being stored in serialized field   
		int tupleSpace = spaceNeededForTuple(tuple); 
		int recStart = tuple.getFieldStart(0);
		for(int i = 0; i < fieldsToTruncate; i++) {
			recStart += cmp.getFields()[i].getLength(tuple.getFieldData(0), recStart);
		}
		int recLen = tupleSpace - recStart - tuple.getFieldStart(0);
		System.arraycopy(tuple.getFieldData(0), recStart, buf.array(), freeSpace, recLen);
		
		// insert slot
		int prefixSlotNum = FieldPrefixSlotManager.RECORD_UNCOMPRESSED;
		if(fieldsToTruncate > 0) prefixSlotNum = buf.getInt(numPrefixRecordsOff)-1;					
		else buf.putInt(numUncompressedRecordsOff, buf.getInt(numUncompressedRecordsOff) + 1);					
		int insSlot = slotManager.encodeSlotFields(prefixSlotNum, FieldPrefixSlotManager.GREATEST_SLOT);				
		slotManager.insertSlot(insSlot, freeSpace);
		
		// update page metadata
		buf.putInt(numRecordsOff, buf.getInt(numRecordsOff) + 1);
		buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + recLen);
		buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - recLen - slotManager.getSlotSize());			
    }
    
    @Override
    public int split(IBTreeFrame rightFrame, ITupleReference tuple, MultiComparator cmp, SplitKey splitKey) throws Exception {
    	
    	FieldPrefixNSMLeafFrame rf = (FieldPrefixNSMLeafFrame)rightFrame;
    	
    	pageTuple.setFrame(this);
		pageTuple.setFields(cmp.getFields());
    	
    	// before doing anything check if key already exists
		int slot = slotManager.findSlot(tuple, cmp, true);
		int recSlotNum = slotManager.decodeSecondSlotField(slot);		
		if(recSlotNum != FieldPrefixSlotManager.GREATEST_SLOT) {				
			pageTuple.openRecSlotNum(recSlotNum);
			if(cmp.compare(tuple, (ITupleReference)pageTuple) == 0) {			
				throw new BTreeException("Inserting duplicate key into unique index");
			}
		}
		
		ByteBuffer right = rf.getBuffer();
		int numRecords = getNumRecords();
		int numPrefixRecords = getNumPrefixRecords();
		
		int recordsToLeft;
		int midSlotNum = numRecords / 2;
		IBTreeFrame targetFrame = null;
		int midSlotOff = slotManager.getRecSlotOff(midSlotNum);
		int midSlot = buf.getInt(midSlotOff);
		int midPrefixSlotNum = slotManager.decodeFirstSlotField(midSlot);
		int midRecOff = slotManager.decodeSecondSlotField(midSlot);		
		pageTuple.openRecSlotNum(midSlotNum);
		int comparison = cmp.compare(tuple, (ITupleReference)pageTuple);		
		if (comparison >= 0) {
			recordsToLeft = midSlotNum + (numRecords % 2);
			targetFrame = rf;
		} else {
			recordsToLeft = midSlotNum;
			targetFrame = this;
		}
		int recordsToRight = numRecords - recordsToLeft;
				
		// copy entire page
		System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());
				
		// determine how many slots go on left and right page
		int prefixesToLeft = numPrefixRecords;		
		for(int i = recordsToLeft; i < numRecords; i++) {			
			int recSlotOff = rf.slotManager.getRecSlotOff(i);						
			int recSlot = right.getInt(recSlotOff);
			int prefixSlotNum = rf.slotManager.decodeFirstSlotField(recSlot);			
			if(prefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
				prefixesToLeft = prefixSlotNum;			
				break;
			}
		}
		
		// if we are splitting in the middle of a prefix both pages need to have the prefix slot and record
		int bounradyRecSlotOff = rf.slotManager.getRecSlotOff(recordsToLeft-1);
		int boundaryRecSlot = buf.getInt(bounradyRecSlotOff);
		int boundaryPrefixSlotNum = rf.slotManager.decodeFirstSlotField(boundaryRecSlot);
		int prefixesToRight = numPrefixRecords - prefixesToLeft;
		if(boundaryPrefixSlotNum == prefixesToLeft && boundaryPrefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
			prefixesToLeft++; // records on both pages share one prefix 
		}			
								
		// move prefix records on right page to beginning of page and adjust prefix slots
		if(prefixesToRight > 0 && prefixesToLeft > 0 && numPrefixRecords > 1) {			
			
			int freeSpace = rf.getOrigFreeSpaceOff();
			int lastPrefixSlotNum = -1;
			
			for(int i = recordsToLeft; i < numRecords; i++) {			
				int recSlotOff = rf.slotManager.getRecSlotOff(i);						
				int recSlot = right.getInt(recSlotOff);
				int prefixSlotNum = rf.slotManager.decodeFirstSlotField(recSlot);			
				if(prefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {								
					int prefixSlotOff = rf.slotManager.getPrefixSlotOff(prefixSlotNum);
					int prefixSlot = right.getInt(prefixSlotOff);
					int numPrefixFields = rf.slotManager.decodeFirstSlotField(prefixSlot);
					
					int prefixRecSize = 0;
					if(lastPrefixSlotNum != prefixSlotNum) {
						int prefixRecOff = rf.slotManager.decodeSecondSlotField(prefixSlot);
						for(int j = 0; j < numPrefixFields; j++) {
							prefixRecSize += cmp.getFields()[j].getLength(buf.array(), prefixRecOff + prefixRecSize);						
						}
						// copy from left page to make sure not to overwrite anything in right page that we may need later
						System.arraycopy(buf.array(), prefixRecOff, right.array(), freeSpace, prefixRecSize);
						
						int newPrefixSlot = rf.slotManager.encodeSlotFields(numPrefixFields, freeSpace);
						right.putInt(prefixSlotOff, newPrefixSlot);
						
						lastPrefixSlotNum = prefixSlotNum;
					}
					
					int recOff = rf.slotManager.decodeSecondSlotField(recSlot);
					int newRecSlot = rf.slotManager.encodeSlotFields(prefixSlotNum - (numPrefixRecords - prefixesToRight), recOff);
					right.putInt(recSlotOff, newRecSlot);
															
					freeSpace += prefixRecSize;
				}
			}
		}
				
		// move the modified prefix slots on the right page
		int prefixSrc = rf.slotManager.getPrefixSlotEndOff();
		int prefixDest = rf.slotManager.getPrefixSlotEndOff() + (numPrefixRecords - prefixesToRight) * rf.slotManager.getSlotSize();
		int prefixLength = rf.slotManager.getSlotSize() * prefixesToRight;
		System.arraycopy(right.array(), prefixSrc, right.array(), prefixDest, prefixLength);
		
		// on right page we need to copy rightmost record slots to left
		int src = rf.slotManager.getRecSlotEndOff();
		int dest = rf.slotManager.getRecSlotEndOff() + recordsToLeft * rf.slotManager.getSlotSize() + (numPrefixRecords - prefixesToRight) * rf.slotManager.getSlotSize();
		int length = rf.slotManager.getSlotSize() * recordsToRight;				
		System.arraycopy(right.array(), src, right.array(), dest, length);
		
		right.putInt(numRecordsOff, recordsToRight);
		right.putInt(numPrefixRecordsOff, prefixesToRight);
		
		// on left page move slots to reflect possibly removed prefixes
		src = slotManager.getRecSlotEndOff() + recordsToRight * slotManager.getSlotSize();
		dest = slotManager.getRecSlotEndOff() + recordsToRight * slotManager.getSlotSize() + (numPrefixRecords - prefixesToLeft) * slotManager.getSlotSize();
		length = slotManager.getSlotSize() * recordsToLeft;				
		System.arraycopy(buf.array(), src, buf.array(), dest, length);
		
		buf.putInt(numRecordsOff, recordsToLeft);
		buf.putInt(numPrefixRecordsOff, prefixesToLeft);		
				
		// compact both pages		
		compact(cmp);
		rightFrame.compact(cmp);
		
		// insert last key
		targetFrame.insert(tuple, cmp);
		
    	// TODO relies on length being stored in serialized field  
		
		// set split key to be highest value in left page		
		int splitKeyRecSlotOff = slotManager.getRecSlotEndOff();		
		
		int keySize = 0;
		FieldPrefixFieldIterator fieldIter = new FieldPrefixFieldIterator(cmp.getFields(), this);
		fieldIter.openRecSlotOff(splitKeyRecSlotOff);		
		for(int i = 0; i < cmp.getKeyLength(); i++) {
			keySize += fieldIter.getFieldSize();
			fieldIter.nextField();
		}									
		splitKey.initData(keySize);
		fieldIter.copyFields(0, cmp.getKeyLength()-1, splitKey.getBuffer().array(), 0);
		
		return 0;
    }
    
	@Override
	public int getFreeSpaceOff() {
		return buf.getInt(freeSpaceOff);
	}
	
	public int getOrigFreeSpaceOff() {
		return nextLeafOff + 4;
	}
	
	@Override
	public void setFreeSpaceOff(int freeSpace) {
		buf.putInt(freeSpaceOff, freeSpace);		
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
	
	public int getNumUncompressedRecords() {
		return buf.getInt(numUncompressedRecordsOff);
	}
	
	public void setNumUncompressedRecords(int numUncompressedRecords) {
		buf.putInt(numUncompressedRecordsOff, numUncompressedRecords);
	}
	
	@Override
	public int getSlotSize() {
		return slotManager.getSlotSize();
	}
	
	@Override
    public IFieldIterator createFieldIterator() {
    	return new FieldPrefixFieldIterator();
    }
		
	@Override
	public void setPageTupleFields(IFieldAccessor[] fields) {
		pageTuple.setFields(fields);
	}		
}
