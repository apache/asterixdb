package edu.uci.ics.asterix.indexing.btree.frames;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.asterix.indexing.btree.compressors.FieldPrefixCompressor;
import edu.uci.ics.asterix.indexing.btree.impls.BTreeException;
import edu.uci.ics.asterix.indexing.btree.impls.FieldIterator;
import edu.uci.ics.asterix.indexing.btree.impls.FieldPrefixSlotManager;
import edu.uci.ics.asterix.indexing.btree.impls.MultiComparator;
import edu.uci.ics.asterix.indexing.btree.impls.SlotOffRecOff;
import edu.uci.ics.asterix.indexing.btree.impls.SplitKey;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrame;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameLeaf;
import edu.uci.ics.asterix.indexing.btree.interfaces.IComparator;
import edu.uci.ics.asterix.indexing.btree.interfaces.IFieldAccessor;
import edu.uci.ics.asterix.indexing.btree.interfaces.IFrameCompressor;
import edu.uci.ics.asterix.indexing.btree.interfaces.IPrefixSlotManager;
import edu.uci.ics.asterix.indexing.btree.interfaces.ISlotManager;
import edu.uci.ics.asterix.indexing.btree.interfaces.SpaceStatus;
import edu.uci.ics.asterix.storage.buffercache.ICachedPage;

public class FieldPrefixNSMLeaf implements IBTreeFrameLeaf {
	
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
    
    public FieldPrefixNSMLeaf() {
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
    public void delete(byte[] data, MultiComparator cmp, boolean exactDelete) throws Exception {        
        int slot = slotManager.findSlot(buf, data, cmp, true);
        int recSlotNum = slotManager.decodeSecondSlotField(slot);
        if(recSlotNum == FieldPrefixSlotManager.GREATEST_SLOT) {
            throw new BTreeException("Key to be deleted does not exist.");   
        }
        else {
            int prefixSlotNum = slotManager.decodeFirstSlotField(slot);            
            int recSlotOff = slotManager.getRecSlotOff(recSlotNum);
            
            if(exactDelete) {                    
                IComparator[] cmps = cmp.getComparators();
                IFieldAccessor[] fields = cmp.getFields();
                FieldIterator fieldIter = new FieldIterator(fields, this);                    
                fieldIter.openRecSlotOff(recSlotOff);
                
                int dataRunner = 0;
                for(int i = 0; i < cmp.getKeyLength(); i++) {
                    dataRunner += fields[i].getLength(data, dataRunner);
                    fieldIter.nextField();                   
                }                
                for(int i = cmp.getKeyLength(); i < fields.length; i++) {
                    if(cmps[i].compare(data, dataRunner, buf.array(), fieldIter.getFieldOff()) != 0) {
                        throw new BTreeException("Cannot delete record. Byte-by-byte comparison failed to prove equality.");
                    }
                    dataRunner += fields[i].getLength(data, dataRunner);
                    fieldIter.nextField();    
                }                                
            }
            
            // perform deletion (we just do a memcpy to overwrite the slot)
            int slotEndOff = slotManager.getRecSlotEndOff();
            int length = recSlotOff - slotEndOff;
            System.arraycopy(buf.array(), slotEndOff, buf.array(), slotEndOff + slotManager.getSlotSize(), length);
            
            // maintain space information, get size of record suffix (suffix could be entire record)
            int recSize = 0;                        
            int suffixFieldStart = 0;
            FieldIterator fieldIter = new FieldIterator(cmp.getFields(), this);                 
            fieldIter.openRecSlotOff(recSlotOff);
            if(prefixSlotNum == FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
                suffixFieldStart = 0;
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
    public SpaceStatus hasSpaceInsert(byte[] data, MultiComparator cmp) {                    	
    	int freeContiguous = buf.capacity() - buf.getInt(freeSpaceOff) - ((buf.getInt(numRecordsOff) + buf.getInt(numPrefixRecordsOff)) * slotManager.getSlotSize());     	    	                        
        
        // see if the record would fit uncompressed
        if(data.length + slotManager.getSlotSize() <= freeContiguous) return SpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        
        // see if record would fit into remaining space after compaction
        if(data.length + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff)) return SpaceStatus.SUFFICIENT_SPACE;
        
        // see if the record matches a prefix and will fit after truncating the prefix
        int prefixSlotNum = slotManager.findPrefix(data, cmp);
        if(prefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
        	int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);
        	int prefixSlot = buf.getInt(prefixSlotOff);
        	int numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
        	
        	int recRunner = 0;
        	for(int i = 0; i < numPrefixFields; i++) {
        		recRunner += cmp.getFields()[i].getLength(data, recRunner);
        	}
        	int compressedSize = data.length - recRunner;
        	if(compressedSize + slotManager.getSlotSize() <= freeContiguous) return SpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;        	
        }
        
        return SpaceStatus.INSUFFICIENT_SPACE;
    }
    
    @Override
    public SpaceStatus hasSpaceUpdate(int rid, byte[] data, MultiComparator cmp) {
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
    public void insert(byte[] data, MultiComparator cmp) throws Exception {    	
    	int slot = slotManager.findSlot(buf, data, cmp, false);        
        slot = slotManager.insertSlot(slot, buf.getInt(freeSpaceOff));
        
        int suffixSize = data.length;
        int suffixStartOff = 0;
        int prefixSlotNum = slotManager.decodeFirstSlotField(slot);
        
        if(prefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
        	
            int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);            
            int prefixSlot = buf.getInt(prefixSlotOff);
            int numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
                                    
            // skip prefix fields
            for(int i = 0; i < numPrefixFields; i++) {
                suffixStartOff += cmp.getFields()[i].getLength(data, suffixStartOff);
            }
            
            // compute suffix size
            suffixSize = suffixStartOff; 
            for(int i = numPrefixFields; i < cmp.getFields().length; i++) {
                suffixSize += cmp.getFields()[i].getLength(data, suffixSize);
            }
            suffixSize -= suffixStartOff;                  
        }
        else {
        	buf.putInt(numUncompressedRecordsOff, buf.getInt(numUncompressedRecordsOff) + 1);
        }
        
        int freeSpace = buf.getInt(freeSpaceOff);
        System.arraycopy(data, suffixStartOff, buf.array(), freeSpace, suffixSize);                    
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
    public void update(int rid, byte[] data) throws Exception {
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
        return null; // TODO: dummy
    }
    
    @Override
    public String printKeys(MultiComparator cmp) {      
        StringBuilder strBuilder = new StringBuilder();
        FieldIterator rec = new FieldIterator(cmp.getFields(), this);      
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
    	return slotManager.getRecSlotOff(slotNum);
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
    public void insertSorted(byte[] data, MultiComparator cmp) throws Exception {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public int split(IBTreeFrame rightFrame, byte[] data, MultiComparator cmp, SplitKey splitKey) throws Exception {
    	    	
    	FieldPrefixNSMLeaf rf = (FieldPrefixNSMLeaf)rightFrame;
    	
    	// before doing anything check if key already exists
		int slot = slotManager.findSlot(buf, data, cmp, true);
		int recSlotNum = slotManager.decodeSecondSlotField(slot);	
		if(recSlotNum != FieldPrefixSlotManager.GREATEST_SLOT) {
			int prefixSlotNum = slotManager.decodeFirstSlotField(slot);				
			if(slotManager.compareCompressed(data, buf.array(), prefixSlotNum, recSlotNum, cmp) == 0) {			
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
		int comparison = 0;
		if(midPrefixSlotNum == FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
			comparison = cmp.compare(data, 0, buf.array(), midRecOff);
		}		
		else {			
			comparison = slotManager.compareCompressed(data, buf.array(), midPrefixSlotNum, midSlotNum, cmp);
		}
				
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
		targetFrame.insert(data, cmp);			
		
		// set split key to be highest value in left page		
		int splitKeyRecSlotOff = slotManager.getRecSlotEndOff();		
		
		int keySize = 0;
		FieldIterator fieldIter = new FieldIterator(cmp.getFields(), this);
		fieldIter.openRecSlotOff(splitKeyRecSlotOff);		
		for(int i = 0; i < cmp.getKeyLength(); i++) {
			keySize += fieldIter.getFieldSize();
			fieldIter.nextField();
		}									
		splitKey.initData(keySize);
		fieldIter.copyFields(0, cmp.getKeyLength()-1, splitKey.getData(), 0);
		
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
}
