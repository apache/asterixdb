package edu.uci.ics.asterix.indexing.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.indexing.btree.frames.FieldPrefixNSMLeaf;
import edu.uci.ics.asterix.indexing.btree.interfaces.IComparator;
import edu.uci.ics.asterix.indexing.btree.interfaces.IPrefixSlotManager;

public class FieldPrefixSlotManager implements IPrefixSlotManager {
	
	private static final int slotSize = 4;		
	public static final int RECORD_UNCOMPRESSED = 0xFF;
	public static final int MAX_PREFIX_SLOTS = 0xFE;
	public static final int GREATEST_SLOT = 0x00FFFFFF;
	
	private ByteBuffer buf;
	private FieldPrefixNSMLeaf frame;
	FieldIterator fieldIter = new FieldIterator(null, null);
	
	public int decodeFirstSlotField(int slot) {
		return (slot & 0xFF000000) >>> 24;
	}
	
	public int decodeSecondSlotField(int slot) {
		return slot & 0x00FFFFFF;
	}
	
	public int encodeSlotFields(int firstField, int secondField) {
		return ((firstField & 0x000000FF) << 24) | (secondField & 0x00FFFFFF);		
	}
	
	// returns prefix slot number, or RECORD_UNCOMPRESSED of no match was found
	public int findPrefix(byte[] data, MultiComparator multiCmp) {
		int prefixMid;
	    int prefixBegin = 0;
	    int prefixEnd = frame.getNumPrefixRecords() - 1;
	     
	    if(frame.getNumPrefixRecords() > 0) {
	    	while(prefixBegin <= prefixEnd) {
	    		prefixMid = (prefixBegin + prefixEnd) / 2;	    			    		
	    		int prefixSlotOff = getPrefixSlotOff(prefixMid);
	    		int prefixSlot = buf.getInt(prefixSlotOff);    		
	    		int numPrefixFields = decodeFirstSlotField(prefixSlot);
	    		int prefixRecOff = decodeSecondSlotField(prefixSlot);
	    		int cmp = multiCmp.fieldRangeCompare(data, 0, buf.array(), prefixRecOff, 0, numPrefixFields);
	    		if(cmp < 0) prefixEnd = prefixMid - 1;				    		
	    		else if(cmp > 0) prefixBegin = prefixMid + 1;	    		
	    		else return prefixMid;
	    	}
	    }
	    
	    return FieldPrefixSlotManager.RECORD_UNCOMPRESSED;
	}
	
	public int findSlot(ByteBuffer buf, byte[] data, MultiComparator multiCmp, boolean exact) {				
		if(frame.getNumRecords() <= 0) encodeSlotFields(RECORD_UNCOMPRESSED, GREATEST_SLOT);
								
	    int prefixMid;
	    int prefixBegin = 0;
	    int prefixEnd = frame.getNumPrefixRecords() - 1;
	    int prefixMatch = RECORD_UNCOMPRESSED;
	    
	    // bounds are inclusive on both ends
	    int recPrefixSlotNumLbound = prefixBegin;
	    int recPrefixSlotNumUbound = prefixEnd;
	    
	    // binary search on the prefix slots to determine upper and lower bounds for the prefixSlotNums in record slots 
	    while(prefixBegin <= prefixEnd) {
	    	prefixMid = (prefixBegin + prefixEnd) / 2;	    			    		
	    	int prefixSlotOff = getPrefixSlotOff(prefixMid);
	    	int prefixSlot = buf.getInt(prefixSlotOff);    		
	    	int numPrefixFields = decodeFirstSlotField(prefixSlot);
	    	int prefixRecOff = decodeSecondSlotField(prefixSlot);
	    	//System.out.println("PREFIX: " + prefixRecOff + " " + buf.getInt(prefixRecOff) + " " + buf.getInt(prefixRecOff+4));
	    	int cmp = multiCmp.fieldRangeCompare(data, 0, buf.array(), prefixRecOff, 0, numPrefixFields);
	    	if(cmp < 0) {	    			
	    		prefixEnd = prefixMid - 1;
	    		recPrefixSlotNumLbound = prefixMid - 1;	    				    		
	    	}
	    	else if(cmp > 0) {	    			
	    		prefixBegin = prefixMid + 1;
	    		recPrefixSlotNumUbound = prefixMid + 1;
	    	}
	    	else {
	    		recPrefixSlotNumLbound = prefixMid;
	    		recPrefixSlotNumUbound = prefixMid;
	    		prefixMatch = prefixMid;	    			
	    		break;
	    	}
	    }
	    
	    //System.out.println("SLOTLBOUND: " + recPrefixSlotNumLbound);
	    //System.out.println("SLOTUBOUND: " + recPrefixSlotNumUbound);
	    
	    int recMid = -1;
	    int recBegin = 0;
	    int recEnd = frame.getNumRecords() - 1;
	    
	    // binary search on records, guided by the lower and upper bounds on prefixSlotNum
	    while(recBegin <= recEnd) {
            recMid = (recBegin + recEnd) / 2;      
            int recSlotOff = getRecSlotOff(recMid);
            int recSlot = buf.getInt(recSlotOff);              
            int prefixSlotNum = decodeFirstSlotField(recSlot);
            int recOff = decodeSecondSlotField(recSlot);
            
            //System.out.println("RECS: " + recBegin + " " + recMid + " " + recEnd);
            int cmp = 0;
            if(prefixSlotNum == RECORD_UNCOMPRESSED) {                
                cmp = multiCmp.compare(data, 0, buf.array(), recOff);                
            }
            else {             	
            	if(prefixSlotNum < recPrefixSlotNumLbound) cmp = 1;
            	else if(prefixSlotNum > recPrefixSlotNumUbound) cmp = -1;
            	else cmp = compareCompressed(data, buf.array(), prefixSlotNum, recMid, multiCmp);            	            	            	
            }    
            
            if(cmp < 0) recEnd = recMid - 1;
            else if(cmp > 0) recBegin = recMid + 1;
            else return encodeSlotFields(prefixMatch, recMid);
        }
	    
	    //System.out.println("RECS: " + recBegin + " " + recMid + " " + recEnd);
	    
        if(exact) return encodeSlotFields(prefixMatch, GREATEST_SLOT);                       
        if(recBegin > (frame.getNumRecords() - 1)) return encodeSlotFields(prefixMatch, GREATEST_SLOT);
        
        // do final comparison to determine whether the search key is greater than all keys or in between some existing keys
        int recSlotOff = getRecSlotOff(recBegin);
        int recSlot = buf.getInt(recSlotOff);
        int prefixSlotNum = decodeFirstSlotField(recSlot);
        int recOff = decodeSecondSlotField(recSlot);
        
        int cmp = 0;
        if(prefixSlotNum == RECORD_UNCOMPRESSED) cmp = multiCmp.compare(data, 0, buf.array(), recOff);        	
        else cmp = compareCompressed(data, buf.array(), prefixSlotNum, recBegin, multiCmp);
        
        if(cmp < 0) return encodeSlotFields(prefixMatch, recBegin);
        else return encodeSlotFields(prefixMatch, GREATEST_SLOT);
	}	
	
	public int compareCompressed(byte[] record, byte[] page, int prefixSlotNum, int recSlotNum, MultiComparator multiCmp) {                          
         IComparator[] cmps = multiCmp.getComparators();
         fieldIter.setFields(multiCmp.getFields());
         fieldIter.setFrame(frame);         
         fieldIter.openRecSlotNum(recSlotNum);
         
         int recRunner = 0;
         int cmp = 0;
         for(int i = 0; i < multiCmp.getKeyLength(); i++) {
        	 cmp = cmps[i].compare(record, recRunner, buf.array(), fieldIter.getFieldOff());             
             if(cmp < 0) return -1;                 
             else if(cmp > 0) return 1;             
             fieldIter.nextField();
             recRunner += multiCmp.getFields()[i].getLength(record, recRunner);
         }
         return 0;
	}
	
	public int getPrefixSlotStartOff() {		
	    return buf.capacity() - slotSize;	    
	}
	
	public int getPrefixSlotEndOff() {
	    return buf.capacity() - slotSize * frame.getNumPrefixRecords();
	}
	
	public int getRecSlotStartOff() {
	    return getPrefixSlotEndOff() - slotSize;
	}
	
	public int getRecSlotEndOff() {		
		return buf.capacity() - slotSize * (frame.getNumPrefixRecords() + frame.getNumRecords());	    
	}	
	
	public int getSlotSize() {
		return slotSize;
	}
	
	public void setSlot(int offset, int value) {	
		frame.getBuffer().putInt(offset, value);
	}
	
	public int insertSlot(int slot, int recOff) {
		int slotNum = decodeSecondSlotField(slot);
		if(slotNum == GREATEST_SLOT) {			
			int slotOff = getRecSlotEndOff() - slotSize;
			int newSlot = encodeSlotFields(decodeFirstSlotField(slot), recOff);
			setSlot(slotOff, newSlot);
			//System.out.println("SETTING A: " + slotOff + " " + recOff);
			return newSlot;
		}
		else {
			int slotEndOff = getRecSlotEndOff();
			int slotOff = getRecSlotOff(slotNum);
			int length = (slotOff - slotEndOff) + slotSize;			
			System.arraycopy(frame.getBuffer().array(), slotEndOff, frame.getBuffer().array(), slotEndOff - slotSize, length);
			//System.out.println("MOVING SLOTS: " + length + " " + (frame.getNumRecords()*4));
			
			int newSlot = encodeSlotFields(decodeFirstSlotField(slot), recOff);
			setSlot(slotOff, newSlot);			
			//System.out.println("SETTING B: " + slotOff + " " + recOff);
			return newSlot;
		}
	}
	
	public void setFrame(FieldPrefixNSMLeaf frame) {
		this.frame = frame;
		this.buf = frame.getBuffer();
	}
	
	public int getPrefixSlotOff(int slotNum) {
		return getPrefixSlotStartOff() - slotNum * slotSize;
	}
	
	public int getRecSlotOff(int slotNum) {
		return getRecSlotStartOff() - slotNum * slotSize;
	}
	
	public void setPrefixSlot(int slotNum, int slot) {
		buf.putInt(getPrefixSlotOff(slotNum), slot);
	}
}
