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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;

public class FieldPrefixSlotManager implements IPrefixSlotManager {
	
	private static final int slotSize = 4;		
	public static final int TUPLE_UNCOMPRESSED = 0xFF;
	public static final int MAX_PREFIX_SLOTS = 0xFE;
	public static final int GREATEST_SLOT = 0x00FFFFFF;
	
	private ByteBuffer buf;
	private FieldPrefixNSMLeafFrame frame;	
	
	public int decodeFirstSlotField(int slot) {
		return (slot & 0xFF000000) >>> 24;
	}
	
	public int decodeSecondSlotField(int slot) {
		return slot & 0x00FFFFFF;
	}
	
	public int encodeSlotFields(int firstField, int secondField) {
		return ((firstField & 0x000000FF) << 24) | (secondField & 0x00FFFFFF);
	}
	
	// returns prefix slot number, or TUPLE_UNCOMPRESSED of no match was found
	public int findPrefix(ITupleReference tuple, IBTreeTupleReference framePrefixTuple, MultiComparator multiCmp) {
		int prefixMid;
	    int prefixBegin = 0;
	    int prefixEnd = frame.getPrefixTupleCount() - 1;
	    
	    if(frame.getPrefixTupleCount() > 0) {
	    	while(prefixBegin <= prefixEnd) {
	    		prefixMid = (prefixBegin + prefixEnd) / 2;	    			    		
	    		framePrefixTuple.resetByTupleIndex(frame, prefixMid);
	    		int cmp = multiCmp.fieldRangeCompare(tuple, framePrefixTuple, 0, framePrefixTuple.getFieldCount());
	    		if(cmp < 0) prefixEnd = prefixMid - 1;				    		
	    		else if(cmp > 0) prefixBegin = prefixMid + 1;	    		
	    		else return prefixMid;
	    	}
	    }
	    
	    return FieldPrefixSlotManager.TUPLE_UNCOMPRESSED;
	}
	
	public int findSlot(ITupleReference tuple, IBTreeTupleReference frameTuple, IBTreeTupleReference framePrefixTuple, MultiComparator multiCmp, boolean exact) {				
		if(frame.getTupleCount() <= 0) encodeSlotFields(TUPLE_UNCOMPRESSED, GREATEST_SLOT);
								
		frameTuple.setFieldCount(multiCmp.getFields().length);
		
	    int prefixMid;
	    int prefixBegin = 0;
	    int prefixEnd = frame.getPrefixTupleCount() - 1;
	    int prefixMatch = TUPLE_UNCOMPRESSED;
	    
	    // bounds are inclusive on both ends
	    int tuplePrefixSlotNumLbound = prefixBegin;
	    int tuplePrefixSlotNumUbound = prefixEnd;
	    
	    // binary search on the prefix slots to determine upper and lower bounds for the prefixSlotNums in tuple slots 
	    while(prefixBegin <= prefixEnd) {
	    	prefixMid = (prefixBegin + prefixEnd) / 2;	    			    		
	    	framePrefixTuple.resetByTupleIndex(frame, prefixMid);	    		    	
	    	int cmp = multiCmp.fieldRangeCompare(tuple, framePrefixTuple, 0, framePrefixTuple.getFieldCount());
	    	if(cmp < 0) {	    			
	    		prefixEnd = prefixMid - 1;
	    		tuplePrefixSlotNumLbound = prefixMid - 1;	    				    		
	    	}
	    	else if(cmp > 0) {	    			
	    		prefixBegin = prefixMid + 1;
	    		tuplePrefixSlotNumUbound = prefixMid + 1;
	    	}
	    	else {
	    		tuplePrefixSlotNumLbound = prefixMid;
	    		tuplePrefixSlotNumUbound = prefixMid;
	    		prefixMatch = prefixMid;	    			
	    		break;
	    	}
	    }
	    
	    //System.out.println("SLOTLBOUND: " + tuplePrefixSlotNumLbound);
	    //System.out.println("SLOTUBOUND: " + tuplePrefixSlotNumUbound);
	    
	    int tupleMid = -1;
	    int tupleBegin = 0;
	    int tupleEnd = frame.getTupleCount() - 1;
	    
	    // binary search on tuples, guided by the lower and upper bounds on prefixSlotNum
	    while(tupleBegin <= tupleEnd) {
            tupleMid = (tupleBegin + tupleEnd) / 2;      
            int tupleSlotOff = getTupleSlotOff(tupleMid);
            int tupleSlot = buf.getInt(tupleSlotOff);              
            int prefixSlotNum = decodeFirstSlotField(tupleSlot);
            
            //System.out.println("RECS: " + recBegin + " " + recMid + " " + recEnd);
            int cmp = 0;            
            if(prefixSlotNum == TUPLE_UNCOMPRESSED) {            	            	
            	frameTuple.resetByTupleIndex(frame, tupleMid);
            	cmp = multiCmp.compare(tuple, frameTuple);      
            }
            else {             	
            	if(prefixSlotNum < tuplePrefixSlotNumLbound) cmp = 1;
            	else if(prefixSlotNum > tuplePrefixSlotNumUbound) cmp = -1;
            	else {
            		frameTuple.resetByTupleIndex(frame, tupleMid);
            		cmp = multiCmp.compare(tuple, frameTuple);            	            	            	
            	}
            }    
            
            if(cmp < 0) tupleEnd = tupleMid - 1;
            else if(cmp > 0) tupleBegin = tupleMid + 1;
            else return encodeSlotFields(prefixMatch, tupleMid);
        }
	    
	    //System.out.println("RECS: " + recBegin + " " + recMid + " " + recEnd);
	    
        if(exact) return encodeSlotFields(prefixMatch, GREATEST_SLOT);                       
        if(tupleBegin > (frame.getTupleCount() - 1)) return encodeSlotFields(prefixMatch, GREATEST_SLOT);
        
        // do final comparison to determine whether the search key is greater than all keys or in between some existing keys
        frameTuple.resetByTupleIndex(frame, tupleBegin);
        int cmp = multiCmp.compare(tuple, (ITupleReference)frameTuple);        
        if(cmp < 0) return encodeSlotFields(prefixMatch, tupleBegin);
        else return encodeSlotFields(prefixMatch, GREATEST_SLOT);
	}	
	
	public int getPrefixSlotStartOff() {		
	    return buf.capacity() - slotSize;	    
	}
	
	public int getPrefixSlotEndOff() {
	    return buf.capacity() - slotSize * frame.getPrefixTupleCount();
	}
	
	public int getTupleSlotStartOff() {
	    return getPrefixSlotEndOff() - slotSize;
	}
	
	public int getTupleSlotEndOff() {		
		return buf.capacity() - slotSize * (frame.getPrefixTupleCount() + frame.getTupleCount());	    
	}	
	
	public int getSlotSize() {
		return slotSize;
	}
	
	public void setSlot(int offset, int value) {	
		frame.getBuffer().putInt(offset, value);
	}
	
	public int insertSlot(int slot, int tupleOff) {
		int slotNum = decodeSecondSlotField(slot);
		if(slotNum == GREATEST_SLOT) {			
			int slotOff = getTupleSlotEndOff() - slotSize;
			int newSlot = encodeSlotFields(decodeFirstSlotField(slot), tupleOff);
			setSlot(slotOff, newSlot);			
			return newSlot;
		}
		else {
			int slotEndOff = getTupleSlotEndOff();
			int slotOff = getTupleSlotOff(slotNum);
			int length = (slotOff - slotEndOff) + slotSize;			
			System.arraycopy(frame.getBuffer().array(), slotEndOff, frame.getBuffer().array(), slotEndOff - slotSize, length);			
			
			int newSlot = encodeSlotFields(decodeFirstSlotField(slot), tupleOff);
			setSlot(slotOff, newSlot);
			return newSlot;
		}
	}
	
	public void setFrame(FieldPrefixNSMLeafFrame frame) {
		this.frame = frame;
		this.buf = frame.getBuffer();
	}
	
	public int getPrefixSlotOff(int slotNum) {
		return getPrefixSlotStartOff() - slotNum * slotSize;
	}
	
	public int getTupleSlotOff(int slotNum) {
		return getTupleSlotStartOff() - slotNum * slotSize;
	}
	
	public void setPrefixSlot(int slotNum, int slot) {
		buf.putInt(getPrefixSlotOff(slotNum), slot);
	}
}
