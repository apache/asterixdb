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

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.ISlotManager;

public class OrderedSlotManager implements ISlotManager {
	
	private static final int slotSize = 4;
	private IBTreeFrame frame;
	
	@Override
	public int findTupleIndex(ITupleReference searchKey, IBTreeTupleReference frameTuple, MultiComparator multiCmp, FindTupleMode mode, FindTupleNoExactMatchPolicy matchPolicy) {
		if(frame.getTupleCount() <= 0) return -1;
				
		int mid;
		int begin = 0;
		int end = frame.getTupleCount() - 1;
		
        while(begin <= end) {
            mid = (begin + end) / 2;        	
        	frameTuple.resetByTupleIndex(frame, mid);
        	
        	int cmp = multiCmp.compare(searchKey, frameTuple);
        	if(cmp < 0) {
        		end = mid - 1;
        	}
        	else if(cmp > 0) {
        		begin = mid + 1;
        	}
        	else {
        		if(mode == FindTupleMode.FTM_EXCLUSIVE) {
        			if(matchPolicy == FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY) begin = mid + 1;
        			else end = mid - 1;
        		}
        		else {        			
        			return mid;
        		}        		        		
        	}
        }
                        
        if(mode == FindTupleMode.FTM_EXACT) return -1;            
        
        if(matchPolicy == FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY) {
        	if(begin > frame.getTupleCount() - 1) return -1;
        	frameTuple.resetByTupleIndex(frame, begin);
        	if(multiCmp.compare(searchKey, frameTuple) < 0) return begin;
            else return -1;
        }
        else {
        	if(end < 0) return -1;
        	frameTuple.resetByTupleIndex(frame, end);
        	if(multiCmp.compare(searchKey, frameTuple) > 0) return end;
            else return -1;
        }               
	}
	
	@Override
	public int getTupleOff(int offset) {		
		return frame.getBuffer().getInt(offset);
	}
	
	@Override
	public void setSlot(int offset, int value) {	
		frame.getBuffer().putInt(offset, value);
	}
	
	@Override
	public int getSlotEndOff() {
		return frame.getBuffer().capacity() - (frame.getTupleCount() * slotSize);
	}
	
	@Override
	public int getSlotStartOff() {
		return frame.getBuffer().capacity() - slotSize;
	}

	@Override
	public int getSlotSize() {
		return slotSize;
	}
	
	@Override
	public int insertSlot(int tupleIndex, int tupleOff) {				
		int slotOff = getSlotOff(tupleIndex);
		if(tupleIndex < 0) {
			slotOff = getSlotEndOff() - slotSize;
			setSlot(slotOff, tupleOff);
			return slotOff;
		}
		else {
			int slotEndOff = getSlotEndOff();
			int length = (slotOff - slotEndOff) + slotSize;			
			System.arraycopy(frame.getBuffer().array(), slotEndOff, frame.getBuffer().array(), slotEndOff - slotSize, length);
			setSlot(slotOff, tupleOff);
			return slotOff;
		}		
	}
	
	@Override
	public void setFrame(IBTreeFrame frame) {
		this.frame = frame;		
	}
	
	@Override
	public int getSlotOff(int tupleIndex) {
		return getSlotStartOff() - tupleIndex * slotSize;
	}	
}
