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

import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.ISlotManager;

public class OrderedSlotManager implements ISlotManager {
	
	private static final int slotSize = 4;
	private IBTreeFrame frame;
		
	// TODO: mix in interpolation search
	@Override
	public int findSlot(ByteBuffer buf, byte[] data, MultiComparator multiCmp, boolean exact) {
		if(frame.getNumRecords() <= 0) return -1;
		
		int mid;
		int begin = 0;
		int end = frame.getNumRecords() - 1;
		
        while(begin <= end) {
            mid = (begin + end) / 2;
        	int slotOff = getSlotOff(mid);        	
        	int recOff = getRecOff(slotOff);
        	int cmp = multiCmp.compare(data, 0, buf.array(), recOff);
        	if(cmp < 0)
        		end = mid - 1;
        	else if(cmp > 0)
        		begin = mid + 1;
        	else
        		return slotOff;
        }
                        
        if(exact) return -1;             
        if(begin > frame.getNumRecords() - 1) return -1;   
        
        int slotOff = getSlotOff(begin);
        int recOff = getRecOff(slotOff);
        if(multiCmp.compare(data, 0, buf.array(), recOff)  < 0)
        	return slotOff;
        else
        	return -1;		
	}
	
	@Override
	public int getRecOff(int offset) {		
		return frame.getBuffer().getInt(offset);
	}
	
	@Override
	public void setSlot(int offset, int value) {	
		frame.getBuffer().putInt(offset, value);
	}
	
	@Override
	public int getSlotEndOff() {
		return frame.getBuffer().capacity() - (frame.getNumRecords() * slotSize);
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
	public int insertSlot(int slotOff, int recOff) {
		if(slotOff < 0) {
			slotOff = getSlotEndOff() - slotSize;
			setSlot(slotOff, recOff);
			return slotOff;
		}
		else {
			int slotEndOff = getSlotEndOff();
			int length = (slotOff - slotEndOff) + slotSize;			
			System.arraycopy(frame.getBuffer().array(), slotEndOff, frame.getBuffer().array(), slotEndOff - slotSize, length);
			setSlot(slotOff, recOff);
			return slotOff;
		}		
	}
	
	@Override
	public void setFrame(IBTreeFrame frame) {
		this.frame = frame;		
	}
	
	@Override
	public int getSlotOff(int slotNum) {
		return getSlotStartOff() - slotNum * slotSize;
	}	
}
