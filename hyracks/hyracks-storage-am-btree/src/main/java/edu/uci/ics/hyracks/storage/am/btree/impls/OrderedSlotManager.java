package edu.uci.ics.asterix.indexing.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.indexing.btree.interfaces.IFrame;
import edu.uci.ics.asterix.indexing.btree.interfaces.ISlotManager;

public class OrderedSlotManager implements ISlotManager {
	
	private static final int slotSize = 4;
	private IFrame frame;
		
	// TODO: mix in interpolation search
	@Override
	public int findSlot(ByteBuffer buf, byte[] data, MultiComparator multiCmp, boolean exact) {
		int mid;
		int begin = getSlotStartOff();
		int end = getSlotEndOff();
				
        // deal with first record insertion
		if(begin == buf.capacity()) return -1;		
		
        while(begin <= end) {
            mid = (begin + end) >> 1;
        	mid -= mid % slotSize;        	
        	int recOff = getRecOff(mid);
        	int cmp = multiCmp.compare(data, 0, buf.array(), recOff);
        	if(cmp < 0)
        		begin = mid + slotSize;
        	else if(cmp > 0)
        		end = mid - slotSize;
        	else
        		return mid;
        }
                        
        if(exact) return -1;        
        if(end < getSlotStartOff()) return -1;        
        if(multiCmp.compare(data, 0, buf.array(), getRecOff(end))  < 0)
        	return end;
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
	public int getSlotStartOff() {
		return frame.getBuffer().capacity() - (frame.getNumRecords() * slotSize);
	}
	
	@Override
	public int getSlotEndOff() {
		return frame.getBuffer().capacity() - slotSize;
	}

	@Override
	public int getSlotSize() {
		return slotSize;
	}
	
	@Override
	public int insertSlot(int slotOff, int recOff) {
		if(slotOff < 0) {
			slotOff = getSlotStartOff() - slotSize;
			setSlot(slotOff, recOff);
			return slotOff;
		}
		else {
			int slotStartOff = getSlotStartOff();
			int length = (slotOff - slotStartOff) + slotSize;			
			System.arraycopy(frame.getBuffer().array(), slotStartOff, frame.getBuffer().array(), slotStartOff - slotSize, length);
			setSlot(slotOff, recOff);
			return slotOff;
		}		
	}
	
	@Override
	public void setFrame(IFrame frame) {
		this.frame = frame;		
	}

	@Override
	public int getSlotOff(int slotNum) {
		return getSlotEndOff() - slotNum * slotSize;
	}	
}
