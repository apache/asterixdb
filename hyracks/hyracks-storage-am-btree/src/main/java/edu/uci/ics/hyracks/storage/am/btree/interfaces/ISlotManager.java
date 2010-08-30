package edu.uci.ics.asterix.indexing.btree.interfaces;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.indexing.btree.impls.MultiComparator;


public interface ISlotManager {
	public void setFrame(IFrame frame);
	
	// TODO: first argument can be removed. frame must be set and buffer can be gotten from there
	public int findSlot(ByteBuffer buf, byte[] data, MultiComparator multiCmp, boolean exact);
	public int insertSlot(int slotOff, int recOff);
	
	public int getSlotStartOff();
	public int getSlotEndOff();
	
	public int getRecOff(int slotOff);		
	public void setSlot(int slotOff, int value);	
	
	public int getSlotOff(int slotNum);
	
	public int getSlotSize();
}
