package edu.uci.ics.asterix.indexing.btree.interfaces;

import edu.uci.ics.asterix.indexing.btree.impls.MultiComparator;
import edu.uci.ics.asterix.indexing.btree.impls.SplitKey;

public interface IBTreeFrame extends IFrame {
	// TODO; what if records more than half-page size?
	public int split(IBTreeFrame rightFrame, byte[] data, MultiComparator cmp, SplitKey splitKey) throws Exception;		
	
	// TODO: check if we do something nicer than returning object
	public ISlotManager getSlotManager();
	
	// ATTENTION: in b-tree operations it may not always be possible to determine whether an ICachedPage is a leaf or interior node
	// a compatible interior and leaf implementation MUST return identical values when given the same ByteBuffer for the functions below	
	public boolean isLeaf();
	public byte getLevel();
	public void setLevel(byte level);	
	public boolean getSmFlag(); // structure modification flag
	public void setSmFlag(boolean smFlag);	
	
	//public int getNumPrefixRecords();
	//public void setNumPrefixRecords(int numPrefixRecords);
	
	public void insertSorted(byte[] data, MultiComparator cmp) throws Exception;
	
	// for debugging
	public int getFreeSpaceOff();
	public void setFreeSpaceOff(int freeSpace);
}
