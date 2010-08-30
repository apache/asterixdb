package edu.uci.ics.asterix.indexing.btree.interfaces;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.indexing.btree.impls.MultiComparator;

// a slot consists of two fields:
// first field is 1 byte, it indicates the slot number of a prefix record
// we call the first field prefixSlotOff
// second field is 3 bytes, it points to the start offset of a record
// we call the second field recOff

// we distinguish between two slot types:
// prefix slots that point to prefix records, 
// a frame is assumed to have a field numPrefixRecords
// record slots that point to data records
// a frame is assumed to have a field numRecords
// a record slot contains a record pointer and a pointer to a prefix slot (prefix slot number) 

// INSERT procedure
// a record insertion may use an existing prefix record 
// a record insertion may never create a new prefix record
// modifying the prefix slots would be extremely expensive because: 
// potentially all records slots would have to change their prefix slot pointers
// all prefixes are recomputed during a reorg or compaction

public interface IBTreeSlotManager {
	public void setFrame(IBTreeFrame frame);		
	
	public int decodeFirstSlotField(int slot);
	public int decodeSecondSlotField(int slot);		
	public int encodeSlotFields(int firstField, int secondField);
	
	// TODO: first argument can be removed. frame must be set and buffer can be gotten from there
	public int findSlot(ByteBuffer buf, byte[] data, MultiComparator multiCmp, boolean exact);
	public int insertSlot(int slotOff, int recOff);
					
	public int getRecSlotStartOff();
	public int getRecSlotEndOff();
	
	public int getPrefixSlotStartOff();
	public int getPrefixSlotEndOff();
	
	public int getRecSlotOff(int slotNum);
	public int getPrefixSlotOff(int slotNum);	
		
	public int getSlotSize();		
	
	// functions for testing
	public void setPrefixSlot(int slotNum, int slot);
	
}
