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

package edu.uci.ics.hyracks.storage.am.btree.interfaces;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeaf;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;

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

public interface IPrefixSlotManager {
	public void setFrame(FieldPrefixNSMLeaf frame);
	
	public int decodeFirstSlotField(int slot);
	public int decodeSecondSlotField(int slot);		
	public int encodeSlotFields(int firstField, int secondField);
	
	// TODO: first argument can be removed. frame must be set and buffer can be gotten from there
	public int findSlot(ByteBuffer buf, byte[] data, MultiComparator multiCmp, boolean exact);	
	public int insertSlot(int slot, int recOff);
					
	// returns prefix slot number, returns RECORD_UNCOMPRESSED if none found
	public int findPrefix(byte[] data, MultiComparator multiCmp);
	
	public int getRecSlotStartOff();
	public int getRecSlotEndOff();
	
	public int getPrefixSlotStartOff();
	public int getPrefixSlotEndOff();
	
	public int getRecSlotOff(int slotNum);
	public int getPrefixSlotOff(int slotNum);	
		
	public int getSlotSize();		
	
	public void setSlot(int offset, int value);
	
	public int compareCompressed(byte[] record, byte[] page, int prefixSlotNum, int recSlotNum, MultiComparator multiCmp);
	
	// functions for testing
	public void setPrefixSlot(int slotNum, int slot);
}
