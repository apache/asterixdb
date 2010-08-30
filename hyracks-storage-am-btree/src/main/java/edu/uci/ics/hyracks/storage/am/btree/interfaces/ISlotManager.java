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

import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;


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
