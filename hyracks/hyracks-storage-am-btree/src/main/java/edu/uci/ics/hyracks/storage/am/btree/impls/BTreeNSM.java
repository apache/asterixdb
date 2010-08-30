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

import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrame;

public abstract class BTreeNSM extends NSMFrame implements IBTreeFrame {
	
	protected static final byte levelOff = totalFreeSpaceOff + 4; 	
	protected static final byte smFlagOff = levelOff + 1;
	
	public BTreeNSM() {
		super();
	}
	
	@Override
	public void initBuffer(byte level) {
		super.initBuffer(level);
		buf.put(levelOff, level);
		buf.put(smFlagOff, (byte)0);
	}
		
	@Override
	public boolean isLeaf() {
		return buf.get(levelOff) == 0;
	}
			
	@Override
	public byte getLevel() {		
		return buf.get(levelOff);
	}
	
	@Override
	public void setLevel(byte level) {
		buf.put(levelOff, level);				
	}
	
	@Override
	public boolean getSmFlag() {
		return buf.get(smFlagOff) != 0;
	}

	@Override
	public void setSmFlag(boolean smFlag) {
		if(smFlag)			
			buf.put(smFlagOff, (byte)1);		
		else
			buf.put(smFlagOff, (byte)0);			
	}		
	
	// TODO: remove
	@Override
	public int getFreeSpaceOff() {
		return buf.getInt(freeSpaceOff);
	}

	@Override
	public void setFreeSpaceOff(int freeSpace) {
		buf.putInt(freeSpaceOff, freeSpace);		
	}
	
	@Override
    public void compress(MultiComparator cmp) {        
    }
}
