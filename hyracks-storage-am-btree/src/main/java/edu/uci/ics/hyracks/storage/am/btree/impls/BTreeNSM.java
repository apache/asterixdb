package edu.uci.ics.asterix.indexing.btree.impls;

import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrame;

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
		if(smFlag)buf.put(smFlagOff, (byte)1);		
		else buf.put(smFlagOff, (byte)0);			
	}		
	
	@Override
	public int getFreeSpaceOff() {
		return buf.getInt(freeSpaceOff);
	}

	@Override
	public void setFreeSpaceOff(int freeSpace) {
		buf.putInt(freeSpaceOff, freeSpace);		
	}		
}
