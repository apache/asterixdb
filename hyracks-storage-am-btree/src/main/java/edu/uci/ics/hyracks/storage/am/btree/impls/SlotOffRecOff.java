package edu.uci.ics.asterix.indexing.btree.impls;

public class SlotOffRecOff implements Comparable<SlotOffRecOff> {
	public int slotOff;
	public int recOff;
	
	public SlotOffRecOff(int slotOff, int recOff) {
		this.slotOff = slotOff;
		this.recOff = recOff;
	}
	
	@Override
	public int compareTo(SlotOffRecOff o) {			
		return recOff - o.recOff;
	}
}

