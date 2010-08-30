package edu.uci.ics.hyracks.storage.am.btree.interfaces;

public interface IBTreeFrameFactory {	
	public IBTreeFrame getFrame(ISlotManager slotManager);
}
