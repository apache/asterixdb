package edu.uci.ics.asterix.indexing.btree.interfaces;

public interface IBTreeFrameFactory {	
	public IBTreeFrame getFrame(ISlotManager slotManager);
}
