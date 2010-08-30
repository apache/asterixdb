package edu.uci.ics.hyracks.storage.am.btree.interfaces;

public interface IComparator {		
	public int compare(byte[] dataA, int recOffA, byte[] dataB, int recOffB);
}
