package edu.uci.ics.asterix.indexing.btree.interfaces;

public interface IComparator {		
	public int compare(byte[] dataA, int recOffA, byte[] dataB, int recOffB);
}
