package edu.uci.ics.asterix.indexing.btree.interfaces;

public interface IBTreeFrameLeaf extends IBTreeFrame {	
	public void setNextLeaf(int nextPage);
	public int getNextLeaf();
	
	public void setPrevLeaf(int prevPage);
	public int getPrevLeaf();
}
