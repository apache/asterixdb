package edu.uci.ics.hyracks.storage.am.btree.api;

import java.nio.ByteBuffer;

public interface ISplitKey {
	public void initData(int keySize);	
	public void reset();
	public ByteBuffer getBuffer();	        
	public ITreeIndexTupleReference getTuple();
	public int getLeftPage();
	public int getRightPage();
	public void setLeftPage(int leftPage);
	public void setRightPage(int rightPage);
	public void setPages(int leftPage, int rightPage);
	public ISplitKey duplicate(ITreeIndexTupleReference copyTuple);
}
