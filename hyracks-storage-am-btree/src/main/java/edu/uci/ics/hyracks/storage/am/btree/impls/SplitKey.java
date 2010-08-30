package edu.uci.ics.asterix.indexing.btree.impls;

import java.nio.ByteBuffer;

public class SplitKey {		
	private byte[] data = null;	
	private ByteBuffer buf = null;
	
	public void initData(int keySize) {
		// try to reuse existing memory from a lower-level split if possible
		// in general this is always possible for fixed-sized keys		
		/*
		if(data != null) {
			if(data.length < keySize + 8) {
				data = new byte[keySize + 8]; // add 8 for left and right page
				buf = ByteBuffer.wrap(data);
			}				
		}
		else { 
			data = new byte[keySize + 8]; // add 8 for left and right page
			buf = ByteBuffer.wrap(data);
		}
		*/		
		data = new byte[keySize + 8]; // add 8 for left and right page
		buf = ByteBuffer.wrap(data);
	}
	
	public void reset() {
		data = null;
		buf = null;
	}
	
	public void setData(byte[] data) {
		this.data = data;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public int getLeftPage() {
		return buf.getInt(data.length - 8);
	}
	
	public int getRightPage() {
		return buf.getInt(data.length - 4);
	}
		
	public void setLeftPage(int leftPage) {
		buf.putInt(data.length - 8, leftPage);
	}
	
	public void setRightPage(int rightPage) {
		buf.putInt(data.length - 4, rightPage);
	}
	
	public void setPages(int leftPage, int rightPage) {
		buf.putInt(data.length - 8, leftPage);
		buf.putInt(data.length - 4, rightPage);
	}
}