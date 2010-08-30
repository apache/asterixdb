package edu.uci.ics.hyracks.storage.am.btree.interfaces;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public interface IFrame {	
	public void setPage(ICachedPage page);
	public ICachedPage getPage();
	public ByteBuffer getBuffer();
	
	public void insert(byte[] data, MultiComparator cmp) throws Exception;
	public void update(int rid, byte[] data) throws Exception;
	public void delete(byte[] data, MultiComparator cmp, boolean exactDelete) throws Exception;
	
	public void compact(MultiComparator cmp);
	public boolean compress(MultiComparator cmp) throws Exception;
	
	public void initBuffer(byte level);
	
	public int getNumRecords();
		
	// assumption: page must be write-latched at this point
	public SpaceStatus hasSpaceInsert(byte[] data, MultiComparator cmp);
	public SpaceStatus hasSpaceUpdate(int rid, byte[] data, MultiComparator cmp);
	
	public int getRecordOffset(int slotNum);
	
	public int getTotalFreeSpace();	
	
	public void setPageLsn(int pageLsn);
	public int getPageLsn();
	
	// for debugging
	public void printHeader();
	public String printKeys(MultiComparator cmp);
}