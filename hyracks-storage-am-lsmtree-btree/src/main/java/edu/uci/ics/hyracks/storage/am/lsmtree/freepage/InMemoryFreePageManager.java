package edu.uci.ics.hyracks.storage.am.lsmtree.freepage;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.PageAllocationException;

public class InMemoryFreePageManager implements IFreePageManager{
	private final int maxCapacity;
	private int currentCapacity;
	private final ITreeIndexMetaDataFrameFactory metaDataFrameFactory;
	
	public InMemoryFreePageManager(int maxCapacity, ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
		this.maxCapacity = maxCapacity-1; // Since the range of CacheArray in InMemoryBufferCache is 0 ~ maxCapacity-1
		currentCapacity = 1;
		this.metaDataFrameFactory = metaDataFrameFactory;
	}

	public int getCurrentCapacity() {
		return currentCapacity;
	}
	
	@Override
	public synchronized int getFreePage(ITreeIndexMetaDataFrame metaFrame)
			throws HyracksDataException, PageAllocationException {
		
		if(currentCapacity == maxCapacity) {
			throw new PageAllocationException("In-mem tree capacity reaches max capacity");	
		}
		currentCapacity++;
		return currentCapacity;
	}
	

	@Override
	public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage)
			throws HyracksDataException {
		System.out.println("InMemoryFreePageManager.addFreePage()");
	}

	@Override
	public int getMaxPage(ITreeIndexMetaDataFrame metaFrame)
			throws HyracksDataException {
		System.out.println("InMemoryFreePageManager.getMaxPage()");
		return 0;
	}

	@Override
	public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage)
			throws HyracksDataException {
		currentCapacity = 1;
	}

	@Override
	public ITreeIndexMetaDataFrameFactory getMetaDataFrameFactory() {
		return metaDataFrameFactory;
	}

	@Override
	public byte getMetaPageLevelIndicator() {
		System.out.println("InMemoryFreePageManager.getMetaPageLevelIndicator()");
		return 0;
	}

	@Override
	public byte getFreePageLevelIndicator() {
		System.out.println("InMemoryFreePageManager.getFreePageLevelIndicator()");
		return 0;
	}

	@Override
	public boolean isMetaPage(ITreeIndexMetaDataFrame metaFrame) {
		System.out.println("InMemoryFreePageManager.isMetaPage()");
		return false;
	}

	@Override
	public boolean isFreePage(ITreeIndexMetaDataFrame metaFrame) {
		System.out.println("InMemoryFreePageManager.isFreePage()");
		return false;
	}
	
	public void reset(){
		currentCapacity = 1;
	}
}
