package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IFreePageManager {
	public int getFreePage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException;
	public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage) throws HyracksDataException;
	public int getMaxPage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException;
	public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage) throws HyracksDataException;
}
