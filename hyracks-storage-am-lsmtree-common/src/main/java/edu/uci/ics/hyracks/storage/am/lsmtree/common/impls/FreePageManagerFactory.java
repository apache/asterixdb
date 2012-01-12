package edu.uci.ics.hyracks.storage.am.lsmtree.common.impls;

import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;


public class FreePageManagerFactory {

	private final ITreeIndexMetaDataFrameFactory metaDataFrameFactory;
	private final IBufferCache bufferCache;
	
	public FreePageManagerFactory(IBufferCache bufferCache, ITreeIndexMetaDataFrameFactory metaDataFrameFactory) {
		this.metaDataFrameFactory = metaDataFrameFactory;
		this.bufferCache = bufferCache;
	}
	
    public IFreePageManager createFreePageManager(int fileId) {
        return new LinkedListFreePageManager(bufferCache, fileId, 0, metaDataFrameFactory);
    }

}
