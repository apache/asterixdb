package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.FreePageManagerFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class BTreeFactory {

	private IBufferCache bufferCache;
	private int fieldCount;
	private MultiComparator cmp;
	private ITreeIndexFrameFactory interiorFrameFactory;
	private ITreeIndexFrameFactory leafFrameFactory;
	private FreePageManagerFactory freePageManagerFactory;
	
	public BTreeFactory(IBufferCache bufferCache, FreePageManagerFactory freePageManagerFactory, MultiComparator cmp, 
			int fieldCount, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
		this.bufferCache = bufferCache;
		this.fieldCount = fieldCount;
		this.cmp = cmp;
		this.interiorFrameFactory = interiorFrameFactory;
		this.leafFrameFactory = leafFrameFactory;
		this.freePageManagerFactory = freePageManagerFactory;
	}
	
    public BTree createBTreeInstance(int fileId) {
        return new BTree(bufferCache, fieldCount, cmp, freePageManagerFactory.createFreePageManager(fileId),
                interiorFrameFactory, leafFrameFactory);
    }
	
	
}
