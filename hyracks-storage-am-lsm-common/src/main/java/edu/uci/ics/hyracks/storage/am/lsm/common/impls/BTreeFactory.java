package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class BTreeFactory extends TreeFactory {

    public BTreeFactory(IBufferCache bufferCache, LinkedListFreePageManagerFactory freePageManagerFactory, MultiComparator cmp,
            int fieldCount, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
        super(bufferCache, freePageManagerFactory, cmp, fieldCount, interiorFrameFactory, leafFrameFactory);
    }

    @Override
    public ITreeIndex createIndexInstance(int fileId) {
        return new BTree(bufferCache, fieldCount, cmp, freePageManagerFactory.createFreePageManager(fileId),
                interiorFrameFactory, leafFrameFactory);
    }

}
