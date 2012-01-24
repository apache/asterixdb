package edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.FreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.impls.TreeFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class RTreeFactory extends TreeFactory {

    public RTreeFactory(IBufferCache bufferCache, FreePageManagerFactory freePageManagerFactory, MultiComparator cmp,
            int fieldCount, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
        super(bufferCache, freePageManagerFactory, cmp, fieldCount, interiorFrameFactory, leafFrameFactory);
    }

    @Override
    public ITreeIndex createIndexInstance(int fileId) {
        return new RTree(bufferCache, fieldCount, cmp, freePageManagerFactory.createFreePageManager(fileId),
                interiorFrameFactory, leafFrameFactory);
    }

}
