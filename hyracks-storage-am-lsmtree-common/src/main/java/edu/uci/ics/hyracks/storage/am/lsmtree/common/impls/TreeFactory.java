package edu.uci.ics.hyracks.storage.am.lsmtree.common.impls;

import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.FreePageManagerFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public abstract class TreeFactory {

    protected IBufferCache bufferCache;
    protected int fieldCount;
    protected MultiComparator cmp;
    protected ITreeIndexFrameFactory interiorFrameFactory;
    protected ITreeIndexFrameFactory leafFrameFactory;
    protected FreePageManagerFactory freePageManagerFactory;

    public TreeFactory(IBufferCache bufferCache, FreePageManagerFactory freePageManagerFactory, MultiComparator cmp,
            int fieldCount, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
        this.bufferCache = bufferCache;
        this.fieldCount = fieldCount;
        this.cmp = cmp;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.freePageManagerFactory = freePageManagerFactory;
    }

    public abstract ITreeIndex createIndexInstance(int fileId);

}
