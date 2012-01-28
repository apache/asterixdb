package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMRTreeCursorInitialState implements ICursorInitialState {

    private int numberOfTrees;
    private ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    private ITreeIndexFrameFactory rtreeLeafFrameFactory;
    private ITreeIndexFrameFactory btreeLeafFrameFactory;
    private MultiComparator btreeCmp;
    private LSMRTree lsmRTree;
    private ITreeIndexAccessor[] bTreeAccessors;
    private final boolean includeMemRTree;
    private final AtomicInteger searcherRefCount;

    public LSMRTreeCursorInitialState(int numberOfTrees, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            MultiComparator btreeCmp, ITreeIndexAccessor[] bTreeAccessors, LSMRTree lsmRTree, boolean includeMemRTree,
            AtomicInteger searcherRefCount) {
        this.numberOfTrees = numberOfTrees;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.btreeCmp = btreeCmp;
        this.lsmRTree = lsmRTree;
        this.bTreeAccessors = bTreeAccessors;
        this.includeMemRTree = includeMemRTree;
        this.searcherRefCount = searcherRefCount;
    }

    public int getNumberOfTrees() {
        return numberOfTrees;
    }

    public ITreeIndexFrameFactory getRTreeInteriorFrameFactory() {
        return rtreeInteriorFrameFactory;
    }

    public ITreeIndexFrameFactory getRTreeLeafFrameFactory() {
        return rtreeLeafFrameFactory;
    }

    public ITreeIndexFrameFactory getBTreeLeafFrameFactory() {
        return btreeLeafFrameFactory;
    }

    public MultiComparator getBTreeCmp() {
        return btreeCmp;
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    public ITreeIndexAccessor[] getBTreeAccessors() {
        return bTreeAccessors;
    }

    public LSMRTree getLsmRTree() {
        return lsmRTree;
    }

    public boolean getIncludeMemRTree() {
        return includeMemRTree;
    }

    public AtomicInteger getSearcherRefCount() {
        return searcherRefCount;
    }

}
