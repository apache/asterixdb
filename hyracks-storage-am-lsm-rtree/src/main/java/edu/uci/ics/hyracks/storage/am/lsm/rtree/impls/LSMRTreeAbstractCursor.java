package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public abstract class LSMRTreeAbstractCursor {

    protected RTreeSearchCursor[] rtreeCursors;

    public abstract void next() throws HyracksDataException;

    public abstract boolean hasNext() throws HyracksDataException;

    public abstract void reset() throws HyracksDataException;

    protected BTreeRangeSearchCursor[] btreeCursors;
    protected ITreeIndexAccessor[] diskRTreeAccessors;
    protected ITreeIndexAccessor[] diskBTreeAccessors;
    private MultiComparator btreeCmp;
    protected int numberOfTrees;
    protected SearchPredicate rtreeSearchPredicate;
    protected RangePredicate btreeRangePredicate;
    protected ITupleReference frameTuple;
    private AtomicInteger searcherRefCount;
    private boolean includeMemRTree;
    private LSMHarness lsmHarness;
    protected boolean foundNext;

    public LSMRTreeAbstractCursor() {
        super();
    }

    public RTreeSearchCursor getCursor(int cursorIndex) {
        return rtreeCursors[cursorIndex];
    }

    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        btreeCmp = lsmInitialState.getBTreeCmp();
        searcherRefCount = lsmInitialState.getSearcherRefCount();
        includeMemRTree = lsmInitialState.getIncludeMemComponent();
        lsmHarness = lsmInitialState.getLSMHarness();
        numberOfTrees = lsmInitialState.getNumberOfTrees();
        diskRTreeAccessors = lsmInitialState.getRTreeAccessors();
        diskBTreeAccessors = lsmInitialState.getBTreeAccessors();

        rtreeCursors = new RTreeSearchCursor[numberOfTrees];
        btreeCursors = new BTreeRangeSearchCursor[numberOfTrees];

        for (int i = 0; i < numberOfTrees; i++) {
            rtreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());

            btreeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory()
                    .createFrame(), false);
        }
        rtreeSearchPredicate = (SearchPredicate) searchPred;
        btreeRangePredicate = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);
    }

    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    public void close() throws HyracksDataException {
        try {
            for (int i = 0; i < numberOfTrees; i++) {
                rtreeCursors[i].close();
                btreeCursors[i].close();
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.closeSearchCursor(searcherRefCount, includeMemRTree);
        }
    }

    public void setBufferCache(IBufferCache bufferCache) {
        // do nothing
    }

    public void setFileId(int fileId) {
        // do nothing
    }

    public ITupleReference getTuple() {
        return frameTuple;
    }

    public boolean exclusiveLatchNodes() {
        return false;
    }

}