package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BloomFilterAwareBTreePointSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public abstract class LSMRTreeAbstractCursor implements ITreeIndexCursor {

    protected RTreeSearchCursor[] rtreeCursors;
    protected boolean open = false;
    protected ITreeIndexCursor[] btreeCursors;
    protected ITreeIndexAccessor[] diskRTreeAccessors;
    protected ITreeIndexAccessor[] diskBTreeAccessors;
    private MultiComparator btreeCmp;
    protected int numberOfTrees;
    protected SearchPredicate rtreeSearchPredicate;
    protected RangePredicate btreeRangePredicate;
    protected ITupleReference frameTuple;
    protected AtomicInteger searcherRefCount;
    protected boolean includeMemRTree;
    protected ILSMHarness lsmHarness;
    protected boolean foundNext;
    protected final ILSMIndexOperationContext opCtx;

    protected List<ILSMComponent> operationalComponents;

    public LSMRTreeAbstractCursor(ILSMIndexOperationContext opCtx) {
        super();
        this.opCtx = opCtx;
    }

    public RTreeSearchCursor getCursor(int cursorIndex) {
        return rtreeCursors[cursorIndex];
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        btreeCmp = lsmInitialState.getBTreeCmp();
        includeMemRTree = lsmInitialState.getIncludeMemComponent();
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        numberOfTrees = lsmInitialState.getNumberOfTrees();
        diskRTreeAccessors = lsmInitialState.getRTreeAccessors();
        diskBTreeAccessors = lsmInitialState.getBTreeAccessors();

        rtreeCursors = new RTreeSearchCursor[numberOfTrees];
        btreeCursors = new ITreeIndexCursor[numberOfTrees];

        int i = 0;
        if (includeMemRTree) {
            rtreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());

            // No need for a bloom filter for the in-memory BTree.
            btreeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory()
                    .createFrame(), false);
            ++i;
        }
        for (; i < numberOfTrees; i++) {
            rtreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());

            btreeCursors[i] = new BloomFilterAwareBTreePointSearchCursor((IBTreeLeafFrame) lsmInitialState
                    .getBTreeLeafFrameFactory().createFrame(), false,
                    ((LSMRTreeImmutableComponent) operationalComponents.get(i)).getBloomFilter());
        }

        rtreeSearchPredicate = (SearchPredicate) searchPred;
        btreeRangePredicate = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);

        open = true;
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            return;
        }

        try {
            if (rtreeCursors != null && btreeCursors != null) {
                for (int i = 0; i < numberOfTrees; i++) {
                    rtreeCursors[i].close();
                    btreeCursors[i].close();
                }
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.endSearch(opCtx);
        }

        open = false;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        // do nothing
    }

    @Override
    public void setFileId(int fileId) {
        // do nothing
    }

    @Override
    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public boolean exclusiveLatchNodes() {
        return false;
    }

}