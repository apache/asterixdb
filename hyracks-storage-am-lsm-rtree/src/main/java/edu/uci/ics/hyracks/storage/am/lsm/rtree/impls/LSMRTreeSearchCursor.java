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
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMRTreeSearchCursor implements ITreeIndexCursor {

    private RTreeSearchCursor[] rtreeCursors;
    private BTreeRangeSearchCursor[] btreeCursors;
    private ITreeIndexAccessor[] diskBTreeAccessors;
    private int currentCursror;
    private MultiComparator btreeCmp;
    private int numberOfTrees;
    private LSMRTree lsmRTree;
    private RangePredicate btreeRangePredicate;
    private ITupleReference frameTuple;
    private boolean includeMemRTree;
    private AtomicInteger searcherRefCount;

    public LSMRTreeSearchCursor() {
        currentCursror = 0;
    }

    public RTreeSearchCursor getCursor(int cursorIndex) {
        return rtreeCursors[cursorIndex];
    }

    @Override
    public void reset() {
        // do nothing
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        for (int i = currentCursror; i < numberOfTrees; i++) {
            while (rtreeCursors[i].hasNext()) {
                rtreeCursors[i].next();
                ITupleReference currentTuple = rtreeCursors[i].getTuple();
                boolean killerTupleFound = false;
                for (int j = 0; j <= i; j++) {
                    btreeRangePredicate.setHighKey(currentTuple, true);
                    btreeRangePredicate.setLowKey(currentTuple, true);

                    try {
                        diskBTreeAccessors[j].search(btreeCursors[j], btreeRangePredicate);
                    } catch (TreeIndexException e) {
                        throw new HyracksDataException(e);
                    }

                    if (btreeCursors[j].hasNext()) {
                        killerTupleFound = true;
                        break;
                    }
                }
                if (!killerTupleFound) {
                    frameTuple = currentTuple;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        while (true) {
            if (currentCursror < numberOfTrees || rtreeCursors[currentCursror].hasNext()) {
                break;
            } else {
                currentCursror++;
            }
        }
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        lsmRTree = lsmInitialState.getLsmRTree();
        btreeCmp = lsmInitialState.getBTreeCmp();
        includeMemRTree = lsmInitialState.getIncludeMemRTree();
        searcherRefCount = lsmInitialState.getSearcherRefCount();
        numberOfTrees = lsmInitialState.getNumberOfTrees();
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
        btreeRangePredicate = new RangePredicate(true, null, null, true, true, btreeCmp, btreeCmp);
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < numberOfTrees; i++) {
            rtreeCursors[i].close();
            btreeCursors[i].close();
        }
        rtreeCursors = null;
        btreeCursors = null;

        // If the in-memory RTree was not included in the search, then we don't
        // need to synchronize with a flush.
        if (includeMemRTree) {
            try {
                lsmRTree.threadExit();
            } catch (TreeIndexException e) {
                throw new HyracksDataException(e);
            }
        } else {
            // Synchronize with ongoing merges.
            searcherRefCount.decrementAndGet();
        }
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
