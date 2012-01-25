package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
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
    private BTree.BTreeAccessor memBtreeAccessor;
    private ITreeIndexAccessor[] onDiskBTreeAccessors;
    private int currentCursror;
    private MultiComparator btreeCmp;
    private int numberOfTrees;
    private LSMRTree lsmRTree;
    private RangePredicate btreeRangePredicate;
    private ITupleReference frameTuple;

    public LSMRTreeSearchCursor() {
        currentCursror = 0;
    }

    public RTreeSearchCursor getRTreeCursor(int cursorIndex) {
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
						if (j == 0) {
							memBtreeAccessor.search(btreeCursors[j],
									btreeRangePredicate);
						} else {
							onDiskBTreeAccessors[j - 1].search(btreeCursors[j],
									btreeRangePredicate);
						}
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

        lsmRTree = ((LSMRTreeCursorInitialState) initialState).getLsmRTree();
        btreeCmp = ((LSMRTreeCursorInitialState) initialState).getBTreeCmp();
        memBtreeAccessor = ((LSMRTreeCursorInitialState) initialState).getMemBtreeAccessor();

        numberOfTrees = ((LSMRTreeCursorInitialState) initialState).getNumberOfTrees();

        rtreeCursors = new RTreeSearchCursor[numberOfTrees];
        btreeCursors = new BTreeRangeSearchCursor[numberOfTrees];

        onDiskBTreeAccessors = new ITreeIndexAccessor[numberOfTrees - 1];

        for (int i = 0; i < numberOfTrees; i++) {
            // we already have an accessor for the in-memory b-tree
            if (i < numberOfTrees - 1) {
                onDiskBTreeAccessors[i] = lsmRTree.getOnDiskBTrees().get(i).createAccessor();
            }

            rtreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) ((LSMRTreeCursorInitialState) initialState)
                    .getRTreeInteriorFrameFactory().createFrame(),
                    (IRTreeLeafFrame) ((LSMRTreeCursorInitialState) initialState).getRTreeLeafFrameFactory()
                            .createFrame());

            btreeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) ((LSMRTreeCursorInitialState) initialState)
                    .getBTreeLeafFrameFactory().createFrame(), false);
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
		try {
			lsmRTree.threadExit();
		} catch (TreeIndexException e) {
			throw new HyracksDataException(e);
		}
		for (int i = 0; i < numberOfTrees; i++) {
			rtreeCursors[i].close();
			btreeCursors[i].close();
		}
		rtreeCursors = null;
		btreeCursors = null;
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
