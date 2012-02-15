/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMRTreeSearchCursor implements ITreeIndexCursor {

    private RTreeSearchCursor[] rtreeCursors;
    private BTreeRangeSearchCursor[] btreeCursors;
    private ITreeIndexAccessor[] diskRTreeAccessors;
    private ITreeIndexAccessor[] diskBTreeAccessors;
    private int currentCursror;
    private MultiComparator btreeCmp;
    private int numberOfTrees;
    private SearchPredicate rtreeSearchPredicate;
    private RangePredicate btreeRangePredicate;
    private ITupleReference frameTuple;
    private AtomicInteger searcherRefCount;
    private boolean includeMemRTree;
    private LSMHarness lsmHarness;
    private boolean foundNext = false;

    public LSMRTreeSearchCursor() {
        currentCursror = 0;
    }

    public RTreeSearchCursor getCursor(int cursorIndex) {
        return rtreeCursors[cursorIndex];
    }

    @Override
    public void reset() {
        currentCursror = 0;
        foundNext = false;
    }

    private void searchNextCursor() throws HyracksDataException {
        if (currentCursror < numberOfTrees) {
            rtreeCursors[currentCursror].reset();
            try {
                diskRTreeAccessors[currentCursror].search(rtreeCursors[currentCursror], rtreeSearchPredicate);
            } catch (TreeIndexException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (foundNext) {
            return true;
        }
        while (currentCursror < numberOfTrees) {
            while (rtreeCursors[currentCursror].hasNext()) {
                rtreeCursors[currentCursror].next();
                ITupleReference currentTuple = rtreeCursors[currentCursror].getTuple();

                boolean killerTupleFound = false;
                for (int i = 0; i <= currentCursror; i++) {
                    try {
                        btreeCursors[i].reset();
                        btreeRangePredicate.setHighKey(currentTuple, true);
                        btreeRangePredicate.setLowKey(currentTuple, true);
                        diskBTreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
                    } catch (TreeIndexException e) {
                        throw new HyracksDataException(e);
                    }
                    try {
                        if (btreeCursors[i].hasNext()) {
                            killerTupleFound = true;
                            break;
                        }
                    } finally {
                        btreeCursors[i].close();
                    }
                }
                if (!killerTupleFound) {
                    frameTuple = currentTuple;
                    foundNext = true;
                    return true;
                }
            }
            rtreeCursors[currentCursror].close();
            currentCursror++;
            searchNextCursor();
        }
        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        foundNext = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        btreeCmp = lsmInitialState.getBTreeCmp();
        searcherRefCount = lsmInitialState.getSearcherRefCount();
        includeMemRTree = lsmInitialState.getIncludeMemRTree();
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
        searchNextCursor();
    }

    @Override
    public ICachedPage getPage() {
        // do nothing
        return null;
    }

    @Override
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