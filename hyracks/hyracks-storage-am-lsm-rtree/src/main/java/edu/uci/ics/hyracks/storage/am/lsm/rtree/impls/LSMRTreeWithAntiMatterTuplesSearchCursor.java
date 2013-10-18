/*
 * Copyright 2009-2013 by The Regents of the University of California
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
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;

public class LSMRTreeWithAntiMatterTuplesSearchCursor extends LSMIndexSearchCursor {

    private ITreeIndexAccessor[] mutableRTreeAccessors;
    private ITreeIndexAccessor[] btreeAccessors;
    private RTreeSearchCursor[] mutableRTreeCursors;
    private ITreeIndexCursor[] btreeCursors;
    private RangePredicate btreeRangePredicate;
    private boolean foundNext;
    private ITupleReference frameTuple;
    private int[] comparatorFields;
    private MultiComparator btreeCmp;
    private int currentCursor;
    private SearchPredicate rtreeSearchPredicate;
    private int numMutableComponents;

    public LSMRTreeWithAntiMatterTuplesSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false);
    }

    public LSMRTreeWithAntiMatterTuplesSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples) {
        super(opCtx, returnDeletedTuples);
        currentCursor = 0;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException,
            IndexException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getHilbertCmp();
        btreeCmp = lsmInitialState.getBTreeCmp();
        lsmHarness = lsmInitialState.getLSMHarness();
        comparatorFields = lsmInitialState.getComparatorFields();
        operationalComponents = lsmInitialState.getOperationalComponents();
        rtreeSearchPredicate = (SearchPredicate) searchPred;

        includeMutableComponent = false;
        numMutableComponents = 0;
        int numImmutableComponents = 0;
        for (ILSMComponent component : operationalComponents) {
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                numMutableComponents++;
            } else {
                numImmutableComponents++;
            }
        }
        if (includeMutableComponent) {
            btreeRangePredicate = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);
        }

        mutableRTreeCursors = new RTreeSearchCursor[numMutableComponents];
        mutableRTreeAccessors = new ITreeIndexAccessor[numMutableComponents];
        btreeCursors = new BTreeRangeSearchCursor[numMutableComponents];
        btreeAccessors = new ITreeIndexAccessor[numMutableComponents];
        for (int i = 0; i < numMutableComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            RTree rtree = (RTree) ((LSMRTreeMemoryComponent) component).getRTree();
            BTree btree = (BTree) ((LSMRTreeMemoryComponent) component).getBTree();
            mutableRTreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());
            btreeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory()
                    .createFrame(), false);
            btreeAccessors[i] = btree.createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            mutableRTreeAccessors[i] = rtree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
        }

        rangeCursors = new RTreeSearchCursor[numImmutableComponents];
        ITreeIndexAccessor[] immutableRTreeAccessors = new ITreeIndexAccessor[numImmutableComponents];
        int j = 0;
        for (int i = numMutableComponents; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);
            rangeCursors[j] = new RTreeSearchCursor((IRTreeInteriorFrame) lsmInitialState
                    .getRTreeInteriorFrameFactory().createFrame(), (IRTreeLeafFrame) lsmInitialState
                    .getRTreeLeafFrameFactory().createFrame());
            RTree rtree = (RTree) ((LSMRTreeDiskComponent) component).getRTree();
            immutableRTreeAccessors[j] = rtree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            immutableRTreeAccessors[j].search(rangeCursors[j], searchPred);
            j++;
        }
        searchNextCursor();
        setPriorityQueueComparator();
        initPriorityQueue();
    }

    private void searchNextCursor() throws HyracksDataException, IndexException {
        if (currentCursor < numMutableComponents) {
            mutableRTreeCursors[currentCursor].reset();
            mutableRTreeAccessors[currentCursor].search(mutableRTreeCursors[currentCursor], rtreeSearchPredicate);
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        if (includeMutableComponent) {
            if (foundNext) {
                return true;
            }

            while (currentCursor < numMutableComponents) {
                while (mutableRTreeCursors[currentCursor].hasNext()) {
                    mutableRTreeCursors[currentCursor].next();
                    ITupleReference currentTuple = mutableRTreeCursors[currentCursor].getTuple();
                    if (searchMemBTrees(currentTuple, currentCursor)) {
                        foundNext = true;
                        frameTuple = currentTuple;
                        return true;
                    }
                }
                mutableRTreeCursors[currentCursor].close();
                currentCursor++;
                searchNextCursor();
            }
            while (super.hasNext()) {
                super.next();
                ITupleReference diskRTreeTuple = super.getTuple();
                if (searchMemBTrees(diskRTreeTuple, numMutableComponents)) {
                    foundNext = true;
                    frameTuple = diskRTreeTuple;
                    return true;
                }
            }
        } else {
            return super.hasNext();
        }

        return false;
    }

    @Override
    public void next() throws HyracksDataException {
        if (includeMutableComponent) {
            foundNext = false;
        } else {
            super.next();
        }

    }

    @Override
    public ITupleReference getTuple() {
        if (includeMutableComponent) {
            return frameTuple;
        } else {
            return super.getTuple();
        }

    }

    @Override
    public void reset() throws HyracksDataException, IndexException {
        if (includeMutableComponent) {
            for (int i = 0; i < numMutableComponents; i++) {
                mutableRTreeCursors[i].reset();
                btreeCursors[i].reset();
            }
        }
        currentCursor = 0;
        super.reset();
    }

    @Override
    public void close() throws HyracksDataException {
        if (includeMutableComponent) {
            for (int i = 0; i < numMutableComponents; i++) {
                mutableRTreeCursors[i].close();
                btreeCursors[i].close();
            }
        }
        currentCursor = 0;
        super.close();
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB) {
        return cmp.selectiveFieldCompare(tupleA, tupleB, comparatorFields);
    }

    private boolean searchMemBTrees(ITupleReference tuple, int lastBTreeToSearch) throws HyracksDataException,
            IndexException {
        for (int i = 0; i < lastBTreeToSearch; i++) {
            btreeCursors[i].reset();
            btreeRangePredicate.setHighKey(tuple, true);
            btreeRangePredicate.setLowKey(tuple, true);
            btreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
            try {
                if (btreeCursors[i].hasNext()) {
                    return false;
                }
            } finally {
                btreeCursors[i].close();
            }
        }
        return true;
    }

    @Override
    protected void setPriorityQueueComparator() {
        if (pqCmp == null || cmp != pqCmp.getMultiComparator()) {
            pqCmp = new PriorityQueueHilbertComparator(cmp, comparatorFields);
        }
    }

    public class PriorityQueueHilbertComparator extends PriorityQueueComparator {

        private final int[] comparatorFields;

        public PriorityQueueHilbertComparator(MultiComparator cmp, int[] comparatorFields) {
            super(cmp);
            this.comparatorFields = comparatorFields;
        }

        @Override
        public int compare(PriorityQueueElement elementA, PriorityQueueElement elementB) {
            int result = cmp.selectiveFieldCompare(elementA.getTuple(), elementB.getTuple(), comparatorFields);
            if (result != 0) {
                return result;
            }
            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }
    }
}
