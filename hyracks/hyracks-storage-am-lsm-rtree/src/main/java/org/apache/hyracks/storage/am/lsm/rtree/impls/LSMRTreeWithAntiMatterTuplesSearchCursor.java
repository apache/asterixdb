/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;

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
    private boolean open;

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
        open = true;
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
        if (!open) {
            return;
        }
        currentCursor = 0;
        foundNext = false;
        if (includeMutableComponent) {
            for (int i = 0; i < numMutableComponents; i++) {
                mutableRTreeCursors[i].reset();
                btreeCursors[i].reset();
            }
        }
        super.reset();
    }

    @Override
    public void close() throws HyracksDataException {
        if (!open) {
            return;
        }
        if (includeMutableComponent) {
            for (int i = 0; i < numMutableComponents; i++) {
                mutableRTreeCursors[i].close();
                btreeCursors[i].close();
            }
        }
        currentCursor = 0;
        open = false;
        super.close();
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {
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
            int result;
            try {
                result = cmp.selectiveFieldCompare(elementA.getTuple(), elementB.getTuple(), comparatorFields);
                if (result != 0) {
                    return result;
                }
            } catch (HyracksDataException e) {
                throw new IllegalArgumentException(e);
            }

            if (elementA.getCursorIndex() > elementB.getCursorIndex()) {
                return 1;
            } else {
                return -1;
            }
        }
    }
}
