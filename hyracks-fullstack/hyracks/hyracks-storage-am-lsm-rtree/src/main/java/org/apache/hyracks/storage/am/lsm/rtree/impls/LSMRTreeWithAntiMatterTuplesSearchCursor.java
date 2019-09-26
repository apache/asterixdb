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
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.util.IndexCursorUtils;

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
    private int numMemoryComponents;
    private boolean open;
    protected ISearchOperationCallback searchCallback;
    private boolean resultOfsearchCallBackProceed = false;

    public LSMRTreeWithAntiMatterTuplesSearchCursor(ILSMIndexOperationContext opCtx) {
        this(opCtx, false, NoOpIndexCursorStats.INSTANCE);
    }

    public LSMRTreeWithAntiMatterTuplesSearchCursor(ILSMIndexOperationContext opCtx, boolean returnDeletedTuples,
            IIndexCursorStats stats) {
        super(opCtx, returnDeletedTuples, stats);
        currentCursor = 0;
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        cmp = lsmInitialState.getHilbertCmp();
        btreeCmp = lsmInitialState.getBTreeCmp();
        lsmHarness = lsmInitialState.getLSMHarness();
        comparatorFields = lsmInitialState.getComparatorFields();
        operationalComponents = lsmInitialState.getOperationalComponents();
        rtreeSearchPredicate = (SearchPredicate) searchPred;
        searchCallback = lsmInitialState.getSearchOperationCallback();
        numMemoryComponents = 0;
        int numImmutableComponents = 0;
        for (ILSMComponent component : operationalComponents) {
            if (component.getType() == LSMComponentType.MEMORY) {
                numMemoryComponents++;
            } else {
                numImmutableComponents++;
            }
        }
        if (numMemoryComponents > 0) {
            btreeRangePredicate = new RangePredicate(null, null, true, true, btreeCmp, btreeCmp);
        }

        mutableRTreeCursors = new RTreeSearchCursor[numMemoryComponents];
        mutableRTreeAccessors = new ITreeIndexAccessor[numMemoryComponents];
        btreeCursors = new BTreeRangeSearchCursor[numMemoryComponents];
        btreeAccessors = new ITreeIndexAccessor[numMemoryComponents];
        for (int i = 0; i < numMemoryComponents; i++) {
            ILSMComponent component = operationalComponents.get(i);
            RTree rtree = ((LSMRTreeMemoryComponent) component).getIndex();
            BTree btree = ((LSMRTreeMemoryComponent) component).getBuddyIndex();
            btreeAccessors[i] = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            mutableRTreeAccessors[i] = rtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            btreeCursors[i] = (ITreeIndexCursor) btreeAccessors[i].createSearchCursor(false);
            mutableRTreeCursors[i] = (RTreeSearchCursor) mutableRTreeAccessors[i].createSearchCursor(false);
        }

        rangeCursors = new RTreeSearchCursor[numImmutableComponents];
        ITreeIndexAccessor[] immutableRTreeAccessors = new ITreeIndexAccessor[numImmutableComponents];
        int j = 0;
        try {
            for (int i = numMemoryComponents; i < operationalComponents.size(); i++) {
                ILSMComponent component = operationalComponents.get(i);

                RTree rtree = ((LSMRTreeWithAntimatterDiskComponent) component).getIndex();
                immutableRTreeAccessors[j] = rtree.createAccessor(iap);
                rangeCursors[j] = immutableRTreeAccessors[j].createSearchCursor(false);
                immutableRTreeAccessors[j].search(rangeCursors[j], searchPred);
                j++;
            }
            searchNextCursor();
            setPriorityQueueComparator();
            initPriorityQueue();
        } catch (Throwable th) { // NOSONAR: Must catch all failures
            IndexCursorUtils.close(rangeCursors, th);
            IndexCursorUtils.close(mutableRTreeCursors, th);
            throw HyracksDataException.create(th);
        }
        open = true;
    }

    private void searchNextCursor() throws HyracksDataException {
        if (currentCursor < numMemoryComponents) {
            mutableRTreeCursors[currentCursor].close();
            mutableRTreeAccessors[currentCursor].search(mutableRTreeCursors[currentCursor], rtreeSearchPredicate);
        }
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        if (numMemoryComponents > 0) {
            if (foundNext) {
                return true;
            }

            while (currentCursor < numMemoryComponents) {
                while (mutableRTreeCursors[currentCursor].hasNext()) {
                    mutableRTreeCursors[currentCursor].next();
                    ITupleReference currentTuple = mutableRTreeCursors[currentCursor].getTuple();
                    // TODO: at this time, we only add proceed() part.
                    // reconcile() and complete() can be added later after considering the semantics.

                    // Call proceed() to do necessary operations before returning this tuple.
                    resultOfsearchCallBackProceed = searchCallback.proceed(currentTuple);
                    if (searchMemBTrees(currentTuple, currentCursor)) {
                        // anti-matter tuple is NOT found
                        foundNext = true;
                        frameTuple = currentTuple;
                        return true;
                    }
                }
                mutableRTreeCursors[currentCursor].close();
                currentCursor++;
                searchNextCursor();
            }
            while (super.doHasNext()) {
                super.doNext();
                ITupleReference diskRTreeTuple = super.doGetTuple();
                // TODO: at this time, we only add proceed().
                // reconcile() and complete() can be added later after considering the semantics.

                // Call proceed() to do necessary operations before returning this tuple.
                resultOfsearchCallBackProceed = true;
                if (searchMemBTrees(diskRTreeTuple, numMemoryComponents)) {
                    // anti-matter tuple is NOT found
                    foundNext = true;
                    frameTuple = diskRTreeTuple;
                    return true;
                }
            }
        } else {
            if (super.doHasNext()) {
                super.doNext();
                ITupleReference diskRTreeTuple = super.doGetTuple();
                // TODO: at this time, we only add proceed() part.
                // reconcile() and complete() can be added later after considering the semantics.
                // Call proceed() to do necessary operations before returning this tuple.
                // Since in-memory components don't exist, we can skip searching in-memory B-Trees.
                resultOfsearchCallBackProceed = searchCallback.proceed(diskRTreeTuple);
                foundNext = true;
                frameTuple = diskRTreeTuple;
                return true;
            }
        }

        return false;
    }

    @Override
    public ITupleReference getFilterMinTuple() {
        ILSMComponentFilter filter = operationalComponents.get(
                currentCursor < numMemoryComponents ? currentCursor : outputElement.getCursorIndex() + currentCursor)
                .getLSMComponentFilter();
        return filter == null ? null : filter.getMinTuple();
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        ILSMComponentFilter filter = operationalComponents.get(
                currentCursor < numMemoryComponents ? currentCursor : outputElement.getCursorIndex() + currentCursor)
                .getLSMComponentFilter();
        return filter == null ? null : filter.getMaxTuple();
    }

    @Override
    public void doNext() throws HyracksDataException {
        foundNext = false;
    }

    @Override
    public ITupleReference doGetTuple() {
        return frameTuple;
    }

    @Override
    public void doClose() throws HyracksDataException {
        if (!open) {
            return;
        }
        currentCursor = 0;
        foundNext = false;
        if (numMemoryComponents > 0) {
            for (int i = 0; i < numMemoryComponents; i++) {
                mutableRTreeCursors[i].close();
                btreeCursors[i].close();
            }
        }
        super.doClose();
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        if (!open) {
            return;
        }
        if (numMemoryComponents > 0) {
            for (int i = 0; i < numMemoryComponents; i++) {
                mutableRTreeCursors[i].destroy();
                btreeCursors[i].destroy();
            }
        }
        currentCursor = 0;
        open = false;
        super.doDestroy();
    }

    @Override
    protected int compare(MultiComparator cmp, ITupleReference tupleA, ITupleReference tupleB)
            throws HyracksDataException {
        return cmp.selectiveFieldCompare(tupleA, tupleB, comparatorFields);
    }

    private boolean searchMemBTrees(ITupleReference tuple, int lastBTreeToSearch) throws HyracksDataException {
        for (int i = 0; i < lastBTreeToSearch; i++) {
            btreeCursors[i].close();
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

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return resultOfsearchCallBackProceed;
    }
}
