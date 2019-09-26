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

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTree.RTreeAccessor;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public abstract class LSMRTreeAbstractCursor extends EnforcedIndexCursor implements ILSMIndexCursor {

    protected boolean open;
    protected RTreeSearchCursor[] rtreeCursors;
    protected BTreeRangeSearchCursor[] btreeCursors;
    protected RTreeAccessor[] rtreeAccessors;
    protected BTreeAccessor[] btreeAccessors;
    protected BloomFilter[] bloomFilters;
    protected MultiComparator btreeCmp;
    protected int numberOfTrees;
    protected SearchPredicate rtreeSearchPredicate;
    protected RangePredicate btreeRangePredicate;
    protected ITupleReference frameTuple;
    protected boolean includeMutableComponent;
    protected ILSMHarness lsmHarness;
    protected boolean foundNext;
    protected final ILSMIndexOperationContext opCtx;
    protected ISearchOperationCallback searchCallback;
    protected List<ILSMComponent> operationalComponents;
    protected long[] hashes = BloomFilter.createHashArray();
    protected final IIndexAccessParameters iap;

    public LSMRTreeAbstractCursor(ILSMIndexOperationContext opCtx, IIndexCursorStats stats) {
        this.opCtx = opCtx;
        btreeRangePredicate = new RangePredicate(null, null, true, true, null, null);
        this.iap = IndexAccessParameters.createNoOpParams(stats);
    }

    public RTreeSearchCursor getCursor(int cursorIndex) {
        return rtreeCursors[cursorIndex];
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMRTreeCursorInitialState lsmInitialState = (LSMRTreeCursorInitialState) initialState;
        if (btreeCmp == null) {
            btreeCmp = lsmInitialState.getBTreeCmp();
            btreeRangePredicate.setLowKeyCmp(btreeCmp);
            btreeRangePredicate.setHighKeyCmp(btreeCmp);
        }
        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        numberOfTrees = operationalComponents.size();
        searchCallback = lsmInitialState.getSearchOperationCallback();

        int numComponenets = operationalComponents.size();
        if (rtreeCursors == null || rtreeCursors.length != numComponenets) {
            // object creation: should be relatively low
            rtreeCursors = new RTreeSearchCursor[numberOfTrees];
            btreeCursors = new BTreeRangeSearchCursor[numberOfTrees];
            rtreeAccessors = new RTreeAccessor[numberOfTrees];
            btreeAccessors = new BTreeAccessor[numberOfTrees];
            bloomFilters = new BloomFilter[numberOfTrees];
        }

        includeMutableComponent = false;
        for (int i = 0; i < numberOfTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            RTree rtree;
            BTree btree;
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                // No need for a bloom filter for the in-memory BTree.
                rtree = ((LSMRTreeMemoryComponent) component).getIndex();
                btree = ((LSMRTreeMemoryComponent) component).getBuddyIndex();
                bloomFilters[i] = null;
            } else {
                rtree = ((LSMRTreeDiskComponent) component).getIndex();
                btree = ((LSMRTreeDiskComponent) component).getBuddyIndex();
                bloomFilters[i] = ((LSMRTreeDiskComponent) component).getBloomFilter();
            }
            if (rtreeAccessors[i] == null) {
                rtreeAccessors[i] = rtree.createAccessor(iap);
                // do not count the random I/Os incurred by btree lookups
                btreeAccessors[i] = btree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            } else {
                rtreeAccessors[i].reset(rtree, iap);
                btreeAccessors[i].reset(btree, iap);
            }
            if (rtreeCursors[i] == null) {
                rtreeCursors[i] = rtreeAccessors[i].createSearchCursor(false);
            } else {
                rtreeCursors[i].close();
            }
            if (btreeCursors[i] == null) {
                // need to create a new one
                btreeCursors[i] = btreeAccessors[i].createPointCursor(false);
            } else {
                // close
                btreeCursors[i].close();
            }
        }

        rtreeSearchPredicate = (SearchPredicate) searchPred;
        btreeRangePredicate.setHighKey(null);
        btreeRangePredicate.setLowKey(null);

        open = true;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        if (!open) {
            return;
        }

        try {
            if (rtreeCursors != null && btreeCursors != null) {
                for (int i = 0; i < numberOfTrees; i++) {
                    rtreeCursors[i].destroy();
                    btreeCursors[i].destroy();
                }
            }
            rtreeCursors = null;
            btreeCursors = null;
        } finally {
            lsmHarness.endSearch(opCtx);
        }

        foundNext = false;
        open = false;
    }

    @Override
    public ITupleReference doGetTuple() {
        return frameTuple;
    }

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return false;
    }

}
