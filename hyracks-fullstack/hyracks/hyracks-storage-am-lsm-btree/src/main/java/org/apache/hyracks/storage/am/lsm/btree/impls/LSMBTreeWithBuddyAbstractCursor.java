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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public abstract class LSMBTreeWithBuddyAbstractCursor extends EnforcedIndexCursor implements ILSMIndexCursor {

    protected boolean open;
    protected BTreeRangeSearchCursor[] btreeCursors;
    protected BTreeRangeSearchCursor[] buddyBtreeCursors;
    protected BTreeAccessor[] btreeAccessors;
    protected BTreeAccessor[] buddyBtreeAccessors;
    protected BloomFilter[] buddyBtreeBloomFilters;
    protected MultiComparator btreeCmp;
    protected MultiComparator buddyBtreeCmp;
    protected int numberOfTrees;
    protected RangePredicate btreeRangePredicate;
    protected RangePredicate buddyBtreeRangePredicate;
    protected ITupleReference frameTuple;
    protected boolean includeMutableComponent;
    protected ILSMHarness lsmHarness;
    protected boolean foundNext;
    protected final ILSMIndexOperationContext opCtx;
    protected final IIndexAccessParameters iap;

    protected final long[] hashes = BloomFilter.createHashArray();

    protected List<ILSMComponent> operationalComponents;

    public LSMBTreeWithBuddyAbstractCursor(ILSMIndexOperationContext opCtx, IIndexCursorStats stats) {
        super();
        this.opCtx = opCtx;
        this.iap = IndexAccessParameters.createNoOpParams(stats);
        buddyBtreeRangePredicate = new RangePredicate(null, null, true, true, null, null);
    }

    public ITreeIndexCursor getCursor(int cursorIndex) {
        return btreeCursors[cursorIndex];
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {

        LSMBTreeWithBuddyCursorInitialState lsmInitialState = (LSMBTreeWithBuddyCursorInitialState) initialState;
        btreeCmp = lsmInitialState.getBTreeCmp();
        buddyBtreeCmp = lsmInitialState.getBuddyBTreeCmp();

        operationalComponents = lsmInitialState.getOperationalComponents();
        lsmHarness = lsmInitialState.getLSMHarness();
        numberOfTrees = operationalComponents.size();

        if (btreeCursors == null || btreeCursors.length != numberOfTrees) {
            // need to re-use the following four instead of re-creating
            btreeCursors = new BTreeRangeSearchCursor[numberOfTrees];
            buddyBtreeCursors = new BTreeRangeSearchCursor[numberOfTrees];
            btreeAccessors = new BTreeAccessor[numberOfTrees];
            buddyBtreeAccessors = new BTreeAccessor[numberOfTrees];
            buddyBtreeBloomFilters = new BloomFilter[numberOfTrees];
        }

        includeMutableComponent = false;

        for (int i = 0; i < numberOfTrees; i++) {
            ILSMComponent component = operationalComponents.get(i);
            BTree btree;
            BTree buddyBtree;
            if (component.getType() == LSMComponentType.MEMORY) {
                // This is not needed at the moment but is implemented anyway
                includeMutableComponent = true;
                // No need for a bloom filter for the in-memory BTree.
                if (buddyBtreeCursors[i] == null) {
                    buddyBtreeCursors[i] = new BTreeRangeSearchCursor(
                            (IBTreeLeafFrame) lsmInitialState.getBuddyBTreeLeafFrameFactory().createFrame(), false);
                } else {
                    buddyBtreeCursors[i].close();
                }
                btree = ((LSMBTreeWithBuddyMemoryComponent) component).getIndex();
                buddyBtree = ((LSMBTreeWithBuddyMemoryComponent) component).getBuddyIndex();
                buddyBtreeBloomFilters[i] = null;
            } else {
                if (buddyBtreeCursors[i] == null) {
                    buddyBtreeCursors[i] = new BTreeRangeSearchCursor(
                            (IBTreeLeafFrame) lsmInitialState.getBuddyBTreeLeafFrameFactory().createFrame(), false);
                } else {
                    buddyBtreeCursors[i].close();
                }
                btree = ((LSMBTreeWithBuddyDiskComponent) component).getIndex();
                buddyBtree = ((LSMBTreeWithBuddyDiskComponent) component).getBuddyIndex();
                buddyBtreeBloomFilters[i] = ((LSMBTreeWithBuddyDiskComponent) component).getBloomFilter();
            }
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) lsmInitialState.getBTreeLeafFrameFactory().createFrame();
            if (btreeAccessors[i] == null) {
                btreeCursors[i] = new BTreeRangeSearchCursor(leafFrame, false);
                btreeAccessors[i] = btree.createAccessor(iap);
                buddyBtreeAccessors[i] = buddyBtree.createAccessor(iap);
            } else {
                btreeCursors[i].close();
                btreeAccessors[i].reset(btree, iap);
                buddyBtreeAccessors[i].reset(buddyBtree, iap);
            }
        }
        btreeRangePredicate = (RangePredicate) searchPred;
        buddyBtreeRangePredicate.reset(null, null, true, true, buddyBtreeCmp, buddyBtreeCmp);
        open = true;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        if (!open) {
            return;
        }
        try {
            if (btreeCursors != null && buddyBtreeCursors != null) {
                for (int i = 0; i < numberOfTrees; i++) {
                    btreeCursors[i].destroy();
                    buddyBtreeCursors[i].destroy();
                }
            }
            btreeCursors = null;
            buddyBtreeCursors = null;
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
