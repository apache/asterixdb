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

import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;

public final class LSMBTreeOpContext extends AbstractLSMIndexOperationContext {

    public ITreeIndexFrameFactory insertLeafFrameFactory;
    public ITreeIndexFrameFactory deleteLeafFrameFactory;
    public IBTreeLeafFrame insertLeafFrame;
    public IBTreeLeafFrame deleteLeafFrame;
    public final BTree[] mutableBTrees;
    public BTree.BTreeAccessor[] mutableBTreeAccessors;
    public BTreeOpContext[] mutableBTreeOpCtxs;
    public BTree.BTreeAccessor currentMutableBTreeAccessor;
    public BTreeOpContext currentMutableBTreeOpCtx;
    public IndexOperation op;
    public final MultiComparator cmp;
    public final MultiComparator bloomFilterCmp;
    public IModificationOperationCallback modificationCallback;
    public ISearchOperationCallback searchCallback;
    private final List<ILSMComponent> componentHolder;
    private final List<ILSMDiskComponent> componentsToBeMerged;
    private final List<ILSMDiskComponent> componentsToBeReplicated;
    public final PermutingTupleReference indexTuple;
    public final MultiComparator filterCmp;
    public final PermutingTupleReference filterTuple;
    public ISearchPredicate searchPredicate;
    public BTreeRangeSearchCursor memCursor;
    public LSMBTreeCursorInitialState searchInitialState;
    public LSMBTreePointSearchCursor insertSearchCursor;

    public LSMBTreeOpContext(List<ILSMMemoryComponent> mutableComponents, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback, int numBloomFilterKeyFields, int[] btreeFields,
            int[] filterFields, ILSMHarness lsmHarness) {
        LSMBTreeMemoryComponent c = (LSMBTreeMemoryComponent) mutableComponents.get(0);
        IBinaryComparatorFactory cmpFactories[] = c.getBTree().getComparatorFactories();
        if (cmpFactories[0] != null) {
            this.cmp = MultiComparator.create(c.getBTree().getComparatorFactories());
        } else {
            this.cmp = null;
        }

        bloomFilterCmp = numBloomFilterKeyFields == 0 ? null
                : MultiComparator.create(c.getBTree().getComparatorFactories(), 0, numBloomFilterKeyFields);

        mutableBTrees = new BTree[mutableComponents.size()];
        mutableBTreeAccessors = new BTree.BTreeAccessor[mutableComponents.size()];
        mutableBTreeOpCtxs = new BTreeOpContext[mutableComponents.size()];
        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) mutableComponents.get(i);
            mutableBTrees[i] = mutableComponent.getBTree();
            mutableBTreeAccessors[i] = (BTree.BTreeAccessor) mutableBTrees[i].createAccessor(modificationCallback,
                    NoOpOperationCallback.INSTANCE);
            mutableBTreeOpCtxs[i] = mutableBTreeAccessors[i].getOpContext();
        }

        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.insertLeafFrame = (IBTreeLeafFrame) insertLeafFrameFactory.createFrame();
        this.deleteLeafFrame = (IBTreeLeafFrame) deleteLeafFrameFactory.createFrame();
        if (insertLeafFrame != null && this.cmp != null) {
            insertLeafFrame.setMultiComparator(cmp);
        }
        if (deleteLeafFrame != null && this.cmp != null) {
            deleteLeafFrame.setMultiComparator(cmp);
        }
        this.componentHolder = new LinkedList<>();
        this.componentsToBeMerged = new LinkedList<>();
        this.componentsToBeReplicated = new LinkedList<>();
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;

        if (filterFields != null) {
            indexTuple = new PermutingTupleReference(btreeFields);
            filterCmp = MultiComparator.create(c.getLSMComponentFilter().getFilterCmpFactories());
            filterTuple = new PermutingTupleReference(filterFields);
        } else {
            indexTuple = null;
            filterCmp = null;
            filterTuple = null;
        }
        searchPredicate = new RangePredicate(null, null, true, true, cmp, cmp);
        if (insertLeafFrame != null) {
            memCursor = new BTreeRangeSearchCursor(insertLeafFrame, false);
        }

        searchInitialState = new LSMBTreeCursorInitialState(insertLeafFrameFactory, cmp, bloomFilterCmp, lsmHarness,
                null, searchCallback, null);
        insertSearchCursor = new LSMBTreePointSearchCursor(this);
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    public void setInsertMode() {
        currentMutableBTreeOpCtx.leafFrame = insertLeafFrame;
        currentMutableBTreeOpCtx.leafFrameFactory = insertLeafFrameFactory;
    }

    public void setDeleteMode() {
        currentMutableBTreeOpCtx.leafFrame = deleteLeafFrame;
        currentMutableBTreeOpCtx.leafFrameFactory = deleteLeafFrameFactory;
    }

    @Override
    public void reset() {
        super.reset();
        componentHolder.clear();
        componentsToBeMerged.clear();
        componentsToBeReplicated.clear();
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    @Override
    public List<ILSMComponent> getComponentHolder() {
        return componentHolder;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public IModificationOperationCallback getModificationCallback() {
        return modificationCallback;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        currentMutableBTreeAccessor = mutableBTreeAccessors[currentMutableComponentId];
        currentMutableBTreeOpCtx = mutableBTreeOpCtxs[currentMutableComponentId];
        switch (op) {
            case SEARCH:
                break;
            case DISKORDERSCAN:
            case UPDATE:
                // Attention: It is important to leave the leafFrame and
                // leafFrameFactory of the mutableBTree as is when doing an update.
                // Update will only be set if a previous attempt to delete or
                // insert failed, so we must preserve the semantics of the
                // previously requested operation.
                break;
            case UPSERT:
            case INSERT:
                setInsertMode();
                break;
            case PHYSICALDELETE:
            case DELETE:
                setDeleteMode();
                break;
        }
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeMerged() {
        return componentsToBeMerged;
    }

    @Override
    public void setSearchPredicate(ISearchPredicate searchPredicate) {
        this.searchPredicate = searchPredicate;
    }

    @Override
    public ISearchPredicate getSearchPredicate() {
        return searchPredicate;
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeReplicated() {
        return componentsToBeReplicated;
    }
}
