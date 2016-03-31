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

import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTreeOpContext;

public final class LSMRTreeOpContext implements ILSMIndexOperationContext {

    public RTree.RTreeAccessor[] mutableRTreeAccessors;
    public RTree.RTreeAccessor currentMutableRTreeAccessor;
    public BTree.BTreeAccessor[] mutableBTreeAccessors;
    public BTree.BTreeAccessor currentMutableBTreeAccessor;

    public RTreeOpContext[] rtreeOpContexts;
    public BTreeOpContext[] btreeOpContexts;
    public RTreeOpContext currentRTreeOpContext;
    public BTreeOpContext currentBTreeOpContext;

    private IndexOperation op;
    public final List<ILSMComponent> componentHolder;
    private final List<ILSMComponent> componentsToBeMerged;
    private final List<ILSMComponent> componentsToBeReplicated;
    private IModificationOperationCallback modificationCallback;
    private ISearchOperationCallback searchCallback;
    public final PermutingTupleReference indexTuple;
    public final MultiComparator filterCmp;
    public final PermutingTupleReference filterTuple;
    public ISearchPredicate searchPredicate;
    public LSMRTreeCursorInitialState searchInitialState;

    public LSMRTreeOpContext(List<ILSMComponent> mutableComponents, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback, int[] rtreeFields, int[] filterFields, ILSMHarness lsmHarness,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray) {
        mutableRTreeAccessors = new RTree.RTreeAccessor[mutableComponents.size()];
        mutableBTreeAccessors = new BTree.BTreeAccessor[mutableComponents.size()];
        rtreeOpContexts = new RTreeOpContext[mutableComponents.size()];
        btreeOpContexts = new BTreeOpContext[mutableComponents.size()];

        LSMRTreeMemoryComponent c = (LSMRTreeMemoryComponent) mutableComponents.get(0);

        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) mutableComponents.get(i);
            mutableRTreeAccessors[i] = (RTree.RTreeAccessor) mutableComponent.getRTree()
                    .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            mutableBTreeAccessors[i] = (BTree.BTreeAccessor) mutableComponent.getBTree()
                    .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

            rtreeOpContexts[i] = mutableRTreeAccessors[i].getOpContext();
            btreeOpContexts[i] = mutableBTreeAccessors[i].getOpContext();
        }

        assert mutableComponents.size() > 0;
        currentRTreeOpContext = rtreeOpContexts[0];
        currentBTreeOpContext = btreeOpContexts[0];
        this.componentHolder = new LinkedList<ILSMComponent>();
        this.componentsToBeMerged = new LinkedList<ILSMComponent>();
        this.componentsToBeReplicated = new LinkedList<ILSMComponent>();
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;

        if (filterFields != null) {
            indexTuple = new PermutingTupleReference(rtreeFields);
            filterCmp = MultiComparator.create(c.getLSMComponentFilter().getFilterCmpFactories());
            filterTuple = new PermutingTupleReference(filterFields);
        } else {
            indexTuple = null;
            filterCmp = null;
            filterTuple = null;
        }
        searchInitialState = new LSMRTreeCursorInitialState(rtreeLeafFrameFactory, rtreeInteriorFrameFactory,
                btreeLeafFrameFactory, getBTreeMultiComparator(), lsmHarness, comparatorFields, linearizerArray,
                searchCallback, componentHolder);
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        currentMutableRTreeAccessor = mutableRTreeAccessors[currentMutableComponentId];
        currentMutableBTreeAccessor = mutableBTreeAccessors[currentMutableComponentId];
        currentRTreeOpContext = rtreeOpContexts[currentMutableComponentId];
        currentBTreeOpContext = btreeOpContexts[currentMutableComponentId];
        if (op == IndexOperation.INSERT) {
            currentRTreeOpContext.setOperation(op);
        } else if (op == IndexOperation.DELETE) {
            currentBTreeOpContext.setOperation(IndexOperation.INSERT);
        }
    }

    @Override
    public void reset() {
        componentHolder.clear();
        componentsToBeMerged.clear();
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    public MultiComparator getBTreeMultiComparator() {
        return currentBTreeOpContext.cmp;
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
    public List<ILSMComponent> getComponentsToBeMerged() {
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
    public List<ILSMComponent> getComponentsToBeReplicated() {
        return componentsToBeReplicated;
    }
}
