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
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTreeOpContext;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

public final class LSMRTreeOpContext extends AbstractLSMIndexOperationContext {

    private RTree.RTreeAccessor[] mutableRTreeAccessors;
    private RTree.RTreeAccessor currentMutableRTreeAccessor;
    private BTree.BTreeAccessor[] mutableBTreeAccessors;
    private BTree.BTreeAccessor currentMutableBTreeAccessor;

    private RTreeOpContext[] rtreeOpContexts;
    private BTreeOpContext[] btreeOpContexts;
    private RTreeOpContext currentRTreeOpContext;
    private BTreeOpContext currentBTreeOpContext;

    private IndexOperation op;
    private final List<ILSMComponent> componentHolder;
    private final List<ILSMDiskComponent> componentsToBeMerged;
    private final List<ILSMDiskComponent> componentsToBeReplicated;
    private IModificationOperationCallback modificationCallback;
    private ISearchOperationCallback searchCallback;
    private final PermutingTupleReference indexTuple;
    private final MultiComparator filterCmp;
    private final PermutingTupleReference filterTuple;
    private ISearchPredicate searchPredicate;
    private LSMRTreeCursorInitialState searchInitialState;

    public LSMRTreeOpContext(List<ILSMMemoryComponent> mutableComponents, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback,
            int[] rtreeFields, int[] filterFields, ILSMHarness lsmHarness, int[] comparatorFields,
            IBinaryComparatorFactory[] linearizerArray) {
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
        this.componentHolder = new LinkedList<>();
        this.componentsToBeMerged = new LinkedList<>();
        this.componentsToBeReplicated = new LinkedList<>();
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
        super.reset();
        componentHolder.clear();
        componentsToBeMerged.clear();
    }

    @Override
    public IndexOperation getOperation() {
        return op;
    }

    public MultiComparator getBTreeMultiComparator() {
        return currentBTreeOpContext.getCmp();
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

    public MultiComparator getFilterCmp() {
        return filterCmp;
    }

    public LSMRTreeCursorInitialState getSearchInitialState() {
        return searchInitialState;
    }

    public PermutingTupleReference getIndexTuple() {
        return indexTuple;
    }

    public RTree.RTreeAccessor getCurrentMutableRTreeAccessor() {
        return currentMutableRTreeAccessor;
    }

    public BTree.BTreeAccessor getCurrentMutableBTreeAccessor() {
        return currentMutableBTreeAccessor;
    }

    public PermutingTupleReference getFilterTuple() {
        return filterTuple;
    }

    public RTreeOpContext getCurrentRTreeOpContext() {
        return currentRTreeOpContext;
    }
}
