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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
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
    private LSMRTreeCursorInitialState searchInitialState;

    public LSMRTreeOpContext(List<ILSMMemoryComponent> mutableComponents, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback,
            int[] rtreeFields, int[] filterFields, ILSMHarness lsmHarness, int[] comparatorFields,
            IBinaryComparatorFactory[] linearizerArray, IBinaryComparatorFactory[] filterComparatorFactories) {
        super(rtreeFields, filterFields, filterComparatorFactories, searchCallback, modificationCallback);
        mutableRTreeAccessors = new RTree.RTreeAccessor[mutableComponents.size()];
        mutableBTreeAccessors = new BTree.BTreeAccessor[mutableComponents.size()];
        rtreeOpContexts = new RTreeOpContext[mutableComponents.size()];
        btreeOpContexts = new BTreeOpContext[mutableComponents.size()];
        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) mutableComponents.get(i);
            if (allFields != null) {
                mutableRTreeAccessors[i] = (RTree.RTreeAccessor) mutableComponent.getRTree()
                        .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE, allFields);
            } else {
                mutableRTreeAccessors[i] = (RTree.RTreeAccessor) mutableComponent.getRTree()
                        .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            }
            mutableBTreeAccessors[i] = (BTree.BTreeAccessor) mutableComponent.getBTree()
                    .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

            rtreeOpContexts[i] = mutableRTreeAccessors[i].getOpContext();
            btreeOpContexts[i] = mutableBTreeAccessors[i].getOpContext();
        }
        currentRTreeOpContext = rtreeOpContexts[0];
        currentBTreeOpContext = btreeOpContexts[0];
        searchInitialState = new LSMRTreeCursorInitialState(rtreeLeafFrameFactory, rtreeInteriorFrameFactory,
                btreeLeafFrameFactory, getBTreeMultiComparator(), lsmHarness, comparatorFields, linearizerArray,
                searchCallback, componentHolder);
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

    public MultiComparator getBTreeMultiComparator() {
        return currentBTreeOpContext.getCmp();
    }

    @Override
    public void setSearchPredicate(ISearchPredicate searchPredicate) {
        this.searchPredicate = searchPredicate;
    }

    @Override
    public ISearchPredicate getSearchPredicate() {
        return searchPredicate;
    }

    public LSMRTreeCursorInitialState getSearchInitialState() {
        return searchInitialState;
    }

    public RTree.RTreeAccessor getCurrentMutableRTreeAccessor() {
        return currentMutableRTreeAccessor;
    }

    public BTree.BTreeAccessor getCurrentMutableBTreeAccessor() {
        return currentMutableBTreeAccessor;
    }

    public RTreeOpContext getCurrentRTreeOpContext() {
        return currentRTreeOpContext;
    }
}