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

import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeOpContext;

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
    public final IModificationOperationCallback modificationCallback;
    public final ISearchOperationCallback searchCallback;

    public LSMRTreeOpContext(List<ILSMComponent> mutableComponents, IRTreeLeafFrame rtreeLeafFrame,
            IRTreeInteriorFrame rtreeInteriorFrame, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        mutableRTreeAccessors = new RTree.RTreeAccessor[mutableComponents.size()];
        mutableBTreeAccessors = new BTree.BTreeAccessor[mutableComponents.size()];
        rtreeOpContexts = new RTreeOpContext[mutableComponents.size()];
        btreeOpContexts = new BTreeOpContext[mutableComponents.size()];

        for (int i = 0; i < mutableComponents.size(); i++) {
            LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) mutableComponents.get(i);
            mutableRTreeAccessors[i] = (RTree.RTreeAccessor) mutableComponent.getRTree().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            mutableBTreeAccessors[i] = (BTree.BTreeAccessor) mutableComponent.getBTree().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

            rtreeOpContexts[i] = mutableRTreeAccessors[i].getOpContext();
            btreeOpContexts[i] = mutableBTreeAccessors[i].getOpContext();
        }

        assert mutableComponents.size() > 0;
        currentRTreeOpContext = rtreeOpContexts[0];
        currentBTreeOpContext = btreeOpContexts[0];
        this.componentHolder = new LinkedList<ILSMComponent>();
        this.componentsToBeMerged = new LinkedList<ILSMComponent>();
        this.modificationCallback = modificationCallback;
        this.searchCallback = searchCallback;
    }

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
}