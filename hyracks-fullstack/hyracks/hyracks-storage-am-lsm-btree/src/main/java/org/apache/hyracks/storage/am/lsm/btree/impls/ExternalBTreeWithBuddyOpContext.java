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
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;

public class ExternalBTreeWithBuddyOpContext extends AbstractLSMIndexOperationContext {
    private IndexOperation op;
    private MultiComparator bTreeCmp;
    private MultiComparator buddyBTreeCmp;
    public final List<ILSMComponent> componentHolder;
    private final List<ILSMDiskComponent> componentsToBeMerged;
    private final List<ILSMDiskComponent> componentsToBeReplicated;
    public final ISearchOperationCallback searchCallback;
    private final int targetIndexVersion;
    public ISearchPredicate searchPredicate;
    public LSMBTreeWithBuddyCursorInitialState searchInitialState;

    public ExternalBTreeWithBuddyOpContext(IBinaryComparatorFactory[] btreeCmpFactories,
            IBinaryComparatorFactory[] buddyBtreeCmpFactories, ISearchOperationCallback searchCallback,
            int targetIndexVersion, ILSMHarness lsmHarness, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ITreeIndexFrameFactory buddyBtreeLeafFrameFactory) {
        this.componentHolder = new LinkedList<>();
        this.componentsToBeMerged = new LinkedList<>();
        this.componentsToBeReplicated = new LinkedList<>();
        this.searchCallback = searchCallback;
        this.targetIndexVersion = targetIndexVersion;
        this.bTreeCmp = MultiComparator.create(btreeCmpFactories);
        this.buddyBTreeCmp = MultiComparator.create(buddyBtreeCmpFactories);
        searchInitialState = new LSMBTreeWithBuddyCursorInitialState(btreeInteriorFrameFactory, btreeLeafFrameFactory,
                buddyBtreeLeafFrameFactory, lsmHarness, MultiComparator.create(btreeCmpFactories),
                MultiComparator.create(buddyBtreeCmpFactories), NoOpOperationCallback.INSTANCE, null);
    }

    @Override
    public void setOperation(IndexOperation newOp) {
        reset();
        this.op = newOp;
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        // Do nothing. this should never be called for disk only indexes
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

    public MultiComparator getBTreeMultiComparator() {
        return bTreeCmp;
    }

    public MultiComparator getBuddyBTreeMultiComparator() {
        return buddyBTreeCmp;
    }

    @Override
    public List<ILSMComponent> getComponentHolder() {
        return componentHolder;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    // This should never be needed for disk only indexes
    @Override
    public IModificationOperationCallback getModificationCallback() {
        return null;
    }

    @Override
    public List<ILSMDiskComponent> getComponentsToBeMerged() {
        return componentsToBeMerged;
    }

    public int getTargetIndexVersion() {
        return targetIndexVersion;
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
