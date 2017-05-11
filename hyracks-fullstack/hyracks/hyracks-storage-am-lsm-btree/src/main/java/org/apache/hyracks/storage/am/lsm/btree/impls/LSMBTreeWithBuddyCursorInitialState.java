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

import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class LSMBTreeWithBuddyCursorInitialState implements ICursorInitialState {
    private final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory btreeLeafFrameFactory;
    private final ITreeIndexFrameFactory buddyBtreeLeafFrameFactory;
    private MultiComparator btreeCmp;
    private MultiComparator buddyBtreeCmp;
    private final ILSMHarness lsmHarness;

    private ISearchOperationCallback searchCallback;
    private List<ILSMComponent> operationalComponents;

    public LSMBTreeWithBuddyCursorInitialState(ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ITreeIndexFrameFactory buddyBtreeLeafFrameFactory,
            ILSMHarness lsmHarness, MultiComparator btreeCmp, MultiComparator buddyBtreeCmp,
            ISearchOperationCallback searchCallback, List<ILSMComponent> operationalComponents) {
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.buddyBtreeLeafFrameFactory = buddyBtreeLeafFrameFactory;
        this.btreeCmp = btreeCmp;
        this.buddyBtreeCmp = buddyBtreeCmp;
        this.lsmHarness = lsmHarness;
        this.searchCallback = searchCallback;
        this.operationalComponents = operationalComponents;
    }

    public ITreeIndexFrameFactory getBTreeInteriorFrameFactory() {
        return btreeInteriorFrameFactory;
    }

    public ITreeIndexFrameFactory getBTreeLeafFrameFactory() {
        return btreeLeafFrameFactory;
    }

    public ITreeIndexFrameFactory getBuddyBTreeLeafFrameFactory() {
        return buddyBtreeLeafFrameFactory;
    }

    public MultiComparator getBTreeCmp() {
        return btreeCmp;
    }

    public MultiComparator getBuddyBTreeCmp() {
        return buddyBtreeCmp;
    }

    public List<ILSMComponent> getOperationalComponents() {
        return operationalComponents;
    }

    public ILSMHarness getLSMHarness() {
        return lsmHarness;
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        this.searchCallback = searchCallback;
    }

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return btreeCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.btreeCmp = originalCmp;
    }

    public void setOperationalComponents(List<ILSMComponent> operationalComponents) {
        this.operationalComponents = operationalComponents;
    }
}
