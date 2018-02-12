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
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

public class LSMBTreeCursorInitialState implements ICursorInitialState {

    private final ITreeIndexFrameFactory leafFrameFactory;
    private MultiComparator cmp;
    private final MultiComparator bloomFilterCmp;
    private final ILSMHarness lsmHarness;
    private ISearchPredicate predicate;
    private ISearchOperationCallback searchCallback;
    private List<ILSMComponent> operationalComponents;
    private boolean isDiskComponentScan;

    public LSMBTreeCursorInitialState(ITreeIndexFrameFactory leafFrameFactory, MultiComparator cmp,
            MultiComparator bloomFilterCmp, ILSMHarness lsmHarness, ISearchPredicate predicate,
            ISearchOperationCallback searchCallback, List<ILSMComponent> operationalComponents) {
        this.leafFrameFactory = leafFrameFactory;
        this.cmp = cmp;
        this.bloomFilterCmp = bloomFilterCmp;
        this.lsmHarness = lsmHarness;
        this.searchCallback = searchCallback;
        this.predicate = predicate;
        this.operationalComponents = operationalComponents;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    public ILSMHarness getLSMHarness() {
        return lsmHarness;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        this.searchCallback = searchCallback;
    }

    public List<ILSMComponent> getOperationalComponents() {
        return operationalComponents;
    }

    public ISearchPredicate getSearchPredicate() {
        return predicate;
    }

    public MultiComparator getBloomFilterComparator() {
        return bloomFilterCmp;
    }

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return cmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.cmp = originalCmp;
    }

    // make the cursor initial state re-usable
    public void reset(ISearchPredicate predicate, List<ILSMComponent> operationalComponents) {
        isDiskComponentScan = false;
        this.predicate = predicate;
        this.operationalComponents = operationalComponents;
    }

    public void setDiskComponentScan(boolean isDiskComponentScan) {
        this.isDiskComponentScan = isDiskComponentScan;
    }

    public boolean isDiskComponentScan() {
        return isDiskComponentScan;
    }
}
