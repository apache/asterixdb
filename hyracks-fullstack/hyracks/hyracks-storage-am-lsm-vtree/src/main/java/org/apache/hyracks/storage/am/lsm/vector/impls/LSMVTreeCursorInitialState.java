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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.List;

import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

/**
 * Cursor initial state for LSM Vector Clustering Tree operations.
 * 
 * This class maintains the initial state and context required for cursors operating on
 * LSM Vector Clustering Trees, including frame factories for different node types,
 * comparators, and operational components.
 */
public class LSMVTreeCursorInitialState implements ICursorInitialState {

    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final ITreeIndexFrameFactory leafFrameFactory;
    private final ITreeIndexFrameFactory metadataFrameFactory;
    private final ITreeIndexFrameFactory dataFrameFactory;
    private final MultiComparator cmp;
    private final ILSMHarness lsmHarness;
    private ISearchPredicate predicate;
    private ISearchOperationCallback searchCallback;
    private List<ILSMComponent> operationalComponents;
    private boolean isDiskComponentScan;

    public LSMVTreeCursorInitialState(ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, ITreeIndexFrameFactory metadataFrameFactory,
            ITreeIndexFrameFactory dataFrameFactory, MultiComparator cmp, ILSMHarness lsmHarness,
            ISearchPredicate predicate, ISearchOperationCallback searchCallback,
            List<ILSMComponent> operationalComponents) {
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.metadataFrameFactory = metadataFrameFactory;
        this.dataFrameFactory = dataFrameFactory;
        this.cmp = cmp;
        this.lsmHarness = lsmHarness;
        this.predicate = predicate;
        this.searchCallback = searchCallback;
        this.operationalComponents = operationalComponents;
        this.isDiskComponentScan = false;
    }

    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public ITreeIndexFrameFactory getMetadataFrameFactory() {
        return metadataFrameFactory;
    }

    public ITreeIndexFrameFactory getDataFrameFactory() {
        return dataFrameFactory;
    }

    public MultiComparator getOriginalKeyComparator() {
        return cmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        // Intentionally inert: the comparator is supplied via the constructor (getOriginalKeyComparator);
        // the VTree cursors never re-set it through the ICursorInitialState interface.
    }

    public ILSMHarness getLSMHarness() {
        return lsmHarness;
    }

    public ISearchPredicate getSearchPredicate() {
        return predicate;
    }

    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        // Intentionally inert: the callback is supplied via the constructor (getSearchOperationCallback);
        // the VTree cursors never re-set it through the ICursorInitialState interface.
    }

    public List<ILSMComponent> getOperationalComponents() {
        return operationalComponents;
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
        // No-op for LSM trees
    }

    // Make the cursor initial state re-usable
    public void reset(ISearchPredicate predicate, List<ILSMComponent> operationalComponents) {
        this.isDiskComponentScan = false;
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
