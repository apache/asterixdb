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

package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import java.util.List;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMBTreeCursorInitialState implements ICursorInitialState {

    private final ITreeIndexFrameFactory leafFrameFactory;
    private MultiComparator cmp;
    private final MultiComparator bloomFilterCmp;
    private final ILSMHarness lsmHarness;

    private final ISearchPredicate predicate;
    private ISearchOperationCallback searchCallback;

    private final List<ILSMComponent> operationalComponents;

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
}