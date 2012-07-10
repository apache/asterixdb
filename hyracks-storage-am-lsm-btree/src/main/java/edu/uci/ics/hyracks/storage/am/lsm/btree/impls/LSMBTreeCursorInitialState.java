/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMBTreeCursorInitialState implements ICursorInitialState {

    private final int numBTrees;
    private final ITreeIndexFrameFactory leafFrameFactory;
    private MultiComparator cmp;
    private final boolean includeMemComponent;
    private final AtomicInteger searcherfRefCount;
    private final LSMHarness lsmHarness;

    private ISearchOperationCallback searchCallback;

    public LSMBTreeCursorInitialState(int numBTrees, ITreeIndexFrameFactory leafFrameFactory, MultiComparator cmp,
            boolean includeMemComponent, AtomicInteger searcherfRefCount, LSMHarness lsmHarness,
            ISearchOperationCallback searchCallback) {
        this.numBTrees = numBTrees;
        this.leafFrameFactory = leafFrameFactory;
        this.cmp = cmp;
        this.includeMemComponent = includeMemComponent;
        this.searcherfRefCount = searcherfRefCount;
        this.lsmHarness = lsmHarness;
        this.searchCallback = searchCallback;
    }

    public int getNumBTrees() {
        return numBTrees;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public MultiComparator getCmp() {
        return cmp;
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    public AtomicInteger getSearcherRefCount() {
        return searcherfRefCount;
    }

    public boolean getIncludeMemComponent() {
        return includeMemComponent;
    }

    public LSMHarness getLSMHarness() {
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

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return cmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.cmp = originalCmp;
    }
}
