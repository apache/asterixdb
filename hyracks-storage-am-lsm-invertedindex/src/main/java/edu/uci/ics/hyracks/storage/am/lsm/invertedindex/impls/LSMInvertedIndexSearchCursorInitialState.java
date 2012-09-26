/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMInvertedIndexSearchCursorInitialState implements ICursorInitialState {

    private final boolean includeMemComponent;
    private final AtomicInteger searcherfRefCount;
    private final LSMHarness lsmHarness;
    private final List<IIndexAccessor> indexAccessors;
    private final List<IIndexAccessor> deletedKeysBTreeAccessors;
    private final LSMInvertedIndexOpContext ctx;
    private ISearchOperationCallback searchCallback;
    private MultiComparator originalCmp;
    private final MultiComparator keyCmp;
    private final PermutingTupleReference keysOnlyTuple;

    public LSMInvertedIndexSearchCursorInitialState(final MultiComparator keyCmp,
            PermutingTupleReference keysOnlyTuple, List<IIndexAccessor> indexAccessors,
            List<IIndexAccessor> deletedKeysBTreeAccessors, IIndexOperationContext ctx, boolean includeMemComponent,
            AtomicInteger searcherfRefCount, LSMHarness lsmHarness) {
        this.keyCmp = keyCmp;
        this.keysOnlyTuple = keysOnlyTuple;
        this.indexAccessors = indexAccessors;
        this.deletedKeysBTreeAccessors = deletedKeysBTreeAccessors;
        this.includeMemComponent = includeMemComponent;
        this.searcherfRefCount = searcherfRefCount;
        this.lsmHarness = lsmHarness;
        this.ctx = (LSMInvertedIndexOpContext) ctx;
        this.searchCallback = this.ctx.searchCallback;
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    public List<IIndexAccessor> getIndexAccessors() {
        return indexAccessors;
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

    public IIndexOperationContext getOpContext() {
        return ctx;
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
        return originalCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.originalCmp = originalCmp;
    }
    
    public MultiComparator getKeyComparator() {
        return keyCmp;
    }
    
    public List<IIndexAccessor> getDeletedKeysBTreeAccessors() {
        return deletedKeysBTreeAccessors;
    }
    
    public PermutingTupleReference getKeysOnlyTuple() {
        return keysOnlyTuple;
    }
}
