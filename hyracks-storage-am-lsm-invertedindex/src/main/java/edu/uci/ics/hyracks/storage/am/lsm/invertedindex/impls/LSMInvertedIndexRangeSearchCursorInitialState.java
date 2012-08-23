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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class LSMInvertedIndexRangeSearchCursorInitialState implements ICursorInitialState {

    private final MultiComparator tokensAndKeyCmp;
    private final MultiComparator keyCmp;
    private final AtomicInteger searcherRefCount;
    private final LSMHarness lsmHarness;

    private final ArrayList<IIndexAccessor> indexAccessors;
    private final ArrayList<IIndexAccessor> deletedKeysBTreeAccessors;
    private final ISearchPredicate predicate;
    
    private final boolean includeMemComponent;

    public LSMInvertedIndexRangeSearchCursorInitialState(MultiComparator tokensAndKeyCmp, MultiComparator keyCmp,
            boolean includeMemComponent, AtomicInteger searcherRefCount, LSMHarness lsmHarness,
            ArrayList<IIndexAccessor> indexAccessors, ArrayList<IIndexAccessor> deletedKeysBTreeAccessors,
            ISearchPredicate predicate) {
        this.tokensAndKeyCmp = tokensAndKeyCmp;
        this.keyCmp = keyCmp;
        this.searcherRefCount = searcherRefCount;
        this.lsmHarness = lsmHarness;
        this.indexAccessors = indexAccessors;
        this.deletedKeysBTreeAccessors = deletedKeysBTreeAccessors;
        this.predicate = predicate;
        this.includeMemComponent = includeMemComponent;
    }

    public int getNumComponents() {
        return indexAccessors.size();
    }

    @Override
    public ICachedPage getPage() {
        return null;
    }

    @Override
    public void setPage(ICachedPage page) {
    }

    public AtomicInteger getSearcherRefCount() {
        return searcherRefCount;
    }

    public LSMHarness getLSMHarness() {
        return lsmHarness;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return null;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        // Do nothing.
    }

    public ArrayList<IIndexAccessor> getIndexAccessors() {
        return indexAccessors;
    }

    public ArrayList<IIndexAccessor> getDeletedKeysBTreeAccessors() {
        return deletedKeysBTreeAccessors;
    }
    
    public ISearchPredicate getSearchPredicate() {
        return predicate;
    }

    public MultiComparator getKeyComparator() {
        return keyCmp;
    }
    
    @Override
    public MultiComparator getOriginalKeyComparator() {
        return tokensAndKeyCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        // Do nothing.
    }
    
    public boolean getIncludeMemComponent() {
        return includeMemComponent;
    }
}
