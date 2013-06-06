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
package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class BTreeCursorInitialState implements ICursorInitialState {

    // This is only used by the LSM-RTree
    private int pageId;
    private ICachedPage page;
    private ISearchOperationCallback searchCallback;
    private MultiComparator originalKeyCmp;
    private final IIndexAccessor accessor;

    public BTreeCursorInitialState(ICachedPage page, ISearchOperationCallback searchCallback, IIndexAccessor accessor) {
        this.page = page;
        this.searchCallback = searchCallback;
        this.accessor = accessor;
    }
    
    public IIndexAccessor getAccessor() {
        return accessor;
    }

    public ICachedPage getPage() {
        return page;
    }

    public void setPage(ICachedPage page) {
        this.page = page;
    }

    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
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
        return originalKeyCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.originalKeyCmp = originalCmp;
    }
}
