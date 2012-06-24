package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class BTreeCursorInitialState implements ICursorInitialState {

    private ICachedPage page;
    
    // This is only used by the LSM-RTree
    private int pageId;

    private ISearchOperationCallback searchCallback;

    public BTreeCursorInitialState(ICachedPage page, ISearchOperationCallback searchCallback) {
        this.page = page;
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
}
