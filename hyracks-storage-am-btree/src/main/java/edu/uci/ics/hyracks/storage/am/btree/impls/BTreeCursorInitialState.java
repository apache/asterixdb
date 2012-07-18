package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class BTreeCursorInitialState implements ICursorInitialState {

    // This is only used by the LSM-RTree
    private int pageId;
    private ICachedPage page;
    private ISearchOperationCallback searchCallback;
    private MultiComparator originalKeyCmp;

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

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return originalKeyCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.originalKeyCmp = originalCmp;
    }
}
