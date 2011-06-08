package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class CursorInitialState implements ICursorInitialState {

    private PathList pathList;
    private int rootPage;
    private ICachedPage page; // for disk order scan

    public CursorInitialState(PathList pathList, int rootPage) {
        this.pathList = pathList;
        this.rootPage = rootPage;
    }

    public PathList getPathList() {
        return pathList;
    }

    public int getRootPage() {
        return rootPage;
    }

    public void setRootPage(int rootPage) {
        this.rootPage = rootPage;
    }

    public ICachedPage getPage() {
        return page;
    }

    public void setPage(ICachedPage page) {
        this.page = page;
    }
}
