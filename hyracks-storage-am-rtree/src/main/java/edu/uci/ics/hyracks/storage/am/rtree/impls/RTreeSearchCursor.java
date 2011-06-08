package edu.uci.ics.hyracks.storage.am.rtree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class RTreeSearchCursor implements ITreeIndexCursor {

    private int fileId = -1;
    private ICachedPage page = null;
    private IRTreeFrame interiorFrame = null;
    private IRTreeFrame leafFrame = null;
    private IBufferCache bufferCache = null;

    private SearchPredicate pred;
    private PathList pathList;
    private int rootPage;
    ITupleReference searchKey;

    private int tupleIndex = 0;
    private int tupleIndexInc = 0;

    private MultiComparator leafCmp;
    private MultiComparator interiorCmp;

    private ITreeIndexTupleReference frameTuple;

    private int pin = 0;
    private int unpin = 0;

    public RTreeSearchCursor(IRTreeFrame interiorFrame, IRTreeFrame leafFrame) {
        this.interiorFrame = interiorFrame;
        this.leafFrame = leafFrame;
        this.frameTuple = leafFrame.createTupleReference();
    }

    @Override
    public void close() throws Exception {
        if (page != null) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
        }
        tupleIndex = 0;
        tupleIndexInc = 0;
        page = null;
        pathList.clear();
    }

    public ITupleReference getTuple() {
        return frameTuple;
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    public boolean fetchNextLeafPage() throws HyracksDataException {
        while (!pathList.isEmpty()) {
            int pageId = pathList.getLastPageId();
            int parentLsn = pathList.getLastPageLsn();
            pathList.removeLast();
            ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            pin++;
            node.acquireReadLatch();
            interiorFrame.setPage(node);
            boolean isLeaf = interiorFrame.isLeaf();
            int pageLsn = interiorFrame.getPageLsn();

            if (pageId != rootPage && parentLsn < interiorFrame.getPageNsn()) {
                // Concurrent split detected, we need to visit the right page
                int rightPage = interiorFrame.getRightPage();
                if (rightPage != -1) {
                    pathList.add(rightPage, parentLsn, -1);
                }
            }

            if (!isLeaf) {
                for (int i = 0; i < interiorFrame.getTupleCount(); i++) {
                    int childPageId = interiorFrame.getChildPageIdIfIntersect(searchKey, i, interiorCmp);
                    if (childPageId != -1) {
                        pathList.add(childPageId, pageLsn, -1);
                    }
                }
            } else {
                if (page != null) {
                    page.releaseReadLatch();
                    bufferCache.unpin(page);
                    unpin++;
                }
                page = node;
                leafFrame.setPage(page);
                tupleIndex = 0;
                return true;
            }
            node.releaseReadLatch();
            bufferCache.unpin(node);
            unpin++;
        }
        return false;
    }

    @Override
    public boolean hasNext() throws Exception {
        if (tupleIndex == leafFrame.getTupleCount()) {
            if (!fetchNextLeafPage()) {
                return false;
            }
        }

        do {
            for (int i = tupleIndex; i < leafFrame.getTupleCount(); i++) {
                if (leafFrame.intersect(searchKey, i, leafCmp)) {
                    frameTuple.resetByTupleIndex(leafFrame, i);
                    tupleIndexInc = i + 1;
                    return true;
                }
            }
        } while (fetchNextLeafPage());
        return false;
    }

    @Override
    public void next() throws Exception {
        tupleIndex = tupleIndexInc;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws Exception {
        // in case open is called multiple times without closing
        if (this.page != null) {
            this.page.releaseReadLatch();
            bufferCache.unpin(this.page);
            pathList.clear();
        }

        pathList = ((CursorInitialState) initialState).getPathList();
        rootPage = ((CursorInitialState) initialState).getRootPage();

        pred = (SearchPredicate) searchPred;
        interiorCmp = pred.getInteriorCmp();
        leafCmp = pred.getLeafCmp();
        searchKey = pred.getSearchKey();

        pathList.add(this.rootPage, -1, -1);
        frameTuple.setFieldCount(leafCmp.getFieldCount());
        tupleIndex = 0;
        fetchNextLeafPage();
    }

    @Override
    public void reset() {
        try {
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }
}