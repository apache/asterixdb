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

package org.apache.hyracks.storage.am.btree.impls;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeFrame;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class DiskBTree extends BTree {

    public DiskBTree(IBufferCache bufferCache, IPageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            IBinaryComparatorFactory[] cmpFactories, int fieldCount, FileReference file) {
        super(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
    }

    private void diskOrderScan(ITreeIndexCursor icursor, BTreeOpContext ctx) throws HyracksDataException {
        TreeIndexDiskOrderScanCursor cursor = (TreeIndexDiskOrderScanCursor) icursor;
        ctx.reset();
        RangePredicate diskOrderScanPred = new RangePredicate(null, null, true, true, ctx.getCmp(), ctx.getCmp());
        int maxPageId = freePageManager.getMaxPageId(ctx.getMetaFrame());
        int currentPageId = bulkloadLeafStart;
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), currentPageId), false);
        try {
            cursor.setBufferCache(bufferCache);
            cursor.setFileId(getFileId());
            cursor.setCurrentPageId(currentPageId);
            cursor.setMaxPageId(maxPageId);
            ctx.getCursorInitialState().setPage(page);
            ctx.getCursorInitialState().setSearchOperationCallback(ctx.getSearchCallback());
            ctx.getCursorInitialState().setOriginialKeyComparator(ctx.getCmp());
            cursor.open(ctx.getCursorInitialState(), diskOrderScanPred);
        } catch (Exception e) {
            bufferCache.unpin(page);
            throw HyracksDataException.create(e);
        }
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate searchPred, BTreeOpContext ctx)
            throws HyracksDataException {
        ctx.reset();
        ctx.setPred((RangePredicate) searchPred);
        ctx.setCursor(cursor);
        // simple index scan
        if (ctx.getPred().getLowKeyComparator() == null) {
            ctx.getPred().setLowKeyComparator(ctx.getCmp());
        }
        if (ctx.getPred().getHighKeyComparator() == null) {
            ctx.getPred().setHighKeyComparator(ctx.getCmp());
        }
        cursor.setBufferCache(bufferCache);
        cursor.setFileId(getFileId());

        DiskBTreeRangeSearchCursor diskCursor = (DiskBTreeRangeSearchCursor) cursor;

        if (diskCursor.numSearchPages() == 0) {
            // we have to search from root to leaf
            ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), rootPage), false);
            diskCursor.addSearchPage(rootPage);
            searchDown(rootNode, rootPage, ctx, diskCursor);
        } else {
            // we first check whether the leaf page matches because page may be shifted during cursor.hasNext
            if (ctx.getLeafFrame().getPage() != diskCursor.getPage()) {
                ctx.getLeafFrame().setPage(diskCursor.getPage());
                ctx.getCursorInitialState().setPage(diskCursor.getPage());
                ctx.getCursorInitialState().setPageId(diskCursor.getPageId());
            }

            if (fitInPage(ctx.getPred().getLowKey(), ctx.getPred().getLowKeyComparator(), ctx.getLeafFrame())) {
                // the input still falls into the previous search leaf
                diskCursor.open(ctx.getCursorInitialState(), searchPred);
            } else {
                // unpin the previous leaf page
                bufferCache.unpin(ctx.getLeafFrame().getPage());
                diskCursor.removeLastSearchPage();

                ICachedPage page = searchUp(ctx, diskCursor);
                int pageId = diskCursor.getLastSearchPage();

                searchDown(page, pageId, ctx, diskCursor);
            }
        }
    }

    private ICachedPage searchUp(BTreeOpContext ctx, DiskBTreeRangeSearchCursor cursor) throws HyracksDataException {
        int index = cursor.numSearchPages() - 1;
        // no need to check root page
        for (; index >= 0; index--) {
            int pageId = cursor.getLastSearchPage();
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
            ctx.getInteriorFrame().setPage(page);
            if (index == 0 || fitInPage(ctx.getPred().getLowKey(), ctx.getPred().getLowKeyComparator(),
                    ctx.getInteriorFrame())) {
                // we've found the right page
                return page;
            } else {
                // unpin the current page
                bufferCache.unpin(page);
                cursor.removeLastSearchPage();
            }
        }

        // if no page is available (which is the case for single-level BTree)
        // we simply return the root page
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), rootPage), false);
        cursor.addSearchPage(rootPage);
        return page;
    }

    private boolean fitInPage(ITupleReference key, MultiComparator comparator, IBTreeFrame frame)
            throws HyracksDataException {
        ITupleReference rightmostTuple = frame.getRightmostTuple();
        int cmp = comparator.compare(key, rightmostTuple);
        if (cmp > 0) {
            return false;
        }
        ITupleReference leftmostTuple = frame.getLeftmostTuple();
        return comparator.compare(key, leftmostTuple) >= 0;
    }

    private void searchDown(ICachedPage page, int pageId, BTreeOpContext ctx, DiskBTreeRangeSearchCursor cursor)
            throws HyracksDataException {
        ICachedPage currentPage = page;
        ctx.getInteriorFrame().setPage(currentPage);

        try {
            int childPageId = pageId;
            while (!ctx.getInteriorFrame().isLeaf()) {
                // walk down the tree until we find the leaf
                childPageId = ctx.getInteriorFrame().getChildPageId(ctx.getPred());

                // save the child page tuple index
                cursor.addSearchPage(childPageId);
                bufferCache.unpin(currentPage);
                currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), childPageId), false);
                ctx.getInteriorFrame().setPage(currentPage);
            }

            ctx.getCursorInitialState().setSearchOperationCallback(ctx.getSearchCallback());
            ctx.getCursorInitialState().setOriginialKeyComparator(ctx.getCmp());
            ctx.getCursorInitialState().setPage(currentPage);
            ctx.getCursorInitialState().setPageId(childPageId);
            ctx.getLeafFrame().setPage(currentPage);
            cursor.open(ctx.getCursorInitialState(), ctx.getPred());
        } catch (HyracksDataException e) {
            if (!ctx.isExceptionHandled() && currentPage != null) {
                bufferCache.unpin(currentPage);
                ctx.setExceptionHandled(true);
            }
            throw e;
        } catch (Exception e) {
            if (currentPage != null) {
                bufferCache.unpin(currentPage);
            }
            HyracksDataException wrappedException = HyracksDataException.create(e);
            ctx.setExceptionHandled(true);
            throw wrappedException;
        }
    }

    @Override
    public BTreeAccessor createAccessor(IIndexAccessParameters iap) {
        return new DiskBTreeAccessor(this, iap);
    }

    public class DiskBTreeAccessor extends BTreeAccessor {

        public DiskBTreeAccessor(DiskBTree btree, IIndexAccessParameters iap) {
            super(btree, iap);
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException("Insert is not supported by DiskBTree. ");
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException("Update is not supported by DiskBTree. ");
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException("Delete is not supported by DiskBTree. ");
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException("Upsert is not supported by DiskBTree. ");
        }

        @Override
        public DiskBTreeRangeSearchCursor createSearchCursor(boolean exclusive) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new DiskBTreeRangeSearchCursor(leafFrame, exclusive, (IIndexCursorStats) iap.getParameters()
                    .getOrDefault(HyracksConstants.INDEX_CURSOR_STATS, NoOpIndexCursorStats.INSTANCE));
        }

        @Override
        public BTreeRangeSearchCursor createPointCursor(boolean exclusive) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new DiskBTreePointSearchCursor(leafFrame, exclusive);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            ctx.setOperation(IndexOperation.SEARCH);
            ((DiskBTree) btree).search((ITreeIndexCursor) cursor, searchPred, ctx);
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new DiskBTreeDiskScanCursor(leafFrame);
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DISKORDERSCAN);
            ((DiskBTree) btree).diskOrderScan(cursor, ctx);
        }
    }

    private class DiskBTreeDiskScanCursor extends TreeIndexDiskOrderScanCursor {

        public DiskBTreeDiskScanCursor(ITreeIndexFrame frame) {
            super(frame);
        }

        @Override
        protected void releasePage() throws HyracksDataException {
            if (page != null) {
                bufferCache.unpin(page);
                page = null;
            }
        }

        @Override
        protected ICachedPage acquireNextPage() throws HyracksDataException {
            ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
            return nextPage;
        }

    }

}
