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
import org.apache.hyracks.storage.common.buffercache.BufferCache;
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
        if (ctx.getPred().getLowKeyComparator() == null) {
            ctx.getPred().setLowKeyComparator(ctx.getCmp());
        }
        if (ctx.getPred().getHighKeyComparator() == null) {
            ctx.getPred().setHighKeyComparator(ctx.getCmp());
        }
        cursor.setBufferCache(bufferCache);
        cursor.setFileId(getFileId());

        if (cursor instanceof DiskBTreePointSearchCursor) {
            DiskBTreePointSearchCursor pointCursor = (DiskBTreePointSearchCursor) cursor;
            int lastPageId = pointCursor.getLastPageId();
            if (lastPageId != BufferCache.INVALID_PAGEID) {
                // check whether the last leaf page contains this key
                ICachedPage lastPage =
                        bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), lastPageId), false);
                ctx.getLeafFrame().setPage(lastPage);
                if (fitInPage(ctx.getPred().getLowKey(), ctx.getPred().getLowKeyComparator(), ctx.getLeafFrame())) {
                    // use this page
                    ctx.getCursorInitialState().setPage(lastPage);
                    ctx.getCursorInitialState().setPageId(lastPageId);
                    pointCursor.open(ctx.getCursorInitialState(), searchPred);
                    return;
                } else {
                    // release the last page and clear the states of this cursor
                    // then retry the search from root to leaf
                    bufferCache.unpin(lastPage);
                    pointCursor.clearSearchState();
                }
            }
        }
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), rootPage), false);
        searchDown(rootNode, rootPage, ctx, cursor);
    }

    private boolean fitInPage(ITupleReference key, MultiComparator comparator, IBTreeFrame frame)
            throws HyracksDataException {
        // assume that search keys are sorted (non-decreasing)
        ITupleReference rightmostTuple = frame.getRightmostTuple();
        int cmp = comparator.compare(key, rightmostTuple);
        return cmp <= 0;
    }

    private void searchDown(ICachedPage page, int pageId, BTreeOpContext ctx, ITreeIndexCursor cursor)
            throws HyracksDataException {
        ICachedPage currentPage = page;
        ctx.getInteriorFrame().setPage(currentPage);
        try {
            int childPageId = pageId;
            while (!ctx.getInteriorFrame().isLeaf()) {
                // walk down the tree until we find the leaf
                childPageId = ctx.getInteriorFrame().getChildPageId(ctx.getPred());
                bufferCache.unpin(currentPage);
                pageId = childPageId;

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
        public BTreeRangeSearchCursor createPointCursor(boolean exclusive, boolean stateful) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new DiskBTreePointSearchCursor(leafFrame, exclusive, stateful);
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
