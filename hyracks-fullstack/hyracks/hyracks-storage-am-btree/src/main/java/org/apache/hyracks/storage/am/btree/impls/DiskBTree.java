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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.api.IDiskBTreeStatefulPointSearchCursor;
import org.apache.hyracks.storage.am.btree.api.ITupleAcceptor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.ILSMIndexBatchPointCursor;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
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
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

import it.unimi.dsi.fastutil.ints.IntArrayList;

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
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), currentPageId));
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

    private void search(ITreeIndexCursor cursor, ISearchPredicate searchPred, BTreeOpContext ctx,
            IBufferCacheReadContext bcOpCtx) throws HyracksDataException {
        ctx.reset();
        RangePredicate rangePredicate = (RangePredicate) searchPred;
        ctx.setPred(rangePredicate);
        ctx.setCursor(cursor);
        if (ctx.getPred().getLowKeyComparator() == null) {
            ctx.getPred().setLowKeyComparator(ctx.getCmp());
        }
        if (ctx.getPred().getHighKeyComparator() == null) {
            ctx.getPred().setHighKeyComparator(ctx.getCmp());
        }
        cursor.setBufferCache(bufferCache);
        cursor.setFileId(getFileId());

        if (cursor instanceof IDiskBTreeStatefulPointSearchCursor) {
            IDiskBTreeStatefulPointSearchCursor pointCursor = (IDiskBTreeStatefulPointSearchCursor) cursor;
            int lastPageId = pointCursor.getLastPageId();
            if (lastPageId != IBufferCache.INVALID_PAGEID) {
                if (fitInPage(ctx.getPred().getLowKey(), ctx.getPred().getLowKeyComparator(), pointCursor.getFrame())) {
                    pointCursor.setCursorToNextKey(searchPred);
                    return;
                } else {
                    // release the last page, clear the states of this cursor, and close the cursor
                    // then retry the search from root to leaf
                    cursor.close();
                }
            }
        }
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), rootPage), bcOpCtx);
        searchDown(rootNode, rootPage, ctx, cursor, bcOpCtx);
    }

    private boolean fitInPage(ITupleReference key, MultiComparator comparator, ITreeIndexFrame frame)
            throws HyracksDataException {
        // assume that search keys are sorted (non-decreasing)
        ITupleReference rightmostTuple = frame.getRightmostTuple();
        int cmp = comparator.compare(key, rightmostTuple);
        return cmp <= 0;
    }

    private void diskSampleScan(ITreeIndexCursor cursor, BTreeOpContext ctx,
            IBufferCacheReadContext bufferCacheReadContext) throws HyracksDataException {
        ctx.reset();
        ctx.setCursor(cursor);
        cursor.setBufferCache(bufferCache);
        cursor.setFileId(getFileId());
        // For sample cursors, we don't pick the first leaf here.
        // Instead, we just set up the initial state and let the cursor pick its own
        // first leaf on the first doHasNext() call using its seeded random generator.
        // This ensures fully reproducible sampling.
        ctx.getCursorInitialState().setSearchOperationCallback(ctx.getSearchCallback());
        ctx.getCursorInitialState().setOriginialKeyComparator(ctx.getCmp());
        ctx.getCursorInitialState().setPage(null); // No pre-selected page
        // No pre-selected leaf; carry the root page id through the existing pageId slot for the cursor to
        // start its own randomized descent.
        ctx.getCursorInitialState().setPageId(rootPage);
        cursor.open(ctx.getCursorInitialState(), ctx.getPred());
    }

    /**
     * Enumerates all leaf page IDs by traversing only the interior pages of the B-tree.
     * <p>
     * This performs a DFS through interior nodes. The key optimisation is at
     * <b>level-1 nodes</b> (the lowest interior level): their children are
     * guaranteed to be leaves, so we record the child page IDs directly
     * <em>without pinning them</em>. This reduces page reads from
     * {@code N_interior + N_leaf} to just {@code N_interior}, which is
     * typically {@code < 0.4 %} of all pages because of fanout.
     * <p>
     * The returned array gives perfectly uniform random access to any leaf,
     * eliminating the tree-walk bias that cause estimation error.
     *
     * <p>
     * Memory: the result holds one int per leaf page, i.e. {@code 4 * N_leaf} bytes. This scales with the
     * component size / page size: at the 128KB analytics page size a 1TB component (~8.4M leaves) needs ~33MB;
     * at a 32KB page size the same component (~33M leaves) needs ~128MB. Transient (one array per component per
     * sample job) and acceptable at current sizes.
     * TODO: shrink this without losing O(1) random-access-by-index (needed for uniform leaf draws). The ids are a
     * sorted, near-sequential list (DFS/key order, bulk load allocates pages roughly sequentially), so a succinct
     * encoding — Elias-Fano, a Roaring/compressed bitmap with select(), or delta+varint over blocks — can cut this
     * ~10-30x while still returning the i-th leaf id cheaply.
     *
     * @param rootPageId root page of the B-tree
     * @param ctx        B-tree operation context
     * @param bcOpCtx    buffer cache read context
     * @return int array of every leaf page ID (in DFS / key order)
     */
    public int[] enumerateLeafPageIds(int rootPageId, BTreeOpContext ctx, IBufferCacheReadContext bcOpCtx)
            throws HyracksDataException {
        IntArrayList leafPageIds = new IntArrayList();
        enumerateLeafPageIdsRecursive(rootPageId, ctx, bcOpCtx, leafPageIds);
        return leafPageIds.toIntArray();
    }

    private void enumerateLeafPageIdsRecursive(int pageId, BTreeOpContext ctx, IBufferCacheReadContext bcOpCtx,
            IntArrayList leafPageIds) throws HyracksDataException {
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), bcOpCtx);
        try {
            ctx.getInteriorFrame().setPage(page);
            if (ctx.getInteriorFrame().isLeaf()) {
                // Height-1 tree: the root itself is a leaf
                leafPageIds.add(pageId);
                return;
            }
            int numberOfChildren = ctx.getInteriorFrame().getTupleCount();
            if (ctx.getInteriorFrame().getRightmostChildPageId() > 0) {
                numberOfChildren += 1;
            }
            int[] childPageIds = new int[numberOfChildren];
            for (int i = 0; i < numberOfChildren; i++) {
                childPageIds[i] = getChildPageId(ctx, i, numberOfChildren);
            }

            // Optimisation: if this interior node is at level 1, its children
            // are guaranteed to be leaves (level 0). Record their page IDs
            // directly without pinning — this avoids reading every leaf page
            // during enumeration, cutting I/O by a factor of ~fan-out.
            boolean childrenAreLeaves = (ctx.getInteriorFrame().getLevel() == 1);

            bufferCache.unpin(page, bcOpCtx);
            page = null;

            if (childrenAreLeaves) {
                for (int childId : childPageIds) {
                    leafPageIds.add(childId);
                }
            } else {
                for (int childId : childPageIds) {
                    enumerateLeafPageIdsRecursive(childId, ctx, bcOpCtx, leafPageIds);
                }
            }
        } finally {
            if (page != null) {
                bufferCache.unpin(page, bcOpCtx);
            }
        }
    }

    private int getChildPageId(BTreeOpContext ctx, int childIndex, int numberOfChildren) {
        // If the childIndex is the last one, return the rightmost child page ID
        if (childIndex == numberOfChildren - 1 && ctx.getInteriorFrame().getRightmostChildPageId() > 0) {
            return ctx.getInteriorFrame().getRightmostChildPageId();
        }
        int tupleOffset = ctx.getInteriorFrame().getTupleOffset(childIndex);
        ITreeIndexTupleReference frameTuple = ctx.getInteriorFrame().getTupleRef();
        frameTuple.setFieldCount(ctx.getCmp().getKeyFieldCount());
        ByteBuffer buf = ctx.getInteriorFrame().getBuffer();
        frameTuple.resetByTupleOffset(buf.array(), tupleOffset);
        int childPageId =
                IntegerPointable.getInteger(buf.array(), frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                        + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1));
        return childPageId;
    }

    private void searchDown(ICachedPage page, int pageId, BTreeOpContext ctx, ITreeIndexCursor cursor,
            IBufferCacheReadContext bcOpCtx) throws HyracksDataException {
        ICachedPage currentPage = page;
        ctx.getInteriorFrame().setPage(currentPage);
        try {
            int childPageId = pageId;
            while (!ctx.getInteriorFrame().isLeaf()) {
                // walk down the tree until we find the leaf
                childPageId = ctx.getInteriorFrame().getChildPageId(ctx.getPred());
                ICachedPage nextPage =
                        bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), childPageId), bcOpCtx);
                bufferCache.unpin(currentPage, bcOpCtx);
                currentPage = nextPage;
                ctx.getInteriorFrame().setPage(currentPage);
            }

            ctx.getCursorInitialState().setSearchOperationCallback(ctx.getSearchCallback());
            ctx.getCursorInitialState().setOriginialKeyComparator(ctx.getCmp());
            ctx.getCursorInitialState().setPage(currentPage);
            ctx.getCursorInitialState().setPageId(childPageId);
            ctx.getLeafFrame().setPage(currentPage);
            cursor.open(ctx.getCursorInitialState(), ctx.getPred());
        } catch (Exception e) {
            if (!ctx.isExceptionHandled() && currentPage != null) {
                bufferCache.unpin(currentPage, bcOpCtx);
            }
            ctx.setExceptionHandled(true);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public BTreeAccessor createAccessor(IIndexAccessParameters iap) {
        return new DiskBTreeAccessor(this, iap);
    }

    public BTreeAccessor createAccessor(IIndexAccessParameters iap, IIndexOperationContext opCtx, int index)
            throws HyracksDataException {
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
        public ITreeIndexCursor createSearchCursor(boolean exclusive) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new DiskBTreeRangeSearchCursor(leafFrame, exclusive, (IIndexCursorStats) iap.getParameters()
                    .getOrDefault(HyracksConstants.INDEX_CURSOR_STATS, NoOpIndexCursorStats.INSTANCE));
        }

        @Override
        public ITreeIndexCursor createPointCursor(boolean exclusive, boolean stateful) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new DiskBTreePointSearchCursor(leafFrame, exclusive, stateful);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            ctx.setOperation(IndexOperation.SEARCH);
            ((DiskBTree) btree).search((ITreeIndexCursor) cursor, searchPred, ctx, getBufferCacheOperationContext());
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new DiskBTreeDiskScanCursor(leafFrame);
        }

        public ITreeIndexCursor createSampleCursor(long componentSampleCardinality, long sampleSeed,
                ILSMIndexBatchPointCursor searchCursor, int maxLeafAttempts, int leafDrawBatchSize,
                int maxLeafTupleCount, int samplesPerPage, ITupleAcceptor antimatterAcceptor) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new DiskBTreeSampleCursor((DiskBTree) btree, leafFrame, componentSampleCardinality, sampleSeed, ctx,
                    getBufferCacheOperationContext(), searchCursor, maxLeafAttempts, leafDrawBatchSize,
                    maxLeafTupleCount, antimatterAcceptor);
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DISKORDERSCAN);
            ((DiskBTree) btree).diskOrderScan(cursor, ctx);
        }

        @Override
        public void diskSampleScan(IIndexCursor cursor) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DISKORDERSCAN);
            ((DiskBTree) btree).diskSampleScan((ITreeIndexCursor) cursor, ctx, getBufferCacheOperationContext());
        }

        protected IBufferCacheReadContext getBufferCacheOperationContext() {
            return DefaultBufferCacheReadContextProvider.DEFAULT;
        }
    }

    private class DiskBTreeDiskScanCursor extends TreeIndexDiskOrderScanCursor {

        public DiskBTreeDiskScanCursor(ITreeIndexFrame frame) {
            super(frame);
        }

        @Override
        protected void releasePage() {
            if (page != null) {
                bufferCache.unpin(page);
                page = null;
            }
        }

        @Override
        protected ICachedPage acquireNextPage() throws HyracksDataException {
            ICachedPage nextPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
            return nextPage;
        }

    }

}
