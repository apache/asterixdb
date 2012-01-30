/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileNameManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTree;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTree extends LSMTree {

    // In-memory components.
    private final RTree memRTree;
    private final static int MEM_RTREE_FILE_ID = 0;
    private final static int MEM_BTREE_FILE_ID = 1;

    // On-disk components.
    // For creating RTree's used in flush and merge.
    private final RTreeFactory diskRTreeFactory;
    private LinkedList<RTree> diskRTrees = new LinkedList<RTree>();

    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory rtreeLeafFrameFactory;
    private final ITreeIndexFrameFactory btreeLeafFrameFactory;

    public LSMRTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMFileNameManager fileNameManager, RTreeFactory diskRTreeFactory, BTreeFactory diskBTreeFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, MultiComparator rtreeCmp, MultiComparator btreeCmp) {

        super(memBufferCache, memFreePageManager, btreeInteriorFrameFactory, btreeLeafFrameFactory, fileNameManager,
                diskBTreeFactory, diskFileMapProvider, fieldCount, btreeCmp);
        memRTree = new RTree(memBufferCache, fieldCount, rtreeCmp, memFreePageManager, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory);

        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;

        this.diskRTreeFactory = diskRTreeFactory;
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        super.create(MEM_BTREE_FILE_ID);
        memRTree.create(MEM_RTREE_FILE_ID);
    }

    /**
     * Opens LSMRTree, assuming a consistent state of the disk-resident
     * components. In particular, registers all files in in base dir of
     * fileNameManager as on-disk RTrees and BTrees.
     * 
     * Example pathological scenario to explain "consistent state assumption":
     * Suppose a merge finished, but before the original files were deleted the
     * system crashes. We are left in a state where we have the original RTrees
     * and BTrees in addition to the merged ones. We assume that prior to
     * calling this method a separate recovery process has ensured the
     * consistent of the disk-resident components.
     * 
     * @param indexFileId
     *            Dummy file id.
     * @throws HyracksDataException
     */
    @Override
    public void open(int indexFileId) throws HyracksDataException {
        memRTree.open(MEM_RTREE_FILE_ID);
        memBTree.open(MEM_BTREE_FILE_ID);
        File dir = new File(fileNameManager.getBaseDir());
        FilenameFilter rtreeFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".") && name.endsWith("rtree");
            }
        };
        String[] rtreeFiles = dir.list(rtreeFilter);

        FilenameFilter btreeFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".") && name.endsWith("btree");
            }
        };
        String[] btreeFiles = dir.list(btreeFilter);

        if (rtreeFiles == null || btreeFiles == null) {
            return;
        }

        Comparator<String> fileNameCmp = fileNameManager.getFileNameComparator();
        Arrays.sort(rtreeFiles, fileNameCmp);
        for (String fileName : rtreeFiles) {
            RTree rtree = (RTree) createDiskTree(diskRTreeFactory, fileName, false);
            diskRTrees.add(rtree);
        }

        Arrays.sort(btreeFiles, fileNameCmp);
        for (String fileName : btreeFiles) {
            BTree btree = (BTree) createDiskTree(diskBTreeFactory, fileName, false);
            diskBTrees.add(btree);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        super.close();
        for (RTree rtree : diskRTrees) {
            diskBufferCache.closeFile(rtree.getFileId());
            rtree.close();
        }
        diskRTrees.clear();
        memRTree.close();
    }

    @Override
    public ITreeIndexAccessor createAccessor() {
        return new LSMRTreeAccessor(this);
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException {
        // Note that by using a flush target file name, we state that the new
        // bulk loaded tree is "newer" than any other merged tree.

        String fileName = fileNameManager.getFlushFileName();
        RTree diskRTree = (RTree) createDiskTree(diskRTreeFactory, fileName + "-rtree", true);
        // For each RTree, we require to have a buddy BTree. thus, we create an
        // empty BTree. This can be optimized later.
        BTree diskBTree = (BTree) createDiskTree(diskBTreeFactory, fileName + "-btree", true);
        LSMRTreeBulkLoadContext bulkLoadCtx = new LSMRTreeBulkLoadContext(diskRTree, diskBTree);
        bulkLoadCtx.beginBulkLoad(fillFactor);
        return bulkLoadCtx;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        LSMRTreeBulkLoadContext bulkLoadCtx = (LSMRTreeBulkLoadContext) ictx;
        bulkLoadCtx.getRTree().bulkLoadAddTuple(tuple, bulkLoadCtx.getBulkLoadCtx());

    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        LSMRTreeBulkLoadContext bulkLoadCtx = (LSMRTreeBulkLoadContext) ictx;
        bulkLoadCtx.getRTree().endBulkLoad(bulkLoadCtx.getBulkLoadCtx());
        synchronized (diskRTrees) {
            diskRTrees.addFirst(bulkLoadCtx.getRTree());
            diskBTrees.addFirst(bulkLoadCtx.getBTree());
        }
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return null;
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return null;
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return null;
    }

    @Override
    public int getFieldCount() {
        return 0;
    }

    @Override
    public int getRootPageId() {
        return 0;
    }

    @Override
    public IndexType getIndexType() {
        return null;
    }

    @Override
    public int getFileId() {
        return 0;
    }

    private void insertOrDelete(ITupleReference tuple, ITreeIndexAccessor accessor) throws HyracksDataException,
            TreeIndexException {
        boolean waitForFlush = true;
        do {
            // Wait for ongoing flush to complete.
            synchronized (this) {
                if (!flushFlag) {
                    // Increments threadRefCount, to force a flush to wait for
                    // this operation to finish.
                    // (a flush can only begin once threadRefCount == 0).
                    threadEnter();
                    // Proceed with operation.
                    waitForFlush = false;
                }
            }
        } while (waitForFlush);
        accessor.insert(tuple);
        try {
            threadExit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insert(ITupleReference tuple, LSMRTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        insertOrDelete(tuple, ctx.memRTreeAccessor);
    }

    private void delete(ITupleReference tuple, LSMRTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        insertOrDelete(tuple, ctx.memBTreeAccessor);
    }

    private Pair<List<ITreeIndex>, List<ITreeIndex>> search(ITreeIndexCursor cursor, ISearchPredicate rtreeSearchPred,
            LSMRTreeOpContext ctx, boolean includeMemRTree) throws HyracksDataException, TreeIndexException {
        // If the search doesn't include the in-memory RTree, then we don't have
        // to synchronize with a flush.
        if (includeMemRTree) {
            boolean waitForFlush = true;
            do {
                synchronized (this) {
                    if (!flushFlag) {
                        // The corresponding threadExit() is in
                        // LSMTreeRangeSearchCursor.close().
                        threadEnter();
                        waitForFlush = false;
                    }
                }
            } while (waitForFlush);
        }

        // Get a snapshot of the current on-disk RTrees and BTrees.
        // If includeMemRTree is true, then no concurrent
        // flush can add another on-disk RTree (due to threadEnter());
        // If includeMemRTree is false, then it is possible that a concurrent
        // flush adds another on-disk RTree.
        // Since this mode is only used for merging trees, it doesn't really
        // matter if the merge excludes the new on-disk RTree.
        List<ITreeIndex> diskRTreesSnapshot = new ArrayList<ITreeIndex>();
        List<ITreeIndex> diskBTreesSnapshot = new ArrayList<ITreeIndex>();
        AtomicInteger localSearcherRefCount = null;
        synchronized (diskRTrees) {
            diskRTreesSnapshot.addAll(diskRTrees);
            diskBTreesSnapshot.addAll(diskBTrees);
            // Only remember the search ref count when performing a merge (i.e.,
            // includeMemRTree is false).
            if (!includeMemRTree) {
                localSearcherRefCount = searcherRefCount;
                localSearcherRefCount.incrementAndGet();
            }
        }

        int numDiskTrees = diskRTreesSnapshot.size();

        ITreeIndexAccessor[] bTreeAccessors;
        int diskBTreeIx = 0;
        if (includeMemRTree) {
            bTreeAccessors = new ITreeIndexAccessor[numDiskTrees + 1];
            bTreeAccessors[0] = ctx.memBTreeAccessor;
            diskBTreeIx++;
        } else {
            bTreeAccessors = new ITreeIndexAccessor[numDiskTrees];
        }

        ListIterator<ITreeIndex> diskBTreesIter = diskBTreesSnapshot.listIterator();
        while (diskBTreesIter.hasNext()) {
            BTree diskBTree = (BTree) diskBTreesIter.next();
            bTreeAccessors[diskBTreeIx] = diskBTree.createAccessor();
            diskBTreeIx++;
        }

        LSMRTreeSearchCursor lsmRTreeCursor = (LSMRTreeSearchCursor) cursor;
        LSMRTreeCursorInitialState initialState = new LSMRTreeCursorInitialState(numDiskTrees + 1,
                rtreeLeafFrameFactory, rtreeInteriorFrameFactory, btreeLeafFrameFactory, cmp, bTreeAccessors, this,
                includeMemRTree, localSearcherRefCount);
        lsmRTreeCursor.open(initialState, rtreeSearchPred);

        int cursorIx = 1;
        if (includeMemRTree) {
            ctx.memRTreeAccessor.search(((LSMRTreeSearchCursor) lsmRTreeCursor).getCursor(0), rtreeSearchPred);
            cursorIx = 1;
        } else {
            cursorIx = 0;
        }

        // Open cursors of on-disk RTrees
        ITreeIndexAccessor[] diskRTreeAccessors = new ITreeIndexAccessor[numDiskTrees];
        ListIterator<ITreeIndex> diskRTreesIter = diskRTreesSnapshot.listIterator();

        int diskRTreeIx = 0;
        while (diskRTreesIter.hasNext()) {
            RTree diskRTree = (RTree) diskRTreesIter.next();
            diskRTreeAccessors[diskRTreeIx] = diskRTree.createAccessor();
            diskRTreeAccessors[diskRTreeIx].search(lsmRTreeCursor.getCursor(cursorIx), rtreeSearchPred);
            cursorIx++;
            diskRTreeIx++;
        }
        return new Pair<List<ITreeIndex>, List<ITreeIndex>>(diskRTreesSnapshot, diskBTreesSnapshot);

    }

    @Override
    public void flush() throws HyracksDataException, TreeIndexException {

        // scan the memory RTree
        ITreeIndexAccessor memRTreeAccessor = memRTree.createAccessor();
        ITreeIndexCursor rtreeScanCursor = memRTreeAccessor.createSearchCursor();
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);

        String fileName = fileNameManager.getFlushFileName();
        RTree diskRTree = (RTree) createDiskTree(diskRTreeFactory, fileName + "-rtree", true);

        // BulkLoad the tuples from the in-memory tree into the new disk RTree.
        IIndexBulkLoadContext rtreeBulkLoadCtx = diskRTree.beginBulkLoad(1.0f);

        try {
            while (rtreeScanCursor.hasNext()) {
                rtreeScanCursor.next();
                ITupleReference frameTuple = rtreeScanCursor.getTuple();
                diskRTree.bulkLoadAddTuple(frameTuple, rtreeBulkLoadCtx);
            }
        } finally {
            rtreeScanCursor.close();
        }
        diskRTree.endBulkLoad(rtreeBulkLoadCtx);

        // scan the memory BTree
        ITreeIndexAccessor memBTreeAccessor = memBTree.createAccessor();
        ITreeIndexCursor btreeScanCursor = memBTreeAccessor.createSearchCursor();
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);

        BTree diskBTree = (BTree) createDiskTree(diskBTreeFactory, fileName + "-btree", true);

        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoadContext btreeBulkLoadCtx = diskBTree.beginBulkLoad(1.0f);
        try {
            while (btreeScanCursor.hasNext()) {
                btreeScanCursor.next();
                ITupleReference frameTuple = btreeScanCursor.getTuple();
                diskBTree.bulkLoadAddTuple(frameTuple, btreeBulkLoadCtx);
            }
        } finally {
            btreeScanCursor.close();
        }
        diskBTree.endBulkLoad(btreeBulkLoadCtx);

        resetMemoryTrees();

        synchronized (diskRTrees) {
            diskRTrees.addFirst(diskRTree);
            diskBTrees.addFirst(diskBTree);
        }
    }

    @Override
    public void merge() throws HyracksDataException, TreeIndexException {
        if (!isMerging.compareAndSet(false, true)) {
            throw new TreeIndexException("Merge already in progress in LSMRTree. Only one concurrent merge allowed.");
        }

        // Point to the current searcher ref count, so we can wait for it later
        // (after we swap the searcher ref count).
        AtomicInteger localSearcherRefCount = searcherRefCount;

        LSMRTreeOpContext ctx = createOpContext();
        ITreeIndexCursor cursor = new LSMRTreeSearchCursor();
        SearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        // Scan the RTrees, ignoring the in-memory RTree.
        Pair<List<ITreeIndex>, List<ITreeIndex>> mergingDiskTreesPair = search(cursor, rtreeSearchPred, ctx, false);
        List<ITreeIndex> mergingDiskRTrees = mergingDiskTreesPair.getFirst();
        List<ITreeIndex> mergingDiskBTrees = mergingDiskTreesPair.getSecond();

        // Bulk load the tuples from all on-disk RTrees into the new RTree.
        String fileName = fileNameManager.getMergeFileName();
        RTree mergedRTree = (RTree) createDiskTree(diskRTreeFactory, fileName + "-rtree", true);
        BTree mergedBTree = (BTree) createDiskTree(diskBTreeFactory, fileName + "-btree", true);

        IIndexBulkLoadContext bulkLoadCtx = mergedRTree.beginBulkLoad(1.0f);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                mergedRTree.bulkLoadAddTuple(frameTuple, bulkLoadCtx);
            }
        } finally {
            cursor.close();
        }
        mergedRTree.endBulkLoad(bulkLoadCtx);

        // Remove the old RTrees and BTrees from the list, and add the new
        // merged RTree and an empty BTree
        // Also, swap the searchRefCount.
        synchronized (diskRTrees) {
            diskRTrees.removeAll(mergingDiskRTrees);
            diskRTrees.addLast(mergedRTree);

            diskBTrees.removeAll(mergingDiskBTrees);
            diskBTrees.addLast(mergedBTree);
            // Swap the searcher ref count reference, and reset it to zero.
            if (searcherRefCount == searcherRefCountA) {
                searcherRefCount = searcherRefCountB;
            } else {
                searcherRefCount = searcherRefCountA;
            }
            searcherRefCount.set(0);
        }

        // Wait for all searchers that are still accessing the old on-disk
        // RTrees and BTrees, then perform the final cleanup of the old RTrees
        // and BTrees.
        while (localSearcherRefCount.get() != 0) {
            try {
                Thread.sleep(AFTER_MERGE_CLEANUP_SLEEP);
            } catch (InterruptedException e) {
                // Propagate the exception to the caller, so that an appropriate
                // cleanup action can be taken.
                throw new HyracksDataException(e);
            }
        }

        // Cleanup. At this point we have guaranteed that no searchers are
        // touching the old on-disk RTrees and BTrees (localSearcherRefCount ==
        // 0).
        cleanupTrees(mergingDiskRTrees);
        cleanupTrees(mergingDiskBTrees);
        isMerging.set(false);

    }

    public void resetMemoryTrees() throws HyracksDataException {
        resetMemBTree();
        memRTree.create(MEM_RTREE_FILE_ID);
    }

    protected LSMRTreeOpContext createOpContext() {

        return new LSMRTreeOpContext((RTree.RTreeAccessor) memRTree.createAccessor(),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(), memFreePageManager
                        .getMetaDataFrameFactory().createFrame(), 8, (BTree.BTreeAccessor) memBTree.createAccessor(),
                btreeLeafFrameFactory, btreeInteriorFrameFactory, memFreePageManager.getMetaDataFrameFactory()
                        .createFrame(), cmp);
    }

    private class LSMRTreeAccessor implements ITreeIndexAccessor {
        private LSMRTree lsmRTree;
        private LSMRTreeOpContext ctx;

        public LSMRTreeAccessor(LSMRTree lsmRTree) {
            this.lsmRTree = lsmRTree;
            this.ctx = lsmRTree.createOpContext();

        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.INSERT);
            lsmRTree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            throw new UnsupportedOperationException("Update not supported by LSMRTree");
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.DELETE);
            lsmRTree.delete(tuple, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMRTreeSearchCursor();
        }

        @Override
        public void search(ITreeIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                TreeIndexException {
            ctx.reset(IndexOp.SEARCH);
            // TODO: fix exception handling throughout LSM tree.
            try {
                lsmRTree.search(cursor, searchPred, ctx, true);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            throw new UnsupportedOperationException("DiskOrderScan not supported by LSMRTree.");
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            throw new UnsupportedOperationException("DiskOrderScan not supported by LSMRTree.");
        }
    }

}
