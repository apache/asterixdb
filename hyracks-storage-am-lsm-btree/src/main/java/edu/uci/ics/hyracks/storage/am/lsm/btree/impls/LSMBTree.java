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

package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileNameManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTree implements ILSMTree {
    protected final Logger LOGGER = Logger.getLogger(LSMBTree.class.getName());
    private static final long AFTER_MERGE_CLEANUP_SLEEP = 100;
    
    // In-memory components.
    private final BTree memBTree;
    private final InMemoryFreePageManager memFreePageManager;    

    // On-disk components.
    private final ILSMFileNameManager fileNameManager;
    // For creating BTree's used in flush and merge.
    private final BTreeFactory diskBTreeFactory;
    // For creating BTree's used in bulk load. Different from diskBTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final BTreeFactory bulkLoadBTreeFactory;
    private final IBufferCache diskBufferCache;
    private final IFileMapProvider diskFileMapProvider;
    private LinkedList<BTree> diskBTrees = new LinkedList<BTree>();
    
    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory insertLeafFrameFactory;
    private final ITreeIndexFrameFactory deleteLeafFrameFactory;
    private final MultiComparator cmp;
    
    // For synchronizing all operations with flushes.
    // Currently, all operations block during a flush.
    private int threadRefCount;
    private boolean flushFlag;

    // For synchronizing searchers with a concurrent merge.
    private AtomicBoolean isMerging = new AtomicBoolean(false);
    private AtomicInteger searcherRefCountA = new AtomicInteger(0);
    private AtomicInteger searcherRefCountB = new AtomicInteger(0);
    // Represents the current number of searcher threads that are operating on
    // the unmerged on-disk BTrees.
    // We alternate between searcherRefCountA and searcherRefCountB.
    private AtomicInteger searcherRefCount = searcherRefCountA;
    
    public LSMBTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMFileNameManager fileNameManager, BTreeFactory diskBTreeFactory,
            BTreeFactory bulkLoadBTreeFactory, IFileMapProvider diskFileMapProvider, int fieldCount, MultiComparator cmp) {
        memBTree = new BTree(memBufferCache, fieldCount, cmp, memFreePageManager, interiorFrameFactory,
                insertLeafFrameFactory);
        this.memFreePageManager = memFreePageManager;
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.diskBufferCache = diskBTreeFactory.getBufferCache();
        this.diskFileMapProvider = diskFileMapProvider;
        this.diskBTreeFactory = diskBTreeFactory;
        this.bulkLoadBTreeFactory = bulkLoadBTreeFactory;
        this.cmp = cmp;
        this.diskBTrees = new LinkedList<BTree>();
        this.threadRefCount = 0;
        this.flushFlag = false;
        this.fileNameManager = fileNameManager;
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        memBTree.create(indexFileId);
    }
    
    /**
     * Opens LSMBTree, assuming a consistent state of the disk-resident
     * components. In particular, registers all files in in base dir of
     * fileNameManager as on-disk BTrees.
     * 
     * Example pathological scenario to explain "consistent state assumption":
     * Suppose a merge finished, but before the original files were deleted the
     * system crashes. We are left in a state where we have the original BTrees
     * in addition to the merged one. We assume that prior to calling this
     * method a separate recovery process has ensured the consistent of the
     * disk-resident components.
     * 
     * @param indexFileId
     *            Dummy file id used for in-memory BTree.
     * @throws HyracksDataException
     */
    @Override
    public void open(int indexFileId) throws HyracksDataException {
        memBTree.open(indexFileId);
        File dir = new File(fileNameManager.getBaseDir());
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".");
            }
        };
        String[] files = dir.list(filter);
        if (files == null) {
        	return;
        }
        Comparator<String> fileNameCmp = fileNameManager.getFileNameComparator();
        Arrays.sort(files, fileNameCmp);
        for (String fileName : files) {
            BTree btree = createDiskBTree(diskBTreeFactory, fileName, false);
            diskBTrees.add(btree);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        for (BTree btree : diskBTrees) {
            diskBufferCache.closeFile(btree.getFileId());
            btree.close();
        }
        diskBTrees.clear();
        memBTree.close();
    }

	private void lsmPerformOp(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
		boolean waitForFlush = true;
		do {
		    // Wait for ongoing flush to complete.
			synchronized (this) {
				if (!flushFlag) {
					// Increments threadRefCount, to force a flush to wait for this operation to finish.
				    // (a flush can only begin once threadRefCount == 0).
				    threadEnter();
				    // Proceed with operation.
					waitForFlush = false;
				}
			}
		} while (waitForFlush);
		// TODO: This will become much simpler once the BTree supports a true upsert operation.
		try {
			ctx.memBTreeAccessor.insert(tuple);
		} catch (BTreeDuplicateKeyException e) {
			// Notice that a flush must wait for the current operation to
			// finish (threadRefCount must reach zero).
            // TODO: This methods below are very inefficient, we'd rather like
            // to flip the antimatter bit one single BTree traversal.
		    if (ctx.getIndexOp() == IndexOp.DELETE) {
		        deleteExistingKey(tuple, ctx);
		    } else {
		        insertOrUpdateExistingKey(tuple, ctx);
		    }
		}
		threadExit();
	}

	private void deleteExistingKey(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        // We assume that tuple given by the user for deletion only contains the
        // key fields, but not any non-key fields.
        // Therefore, to set the delete bit in the tuple that already exist in
        // the BTree, we must retrieve the original tuple first. This is to
        // ensure that we have the proper value field.
        if (cmp.getKeyFieldCount() != memBTree.getFieldCount()) {
            ctx.reset(IndexOp.SEARCH);
            RangePredicate rangePredicate = new RangePredicate(true, tuple, tuple, true, true, cmp, cmp);
            ITreeIndexCursor cursor = ctx.memBTreeAccessor.createSearchCursor();
            ctx.memBTreeAccessor.search(cursor, rangePredicate);
            ITupleReference tupleCopy = null;
            try {
                if (cursor.hasNext()) {
                    cursor.next();
                    tupleCopy = TupleUtils.copyTuple(cursor.getTuple());
                }
            } finally {
                cursor.close();
            }
            // This means the tuple we are looking for must have been truly deleted by another thread.
            // Simply restart the original operation to insert the antimatter tuple. 
            // There is a remote chance of livelocks due to this behavior.
            if (tupleCopy == null) {
                ctx.reset(IndexOp.DELETE);
                lsmPerformOp(tuple, ctx);
                return;
            }
            memBTreeUpdate(tupleCopy, ctx);
        } else {
            // Since the existing tuple could be a matter tuple, we must delete it and re-insert.
            memBTreeDeleteAndReinsert(tuple, ctx);
        }            
	}
	
    private void insertOrUpdateExistingKey(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException,
            TreeIndexException {        
        // If all fields are keys, and the key we are trying to insert/update
        // already exists, then we are already done.
        // Otherwise, we must update the non-key fields.        
        if (cmp.getKeyFieldCount() != memBTree.getFieldCount()) {
            memBTreeUpdate(tuple, ctx);
        } else {
            // Since the existing tuple could be an antimatter tuple, we must delete it and re-insert.
            memBTreeDeleteAndReinsert(tuple, ctx);
        }
    }
    
    private void memBTreeDeleteAndReinsert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException,
            TreeIndexException {
        // All fields are key fields, therefore a true BTree update is not
        // allowed.
        // In order to set/unset the antimatter bit, we
        // must truly delete the existing tuple from the BTree, and then
        // re-insert it (with the antimatter bit set/unset).
        // Since the tuple given by the user already has all fields, we
        // don't need to retrieve the already existing tuple.
        IndexOp originalOp = ctx.getIndexOp();
        try {
            ctx.memBTreeAccessor.delete(tuple);
        } catch (BTreeNonExistentKeyException e) {
            // Tuple has been deleted in the meantime. We will restart
            // our operation anyway.
        }
        // Restart performOp to insert the tuple.
        ctx.reset(originalOp);
        lsmPerformOp(tuple, ctx);
    }
    
    private void memBTreeUpdate(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException,
            TreeIndexException {
        IndexOp originalOp = ctx.getIndexOp();
        try {
            ctx.reset(IndexOp.UPDATE);
            ctx.memBTreeAccessor.update(tuple);
        } catch (BTreeNonExistentKeyException e) {
            // It is possible that the key has truly been deleted.
            // Simply restart the operation. There is a remote chance of
            // livelocks due to this behavior.
            ctx.reset(originalOp);
            lsmPerformOp(tuple, ctx);
        }
    }
    
    public void threadEnter() {
        threadRefCount++;
    }
    
    public void threadExit() throws HyracksDataException, TreeIndexException {
        synchronized (this) {
            threadRefCount--;
            // Check if we've reached or exceeded the maximum number of pages.
            if (!flushFlag && memFreePageManager.isFull()) {
                flushFlag = true;
            }
            // Flush will only be handled by last exiting thread.
            if (flushFlag && threadRefCount == 0) {
                flush();
                flushFlag = false;
            }
        }
    }

    @Override
    public void flush() throws HyracksDataException, TreeIndexException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Flushing LSM-BTree.");
        }
        // Bulk load a new on-disk BTree from the in-memory BTree.        
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        ITreeIndexAccessor memBTreeAccessor = memBTree.createAccessor();
        ITreeIndexCursor scanCursor = memBTreeAccessor.createSearchCursor();
        memBTreeAccessor.search(scanCursor, nullPred);
        BTree diskBTree = createFlushTargetBTree();
        // Bulk load the tuples from the in-memory BTree into the new disk BTree.
        IIndexBulkLoadContext bulkLoadCtx = diskBTree.beginBulkLoad(1.0f);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                diskBTree.bulkLoadAddTuple(scanCursor.getTuple(), bulkLoadCtx);
            }
        } finally {
            scanCursor.close();
        }
        diskBTree.endBulkLoad(bulkLoadCtx);
        resetMemBTree();
        diskBTrees.addFirst(diskBTree);
    }

    private void resetMemBTree() throws HyracksDataException {
        memFreePageManager.reset();
        memBTree.create(memBTree.getFileId());
    }
    
    private BTree bulkLoadTargetBTree() throws HyracksDataException {
        // Note that by using a flush target file name, we state that the new
        // bulk loaded tree is "newer" than any other merged tree.
        String fileName = fileNameManager.getFlushFileName();
        return createDiskBTree(bulkLoadBTreeFactory, fileName, true);
    }
    
    private BTree createFlushTargetBTree() throws HyracksDataException {
        String fileName = fileNameManager.getFlushFileName();
        return createDiskBTree(diskBTreeFactory, fileName, true);
    }
    
    private BTree createMergeTargetBTree() throws HyracksDataException {
        String fileName = fileNameManager.getMergeFileName();
        return createDiskBTree(diskBTreeFactory, fileName, true);
    }
    
    private BTree createDiskBTree(BTreeFactory factory, String fileName, boolean createBTree) throws HyracksDataException {
        // Register the new BTree file.        
        FileReference file = new FileReference(new File(fileName));
        // File will be deleted during cleanup of merge().
        diskBufferCache.createFile(file);
        int diskBTreeFileId = diskFileMapProvider.lookupFileId(file);
        // File will be closed during cleanup of merge().
        diskBufferCache.openFile(diskBTreeFileId);
        // Create new BTree instance.
        BTree diskBTree = factory.createBTreeInstance(diskBTreeFileId);
        if (createBTree) {
            diskBTree.create(diskBTreeFileId);
        }
        // BTree will be closed during cleanup of merge().
        diskBTree.open(diskBTreeFileId);
        return diskBTree;
    }
    
    private List<BTree> search(ITreeIndexCursor cursor, RangePredicate pred, LSMBTreeOpContext ctx, boolean includeMemBTree) throws HyracksDataException, TreeIndexException {                
        // If the search doesn't include the in-memory BTree, then we don't have
        // to synchronize with a flush.
        if (includeMemBTree) {
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

        // Get a snapshot of the current on-disk BTrees.
        // If includeMemBTree is true, then no concurrent
        // flush can add another on-disk BTree (due to threadEnter());
        // If includeMemBTree is false, then it is possible that a concurrent
        // flush adds another on-disk BTree.
        // Since this mode is only used for merging trees, it doesn't really
        // matter if the merge excludes the new on-disk BTree,
        List<BTree> diskBTreesSnapshot = new ArrayList<BTree>();
        AtomicInteger localSearcherRefCount = null;
        synchronized (diskBTrees) {
            diskBTreesSnapshot.addAll(diskBTrees);
            // Only remember the search ref count when performing a merge (i.e., includeMemBTree is false).
            if (!includeMemBTree) {
                localSearcherRefCount = searcherRefCount;
                localSearcherRefCount.incrementAndGet();
            }
        }
        
        LSMBTreeRangeSearchCursor lsmTreeCursor = (LSMBTreeRangeSearchCursor) cursor;
        int numDiskBTrees = diskBTreesSnapshot.size();
        int numBTrees = (includeMemBTree) ? numDiskBTrees + 1 : numDiskBTrees;                
        LSMBTreeCursorInitialState initialState = new LSMBTreeCursorInitialState(numBTrees,
                insertLeafFrameFactory, cmp, this, includeMemBTree, localSearcherRefCount);
        lsmTreeCursor.open(initialState, pred);
        
        int cursorIx;
        if (includeMemBTree) {
            // Open cursor of in-memory BTree at index 0.
            ctx.memBTreeAccessor.search(lsmTreeCursor.getCursor(0), pred);
            // Skip 0 because it is the in-memory BTree.
            cursorIx = 1;
        } else {
            cursorIx = 0;
        }
        
        // Open cursors of on-disk BTrees.
        ITreeIndexAccessor[] diskBTreeAccessors = new ITreeIndexAccessor[numDiskBTrees];
        int diskBTreeIx = 0;
        ListIterator<BTree> diskBTreesIter = diskBTreesSnapshot.listIterator();
        while(diskBTreesIter.hasNext()) {
            BTree diskBTree = diskBTreesIter.next();
            diskBTreeAccessors[diskBTreeIx] = diskBTree.createAccessor();
            diskBTreeAccessors[diskBTreeIx].search(lsmTreeCursor.getCursor(cursorIx), pred);
            cursorIx++;
            diskBTreeIx++;
        }
        lsmTreeCursor.initPriorityQueue();
        return diskBTreesSnapshot;
    }
    
    private void insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        lsmPerformOp(tuple, ctx);
    }

    private void delete(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        lsmPerformOp(tuple, ctx);
    }

    public void merge() throws HyracksDataException, TreeIndexException  {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Merging LSM-BTree.");
        }
        if (isMerging.get()) {
            throw new TreeIndexException("Merge already in progress in LSMBTree. Only one concurrent merge allowed.");
        }
        isMerging.set(true);
        
        // Point to the current searcher ref count, so we can wait for it later
        // (after we swap the searcher ref count).
        AtomicInteger localSearcherRefCount = searcherRefCount;
        
        LSMBTreeOpContext ctx = createOpContext();
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor();
        RangePredicate rangePred = new RangePredicate(true, null, null, true, true, null, null);
        // Ordered scan, ignoring the in-memory BTree.
        // We get back a snapshot of the on-disk BTrees that are going to be
        // merged now, so we can clean them up after the merge has completed.
        List<BTree> mergingDiskBTrees = search(cursor, (RangePredicate) rangePred, ctx, false);

        // Bulk load the tuples from all on-disk BTrees into the new BTree.
        BTree mergedBTree = createMergeTargetBTree();
        IIndexBulkLoadContext bulkLoadCtx = mergedBTree.beginBulkLoad(1.0f);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                mergedBTree.bulkLoadAddTuple(frameTuple, bulkLoadCtx);
            }
        } finally {
            cursor.close();
        }
        mergedBTree.endBulkLoad(bulkLoadCtx);
        
        // Remove the old BTrees from the list, and add the new merged BTree.
        // Also, swap the searchRefCount.
        synchronized (diskBTrees) {
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
        // BTrees, then perform the final cleanup of the old BTrees.
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
        // touching the old on-disk BTrees (localSearcherRefCount == 0).
        for (BTree oldBTree : mergingDiskBTrees) {
            oldBTree.close();
            FileReference fileRef = diskFileMapProvider.lookupFileName(oldBTree.getFileId());
            diskBufferCache.closeFile(oldBTree.getFileId());
            diskBufferCache.deleteFile(oldBTree.getFileId());
            fileRef.getFile().delete();
        }
        isMerging.set(false);
    }
    
    public class LSMTreeBulkLoadContext implements IIndexBulkLoadContext {
        private final BTree btree;
        private IIndexBulkLoadContext bulkLoadCtx;
        
        public LSMTreeBulkLoadContext(BTree btree) {
            this.btree = btree;
        }

        public void beginBulkLoad(float fillFactor) throws HyracksDataException, TreeIndexException {
            bulkLoadCtx = btree.beginBulkLoad(fillFactor);
        }
    
        public BTree getBTree() {
            return btree;
        }
        
        public IIndexBulkLoadContext getBulkLoadCtx() {
            return bulkLoadCtx;
        }
    }
    
    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException {
        BTree diskBTree = bulkLoadTargetBTree();
        LSMTreeBulkLoadContext bulkLoadCtx = new LSMTreeBulkLoadContext(diskBTree);        
        bulkLoadCtx.beginBulkLoad(fillFactor);
        return bulkLoadCtx;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        LSMTreeBulkLoadContext bulkLoadCtx = (LSMTreeBulkLoadContext) ictx;
        bulkLoadCtx.getBTree().bulkLoadAddTuple(tuple, bulkLoadCtx.getBulkLoadCtx());
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        LSMTreeBulkLoadContext bulkLoadCtx = (LSMTreeBulkLoadContext) ictx;
        bulkLoadCtx.getBTree().endBulkLoad(bulkLoadCtx.getBulkLoadCtx());
        diskBTrees.addFirst(bulkLoadCtx.getBTree());
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return memBTree.getLeafFrameFactory();
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return memBTree.getInteriorFrameFactory();
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return memBTree.getFreePageManager();
    }

    @Override
    public int getFieldCount() {
        return memBTree.getFieldCount();
    }

    @Override
    public int getRootPageId() {
        return memBTree.getRootPageId();
    }

    @Override
    public IndexType getIndexType() {
        return memBTree.getIndexType();
    }

    public MultiComparator getMultiComparator() {
        return cmp;
    }
    
    public LSMBTreeOpContext createOpContext() {
        return new LSMBTreeOpContext(memBTree, insertLeafFrameFactory, deleteLeafFrameFactory);
    }

    @Override
    public ITreeIndexAccessor createAccessor() {
        return new LSMTreeIndexAccessor(this);
    }

    private class LSMTreeIndexAccessor implements ITreeIndexAccessor {
        private LSMBTree lsmTree;
        private LSMBTreeOpContext ctx;

        public LSMTreeIndexAccessor(LSMBTree lsmTree) {
            this.lsmTree = lsmTree;
            this.ctx = lsmTree.createOpContext();
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.INSERT);
            lsmTree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            // Update is the same as insert.
            ctx.reset(IndexOp.INSERT);
            insert(tuple);
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.DELETE);
            lsmTree.delete(tuple, ctx);            
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMBTreeRangeSearchCursor();
        }
        
        @Override
        public void search(ITreeIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                TreeIndexException {
            ctx.reset(IndexOp.SEARCH);
            lsmTree.search(cursor, (RangePredicate) searchPred, ctx, true);
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            // Disk-order scan doesn't make sense for the LSMBTree because it cannot correctly resolve deleted tuples.
            throw new UnsupportedOperationException("DiskOrderScan not supported by LSMTree.");
        }
        
        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            // Disk-order scan doesn't make sense for the LSMBTree because it cannot correctly resolve deleted tuples.
            throw new UnsupportedOperationException("DiskOrderScan not supported by LSMTree.");
        }
    }
}
