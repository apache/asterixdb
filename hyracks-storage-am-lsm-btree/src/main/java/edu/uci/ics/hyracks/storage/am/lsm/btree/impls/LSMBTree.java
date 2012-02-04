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
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
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
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTree implements ILSMTree {
    protected final Logger LOGGER = Logger.getLogger(LSMBTree.class.getName());
    
    private final LSMHarness lsmHarness;
    
    // In-memory components.
    private final BTree memBTree;
    private final InMemoryFreePageManager memFreePageManager;

    // On-disk components.
    private final ILSMFileManager fileManager;
    // For creating BTree's used in flush and merge.
    private final BTreeFactory diskBTreeFactory;
    // For creating BTree's used in bulk load. Different from diskBTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final BTreeFactory bulkLoadBTreeFactory;
    private final IBufferCache diskBufferCache;
    private final IFileMapProvider diskFileMapProvider;
    // List of BTree instances. Using Object for better sharing via ILSMTree + LSMHarness.
    private LinkedList<Object> diskBTrees = new LinkedList<Object>();
    
    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory insertLeafFrameFactory;
    private final ITreeIndexFrameFactory deleteLeafFrameFactory;
    private final IBinaryComparatorFactory[] cmpFactories;
    
    public LSMBTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMFileManager fileNameManager, BTreeFactory diskBTreeFactory,
            BTreeFactory bulkLoadBTreeFactory, IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] cmpFactories) {
        memBTree = new BTree(memBufferCache, fieldCount, cmpFactories, memFreePageManager, interiorFrameFactory,
                insertLeafFrameFactory);
        this.memFreePageManager = memFreePageManager;
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.diskBufferCache = diskBTreeFactory.getBufferCache();
        this.diskFileMapProvider = diskFileMapProvider;
        this.diskBTreeFactory = diskBTreeFactory;
        this.bulkLoadBTreeFactory = bulkLoadBTreeFactory;
        this.cmpFactories = cmpFactories;
        this.diskBTrees = new LinkedList<Object>();
        this.fileManager = fileNameManager;
        lsmHarness = new LSMHarness(this);
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
        List<String> validFileNames = fileManager.cleanupAndGetValidFiles();        
        for (String fileName : validFileNames) {
            BTree btree = createDiskBTree(diskBTreeFactory, fileName, false);
            diskBTrees.add(btree);
        }
    }

    @Override
    public void close() throws HyracksDataException {        
        for (Object o : diskBTrees) {
            BTree btree = (BTree) o;
            diskBufferCache.closeFile(btree.getFileId());
            btree.close();
        }
        diskBTrees.clear();
        memBTree.close();
    }

    @Override
    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException, TreeIndexException {        
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        // TODO: This will become much simpler once the BTree supports a true upsert operation.
        try {
            ctx.memBTreeAccessor.insert(tuple);
        } catch (BTreeDuplicateKeyException e) {
            // Notice that a flush must wait for the current operation to
            // finish (threadRefCount must reach zero).
            // TODO: The methods below are very inefficient, we'd rather like
            // to flip the antimatter bit one single BTree traversal.
            if (ctx.getIndexOp() == IndexOp.DELETE) {
                return deleteExistingKey(tuple, ctx);
            } else {
                return insertOrUpdateExistingKey(tuple, ctx);
            }
        }
        return true;
    }
    
	private boolean deleteExistingKey(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        // We assume that tuple given by the user for deletion only contains the
        // key fields, but not any non-key fields.
        // Therefore, to set the delete bit in the tuple that already exist in
        // the BTree, we must retrieve the original tuple first. This is to
        // ensure that we have the proper value field.
        if (cmpFactories.length != memBTree.getFieldCount()) {
            ctx.reset(IndexOp.SEARCH);
            RangePredicate rangePredicate = new RangePredicate(tuple, tuple, true, true, ctx.cmp, ctx.cmp);
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
                return false;
            }
            return memBTreeUpdate(tupleCopy, ctx);
        } else {
            // Since the existing tuple could be a matter tuple, we must delete it and re-insert.
            return memBTreeDeleteAndReinsert(tuple, ctx);
        }            
	}
	
    private boolean insertOrUpdateExistingKey(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException,
            TreeIndexException {        
        // If all fields are keys, and the key we are trying to insert/update
        // already exists, then we are already done.
        // Otherwise, we must update the non-key fields.        
        if (cmpFactories.length != memBTree.getFieldCount()) {
            return memBTreeUpdate(tuple, ctx);
        } else {
            // Since the existing tuple could be an antimatter tuple, we must delete it and re-insert.
            return memBTreeDeleteAndReinsert(tuple, ctx);
        }
    }
    
    private boolean memBTreeDeleteAndReinsert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException,
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
        return false;
    }
    
    private boolean memBTreeUpdate(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException,
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
            return false;
        }
        return true;
    }

    @Override
    public ITreeIndex flush() throws HyracksDataException, TreeIndexException {
        // Bulk load a new on-disk BTree from the in-memory BTree.        
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
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
        return diskBTree;
    }

    @Override
    public void addFlushedComponent(Object index) {
        diskBTrees.addFirst(index);
    }
    
    @Override
    public void resetInMemoryComponent() throws HyracksDataException {
        memFreePageManager.reset();
        memBTree.create(memBTree.getFileId());
    }
    
    private BTree bulkLoadTargetBTree() throws HyracksDataException {
        // Note that by using a flush target file name, we state that the new
        // bulk loaded tree is "newer" than any other merged tree.
        String fileName = fileManager.getFlushFileName();
        return createDiskBTree(bulkLoadBTreeFactory, fileName, true);
    }
    
    private BTree createFlushTargetBTree() throws HyracksDataException {
        String fileName = fileManager.getFlushFileName();
        return createDiskBTree(diskBTreeFactory, fileName, true);
    }
    
    private BTree createMergeTargetBTree(List<Object> mergingDiskBTrees) throws HyracksDataException {
        BTree firstBTree = (BTree) mergingDiskBTrees.get(0);
        BTree lastBTree = (BTree) mergingDiskBTrees.get(mergingDiskBTrees.size() - 1);
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstBTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastBTree.getFileId());
        String fileName = fileManager.getMergeFileName(firstFile.getFile().getName(), lastFile.getFile().getName());
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
    
    public void search(ITreeIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred, IIndexOpContext ictx, boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, TreeIndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        LSMBTreeRangeSearchCursor lsmTreeCursor = (LSMBTreeRangeSearchCursor) cursor;
        int numDiskBTrees = diskComponents.size();
        int numBTrees = (includeMemComponent) ? numDiskBTrees + 1 : numDiskBTrees;                
        LSMBTreeCursorInitialState initialState = new LSMBTreeCursorInitialState(numBTrees,
                insertLeafFrameFactory, ctx.cmp, includeMemComponent, searcherRefCount, lsmHarness);
        lsmTreeCursor.open(initialState, pred);
        
        int cursorIx;
        if (includeMemComponent) {
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
        ListIterator<Object> diskBTreesIter = diskComponents.listIterator();
        while(diskBTreesIter.hasNext()) {
            BTree diskBTree = (BTree) diskBTreesIter.next();
            diskBTreeAccessors[diskBTreeIx] = diskBTree.createAccessor();
            diskBTreeAccessors[diskBTreeIx].search(lsmTreeCursor.getCursor(cursorIx), pred);
            cursorIx++;
            diskBTreeIx++;
        }
        lsmTreeCursor.initPriorityQueue();
    }
    
    public ITreeIndex merge(List<Object> mergedComponents) throws HyracksDataException, TreeIndexException  {
        LSMBTreeOpContext ctx = createOpContext();
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor();
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        // Ordered scan, ignoring the in-memory BTree.
        // We get back a snapshot of the on-disk BTrees that are going to be
        // merged now, so we can clean them up after the merge has completed.
        List<Object> mergingDiskBTrees = lsmHarness.search(cursor, (RangePredicate) rangePred, ctx, false);
        mergedComponents.addAll(mergingDiskBTrees);
        
        // Bulk load the tuples from all on-disk BTrees into the new BTree.
        BTree mergedBTree = createMergeTargetBTree(mergedComponents);
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
        return mergedBTree;
    }
    
    @Override
    public void addMergedComponent(Object newComponent, List<Object> mergedComponents) {
        diskBTrees.removeAll(mergedComponents);
        diskBTrees.addLast(newComponent);
    }
    
    @Override
    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException {
        for (Object o : mergedComponents) {
            BTree oldBTree = (BTree) o;
            FileReference fileRef = diskFileMapProvider.lookupFileName(oldBTree.getFileId());
            diskBufferCache.closeFile(oldBTree.getFileId());
            oldBTree.close();
            fileRef.getFile().delete();
        }
    }
    
    @Override
    public InMemoryFreePageManager getInMemoryFreePageManager() {
        return (InMemoryFreePageManager) memBTree.getFreePageManager();
    }

    @Override
    public List<Object> getDiskComponents() {
        return diskBTrees;
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
        lsmHarness.addBulkLoadedComponent(bulkLoadCtx.getBTree());
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

    @Override
    public int getFileId() {
        return memBTree.getFileId();
    }
    
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }
    
    public LSMBTreeOpContext createOpContext() {
        return new LSMBTreeOpContext(memBTree, insertLeafFrameFactory, deleteLeafFrameFactory);
    }

    @Override
    public ITreeIndexAccessor createAccessor() {
        return new LSMBTreeIndexAccessor(lsmHarness, createOpContext());
    }
    
    public class LSMBTreeIndexAccessor extends LSMTreeIndexAccessor {
        public LSMBTreeIndexAccessor(LSMHarness lsmHarness, IIndexOpContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMBTreeRangeSearchCursor();
        }
        
        public MultiComparator getMultiComparator() {
            LSMBTreeOpContext concreteCtx = (LSMBTreeOpContext) ctx;
            return concreteCtx.cmp;
        }
    }
}
