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
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushPolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexComponentFinalizer;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTree implements ILSMIndex, ITreeIndex {
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
    // Helps to guarantees physical consistency of LSM components.
    private final ILSMComponentFinalizer componentFinalizer;

    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory insertLeafFrameFactory;
    private final ITreeIndexFrameFactory deleteLeafFrameFactory;
    private final IBinaryComparatorFactory[] cmpFactories;

    private boolean isOpen = false;

    public LSMBTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMFileManager fileNameManager,
            BTreeFactory diskBTreeFactory, BTreeFactory bulkLoadBTreeFactory, IFileMapProvider diskFileMapProvider,
            int fieldCount, IBinaryComparatorFactory[] cmpFactories, ILSMFlushPolicy flushPolicy,
            ILSMMergePolicy mergePolicy) {
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
        lsmHarness = new LSMHarness(this, flushPolicy, mergePolicy);
        componentFinalizer = new TreeIndexComponentFinalizer(diskFileMapProvider);
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        memBTree.create(indexFileId);
        fileManager.createDirs();
    }

    /**
     * Opens LSMBTree, cleaning up invalid files from base dir, and registering
     * all valid files as on-disk BTrees.
     * 
     * @param indexFileId
     *            Dummy file id used for in-memory BTree.
     * @throws HyracksDataException
     */
    @Override
    public void open(int indexFileId) throws HyracksDataException {
        synchronized (this) {
            if (isOpen) {
                return;
            }
            memBTree.open(indexFileId);
            BTree dummyBTree = diskBTreeFactory.createIndexInstance();
            List<Object> validFileNames = fileManager.cleanupAndGetValidFiles(dummyBTree, componentFinalizer);
            for (Object o : validFileNames) {
                String fileName = (String) o;
                FileReference fileRef = new FileReference(new File(fileName));
                BTree btree = createDiskBTree(diskBTreeFactory, fileRef, false);
                diskBTrees.add(btree);
            }
            isOpen = true;
        }
    }

    @Override
    public void close() throws HyracksDataException {
        synchronized (this) {
            if (!isOpen) {
                return;
            }
            for (Object o : diskBTrees) {
                BTree btree = (BTree) o;
                diskBufferCache.closeFile(btree.getFileId());
                diskBufferCache.deleteFile(btree.getFileId(), false);
                btree.close();
            }
            diskBTrees.clear();
            memBTree.close();
            isOpen = false;
        }
    }

    @Override
    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            TreeIndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;

        if (ctx.getIndexOp() == IndexOp.PHYSICALDELETE) {
            ctx.memBTreeAccessor.delete(tuple);
            return true;
        }

        ctx.memBTreeAccessor.upsert(tuple);

        return true;
    }

    @Override
    public ITreeIndex flush() throws HyracksDataException, IndexException {
        // Bulk load a new on-disk BTree from the in-memory BTree.        
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        ITreeIndexAccessor memBTreeAccessor = memBTree.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        IIndexCursor scanCursor = memBTreeAccessor.createSearchCursor();
        memBTreeAccessor.search(scanCursor, nullPred);
        BTree diskBTree = createFlushTarget();
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

    private BTree createBulkLoadTarget() throws HyracksDataException {
        String relFlushFileName = (String) fileManager.getRelFlushFileName();
        FileReference fileRef = fileManager.createFlushFile(relFlushFileName);
        return createDiskBTree(bulkLoadBTreeFactory, fileRef, true);
    }

    private BTree createFlushTarget() throws HyracksDataException {
        String relFlushFileName = (String) fileManager.getRelFlushFileName();
        FileReference fileRef = fileManager.createFlushFile(relFlushFileName);
        return createDiskBTree(diskBTreeFactory, fileRef, true);
    }

    private BTree createMergeTarget(List<Object> mergingDiskBTrees) throws HyracksDataException {
        BTree firstBTree = (BTree) mergingDiskBTrees.get(0);
        BTree lastBTree = (BTree) mergingDiskBTrees.get(mergingDiskBTrees.size() - 1);
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstBTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastBTree.getFileId());
        String relMergeFileName = (String) fileManager.getRelMergeFileName(firstFile.getFile().getName(), lastFile
                .getFile().getName());
        FileReference fileRef = fileManager.createMergeFile(relMergeFileName);
        return createDiskBTree(diskBTreeFactory, fileRef, true);
    }

    private BTree createDiskBTree(BTreeFactory factory, FileReference fileRef, boolean createBTree)
            throws HyracksDataException {
        // File will be deleted during cleanup of merge().
        diskBufferCache.createFile(fileRef);
        int diskBTreeFileId = diskFileMapProvider.lookupFileId(fileRef);
        // File will be closed during cleanup of merge().
        diskBufferCache.openFile(diskBTreeFileId);
        // Create new BTree instance.
        BTree diskBTree = factory.createIndexInstance();
        if (createBTree) {
            diskBTree.create(diskBTreeFileId);
        }
        // BTree will be closed during cleanup of merge().
        diskBTree.open(diskBTreeFileId);
        return diskBTree;
    }

    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred, IIndexOpContext ictx,
            boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        LSMBTreeRangeSearchCursor lsmTreeCursor = (LSMBTreeRangeSearchCursor) cursor;
        int numDiskBTrees = diskComponents.size();
        int numBTrees = (includeMemComponent) ? numDiskBTrees + 1 : numDiskBTrees;
        LSMBTreeCursorInitialState initialState = new LSMBTreeCursorInitialState(numBTrees, insertLeafFrameFactory,
                ctx.cmp, includeMemComponent, searcherRefCount, lsmHarness);
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
        while (diskBTreesIter.hasNext()) {
            BTree diskBTree = (BTree) diskBTreesIter.next();
            diskBTreeAccessors[diskBTreeIx] = diskBTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            diskBTreeAccessors[diskBTreeIx].search(lsmTreeCursor.getCursor(cursorIx), pred);
            cursorIx++;
            diskBTreeIx++;
        }
        lsmTreeCursor.initPriorityQueue();
    }

    public ITreeIndex merge(List<Object> mergedComponents) throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor();
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        // Ordered scan, ignoring the in-memory BTree.
        // We get back a snapshot of the on-disk BTrees that are going to be
        // merged now, so we can clean them up after the merge has completed.
        List<Object> mergingDiskBTrees = lsmHarness.search(cursor, (RangePredicate) rangePred, ctx, false);
        mergedComponents.addAll(mergingDiskBTrees);

        // Nothing to merge.
        if (mergedComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        // Bulk load the tuples from all on-disk BTrees into the new BTree.
        BTree mergedBTree = createMergeTarget(mergingDiskBTrees);
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
            diskBufferCache.deleteFile(oldBTree.getFileId(), false);
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
        BTree diskBTree = createBulkLoadTarget();
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

    public LSMBTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMBTreeOpContext(memBTree, insertLeafFrameFactory, deleteLeafFrameFactory, modificationCallback,
                searchCallback);
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMBTreeIndexAccessor(lsmHarness, createOpContext(modificationCallback, searchCallback));
    }

    public class LSMBTreeIndexAccessor extends LSMTreeIndexAccessor {
        public LSMBTreeIndexAccessor(LSMHarness lsmHarness, IIndexOpContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public IIndexCursor createSearchCursor() {
            return new LSMBTreeRangeSearchCursor();
        }

        public MultiComparator getMultiComparator() {
            LSMBTreeOpContext concreteCtx = (LSMBTreeOpContext) ctx;
            return concreteCtx.cmp;
        }
    }

    @Override
    public ILSMComponentFinalizer getComponentFinalizer() {
        return componentFinalizer;
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }
}
