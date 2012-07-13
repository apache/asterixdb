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
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
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
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexComponentFinalizer;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTree implements ILSMIndex, ITreeIndex {
    protected final Logger LOGGER = Logger.getLogger(LSMBTree.class.getName());

    private final LSMHarness lsmHarness;

    // In-memory components.   
    private final BTree memBTree;
    private final FileReference memBtreeFile = new FileReference(new File("memBtree"));
    private final InMemoryFreePageManager memFreePageManager;
    private final AntimatterAwareTupleAcceptor acceptor = new AntimatterAwareTupleAcceptor();

    // On-disk components.    
    private final ILSMFileManager fileManager;
    // For creating BTree's used in flush and merge.
    private final TreeFactory<BTree> diskBTreeFactory;
    // For creating BTree's used in bulk load. Different from diskBTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final TreeFactory<BTree> bulkLoadBTreeFactory;
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
            TreeFactory<BTree> diskBTreeFactory, TreeFactory<BTree> bulkLoadBTreeFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMFlushController flushController, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOScheduler ioScheduler) {
        memBTree = new BTree(memBufferCache, ((InMemoryBufferCache) memBufferCache).getFileMapProvider(),
                memFreePageManager, interiorFrameFactory, insertLeafFrameFactory, cmpFactories, fieldCount,
                memBtreeFile);
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
        lsmHarness = new LSMHarness(this, flushController, mergePolicy, opTracker, ioScheduler);
        componentFinalizer = new TreeIndexComponentFinalizer(diskFileMapProvider);
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to create since index is already open.");
        }

        fileManager.deleteDirs();
        fileManager.createDirs();
    }

    /**
     * Opens LSMBTree, cleaning up invalid files from base dir, and registering
     * all valid files as on-disk BTrees.
     * 
     * @param fileReference
     *            Dummy file id used for in-memory BTree.
     * @throws HyracksDataException
     */
    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isOpen) {
            return;
        }

        ((InMemoryBufferCache) memBTree.getBufferCache()).open();
        memBTree.create();
        memBTree.activate();
        List<Object> validFileNames = fileManager.cleanupAndGetValidFiles(componentFinalizer);
        for (Object o : validFileNames) {
            String fileName = (String) o;
            FileReference fileRef = new FileReference(new File(fileName));
            BTree btree = createDiskBTree(diskBTreeFactory, fileRef, false);
            diskBTrees.add(btree);
        }
        isOpen = true;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        if (!isOpen) {
            return;
        }

        for (Object o : diskBTrees) {
            BTree btree = (BTree) o;
            btree.deactivate();
        }
        diskBTrees.clear();
        memBTree.deactivate();
        memBTree.destroy();
        ((InMemoryBufferCache) memBTree.getBufferCache()).close();
        isOpen = false;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to destroy since index is already open.");
        }

        for (Object o : diskBTrees) {
            BTree btree = (BTree) o;
            btree.destroy();
        }
        memBTree.destroy();
        fileManager.deleteDirs();
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isOpen) {
            throw new HyracksDataException("Failed to clear since index is not open.");
        }

        memBTree.clear();
        for (Object o : diskBTrees) {
            BTree btree = (BTree) o;
            btree.deactivate();
            btree.destroy();
        }
        diskBTrees.clear();
    }

    @Override
    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;

        switch (ctx.getIndexOp()) {
            case PHYSICALDELETE:
                ctx.memBTreeAccessor.delete(tuple);
                break;
            case INSERT:
                insert(tuple, ctx);
                break;
            default:
                ctx.memBTreeAccessor.upsert(tuple);
                break;
        }

        return true;
    }

    private boolean insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, IndexException {
        MultiComparator comparator = MultiComparator.create(memBTree.getComparatorFactories());
        LSMBTreeRangeSearchCursor searchCursor = new LSMBTreeRangeSearchCursor();
        IIndexCursor memCursor = new BTreeRangeSearchCursor(ctx.memBTreeOpCtx.leafFrame, false);
        RangePredicate predicate = new RangePredicate(tuple, tuple, true, true, comparator, comparator);

        // first check the inmemory component
        ctx.memBTreeAccessor.search(memCursor, predicate);
        try {
            if (memCursor.hasNext()) {
                memCursor.next();
                LSMBTreeTupleReference lsmbtreeTuple = (LSMBTreeTupleReference) memCursor.getTuple();
                if (!lsmbtreeTuple.isAntimatter()) {
                    throw new BTreeDuplicateKeyException("Failed to insert key since key already exists.");
                } else {
                    memCursor.close();
                    ctx.memBTreeAccessor.upsertIfConditionElseInsert(tuple, acceptor);
                    return true;
                }
            }
        } finally {
            memCursor.close();
        }

        // the key was not in the inmemory component, so check the disk components
        lsmHarness.search(searchCursor, predicate, ctx, false);
        try {
            if (searchCursor.hasNext()) {
                throw new BTreeDuplicateKeyException("Failed to insert key since key already exists.");
            }
        } finally {
            searchCursor.close();
        }
        ctx.memBTreeAccessor.upsertIfConditionElseInsert(tuple, acceptor);

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
        IIndexBulkLoader bulkLoader = diskBTree.createBulkLoader(1.0f);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                bulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
        }
        bulkLoader.end();
        return diskBTree;
    }

    @Override
    public void addFlushedComponent(Object index) {
        diskBTrees.addFirst(index);
    }

    @Override
    public void resetInMemoryComponent() throws HyracksDataException {
        memFreePageManager.reset();
        memBTree.clear();
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

    private BTree createDiskBTree(TreeFactory<BTree> factory, FileReference fileRef, boolean createBTree)
            throws HyracksDataException {
        // Create new BTree instance.
        BTree diskBTree = factory.createIndexInstance(fileRef);
        if (createBTree) {
            diskBTree.create();
        }
        // BTree will be closed during cleanup of merge().
        diskBTree.activate();
        return diskBTree;
    }

    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred, IIndexOpContext ictx,
            boolean includeMemComponent, AtomicInteger searcherRefCount) throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        LSMBTreeRangeSearchCursor lsmTreeCursor = (LSMBTreeRangeSearchCursor) cursor;
        int numDiskBTrees = diskComponents.size();
        int numBTrees = (includeMemComponent) ? numDiskBTrees + 1 : numDiskBTrees;
        LSMBTreeCursorInitialState initialState = new LSMBTreeCursorInitialState(numBTrees, insertLeafFrameFactory,
                ctx.cmp, includeMemComponent, searcherRefCount, lsmHarness, null);
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
            diskBTreeAccessors[diskBTreeIx] = diskBTree.createAccessor(ctx.modificationCallback, ctx.searchCallback);
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
        IIndexBulkLoader bulkLoader = mergedBTree.createBulkLoader(1.0f);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                bulkLoader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }
        bulkLoader.end();
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
            oldBTree.deactivate();
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

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel) throws TreeIndexException {
        return new LSMBTreeBulkLoader(fillLevel);
    }

    public class LSMBTreeBulkLoader implements IIndexBulkLoader {
        private final BTree diskBTree;
        private final BTreeBulkLoader bulkLoader;

        public LSMBTreeBulkLoader(float fillFactor) throws TreeIndexException {
            try {
                diskBTree = createBulkLoadTarget();
            } catch (HyracksDataException e) {
                throw new TreeIndexException(e);
            }
            bulkLoader = (BTreeBulkLoader) diskBTree.createBulkLoader(0.7f);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            bulkLoader.add(tuple);
        }

        @Override
        public void end() throws HyracksDataException {
            bulkLoader.end();
            lsmHarness.addBulkLoadedComponent(diskBTree);
        }

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

    @Override
    public ILSMFlushController getFlushController() {
        return lsmHarness.getFlushController();
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return lsmHarness.getOperationTracker();
    }

    @Override
    public ILSMIOScheduler getIOScheduler() {
        return lsmHarness.getIOScheduler();
    }

    public boolean isEmptyIndex() throws HyracksDataException {
        return diskBTrees.isEmpty() && memBTree.isEmptyTree(memBTree.getInteriorFrameFactory().createFrame());
    }
}
