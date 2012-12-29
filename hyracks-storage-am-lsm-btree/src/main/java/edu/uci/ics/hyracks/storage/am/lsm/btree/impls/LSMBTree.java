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
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

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
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentState;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMFlushOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

    // In-memory components.   
    private final LSMBTreeComponent mutableComponent;

    // On-disk components.    
    // For creating BTree's used in flush and merge.
    private final LSMBTreeComponentFactory componentFactory;
    // For creating BTree's used in bulk load. Different from diskBTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final LSMBTreeComponentFactory bulkLoadComponentFactory;

    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory insertLeafFrameFactory;
    private final ITreeIndexFrameFactory deleteLeafFrameFactory;
    private final IBinaryComparatorFactory[] cmpFactories;

    public LSMBTree(IInMemoryBufferCache memBufferCache, IInMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMIndexFileManager fileManager,
            TreeIndexFactory<BTree> diskBTreeFactory, TreeIndexFactory<BTree> bulkLoadBTreeFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler) {
        super(memFreePageManager, diskBTreeFactory.getBufferCache(), fileManager, diskFileMapProvider, mergePolicy,
                opTrackerFactory, ioScheduler);
        mutableComponent = new LSMBTreeComponent(new BTree(memBufferCache,
                ((InMemoryBufferCache) memBufferCache).getFileMapProvider(), memFreePageManager, interiorFrameFactory,
                insertLeafFrameFactory, cmpFactories, fieldCount, new FileReference(new File("membtree"))));
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        componentFactory = new LSMBTreeComponentFactory(diskBTreeFactory);
        bulkLoadComponentFactory = new LSMBTreeComponentFactory(bulkLoadBTreeFactory);
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        fileManager.deleteDirs();
        fileManager.createDirs();
        immutableComponents.clear();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActivated) {
            return;
        }

        ((InMemoryBufferCache) mutableComponent.getBTree().getBufferCache()).open();
        mutableComponent.getBTree().create();
        mutableComponent.getBTree().activate();
        immutableComponents.clear();
        List<LSMComponentFileReferences> validFileReferences;
        try {
            validFileReferences = fileManager.cleanupAndGetValidFiles();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            LSMBTreeComponent btree;
            try {
                btree = createDiskComponent(componentFactory, lsmComonentFileReference.getInsertIndexFileReference(),
                        false);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            immutableComponents.add(btree);
        }
        isActivated = true;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        if (!isActivated) {
            return;
        }

        BlockingIOOperationCallback cb = new BlockingIOOperationCallback();
        ILSMIndexAccessor accessor = createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        lsmHarness.getIOScheduler().scheduleOperation(accessor.createFlushOperation(cb));
        try {
            cb.waitForIO();
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        }

        for (ILSMComponent c : immutableComponents) {
            BTree btree = (BTree) ((LSMBTreeComponent) c).getBTree();
            btree.deactivate();
        }
        mutableComponent.getBTree().deactivate();
        mutableComponent.getBTree().destroy();
        ((InMemoryBufferCache) mutableComponent.getBTree().getBufferCache()).close();
        isActivated = false;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }

        for (ILSMComponent c : immutableComponents) {
            BTree btree = (BTree) ((LSMBTreeComponent) c).getBTree();
            btree.destroy();
        }
        mutableComponent.getBTree().destroy();
        fileManager.deleteDirs();
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }

        mutableComponent.getBTree().clear();
        for (ILSMComponent c : immutableComponents) {
            BTree btree = (BTree) ((LSMBTreeComponent) c).getBTree();
            btree.deactivate();
            btree.destroy();
        }
        immutableComponents.clear();
    }

    @Override
    public List<ILSMComponent> getOperationalComponents(IIndexOperationContext ctx) {
        List<ILSMComponent> operationalComponents = new ArrayList<ILSMComponent>();
        switch (ctx.getOperation()) {
            case SEARCH:
            case INSERT:
                // TODO: We should add the mutable component at some point.
                operationalComponents.addAll(immutableComponents);
                break;
            case MERGE:
                // TODO: determining the participating components in a merge should probably the task of the merge policy.
                if (immutableComponents.size() > 1) {
                    for (ILSMComponent c : immutableComponents) {
                        if (c.negativeCompareAndSet(LSMComponentState.MERGING, LSMComponentState.MERGING)) {
                            operationalComponents.add(c);
                        }
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
        return operationalComponents;
    }

    @Override
    public void insertUpdateOrDelete(ITupleReference tuple, IIndexOperationContext ictx) throws HyracksDataException,
            IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        switch (ctx.getOperation()) {
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
    }

    private boolean insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, IndexException {
        MultiComparator comparator = MultiComparator.createIgnoreFieldLength(mutableComponent.getBTree()
                .getComparatorFactories());
        LSMBTreeRangeSearchCursor searchCursor = new LSMBTreeRangeSearchCursor(ctx);
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
                    ctx.memBTreeAccessor.upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);
                    return true;
                }
            }
        } finally {
            memCursor.close();
        }

        // TODO: Can we just remove the above code that search the mutable component and do it together with the search call below? i.e. instead of passing false to the lsmHarness.search(), we pass true to include the mutable component?
        // the key was not in the inmemory component, so check the disk components
        lsmHarness.search(searchCursor, predicate, ctx, false);
        try {
            if (searchCursor.hasNext()) {
                throw new BTreeDuplicateKeyException("Failed to insert key since key already exists.");
            }
        } finally {
            searchCursor.close();
        }
        ctx.memBTreeAccessor.upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);

        return true;
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMFlushOperation flushOp = (LSMFlushOperation) operation;
        // Bulk load a new on-disk BTree from the in-memory BTree.        
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        ITreeIndexAccessor memBTreeAccessor = mutableComponent.getBTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IIndexCursor scanCursor = memBTreeAccessor.createSearchCursor();
        memBTreeAccessor.search(scanCursor, nullPred);
        LSMBTreeComponent component = createDiskComponent(componentFactory, flushOp.getFlushTarget(), true);
        // Bulk load the tuples from the in-memory BTree into the new disk BTree.
        IIndexBulkLoader bulkLoader = component.getBTree().createBulkLoader(1.0f, false);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                bulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
        }
        bulkLoader.end();
        return component;
    }

    @Override
    public void resetMutableComponent() throws HyracksDataException {
        memFreePageManager.reset();
        mutableComponent.getBTree().clear();
    }

    private ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(), true);
    }

    private LSMBTreeComponent createDiskComponent(LSMBTreeComponentFactory factory, FileReference fileRef,
            boolean createComponent) throws HyracksDataException, IndexException {
        // Create new BTree instance.
        LSMBTreeComponent component = (LSMBTreeComponent) factory
                .createLSMComponentInstance(new LSMComponentFileReferences(fileRef, null));
        if (createComponent) {
            component.getBTree().create();
        }
        // BTree will be closed during cleanup of merge().
        component.getBTree().activate();
        return component;
    }

    @Override
    public void search(IIndexCursor cursor, List<ILSMComponent> immutableComponents, ISearchPredicate pred,
            IIndexOperationContext ictx, boolean includeMutableComponent) throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        LSMBTreeRangeSearchCursor lsmTreeCursor = (LSMBTreeRangeSearchCursor) cursor;
        int numDiskComponents = immutableComponents.size();
        int numBTrees = (includeMutableComponent) ? numDiskComponents + 1 : numDiskComponents;

        List<ILSMComponent> operationalComponents = new ArrayList<ILSMComponent>();
        if (includeMutableComponent) {
            operationalComponents.add(getMutableComponent());
        }
        operationalComponents.addAll(immutableComponents);
        LSMBTreeCursorInitialState initialState = new LSMBTreeCursorInitialState(numBTrees, insertLeafFrameFactory,
                ctx.cmp, includeMutableComponent, lsmHarness, ctx.memBTreeAccessor, pred, ctx.searchCallback,
                operationalComponents);
        lsmTreeCursor.open(initialState, pred);

        int cursorIx;
        if (includeMutableComponent) {
            // Open cursor of in-memory BTree at index 0.
            ctx.memBTreeAccessor.search(lsmTreeCursor.getCursor(0), pred);
            // Skip 0 because it is the in-memory BTree.
            cursorIx = 1;
        } else {
            cursorIx = 0;
        }

        // Open cursors of on-disk BTrees.
        ITreeIndexAccessor[] diskBTreeAccessors = new ITreeIndexAccessor[numDiskComponents];
        int diskBTreeIx = 0;
        ListIterator<ILSMComponent> diskBTreesIter = immutableComponents.listIterator();
        while (diskBTreesIter.hasNext()) {
            BTree diskBTree = (BTree) ((LSMBTreeComponent) diskBTreesIter.next()).getBTree();
            diskBTreeAccessors[diskBTreeIx] = diskBTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            diskBTreeAccessors[diskBTreeIx].search(lsmTreeCursor.getCursor(cursorIx), pred);
            cursorIx++;
            diskBTreeIx++;
        }
        lsmTreeCursor.initPriorityQueue();
    }

    @Override
    public ILSMComponent merge(List<ILSMComponent> mergedComponents, ILSMIOOperation operation)
            throws HyracksDataException, IndexException {
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        ITreeIndexCursor cursor = mergeOp.getCursor();

        mergedComponents.addAll(mergeOp.getMergingComponents());

        // Nothing to merge.
        if (mergedComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        // Bulk load the tuples from all on-disk BTrees into the new BTree.
        LSMBTreeComponent mergedBTree = createDiskComponent(componentFactory, mergeOp.getMergeTarget(), true);
        IIndexBulkLoader bulkLoader = mergedBTree.getBTree().createBulkLoader(1.0f, false);
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
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput) throws TreeIndexException {
        return new LSMBTreeBulkLoader(fillLevel, verifyInput);
    }

    public class LSMBTreeBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final BTreeBulkLoader bulkLoader;

        public LSMBTreeBulkLoader(float fillFactor, boolean verifyInput) throws TreeIndexException {
            try {
                component = createBulkLoadTarget();
            } catch (HyracksDataException e) {
                throw new TreeIndexException(e);
            } catch (IndexException e) {
                throw new TreeIndexException(e);
            }
            bulkLoader = (BTreeBulkLoader) ((LSMBTreeComponent) component).getBTree().createBulkLoader(fillFactor,
                    verifyInput);
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                bulkLoader.add(tuple);
            } catch (IndexException e) {
                handleException();
                throw e;
            } catch (HyracksDataException e) {
                handleException();
                throw e;
            } catch (RuntimeException e) {
                handleException();
                throw e;
            }
        }

        protected void handleException() throws HyracksDataException {
            ((LSMBTreeComponent) component).getBTree().deactivate();
            ((LSMBTreeComponent) component).getBTree().destroy();
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            bulkLoader.end();
            lsmHarness.addBulkLoadedComponent(component);
        }

    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return mutableComponent.getBTree().getLeafFrameFactory();
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return mutableComponent.getBTree().getInteriorFrameFactory();
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return mutableComponent.getBTree().getFreePageManager();
    }

    @Override
    public int getFieldCount() {
        return mutableComponent.getBTree().getFieldCount();
    }

    @Override
    public int getRootPageId() {
        return mutableComponent.getBTree().getRootPageId();
    }

    @Override
    public int getFileId() {
        return mutableComponent.getBTree().getFileId();
    }

    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    public LSMBTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMBTreeOpContext(mutableComponent.getBTree(), insertLeafFrameFactory, deleteLeafFrameFactory,
                modificationCallback, searchCallback);
    }

    @Override
    public ILSMIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMBTreeAccessor(lsmHarness, createOpContext(modificationCallback, searchCallback));
    }

    public class LSMBTreeAccessor extends LSMTreeIndexAccessor {
        public LSMBTreeAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public IIndexCursor createSearchCursor() {
            return new LSMBTreeRangeSearchCursor(ctx);
        }

        public MultiComparator getMultiComparator() {
            LSMBTreeOpContext concreteCtx = (LSMBTreeOpContext) ctx;
            return concreteCtx.cmp;
        }

        @Override
        public ILSMIOOperation createFlushOperation(ILSMIOOperationCallback callback) {
            LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
            return new LSMFlushOperation(lsmHarness.getIndex(), componentFileRefs.getInsertIndexFileReference(),
                    callback);
        }
    }

    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException {
        LSMBTreeOpContext ctx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        ctx.setOperation(IndexOperation.MERGE);
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor(ctx);
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        // Ordered scan, ignoring the in-memory BTree.
        // We get back a snapshot of the on-disk BTrees that are going to be
        // merged now, so we can clean them up after the merge has completed.
        List<ILSMComponent> mergingDiskComponents;
        try {
            mergingDiskComponents = lsmHarness.search(cursor, (RangePredicate) rangePred, ctx, false);
            if (mergingDiskComponents.size() <= 1) {
                cursor.close();
                return null;
            }
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }

        BTree firstBTree = (BTree) ((LSMBTreeComponent) mergingDiskComponents.get(0)).getBTree();
        BTree lastBTree = (BTree) ((LSMBTreeComponent) mergingDiskComponents.get(mergingDiskComponents.size() - 1))
                .getBTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstBTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastBTree.getFileId());
        LSMComponentFileReferences relMergeFileRefs = fileManager.getRelMergeFileReference(firstFile.getFile()
                .getName(), lastFile.getFile().getName());
        return new LSMBTreeMergeOperation(lsmHarness.getIndex(), mergingDiskComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), callback);
    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        BTree btree = ((LSMBTreeComponent) lsmComponent).getBTree();
        forceFlushDirtyPages(btree);
        markAsValidInternal(btree);
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }

    public boolean isEmptyIndex() throws HyracksDataException {
        return immutableComponents.isEmpty()
                && mutableComponent.getBTree().isEmptyTree(
                        mutableComponent.getBTree().getInteriorFrameFactory().createFrame());
    }

    @Override
    public void validate() throws HyracksDataException {
        mutableComponent.getBTree().validate();
        for (ILSMComponent c : immutableComponents) {
            BTree btree = (BTree) ((LSMBTreeComponent) c).getBTree();
            btree.validate();
        }
    }

    @Override
    public long getMemoryAllocationSize() {
        InMemoryBufferCache memBufferCache = (InMemoryBufferCache) mutableComponent.getBTree().getBufferCache();
        return memBufferCache.getNumPages() * memBufferCache.getPageSize();
    }

    @Override
    public ILSMComponent getMutableComponent() {
        return mutableComponent;
    }
}
