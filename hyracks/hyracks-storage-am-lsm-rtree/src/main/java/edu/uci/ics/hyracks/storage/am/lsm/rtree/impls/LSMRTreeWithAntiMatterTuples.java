/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import java.util.List;
import java.util.ListIterator;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTreeWithAntiMatterTuples extends AbstractLSMRTree {

    private TreeTupleSorter bTreeTupleSorter;

    // On-disk components.
    // For creating RTree's used in bulk load. Different from diskRTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final ILSMComponentFactory bulkLoaComponentFactory;

    public LSMRTreeWithAntiMatterTuples(IVirtualBufferCache virtualBufferCache,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMIndexFileManager fileManager, TreeIndexFactory<RTree> diskRTreeFactory,
            TreeIndexFactory<RTree> bulkLoadRTreeFactory, IFileMapProvider diskFileMapProvider, int fieldCount,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILinearizeComparatorFactory linearizer, int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackProvider ioOpCallbackProvider) {
        super(virtualBufferCache, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, btreeInteriorFrameFactory,
                btreeLeafFrameFactory, fileManager, diskRTreeFactory, new LSMRTreeWithAntiMatterTuplesComponentFactory(
                        diskRTreeFactory), diskFileMapProvider, fieldCount, rtreeCmpFactories, btreeCmpFactories,
                linearizer, comparatorFields, linearizerArray, 0, mergePolicy, opTracker, ioScheduler,
                ioOpCallbackProvider);
        bulkLoaComponentFactory = new LSMRTreeWithAntiMatterTuplesComponentFactory(bulkLoadRTreeFactory);
        this.bTreeTupleSorter = null;
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        super.activate();
        List<ILSMComponent> immutableComponents = componentsRef.get();
        immutableComponents.clear();
        List<LSMComponentFileReferences> validFileReferences;
        try {
            validFileReferences = fileManager.cleanupAndGetValidFiles();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            LSMRTreeImmutableComponent component;
            try {
                component = createDiskComponent(componentFactory,
                        lsmComonentFileReference.getInsertIndexFileReference(), null, null, false);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            immutableComponents.add(component);
        }
        isActivated = true;
    }

    @Override
    public synchronized void deactivate(boolean flushOnExit) throws HyracksDataException {
        super.deactivate(flushOnExit);
        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = (RTree) ((LSMRTreeImmutableComponent) c).getRTree();
            rtree.deactivate();
        }
        isActivated = false;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        deactivate(true);
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        super.destroy();
        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = (RTree) ((LSMRTreeImmutableComponent) c).getRTree();
            rtree.destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        super.clear();
        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = (RTree) ((LSMRTreeImmutableComponent) c).getRTree();
            rtree.deactivate();
            rtree.destroy();
        }
        immutableComponents.clear();
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();
        boolean includeMutableComponent = operationalComponents.get(0) == mutableComponent;
        LSMRTreeWithAntiMatterTuplesSearchCursor lsmTreeCursor = (LSMRTreeWithAntiMatterTuplesSearchCursor) cursor;
        int numDiskRComponents = operationalComponents.size();

        LSMRTreeCursorInitialState initialState;
        ITreeIndexAccessor[] bTreeAccessors = null;
        if (includeMutableComponent) {
            // Only in-memory BTree
            bTreeAccessors = new ITreeIndexAccessor[1];
            bTreeAccessors[0] = ctx.memBTreeAccessor;
        }

        initialState = new LSMRTreeCursorInitialState(numDiskRComponents, rtreeLeafFrameFactory,
                rtreeInteriorFrameFactory, btreeLeafFrameFactory, ctx.getBTreeMultiComparator(), null, bTreeAccessors,
                includeMutableComponent, lsmHarness, comparatorFields, linearizerArray, ctx.searchCallback,
                operationalComponents);

        lsmTreeCursor.open(initialState, pred);

        ListIterator<ILSMComponent> diskComponentsIter = operationalComponents.listIterator();
        int diskComponentIx = 0;
        if (includeMutableComponent) {
            // Open cursor of in-memory RTree
            ctx.memRTreeAccessor.search(lsmTreeCursor.getMemRTreeCursor(), pred);
            diskComponentIx++;
            diskComponentsIter.next();
        }

        // Open cursors of on-disk RTrees.
        ITreeIndexAccessor[] diskRTreeAccessors = new ITreeIndexAccessor[numDiskRComponents];
        while (diskComponentsIter.hasNext()) {
            RTree diskRTree = (RTree) ((LSMRTreeImmutableComponent) diskComponentsIter.next()).getRTree();
            diskRTreeAccessors[diskComponentIx] = diskRTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            diskRTreeAccessors[diskComponentIx].search(lsmTreeCursor.getCursor(diskComponentIx), pred);
            diskComponentIx++;
        }
        lsmTreeCursor.initPriorityQueue();
    }

    @Override
    public boolean scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        if (!mutableComponent.isModified()) {
            return false;
        }
        LSMRTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE);
        LSMComponentFileReferences relFlushFileRefs = fileManager.getRelFlushFileReference();
        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
        opCtx.setOperation(IndexOperation.FLUSH);
        opCtx.getComponentHolder().add(flushingComponent);
        ILSMIndexAccessorInternal accessor = new LSMRTreeWithAntiMatterTuplesAccessor(lsmHarness, opCtx);
        ioScheduler.scheduleOperation(new LSMRTreeFlushOperation(accessor, flushingComponent, relFlushFileRefs
                .getInsertIndexFileReference(), null, null, callback));
        return true;
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMRTreeFlushOperation flushOp = (LSMRTreeFlushOperation) operation;
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.
        LSMRTreeMutableComponent flushingComponent = (LSMRTreeMutableComponent) flushOp.getFlushingComponent();
        ITreeIndexAccessor memRTreeAccessor = flushingComponent.getRTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        RTreeSearchCursor rtreeScanCursor = (RTreeSearchCursor) memRTreeAccessor.createSearchCursor();
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
        LSMRTreeImmutableComponent component = createDiskComponent(componentFactory, flushOp.getRTreeFlushTarget(),
                null, null, true);
        RTree diskRTree = component.getRTree();

        // scan the memory BTree
        ITreeIndexAccessor memBTreeAccessor = flushingComponent.getBTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        BTreeRangeSearchCursor btreeScanCursor = (BTreeRangeSearchCursor) memBTreeAccessor.createSearchCursor();
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);

        // Since the LSM-RTree is used as a secondary assumption, the
        // primary key will be the last comparator in the BTree comparators
        rTreeTupleSorter = new TreeTupleSorter(flushingComponent.getRTree().getFileId(), linearizerArray,
                rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(), flushingComponent.getRTree()
                        .getBufferCache(), comparatorFields);

        bTreeTupleSorter = new TreeTupleSorter(flushingComponent.getBTree().getFileId(), linearizerArray,
                btreeLeafFrameFactory.createFrame(), btreeLeafFrameFactory.createFrame(), flushingComponent.getBTree()
                        .getBufferCache(), comparatorFields);
        // BulkLoad the tuples from the in-memory tree into the new disk
        // RTree.

        boolean isEmpty = true;
        try {
            while (rtreeScanCursor.hasNext()) {
                isEmpty = false;
                rtreeScanCursor.next();
                rTreeTupleSorter.insertTupleEntry(rtreeScanCursor.getPageId(), rtreeScanCursor.getTupleOffset());
            }
        } finally {
            rtreeScanCursor.close();
        }
        if (!isEmpty) {
            rTreeTupleSorter.sort();
        }

        isEmpty = true;
        try {
            while (btreeScanCursor.hasNext()) {
                isEmpty = false;
                btreeScanCursor.next();
                bTreeTupleSorter.insertTupleEntry(btreeScanCursor.getPageId(), btreeScanCursor.getTupleOffset());
            }
        } finally {
            btreeScanCursor.close();
        }
        if (!isEmpty) {
            bTreeTupleSorter.sort();
        }

        IIndexBulkLoader rTreeBulkloader = diskRTree.createBulkLoader(1.0f, false, 0L, false);
        LSMRTreeWithAntiMatterTuplesFlushCursor cursor = new LSMRTreeWithAntiMatterTuplesFlushCursor(rTreeTupleSorter,
                bTreeTupleSorter, comparatorFields, linearizerArray);
        cursor.open(null, null);

        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();

                rTreeBulkloader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }

        rTreeBulkloader.end();
        return component;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        LSMRTreeOpContext rctx = createOpContext(NoOpOperationCallback.INSTANCE);
        rctx.getComponentHolder().addAll(mergingComponents);
        ITreeIndexCursor cursor = new LSMRTreeWithAntiMatterTuplesSearchCursor(ctx);
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        search(rctx, cursor, (SearchPredicate) rtreeSearchPred);
        rctx.setOperation(IndexOperation.MERGE);
        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        ILSMIndexAccessorInternal accessor = new LSMRTreeWithAntiMatterTuplesAccessor(lsmHarness, rctx);
        ioScheduler.scheduleOperation(new LSMRTreeMergeOperation(accessor, mergingComponents, cursor, relMergeFileRefs
                .getInsertIndexFileReference(), null, null, callback));
    }

    @Override
    public ILSMComponent merge(List<ILSMComponent> mergedComponents, ILSMIOOperation operation)
            throws HyracksDataException, IndexException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        ITreeIndexCursor cursor = mergeOp.getCursor();
        mergedComponents.addAll(mergeOp.getMergingComponents());

        // Nothing to merge.
        if (mergedComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        // Bulk load the tuples from all on-disk RTrees into the new RTree.
        LSMRTreeImmutableComponent component = createDiskComponent(componentFactory, mergeOp.getRTreeMergeTarget(),
                null, null, true);
        RTree mergedRTree = component.getRTree();
        IIndexBulkLoader bulkloader = mergedRTree.createBulkLoader(1.0f, false, 0L, false);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                bulkloader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }
        bulkloader.end();
        return component;
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMRTreeWithAntiMatterTuplesAccessor(lsmHarness, createOpContext(modificationCallback));
    }

    public class LSMRTreeWithAntiMatterTuplesAccessor extends LSMTreeIndexAccessor {
        public LSMRTreeWithAntiMatterTuplesAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMRTreeWithAntiMatterTuplesSearchCursor(ctx);
        }

        public MultiComparator getMultiComparator() {
            LSMRTreeOpContext concreteCtx = (LSMRTreeOpContext) ctx;
            return concreteCtx.rtreeOpContext.cmp;
        }
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMRTreeWithAntiMatterTuplesBulkLoader(fillLevel, verifyInput, numElementsHint,
                    checkIfEmptyIndex);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    private ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences relFlushFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkLoaComponentFactory, relFlushFileRefs.getInsertIndexFileReference(), null, null,
                true);
    }

    public class LSMRTreeWithAntiMatterTuplesBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final IIndexBulkLoader bulkLoader;
        private boolean isEmptyComponent = true;

        public LSMRTreeWithAntiMatterTuplesBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex) throws TreeIndexException, HyracksDataException {
            if (checkIfEmptyIndex && !isEmptyIndex()) {
                throw new TreeIndexException("Cannot load an index that is not empty");
            }
            // Note that by using a flush target file name, we state that the
            // new bulk loaded tree is "newer" than any other merged tree.
            try {
                component = createBulkLoadTarget();
            } catch (HyracksDataException | IndexException e) {
                throw new TreeIndexException(e);
            }
            bulkLoader = ((LSMRTreeImmutableComponent) component).getRTree().createBulkLoader(fillFactor, verifyInput,
                    numElementsHint, false);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException, IndexException {
            if (isEmptyComponent) {
                isEmptyComponent = false;
            }
            try {
                bulkLoader.add(tuple);
            } catch (IndexException | HyracksDataException | RuntimeException e) {
                cleanupArtifacts();
                throw e;
            }
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            bulkLoader.end();
            if (isEmptyComponent) {
                cleanupArtifacts();
            } else {
                lsmHarness.addBulkLoadedComponent(component);
            }
        }

        protected void cleanupArtifacts() throws HyracksDataException {
            ((LSMRTreeImmutableComponent) component).getRTree().deactivate();
            ((LSMRTreeImmutableComponent) component).getRTree().destroy();
        }

    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        RTree rtree = ((LSMRTreeImmutableComponent) lsmComponent).getRTree();
        forceFlushDirtyPages(rtree);
        markAsValidInternal(rtree);
    }
}
