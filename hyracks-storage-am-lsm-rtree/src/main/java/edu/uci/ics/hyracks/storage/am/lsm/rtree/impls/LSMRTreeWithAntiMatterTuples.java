/*
 * Copyright 2009-2012 by The Regents of the University of California
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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
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
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMFlushOperation;
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

    public LSMRTreeWithAntiMatterTuples(IInMemoryBufferCache memBufferCache,
            IInMemoryFreePageManager memFreePageManager, ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ILSMIndexFileManager fileManager,
            TreeIndexFactory<RTree> diskRTreeFactory, TreeIndexFactory<RTree> bulkLoadRTreeFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMMergePolicy mergePolicy,
            ILSMOperationTrackerFactory opTrackerFactory, ILSMIOOperationScheduler ioScheduler) {
        super(memBufferCache, memFreePageManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, fileManager, diskRTreeFactory,
                new LSMRTreeWithAntiMatterTuplesComponentFactory(diskRTreeFactory), diskFileMapProvider, fieldCount,
                rtreeCmpFactories, btreeCmpFactories, linearizer, comparatorFields, linearizerArray, mergePolicy,
                opTrackerFactory, ioScheduler);
        bulkLoaComponentFactory = new LSMRTreeWithAntiMatterTuplesComponentFactory(bulkLoadRTreeFactory);
        this.bTreeTupleSorter = null;
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        super.activate();
        immutableComponents.clear();
        List<LSMComponentFileReferences> validFileReferences;
        try {
            validFileReferences = fileManager.cleanupAndGetValidFiles();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            LSMRTreeComponent component;
            try {
                component = createDiskComponent(componentFactory,
                        lsmComonentFileReference.getInsertIndexFileReference(), null, false);
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            immutableComponents.add(component);
        }
        isActivated = true;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        super.deactivate();
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = (RTree) ((LSMRTreeComponent) c).getRTree();
            rtree.deactivate();
        }
        isActivated = false;
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        super.destroy();
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = (RTree) ((LSMRTreeComponent) c).getRTree();
            rtree.destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        super.clear();
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = (RTree) ((LSMRTreeComponent) c).getRTree();
            rtree.deactivate();
            rtree.destroy();
        }
        immutableComponents.clear();
    }

    @Override
    public void search(IIndexCursor cursor, List<ILSMComponent> immutableComponents, ISearchPredicate pred,
            IIndexOperationContext ictx, boolean includeMutableComponent) throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        LSMRTreeWithAntiMatterTuplesSearchCursor lsmTreeCursor = (LSMRTreeWithAntiMatterTuplesSearchCursor) cursor;
        int numDiskRComponents = immutableComponents.size();

        LSMRTreeCursorInitialState initialState;
        ITreeIndexAccessor[] bTreeAccessors = null;
        if (includeMutableComponent) {
            // Only in-memory BTree
            bTreeAccessors = new ITreeIndexAccessor[1];
            bTreeAccessors[0] = ctx.memBTreeAccessor;
        }

        List<ILSMComponent> operationalComponents = new ArrayList<ILSMComponent>();
        if (includeMutableComponent) {
            operationalComponents.add(getMutableComponent());
        }
        operationalComponents.addAll(immutableComponents);
        initialState = new LSMRTreeCursorInitialState(numDiskRComponents, rtreeLeafFrameFactory,
                rtreeInteriorFrameFactory, btreeLeafFrameFactory, ctx.getBTreeMultiComparator(), null, bTreeAccessors,
                includeMutableComponent, lsmHarness, comparatorFields, linearizerArray, ctx.searchCallback,
                operationalComponents);

        lsmTreeCursor.open(initialState, pred);

        if (includeMutableComponent) {
            // Open cursor of in-memory RTree
            ctx.memRTreeAccessor.search(lsmTreeCursor.getMemRTreeCursor(), pred);
        }

        // Open cursors of on-disk RTrees.
        ITreeIndexAccessor[] diskRTreeAccessors = new ITreeIndexAccessor[numDiskRComponents];
        int diskComponentIx = 0;
        ListIterator<ILSMComponent> diskComponentsIter = immutableComponents.listIterator();
        while (diskComponentsIter.hasNext()) {
            RTree diskRTree = (RTree) ((LSMRTreeComponent) diskComponentsIter.next()).getRTree();
            diskRTreeAccessors[diskComponentIx] = diskRTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            diskRTreeAccessors[diskComponentIx].search(lsmTreeCursor.getCursor(diskComponentIx), pred);
            diskComponentIx++;
        }
        lsmTreeCursor.initPriorityQueue();
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMFlushOperation flushOp = (LSMFlushOperation) operation;
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.

        // scan the memory RTree
        ITreeIndexAccessor memRTreeAccessor = mutableComponent.getRTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        RTreeSearchCursor rtreeScanCursor = (RTreeSearchCursor) memRTreeAccessor.createSearchCursor();
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
        LSMRTreeComponent component = createDiskComponent(componentFactory, flushOp.getFlushTarget(), null, true);
        RTree diskRTree = component.getRTree();

        // scan the memory BTree
        ITreeIndexAccessor memBTreeAccessor = mutableComponent.getBTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        BTreeRangeSearchCursor btreeScanCursor = (BTreeRangeSearchCursor) memBTreeAccessor.createSearchCursor();
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);

        // Since the LSM-RTree is used as a secondary assumption, the
        // primary key will be the last comparator in the BTree comparators
        if (rTreeTupleSorter == null) {
            rTreeTupleSorter = new TreeTupleSorter(memRTreeTuples, mutableComponent.getRTree().getFileId(),
                    linearizerArray, rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(),
                    mutableComponent.getRTree().getBufferCache(), comparatorFields);

            bTreeTupleSorter = new TreeTupleSorter(memBTreeTuples, mutableComponent.getBTree().getFileId(),
                    linearizerArray, btreeLeafFrameFactory.createFrame(), btreeLeafFrameFactory.createFrame(),
                    mutableComponent.getBTree().getBufferCache(), comparatorFields);
        } else {
            rTreeTupleSorter.reset();
            bTreeTupleSorter.reset();
        }
        // BulkLoad the tuples from the in-memory tree into the new disk
        // RTree.

        boolean isEmpty = true;
        if (rtreeScanCursor.hasNext()) {
            isEmpty = false;
        }
        try {
            while (rtreeScanCursor.hasNext()) {
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
        if (btreeScanCursor.hasNext()) {
            isEmpty = false;
        }
        try {
            while (btreeScanCursor.hasNext()) {
                btreeScanCursor.next();
                bTreeTupleSorter.insertTupleEntry(btreeScanCursor.getPageId(), btreeScanCursor.getTupleOffset());
            }
        } finally {
            btreeScanCursor.close();
        }
        if (!isEmpty) {
            bTreeTupleSorter.sort();
        }

        IIndexBulkLoader rTreeBulkloader = diskRTree.createBulkLoader(1.0f, false);
        LSMRTreeFlushCursor cursor = new LSMRTreeFlushCursor(rTreeTupleSorter, bTreeTupleSorter, comparatorFields,
                linearizerArray);
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
        LSMRTreeComponent component = createDiskComponent(componentFactory, mergeOp.getRTreeMergeTarget(), null, true);
        RTree mergedRTree = component.getRTree();
        IIndexBulkLoader bulkloader = mergedRTree.createBulkLoader(1.0f, false);
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
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMRTreeWithAntiMatterTuplesAccessor(lsmHarness, createOpContext());
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

        @Override
        public void scheduleFlush(ILSMIOOperationCallback callback) throws HyracksDataException {
            LSMComponentFileReferences relFlushFileRefs = fileManager.getRelFlushFileReference();
            lsmHarness.getIOScheduler().scheduleOperation(
                    new LSMFlushOperation(lsmHarness.getIndex(), relFlushFileRefs.getInsertIndexFileReference(),
                            callback));
        }
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput) throws TreeIndexException {
        return new LSMRTreeWithAntiMatterTuplesBulkLoader(fillLevel, verifyInput);
    }

    private ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences relFlushFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkLoaComponentFactory, relFlushFileRefs.getInsertIndexFileReference(), null, true);
    }

    public class LSMRTreeWithAntiMatterTuplesBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final IIndexBulkLoader bulkLoader;

        public LSMRTreeWithAntiMatterTuplesBulkLoader(float fillFactor, boolean verifyInput) throws TreeIndexException {
            // Note that by using a flush target file name, we state that the
            // new bulk loaded tree is "newer" than any other merged tree.
            try {
                component = createBulkLoadTarget();
            } catch (HyracksDataException e) {
                throw new TreeIndexException(e);
            } catch (IndexException e) {
                throw new TreeIndexException(e);
            }
            bulkLoader = ((LSMRTreeComponent) component).getRTree().createBulkLoader(fillFactor, verifyInput);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException, IndexException {
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

        @Override
        public void end() throws HyracksDataException, IndexException {
            bulkLoader.end();
            lsmHarness.addBulkLoadedComponent(component);
        }

        protected void handleException() throws HyracksDataException {
            ((LSMRTreeComponent) component).getRTree().deactivate();
            ((LSMRTreeComponent) component).getRTree().destroy();
        }

    }

    @Override
    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException {
        LSMRTreeOpContext ctx = createOpContext();
        ctx.setOperation(IndexOperation.MERGE);
        ITreeIndexCursor cursor = new LSMRTreeWithAntiMatterTuplesSearchCursor(ctx);
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        // Ordered scan, ignoring the in-memory RTree.
        // We get back a snapshot of the on-disk RTrees that are going to be
        // merged now, so we can clean them up after the merge has completed.
        List<ILSMComponent> mergingComponents;
        try {
            mergingComponents = lsmHarness.search(cursor, (SearchPredicate) rtreeSearchPred, ctx, false);
            if (mergingComponents.size() <= 1) {
                cursor.close();
                return null;
            }
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }

        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        return new LSMRTreeMergeOperation(lsmHarness.getIndex(), mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), null, callback);
    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        RTree rtree = ((LSMRTreeComponent) lsmComponent).getRTree();
        forceFlushDirtyPages(rtree);
        markAsValidInternal(rtree);
    }
}
