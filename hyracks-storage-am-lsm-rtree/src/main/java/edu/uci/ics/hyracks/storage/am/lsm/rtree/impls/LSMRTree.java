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
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
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
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTree extends AbstractLSMRTree {

    public LSMRTree(IInMemoryBufferCache memBufferCache, IInMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMIndexFileManager fileNameManager, TreeIndexFactory<RTree> diskRTreeFactory,
            TreeIndexFactory<BTree> diskBTreeFactory, IFileMapProvider diskFileMapProvider, int fieldCount,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILinearizeComparatorFactory linearizer, int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler) {
        super(memBufferCache, memFreePageManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, fileNameManager, diskRTreeFactory,
                new LSMRTreeComponentFactory(diskRTreeFactory, diskBTreeFactory), diskFileMapProvider, fieldCount,
                rtreeCmpFactories, btreeCmpFactories, linearizer, comparatorFields, linearizerArray, mergePolicy,
                opTrackerFactory, ioScheduler);
    }

    /**
     * Opens LSMRTree, cleaning up invalid files from base dir, and registering
     * all valid files as on-disk RTrees and BTrees.
     * 
     * @param fileReference
     *            Dummy file id.
     * @throws HyracksDataException
     */
    @Override
    public synchronized void activate() throws HyracksDataException {
        super.activate();
        List<LSMComponentFileReferences> validFileReferences;
        try {
            validFileReferences = fileManager.cleanupAndGetValidFiles();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        immutableComponents.clear();
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            LSMRTreeComponent component;
            try {
                component = createDiskComponent(componentFactory,
                        lsmComonentFileReference.getInsertIndexFileReference(),
                        lsmComonentFileReference.getDeleteIndexFileReference(), false);
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
            LSMRTreeComponent component = (LSMRTreeComponent) c;
            RTree rtree = component.getRTree();
            BTree btree = component.getBTree();
            rtree.deactivate();
            btree.deactivate();
        }
        isActivated = false;
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        super.destroy();
        for (ILSMComponent c : immutableComponents) {
            LSMRTreeComponent component = (LSMRTreeComponent) c;
            component.getBTree().destroy();
            component.getRTree().destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        super.clear();
        for (ILSMComponent c : immutableComponents) {
            LSMRTreeComponent component = (LSMRTreeComponent) c;
            component.getBTree().deactivate();
            component.getRTree().deactivate();
            component.getBTree().destroy();
            component.getRTree().destroy();
        }
        immutableComponents.clear();
    }

    @Override
    public void search(IIndexCursor cursor, List<ILSMComponent> immutableComponents, ISearchPredicate pred,
            IIndexOperationContext ictx, boolean includeMutableComponent) throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        int numDiskComponents = immutableComponents.size();
        int numTrees = (includeMutableComponent) ? numDiskComponents + 1 : numDiskComponents;

        ITreeIndexAccessor[] rTreeAccessors = new ITreeIndexAccessor[numTrees];
        ITreeIndexAccessor[] bTreeAccessors = new ITreeIndexAccessor[numTrees];
        int diskComponentIx = 0;
        if (includeMutableComponent) {
            rTreeAccessors[0] = ctx.memRTreeAccessor;
            bTreeAccessors[0] = ctx.memBTreeAccessor;
            diskComponentIx++;
        }

        ListIterator<ILSMComponent> diskComponentIter = immutableComponents.listIterator();
        while (diskComponentIter.hasNext()) {
            LSMRTreeComponent component = (LSMRTreeComponent) diskComponentIter.next();
            RTree diskRTree = component.getRTree();
            BTree diskBTree = component.getBTree();
            rTreeAccessors[diskComponentIx] = diskRTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            bTreeAccessors[diskComponentIx] = diskBTree.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            diskComponentIx++;
        }

        List<ILSMComponent> operationalComponents = new ArrayList<ILSMComponent>();
        if (includeMutableComponent) {
            operationalComponents.add(getMutableComponent());
        }
        operationalComponents.addAll(immutableComponents);
        LSMRTreeCursorInitialState initialState = new LSMRTreeCursorInitialState(numTrees, rtreeLeafFrameFactory,
                rtreeInteriorFrameFactory, btreeLeafFrameFactory, ctx.getBTreeMultiComparator(), rTreeAccessors,
                bTreeAccessors, includeMutableComponent, lsmHarness, comparatorFields, linearizerArray,
                ctx.searchCallback, operationalComponents);
        cursor.open(initialState, pred);
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMRTreeFlushOperation flushOp = (LSMRTreeFlushOperation) operation;
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.

        // scan the memory RTree
        ITreeIndexAccessor memRTreeAccessor = mutableComponent.getRTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        RTreeSearchCursor rtreeScanCursor = (RTreeSearchCursor) memRTreeAccessor.createSearchCursor();
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
        LSMRTreeComponent component = createDiskComponent(componentFactory, flushOp.getRTreeFlushTarget(),
                flushOp.getBTreeFlushTarget(), true);
        RTree diskRTree = component.getRTree();
        IIndexBulkLoader rTreeBulkloader;
        ITreeIndexCursor cursor;

        IBinaryComparatorFactory[] linearizerArray = { linearizer };

        if (rTreeTupleSorter == null) {
            rTreeTupleSorter = new TreeTupleSorter(memRTreeTuples, mutableComponent.getRTree().getFileId(),
                    linearizerArray, rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(),
                    mutableComponent.getRTree().getBufferCache(), comparatorFields);
        } else {
            rTreeTupleSorter.reset();
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
        rTreeBulkloader = diskRTree.createBulkLoader(1.0f, false);
        cursor = rTreeTupleSorter;

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

        // scan the memory BTree
        ITreeIndexAccessor memBTreeAccessor = mutableComponent.getBTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IIndexCursor btreeScanCursor = memBTreeAccessor.createSearchCursor();
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);
        BTree diskBTree = component.getBTree();

        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoader bTreeBulkloader = diskBTree.createBulkLoader(1.0f, false);
        try {
            while (btreeScanCursor.hasNext()) {
                btreeScanCursor.next();
                ITupleReference frameTuple = btreeScanCursor.getTuple();
                bTreeBulkloader.add(frameTuple);
            }
        } finally {
            btreeScanCursor.close();
        }
        bTreeBulkloader.end();
        return new LSMRTreeComponent(diskRTree, diskBTree);
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
        LSMRTreeComponent component = createDiskComponent(componentFactory, mergeOp.getRTreeMergeTarget(),
                mergeOp.getBTreeMergeTarget(), true);
        RTree mergedRTree = component.getRTree();
        BTree mergedBTree = component.getBTree();

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

        // Load an empty BTree tree.
        mergedBTree.createBulkLoader(1.0f, false).end();

        return new LSMRTreeComponent(mergedRTree, mergedBTree);
    }

    @Override
    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException {
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.
        ILSMIndexOperationContext ctx = createOpContext();
        ctx.setOperation(IndexOperation.MERGE);
        ITreeIndexCursor cursor;
        cursor = new LSMRTreeSortedCursor(ctx, linearizer);
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        // Scan the RTrees, ignoring the in-memory RTree.
        List<ILSMComponent> mergingComponents;
        try {
            mergingComponents = lsmHarness.search(cursor, rtreeSearchPred, ctx, false);
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        // Nothing to merge.
        if (mergingComponents.size() <= 1) {
            cursor.close();
            return null;
        }
        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        return new LSMRTreeMergeOperation(lsmHarness.getIndex(), mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getDeleteIndexFileReference(),
                callback);
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMRTreeAccessor(lsmHarness, createOpContext());
    }

    public class LSMRTreeAccessor extends LSMTreeIndexAccessor {
        public LSMRTreeAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMRTreeSearchCursor(ctx);
        }

        public MultiComparator getMultiComparator() {
            LSMRTreeOpContext concreteCtx = (LSMRTreeOpContext) ctx;
            return concreteCtx.rtreeOpContext.cmp;
        }

        @Override
        public ILSMIOOperation createFlushOperation(ILSMIOOperationCallback callback) {
            LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
            return new LSMRTreeFlushOperation(lsmHarness.getIndex(), componentFileRefs.getInsertIndexFileReference(),
                    componentFileRefs.getDeleteIndexFileReference(), callback);
        }
    }

    private ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(componentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), true);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput) throws TreeIndexException {
        return new LSMRTreeBulkLoader(fillLevel, verifyInput);
    }

    public class LSMRTreeBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final IIndexBulkLoader bulkLoader;

        public LSMRTreeBulkLoader(float fillFactor, boolean verifyInput) throws TreeIndexException {
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
            ((LSMRTreeComponent) component).getBTree().deactivate();
            ((LSMRTreeComponent) component).getBTree().destroy();
        }
    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        LSMRTreeComponent component = (LSMRTreeComponent) lsmComponent;
        forceFlushDirtyPages(component.getRTree());
        markAsValidInternal(component.getRTree());
        forceFlushDirtyPages(component.getBTree());
        markAsValidInternal(component.getBTree());
    }
}
