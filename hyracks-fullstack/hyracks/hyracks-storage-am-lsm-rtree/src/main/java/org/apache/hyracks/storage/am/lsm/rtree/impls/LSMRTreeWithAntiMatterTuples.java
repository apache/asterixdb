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

package org.apache.hyracks.storage.am.lsm.rtree.impls;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTreeWithAntiMatterTuples extends AbstractLSMRTree {
    // On-disk components.
    // For creating RTree's used in bulk load. Different from diskRTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final ILSMComponentFactory bulkLoaComponentFactory;

    public LSMRTreeWithAntiMatterTuples(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMIndexFileManager fileManager, TreeIndexFactory<RTree> diskRTreeFactory,
            TreeIndexFactory<RTree> bulkLoadRTreeFactory, ILSMComponentFilterFactory filterFactory,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            int[] rtreeFields, int[] filterFields, boolean durable, boolean isPointMBR) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory,
                btreeLeafFrameFactory, fileManager,
                new LSMRTreeWithAntiMatterTuplesDiskComponentFactory(diskRTreeFactory, filterFactory),
                diskFileMapProvider, fieldCount, rtreeCmpFactories, btreeCmpFactories, linearizer, comparatorFields,
                linearizerArray, 0, mergePolicy, opTracker, ioScheduler, ioOpCallback, filterFactory,
                filterFrameFactory, filterManager, rtreeFields, filterFields, durable, isPointMBR);
        bulkLoaComponentFactory = new LSMRTreeWithAntiMatterTuplesDiskComponentFactory(bulkLoadRTreeFactory,
                filterFactory);
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        super.activate();
        List<ILSMComponent> immutableComponents = diskComponents;
        immutableComponents.clear();
        List<LSMComponentFileReferences> validFileReferences;
        try {
            validFileReferences = fileManager.cleanupAndGetValidFiles();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            LSMRTreeDiskComponent component;
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
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = ((LSMRTreeDiskComponent) c).getRTree();
            rtree.deactivateCloseHandle();
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
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = ((LSMRTreeDiskComponent) c).getRTree();
            rtree.destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        super.clear();
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            RTree rtree = ((LSMRTreeDiskComponent) c).getRTree();
            rtree.deactivate();
            rtree.destroy();
        }
        immutableComponents.clear();
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
        LSMRTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        LSMComponentFileReferences relFlushFileRefs = fileManager.getRelFlushFileReference();
        opCtx.setOperation(IndexOperation.FLUSH);
        opCtx.getComponentHolder().add(flushingComponent);
        ILSMIndexAccessorInternal accessor = new LSMRTreeWithAntiMatterTuplesAccessor(lsmHarness, opCtx);
        ioScheduler.scheduleOperation(new LSMRTreeFlushOperation(accessor, flushingComponent,
                relFlushFileRefs.getInsertIndexFileReference(), null, null, callback, fileManager.getBaseDir()));
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMRTreeFlushOperation flushOp = (LSMRTreeFlushOperation) operation;
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.
        LSMRTreeMemoryComponent flushingComponent = (LSMRTreeMemoryComponent) flushOp.getFlushingComponent();
        ITreeIndexAccessor memRTreeAccessor = flushingComponent.getRTree()
                .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        RTreeSearchCursor rtreeScanCursor = (RTreeSearchCursor) memRTreeAccessor.createSearchCursor(false);
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
        LSMRTreeDiskComponent component = createDiskComponent(componentFactory, flushOp.getRTreeFlushTarget(), null,
                null, true);
        RTree diskRTree = component.getRTree();

        // scan the memory BTree
        ITreeIndexAccessor memBTreeAccessor = flushingComponent.getBTree()
                .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        BTreeRangeSearchCursor btreeScanCursor = (BTreeRangeSearchCursor) memBTreeAccessor.createSearchCursor(false);
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);

        // Since the LSM-RTree is used as a secondary assumption, the
        // primary key will be the last comparator in the BTree comparators
        TreeTupleSorter rTreeTupleSorter = new TreeTupleSorter(flushingComponent.getRTree().getFileId(),
                linearizerArray, rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(),
                flushingComponent.getRTree().getBufferCache(), comparatorFields);

        TreeTupleSorter bTreeTupleSorter = new TreeTupleSorter(flushingComponent.getBTree().getFileId(),
                linearizerArray, btreeLeafFrameFactory.createFrame(), btreeLeafFrameFactory.createFrame(),
                flushingComponent.getBTree().getBufferCache(), comparatorFields);
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

        IIndexBulkLoader rTreeBulkloader = diskRTree.createBulkLoader(1.0f, false, 0L, false, true);
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

        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            filterManager.updateFilterInfo(component.getLSMComponentFilter(), filterTuples);
            filterManager.writeFilterInfo(component.getLSMComponentFilter(), component.getRTree());
        }
        rTreeBulkloader.end();

        return component;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        LSMRTreeOpContext rctx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        rctx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        boolean returnDeletedTuples = false;
        if (ctx.getComponentHolder().get(ctx.getComponentHolder().size() - 1) != diskComponents
                .get(diskComponents.size() - 1)) {
            returnDeletedTuples = true;
        }
        ITreeIndexCursor cursor = new LSMRTreeWithAntiMatterTuplesSearchCursor(rctx, returnDeletedTuples);
        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        ILSMIndexAccessorInternal accessor = new LSMRTreeWithAntiMatterTuplesAccessor(lsmHarness, rctx);
        ioScheduler.scheduleOperation(new LSMRTreeMergeOperation(accessor, mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), null, null, callback, fileManager.getBaseDir()));
    }

    @Override
    public ILSMComponent merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        ITreeIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        opCtx.getComponentHolder().addAll(mergeOp.getMergingComponents());
        search(opCtx, cursor, rtreeSearchPred);

        // Bulk load the tuples from all on-disk RTrees into the new RTree.
        LSMRTreeDiskComponent component = createDiskComponent(componentFactory, mergeOp.getRTreeMergeTarget(), null,
                null, true);
        RTree mergedRTree = component.getRTree();
        IIndexBulkLoader bulkloader = mergedRTree.createBulkLoader(1.0f, false, 0L, false, true);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                bulkloader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }
        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
            }
            filterManager.updateFilterInfo(component.getLSMComponentFilter(), filterTuples);
            filterManager.writeFilterInfo(component.getLSMComponentFilter(), component.getRTree());
        }
        bulkloader.end();

        return component;
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMRTreeWithAntiMatterTuplesAccessor(lsmHarness,
                createOpContext(modificationCallback, searchCallback));
    }

    public class LSMRTreeWithAntiMatterTuplesAccessor extends LSMTreeIndexAccessor {
        public LSMRTreeWithAntiMatterTuplesAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor(boolean exclusive) {
            return new LSMRTreeWithAntiMatterTuplesSearchCursor(ctx);
        }

        public MultiComparator getMultiComparator() {
            LSMRTreeOpContext concreteCtx = (LSMRTreeOpContext) ctx;
            return concreteCtx.currentRTreeOpContext.cmp;
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
        private boolean cleanedUpArtifacts = false;
        private boolean isEmptyComponent = true;
        public final PermutingTupleReference indexTuple;
        public final PermutingTupleReference filterTuple;
        public final MultiComparator filterCmp;

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
            bulkLoader = ((LSMRTreeDiskComponent) component).getRTree().createBulkLoader(fillFactor, verifyInput,
                    numElementsHint, false, true);

            if (filterFields != null) {
                indexTuple = new PermutingTupleReference(rtreeFields);
                filterCmp = MultiComparator.create(component.getLSMComponentFilter().getFilterCmpFactories());
                filterTuple = new PermutingTupleReference(filterFields);
            } else {
                indexTuple = null;
                filterCmp = null;
                filterTuple = null;
            }
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException, IndexException {
            try {
                ITupleReference t;
                if (indexTuple != null) {
                    indexTuple.reset(tuple);
                    t = indexTuple;
                } else {
                    t = tuple;
                }

                bulkLoader.add(t);

                if (filterTuple != null) {
                    filterTuple.reset(tuple);
                    component.getLSMComponentFilter().update(filterTuple, filterCmp);
                }

            } catch (IndexException | HyracksDataException | RuntimeException e) {
                cleanupArtifacts();
                throw e;
            }
            if (isEmptyComponent) {
                isEmptyComponent = false;
            }
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            if (!cleanedUpArtifacts) {

                if (component.getLSMComponentFilter() != null) {
                    filterManager.writeFilterInfo(component.getLSMComponentFilter(),
                            ((LSMRTreeDiskComponent) component).getRTree());
                }
                bulkLoader.end();

                if (isEmptyComponent) {
                    cleanupArtifacts();
                } else {
                    lsmHarness.addBulkLoadedComponent(component);
                }
            }
        }

        @Override
        public void abort() throws HyracksDataException {
            if (bulkLoader != null) {
                bulkLoader.abort();
            }
        }

        protected void cleanupArtifacts() throws HyracksDataException {
            if (!cleanedUpArtifacts) {
                cleanedUpArtifacts = true;
                ((LSMRTreeDiskComponent) component).getRTree().deactivate();
                ((LSMRTreeDiskComponent) component).getRTree().destroy();
            }
        }

    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        RTree rtree = ((LSMRTreeDiskComponent) lsmComponent).getRTree();
        markAsValidInternal(rtree);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, boolean appendOnly) throws IndexException {
        if (!appendOnly) {
            throw new UnsupportedOperationException("LSM indexes don't support in-place modification");
        }
        return createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles(ILSMComponent lsmComponent) {
        Set<String> files = new HashSet<>();
        RTree rtree = ((LSMRTreeDiskComponent) lsmComponent).getRTree();
        files.add(rtree.getFileReference().getFile().getAbsolutePath());
        return files;
    }
}
