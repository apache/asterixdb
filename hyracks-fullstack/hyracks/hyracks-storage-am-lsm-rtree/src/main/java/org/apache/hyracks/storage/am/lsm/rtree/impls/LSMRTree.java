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
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.DualTupleReference;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
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
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTree extends AbstractLSMRTree {
    protected int[] buddyBTreeFields;

    public LSMRTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ILSMIndexFileManager fileNameManager,
            TreeIndexFactory<RTree> diskRTreeFactory, TreeIndexFactory<BTree> diskBTreeFactory,
            BloomFilterFactory bloomFilterFactory, ILSMComponentFilterFactory filterFactory,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, IFileMapProvider diskFileMapProvider, int fieldCount,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILinearizeComparatorFactory linearizer, int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, int[] rtreeFields, int[] buddyBTreeFields, int[] filterFields,
            boolean durable, boolean isPointMBR) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory,
                btreeLeafFrameFactory, fileNameManager,
                new LSMRTreeDiskComponentFactory(diskRTreeFactory, diskBTreeFactory, bloomFilterFactory, filterFactory),
                diskFileMapProvider, fieldCount, rtreeCmpFactories, btreeCmpFactories, linearizer, comparatorFields,
                linearizerArray, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallback,
                filterFactory, filterFrameFactory, filterManager, rtreeFields, filterFields, durable, isPointMBR);
        this.buddyBTreeFields = buddyBTreeFields;
    }

    /*
     * For External indexes with no memory components
     */
    public LSMRTree(IIOManager ioManager, ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMIndexFileManager fileNameManager, TreeIndexFactory<RTree> diskRTreeFactory,
            TreeIndexFactory<BTree> diskBTreeFactory, BloomFilterFactory bloomFilterFactory,
            double bloomFilterFalsePositiveRate, IFileMapProvider diskFileMapProvider, int fieldCount,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILinearizeComparatorFactory linearizer, int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, int[] buddyBTreeFields, boolean durable, boolean isPointMBR) {
        super(ioManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, btreeInteriorFrameFactory,
                btreeLeafFrameFactory,
                fileNameManager,
                new LSMRTreeDiskComponentFactory(diskRTreeFactory, diskBTreeFactory, bloomFilterFactory, null),
                diskFileMapProvider, fieldCount, rtreeCmpFactories, btreeCmpFactories, linearizer, comparatorFields,
                linearizerArray, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallback,
                durable, isPointMBR);
        this.buddyBTreeFields = buddyBTreeFields;
    }

    /**
     * Opens LSMRTree, cleaning up invalid files from base dir, and registering
     * all valid files as on-disk RTrees and BTrees.
     * 
     * @throws HyracksDataException
     */
    @Override
    public synchronized void activate() throws HyracksDataException {
        super.activate();
        List<ILSMComponent> immutableComponents = diskComponents;
        List<LSMComponentFileReferences> validFileReferences;
        try {
            validFileReferences = fileManager.cleanupAndGetValidFiles();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        immutableComponents.clear();
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            LSMRTreeDiskComponent component;
            try {
                component = createDiskComponent(componentFactory,
                        lsmComonentFileReference.getInsertIndexFileReference(),
                        lsmComonentFileReference.getDeleteIndexFileReference(),
                        lsmComonentFileReference.getBloomFilterFileReference(), false);
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
            LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
            RTree rtree = component.getRTree();
            BTree btree = component.getBTree();
            BloomFilter bloomFilter = component.getBloomFilter();
            rtree.deactivateCloseHandle();
            btree.deactivateCloseHandle();
            bloomFilter.deactivate();
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
            LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
            component.getRTree().destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        super.clear();
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
            component.getBTree().deactivate();
            component.getBloomFilter().deactivate();
            component.getRTree().deactivate();
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
            component.getRTree().destroy();
        }
        immutableComponents.clear();
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        ILSMIndexOperationContext rctx = createOpContext(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        rctx.setOperation(IndexOperation.FLUSH);
        rctx.getComponentHolder().addAll(ctx.getComponentHolder());
        LSMRTreeAccessor accessor = new LSMRTreeAccessor(lsmHarness, rctx);
        ioScheduler.scheduleOperation(new LSMRTreeFlushOperation(accessor, flushingComponent,
                componentFileRefs.getInsertIndexFileReference(), componentFileRefs.getDeleteIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir()));
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMRTreeFlushOperation flushOp = (LSMRTreeFlushOperation) operation;
        LSMRTreeMemoryComponent flushingComponent = (LSMRTreeMemoryComponent) flushOp.getFlushingComponent();
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.

        // scan the memory RTree
        ITreeIndexAccessor memRTreeAccessor = flushingComponent.getRTree()
                .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        RTreeSearchCursor rtreeScanCursor = (RTreeSearchCursor) memRTreeAccessor.createSearchCursor(false);
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
        LSMRTreeDiskComponent component = createDiskComponent(componentFactory, flushOp.getRTreeFlushTarget(),
                flushOp.getBTreeFlushTarget(), flushOp.getBloomFilterFlushTarget(), true);
        RTree diskRTree = component.getRTree();
        IIndexBulkLoader rTreeBulkloader;
        ITreeIndexCursor cursor;

        IBinaryComparatorFactory[] linearizerArray = { linearizer };

        TreeTupleSorter rTreeTupleSorter = new TreeTupleSorter(flushingComponent.getRTree().getFileId(),
                linearizerArray, rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(),
                flushingComponent.getRTree().getBufferCache(), comparatorFields);
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
        rTreeTupleSorter.sort();

        rTreeBulkloader = diskRTree.createBulkLoader(1.0f, false, 0L, false, true);
        cursor = rTreeTupleSorter;

        if (!isEmpty) {
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference frameTuple = cursor.getTuple();
                    rTreeBulkloader.add(frameTuple);
                }
            } finally {
                cursor.close();
            }
        }

        rTreeBulkloader.end();

        ITreeIndexAccessor memBTreeAccessor = flushingComponent.getBTree()
                .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        IIndexCursor btreeCountingCursor = ((BTreeAccessor) memBTreeAccessor).createCountingSearchCursor();
        memBTreeAccessor.search(btreeCountingCursor, btreeNullPredicate);
        long numBTreeTuples = 0L;
        try {
            while (btreeCountingCursor.hasNext()) {
                btreeCountingCursor.next();
                ITupleReference countTuple = btreeCountingCursor.getTuple();
                numBTreeTuples = IntegerPointable.getInteger(countTuple.getFieldData(0), countTuple.getFieldStart(0));
            }
        } finally {
            btreeCountingCursor.close();
        }

        int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numBTreeTuples);
        BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                bloomFilterFalsePositiveRate);

        IIndexCursor btreeScanCursor = memBTreeAccessor.createSearchCursor(false);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);
        BTree diskBTree = component.getBTree();

        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoader bTreeBulkloader = diskBTree.createBulkLoader(1.0f, false, numBTreeTuples, false, true);
        IIndexBulkLoader builder = component.getBloomFilter().createBuilder(numBTreeTuples,
                bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        // scan the memory BTree
        try {
            while (btreeScanCursor.hasNext()) {
                btreeScanCursor.next();
                ITupleReference frameTuple = btreeScanCursor.getTuple();
                bTreeBulkloader.add(frameTuple);
                builder.add(frameTuple);
            }
        } finally {
            btreeScanCursor.close();
            builder.end();
        }

        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            filterManager.updateFilterInfo(component.getLSMComponentFilter(), filterTuples);
            filterManager.writeFilterInfo(component.getLSMComponentFilter(), component.getRTree());
        }

        bTreeBulkloader.end();

        return component;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        ILSMIndexOperationContext rctx = createOpContext(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        rctx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        ITreeIndexCursor cursor = new LSMRTreeSortedCursor(rctx, linearizer, buddyBTreeFields);
        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        ILSMIndexAccessorInternal accessor = new LSMRTreeAccessor(lsmHarness, rctx);
        ioScheduler.scheduleOperation(new LSMRTreeMergeOperation(accessor, mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getDeleteIndexFileReference(),
                relMergeFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir()));
    }

    @Override
    public ILSMComponent merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        ITreeIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMRTreeSortedCursor) cursor).getOpCtx();
        opCtx.getComponentHolder().addAll(mergeOp.getMergingComponents());
        search(opCtx, cursor, rtreeSearchPred);

        LSMRTreeDiskComponent mergedComponent = createDiskComponent(componentFactory, mergeOp.getRTreeMergeTarget(),
                mergeOp.getBTreeMergeTarget(), mergeOp.getBloomFilterMergeTarget(), true);

        // In case we must keep the deleted-keys BTrees, then they must be merged *before* merging the r-trees so that
        // lsmHarness.endSearch() is called once when the r-trees have been merged.
        BTree btree = mergedComponent.getBTree();
        IIndexBulkLoader btreeBulkLoader = btree.createBulkLoader(1.0f, true, 0L, false, true);
        if (mergeOp.getMergingComponents().get(mergeOp.getMergingComponents().size() - 1) != diskComponents
                .get(diskComponents.size() - 1)) {
            // Keep the deleted tuples since the oldest disk component is not included in the merge operation

            LSMRTreeDeletedKeysBTreeMergeCursor btreeCursor = new LSMRTreeDeletedKeysBTreeMergeCursor(opCtx);
            search(opCtx, btreeCursor, rtreeSearchPred);

            long numElements = 0L;
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                numElements += ((LSMRTreeDiskComponent) mergeOp.getMergingComponents().get(i)).getBloomFilter()
                        .getNumElements();
            }

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            IIndexBulkLoader builder = mergedComponent.getBloomFilter().createBuilder(numElements,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());

            try {
                while (btreeCursor.hasNext()) {
                    btreeCursor.next();
                    ITupleReference tuple = btreeCursor.getTuple();
                    btreeBulkLoader.add(tuple);
                    builder.add(tuple);
                }
            } finally {
                btreeCursor.close();
                builder.end();
            }
        }

        if (mergedComponent.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
            }
            filterManager.updateFilterInfo(mergedComponent.getLSMComponentFilter(), filterTuples);
            filterManager.writeFilterInfo(mergedComponent.getLSMComponentFilter(), mergedComponent.getRTree());
        }
        btreeBulkLoader.end();

        IIndexBulkLoader bulkLoader = mergedComponent.getRTree().createBulkLoader(1.0f, false, 0L, false, true);
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

        return mergedComponent;
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMRTreeAccessor(lsmHarness, createOpContext(modificationCallback, searchCallback));
    }

    public class LSMRTreeAccessor extends LSMTreeIndexAccessor {
        private final DualTupleReference dualTuple;

        public LSMRTreeAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
            super(lsmHarness, ctx);
            dualTuple = new DualTupleReference(buddyBTreeFields);
        }

        @Override
        public ITreeIndexCursor createSearchCursor(boolean exclusive) {
            return new LSMRTreeSearchCursor(ctx, buddyBTreeFields);
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, IndexException {
            ctx.setOperation(IndexOperation.DELETE);
            dualTuple.reset(tuple);
            lsmHarness.modify(ctx, false, dualTuple);
        }

        @Override
        public boolean tryDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
            ctx.setOperation(IndexOperation.DELETE);
            dualTuple.reset(tuple);
            return lsmHarness.modify(ctx, true, dualTuple);
        }

        @Override
        public void forceDelete(ITupleReference tuple) throws HyracksDataException, IndexException {
            ctx.setOperation(IndexOperation.DELETE);
            dualTuple.reset(tuple);
            lsmHarness.forceModify(ctx, dualTuple);
        }

        public MultiComparator getMultiComparator() {
            LSMRTreeOpContext concreteCtx = (LSMRTreeOpContext) ctx;
            return concreteCtx.currentRTreeOpContext.cmp;
        }
    }

    protected ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(componentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), true);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMRTreeBulkLoader(fillLevel, verifyInput, numElementsHint, checkIfEmptyIndex);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    // This function is modified for R-Trees without antimatter tuples to allow buddy B-Tree to have only primary keys
    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        if (ctx.getOperation() == IndexOperation.PHYSICALDELETE) {
            throw new UnsupportedOperationException("Physical delete not supported in the LSM-RTree");
        }

        ITupleReference indexTuple;
        if (ctx.indexTuple != null) {
            ctx.indexTuple.reset(tuple);
            indexTuple = ctx.indexTuple;
        } else {
            indexTuple = tuple;
        }

        ctx.getModificationCallback().before(indexTuple);
        ctx.getModificationCallback().found(null, indexTuple);
        if (ctx.getOperation() == IndexOperation.INSERT) {
            ctx.currentMutableRTreeAccessor.insert(indexTuple);
        } else {
            // First remove all entries in the in-memory rtree (if any).
            ctx.currentMutableRTreeAccessor.delete(indexTuple);
            try {
                ctx.currentMutableBTreeAccessor.insert(((DualTupleReference) tuple).getPermutingTuple());
            } catch (TreeIndexDuplicateKeyException e) {
                // Do nothing, because one delete tuple is enough to indicate
                // that all the corresponding insert tuples are deleted
            }
        }
        if (ctx.filterTuple != null) {
            ctx.filterTuple.reset(tuple);
            memoryComponents.get(currentMutableComponentId.get()).getLSMComponentFilter().update(ctx.filterTuple,
                    ctx.filterCmp);
        }
    }

    public class LSMRTreeBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final IIndexBulkLoader bulkLoader;
        private final IIndexBulkLoader buddyBTreeBulkloader;
        private boolean cleanedUpArtifacts = false;
        private boolean isEmptyComponent = true;
        public final PermutingTupleReference indexTuple;
        public final PermutingTupleReference filterTuple;
        public final MultiComparator filterCmp;

        public LSMRTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
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
            buddyBTreeBulkloader = ((LSMRTreeDiskComponent) component).getBTree().createBulkLoader(fillFactor,
                    verifyInput, numElementsHint, false, true);
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
                buddyBTreeBulkloader.end();

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
            if (buddyBTreeBulkloader != null) {
                buddyBTreeBulkloader.abort();
            }
        }

        protected void cleanupArtifacts() throws HyracksDataException {
            if (!cleanedUpArtifacts) {
                cleanedUpArtifacts = true;
                ((LSMRTreeDiskComponent) component).getRTree().deactivate();
                ((LSMRTreeDiskComponent) component).getRTree().destroy();
                ((LSMRTreeDiskComponent) component).getBTree().deactivate();
                ((LSMRTreeDiskComponent) component).getBTree().destroy();
                ((LSMRTreeDiskComponent) component).getBloomFilter().deactivate();
                ((LSMRTreeDiskComponent) component).getBloomFilter().destroy();
            }
        }
    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) lsmComponent;
        markAsValidInternal(component.getBTree().getBufferCache(), component.getBloomFilter());
        markAsValidInternal((component).getBTree());
        markAsValidInternal((component).getRTree());
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
        LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) lsmComponent;
        files.add(component.getBTree().getFileReference().getFile().getAbsolutePath());
        files.add(component.getRTree().getFileReference().getFile().getAbsolutePath());
        files.add(component.getBloomFilter().getFileReference().getFile().getAbsolutePath());
        return files;
    }

}
