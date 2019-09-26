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
import java.util.List;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.DualTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.rtree.frames.RTreeFrameFactory;
import org.apache.hyracks.storage.am.rtree.impls.RTree.RTreeAccessor;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.IndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class LSMRTree extends AbstractLSMRTree {
    protected final int[] buddyBTreeFields;

    public LSMRTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            RTreeFrameFactory rtreeInteriorFrameFactory, RTreeFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileNameManager,
            ILSMDiskComponentFactory componentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int[] rtreeFields, int[] buddyBTreeFields, int[] filterFields, boolean durable, boolean isPointMBR)
            throws HyracksDataException {
        super(ioManager, virtualBufferCaches, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, diskBufferCache, fileNameManager, componentFactory,
                componentFactory, fieldCount, rtreeCmpFactories, btreeCmpFactories, linearizer, comparatorFields,
                linearizerArray, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, filterHelper, filterFrameFactory, filterManager, rtreeFields, filterFields,
                durable, isPointMBR);
        this.buddyBTreeFields = buddyBTreeFields;
    }

    /*
     * For External indexes with no memory components
     */
    public LSMRTree(IIOManager ioManager, ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileNameManager, ILSMDiskComponentFactory componentFactory,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int[] buddyBTreeFields, boolean durable, boolean isPointMBR, ITracer tracer) throws HyracksDataException {
        super(ioManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, btreeInteriorFrameFactory,
                btreeLeafFrameFactory, diskBufferCache, fileNameManager, componentFactory, rtreeCmpFactories,
                btreeCmpFactories, linearizer, comparatorFields, linearizerArray, bloomFilterFalsePositiveRate,
                mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, durable, isPointMBR,
                tracer);
        this.buddyBTreeFields = buddyBTreeFields;
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMRTreeFlushOperation flushOp = (LSMRTreeFlushOperation) operation;
        LSMRTreeMemoryComponent flushingComponent = (LSMRTreeMemoryComponent) flushOp.getFlushingComponent();
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.
        // scan the memory RTree
        TreeTupleSorter rTreeTupleSorter = null;
        MutableBoolean isEmpty = new MutableBoolean(true);
        MutableLong numBTreeTuples = new MutableLong(0L);
        ILSMDiskComponent component;
        ILSMDiskComponentBulkLoader componentBulkLoader = null;
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        BTreeAccessor memBTreeAccessor =
                flushingComponent.getBuddyIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        boolean abort = true;
        try {
            try {
                rTreeTupleSorter = getRTreeTupleSorter(flushingComponent, memBTreeAccessor, btreeNullPredicate,
                        numBTreeTuples, isEmpty);
                rTreeTupleSorter.sort();
                component = createDiskComponent(componentFactory, flushOp.getTarget(), flushOp.getBTreeTarget(),
                        flushOp.getBloomFilterTarget(), true);
                componentBulkLoader = component.createBulkLoader(operation, 1.0f, false, numBTreeTuples.longValue(),
                        false, false, false, pageWriteCallbackFactory.createPageWriteCallback());
                flushLoadRTree(isEmpty, rTreeTupleSorter, componentBulkLoader);
                // scan the memory BTree and bulk load delete tuples
                flushLoadBtree(memBTreeAccessor, componentBulkLoader, btreeNullPredicate);
            } finally {
                try {
                    memBTreeAccessor.destroy();
                } finally {
                    if (rTreeTupleSorter != null) {
                        rTreeTupleSorter.destroy();
                    }
                }
            }
            if (component.getLSMComponentFilter() != null) {
                List<ITupleReference> filterTuples = new ArrayList<>();
                filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
                filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
                getFilterManager().updateFilter(component.getLSMComponentFilter(), filterTuples,
                        NoOpOperationCallback.INSTANCE);
                getFilterManager().writeFilter(component.getLSMComponentFilter(), component.getMetadataHolder());
            }
            // Note. If we change the filter to write to metadata object, we don't need the if block above
            flushingComponent.getMetadata().copy(component.getMetadata());
            abort = false;
            componentBulkLoader.end();
        } finally {
            if (abort && componentBulkLoader != null) {
                componentBulkLoader.abort();
            }
        }
        return component;
    }

    private void flushLoadRTree(MutableBoolean isEmpty, TreeTupleSorter rTreeTupleSorter,
            ILSMDiskComponentBulkLoader componentBulkLoader) throws HyracksDataException {
        if (!isEmpty.booleanValue()) {
            rTreeTupleSorter.open(null, null);
            try {
                while (rTreeTupleSorter.hasNext()) {
                    rTreeTupleSorter.next();
                    ITupleReference frameTuple = rTreeTupleSorter.getTuple();
                    componentBulkLoader.add(frameTuple);
                }
            } finally {
                rTreeTupleSorter.close();
            }
        }
    }

    private void flushLoadBtree(BTreeAccessor memBTreeAccessor, ILSMDiskComponentBulkLoader componentBulkLoader,
            RangePredicate btreeNullPredicate) throws HyracksDataException {
        IIndexCursor btreeScanCursor = memBTreeAccessor.createSearchCursor(false);
        try {
            memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);
            try {
                while (btreeScanCursor.hasNext()) {
                    btreeScanCursor.next();
                    ITupleReference frameTuple = btreeScanCursor.getTuple();
                    componentBulkLoader.delete(frameTuple);
                }
            } finally {
                btreeScanCursor.close();
            }
        } finally {
            btreeScanCursor.destroy();
        }
    }

    private TreeTupleSorter getRTreeTupleSorter(LSMRTreeMemoryComponent flushingComponent,
            BTreeAccessor memBTreeAccessor, RangePredicate btreeNullPredicate, MutableLong numBTreeTuples,
            MutableBoolean isEmpty) throws HyracksDataException {
        RTreeAccessor memRTreeAccessor =
                flushingComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        try {
            RTreeSearchCursor rtreeScanCursor = memRTreeAccessor.createSearchCursor(false);
            try {
                SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
                memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
                try {
                    //count the number of tuples in the buddy btree
                    countTuples(memBTreeAccessor, btreeNullPredicate, numBTreeTuples);
                    IBinaryComparatorFactory[] linearizerArray = { linearizer };
                    boolean failed = true;
                    TreeTupleSorter rTreeTupleSorter = new TreeTupleSorter(flushingComponent.getIndex().getFileId(),
                            linearizerArray, rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(),
                            flushingComponent.getIndex().getBufferCache(), comparatorFields);
                    try {
                        // BulkLoad the tuples from the in-memory tree into the new disk
                        // RTree.
                        isEmpty.setValue(fill(rtreeScanCursor, rTreeTupleSorter));
                        failed = false;
                    } finally {
                        if (failed) {
                            rTreeTupleSorter.destroy();
                        }
                    }
                    return rTreeTupleSorter;
                } finally {
                    rtreeScanCursor.close();
                }
            } finally {
                rtreeScanCursor.destroy();
            }
        } finally {
            memRTreeAccessor.destroy();
        }
    }

    private boolean fill(RTreeSearchCursor rtreeScanCursor, TreeTupleSorter rTreeTupleSorter)
            throws HyracksDataException {
        boolean isEmpty = true;
        while (rtreeScanCursor.hasNext()) {
            isEmpty = false;
            rtreeScanCursor.next();
            rTreeTupleSorter.insertTupleEntry(rtreeScanCursor.getPageId(), rtreeScanCursor.getTupleOffset());
        }
        return isEmpty;
    }

    private void countTuples(BTreeAccessor memBTreeAccessor, RangePredicate btreeNullPredicate,
            MutableLong numBTreeTuples) throws HyracksDataException {
        IIndexCursor btreeCountingCursor = memBTreeAccessor.createCountingSearchCursor();
        try {
            memBTreeAccessor.search(btreeCountingCursor, btreeNullPredicate);
            try {
                while (btreeCountingCursor.hasNext()) {
                    btreeCountingCursor.next();
                    ITupleReference countTuple = btreeCountingCursor.getTuple();
                    numBTreeTuples.setValue(
                            IntegerPointable.getInteger(countTuple.getFieldData(0), countTuple.getFieldStart(0)));
                }
            } finally {
                btreeCountingCursor.close();
            }
        } finally {
            btreeCountingCursor.destroy();
        }
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ILSMDiskComponentBulkLoader componentBulkLoader = null;
        ILSMDiskComponent mergedComponent = null;
        boolean abort = true;
        try {
            mergedComponent = createDiskComponent(componentFactory, mergeOp.getTarget(), mergeOp.getBTreeTarget(),
                    mergeOp.getBloomFilterTarget(), true);
            componentBulkLoader = loadMergeBulkLoader(mergeOp, cursor, mergedComponent);
            if (mergedComponent.getLSMComponentFilter() != null) {
                List<ITupleReference> filterTuples = new ArrayList<>();
                for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                    filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
                    filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
                }
                getFilterManager().updateFilter(mergedComponent.getLSMComponentFilter(), filterTuples,
                        NoOpOperationCallback.INSTANCE);
                getFilterManager().writeFilter(mergedComponent.getLSMComponentFilter(),
                        mergedComponent.getMetadataHolder());
            }
            abort = false;
            componentBulkLoader.end();
        } finally {
            try {
                cursor.destroy();
            } finally {
                if (abort && componentBulkLoader != null) {
                    componentBulkLoader.abort();
                }
            }
        }
        return mergedComponent;
    }

    private ILSMDiskComponentBulkLoader loadMergeBulkLoader(LSMRTreeMergeOperation mergeOp, IIndexCursor cursor,
            ILSMDiskComponent mergedComponent) throws HyracksDataException {
        ILSMDiskComponentBulkLoader componentBulkLoader = null;
        boolean abort = true;
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMRTreeSortedCursor) cursor).getOpCtx();
        search(opCtx, cursor, rtreeSearchPred);
        try {
            try {
                // In case we must keep the deleted-keys BTrees, then they must be merged
                // *before* merging the r-trees so that
                // lsmHarness.endSearch() is called once when the r-trees have been merged.
                if (mergeOp.getMergingComponents().get(mergeOp.getMergingComponents().size() - 1) != diskComponents
                        .get(diskComponents.size() - 1)) {
                    // Keep the deleted tuples since the oldest disk component
                    // is not included in the merge operation
                    long numElements = 0L;
                    for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                        numElements += ((LSMRTreeDiskComponent) mergeOp.getMergingComponents().get(i)).getBloomFilter()
                                .getNumElements();
                    }
                    componentBulkLoader = mergedComponent.createBulkLoader(mergeOp, 1.0f, false, numElements, false,
                            false, false, pageWriteCallbackFactory.createPageWriteCallback());
                    mergeLoadBTree(mergeOp, opCtx, rtreeSearchPred, componentBulkLoader);
                } else {
                    //no buddy-btree needed
                    componentBulkLoader = mergedComponent.createBulkLoader(mergeOp, 1.0f, false, 0L, false, false,
                            false, pageWriteCallbackFactory.createPageWriteCallback());
                }
                //search old rtree components
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference frameTuple = cursor.getTuple();
                    componentBulkLoader.add(frameTuple);
                }
            } finally {
                cursor.close();
            }
            abort = false;
        } finally {
            if (abort && componentBulkLoader != null) {
                componentBulkLoader.abort();
            }
        }
        return componentBulkLoader;
    }

    private void mergeLoadBTree(LSMRTreeMergeOperation mergeOp, ILSMIndexOperationContext opCtx,
            ISearchPredicate rtreeSearchPred, ILSMDiskComponentBulkLoader componentBulkLoader)
            throws HyracksDataException {
        LSMRTreeDeletedKeysBTreeMergeCursor btreeCursor =
                new LSMRTreeDeletedKeysBTreeMergeCursor(opCtx, mergeOp.getCursorStats());
        try {
            search(opCtx, btreeCursor, rtreeSearchPred);
            try {
                while (btreeCursor.hasNext()) {
                    btreeCursor.next();
                    ITupleReference tuple = btreeCursor.getTuple();
                    componentBulkLoader.delete(tuple);
                }
            } finally {
                btreeCursor.close();
            }
        } finally {
            btreeCursor.destroy();
        }
    }

    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return new LSMRTreeAccessor(getHarness(), createOpContext(iap), buddyBTreeFields);
    }

    // This function is modified for R-Trees without antimatter tuples to allow buddy B-Tree to have only primary keys
    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        if (ctx.getOperation() == IndexOperation.PHYSICALDELETE) {
            throw new UnsupportedOperationException("Physical delete not supported in the LSM-RTree");
        }

        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
        } else {
            indexTuple = tuple;
        }

        ctx.getModificationCallback().before(indexTuple);
        ctx.getModificationCallback().found(null, indexTuple);
        if (ctx.getOperation() == IndexOperation.INSERT) {
            ctx.getCurrentMutableRTreeAccessor().insert(indexTuple);
        } else {
            // First remove all entries in the in-memory rtree (if any).
            ctx.getCurrentMutableRTreeAccessor().delete(indexTuple);
            try {
                ctx.getCurrentMutableBTreeAccessor().insert(((DualTupleReference) tuple).getPermutingTuple());
            } catch (HyracksDataException e) {
                // Do nothing, because one delete tuple is enough to indicate
                // that all the corresponding insert tuples are deleted
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
        }
        updateFilter(ctx, tuple);
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        LSMRTreeAccessor accessor = new LSMRTreeAccessor(getHarness(), opCtx, buddyBTreeFields);
        return new LSMRTreeFlushOperation(accessor, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(),
                callback, getIndexIdentifier());
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException {
        IIndexCursorStats stats = new IndexCursorStats();
        LSMRTreeSortedCursor cursor = new LSMRTreeSortedCursor(opCtx, linearizer, buddyBTreeFields, stats);
        ILSMIndexAccessor accessor = new LSMRTreeAccessor(getHarness(), opCtx, buddyBTreeFields);
        return new LSMRTreeMergeOperation(accessor, cursor, stats, mergeFileRefs.getInsertIndexFileReference(),
                mergeFileRefs.getDeleteIndexFileReference(), mergeFileRefs.getBloomFilterFileReference(), callback,
                getIndexIdentifier());
    }
}
