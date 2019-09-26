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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
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
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.MergeOperation;
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

public class LSMRTreeWithAntiMatterTuples extends AbstractLSMRTree {
    private static final ICursorFactory cursorFactory = opCtx -> new LSMRTreeWithAntiMatterTuplesSearchCursor(opCtx);

    public LSMRTreeWithAntiMatterTuples(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            RTreeFrameFactory rtreeInteriorFrameFactory, RTreeFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager, int fieldCount,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeComparatorFactories,
            ILinearizeComparatorFactory linearizer, int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int[] rtreeFields, int[] filterFields, boolean durable, boolean isPointMBR) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, diskBufferCache, fileManager, componentFactory,
                bulkLoadComponentFactory, fieldCount, rtreeCmpFactories, btreeComparatorFactories, linearizer,
                comparatorFields, linearizerArray, 0, mergePolicy, opTracker, ioScheduler, ioOpCallbackFactory,
                pageWriteCallbackFactory, filterHelper, filterFrameFactory, filterManager, rtreeFields, filterFields,
                durable, isPointMBR);
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMRTreeFlushOperation flushOp = (LSMRTreeFlushOperation) operation;
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.
        LSMRTreeMemoryComponent flushingComponent = (LSMRTreeMemoryComponent) flushOp.getFlushingComponent();
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        ILSMDiskComponent component = null;
        ILSMDiskComponentBulkLoader componentBulkLoader = null;
        TreeTupleSorter rTreeTupleSorter = null;
        TreeTupleSorter bTreeTupleSorter = null;
        boolean isEmpty = true;
        boolean abort = true;
        try {
            RTreeAccessor memRTreeAccessor =
                    flushingComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);

            try {
                RTreeSearchCursor rtreeScanCursor = memRTreeAccessor.createSearchCursor(false);
                try {
                    memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
                    component = createDiskComponent(componentFactory, flushOp.getTarget(), null, null, true);
                    componentBulkLoader = component.createBulkLoader(operation, 1.0f, false, 0L, false, false, false,
                            pageWriteCallbackFactory.createPageWriteCallback());
                    // Since the LSM-RTree is used as a secondary assumption, the
                    // primary key will be the last comparator in the BTree comparators
                    rTreeTupleSorter = new TreeTupleSorter(flushingComponent.getIndex().getFileId(), linearizerArray,
                            rtreeLeafFrameFactory.createFrame(), rtreeLeafFrameFactory.createFrame(),
                            flushingComponent.getIndex().getBufferCache(), comparatorFields);
                    try {
                        isEmpty = scanAndSort(rtreeScanCursor, rTreeTupleSorter);
                    } finally {
                        rtreeScanCursor.close();
                    }
                } finally {
                    rtreeScanCursor.destroy();
                }
            } finally {
                memRTreeAccessor.destroy();
            }
            if (!isEmpty) {
                rTreeTupleSorter.sort();
            }
            // scan the memory BTree
            RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
            BTreeAccessor memBTreeAccessor =
                    flushingComponent.getBuddyIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            try {
                bTreeTupleSorter = new TreeTupleSorter(flushingComponent.getBuddyIndex().getFileId(), linearizerArray,
                        btreeLeafFrameFactory.createFrame(), btreeLeafFrameFactory.createFrame(),
                        flushingComponent.getBuddyIndex().getBufferCache(), comparatorFields);
                BTreeRangeSearchCursor btreeScanCursor = memBTreeAccessor.createSearchCursor(false);
                try {
                    isEmpty = true;
                    memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);
                    try {
                        isEmpty = scanAndSort(btreeScanCursor, bTreeTupleSorter);
                    } finally {
                        btreeScanCursor.close();
                    }
                } finally {
                    btreeScanCursor.destroy();
                }
            } finally {
                memBTreeAccessor.destroy();
            }
            if (!isEmpty) {
                bTreeTupleSorter.sort();
            }
            LSMRTreeWithAntiMatterTuplesFlushCursor cursor = new LSMRTreeWithAntiMatterTuplesFlushCursor(
                    rTreeTupleSorter, bTreeTupleSorter, comparatorFields, linearizerArray);
            try {
                cursor.open(null, null);
                try {
                    while (cursor.hasNext()) {
                        cursor.next();
                        ITupleReference frameTuple = cursor.getTuple();
                        componentBulkLoader.add(frameTuple);
                    }
                } finally {
                    cursor.close();
                }
            } finally {
                cursor.destroy();
            }
            if (component.getLSMComponentFilter() != null) {
                List<ITupleReference> filterTuples = new ArrayList<>();
                filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
                filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
                getFilterManager().updateFilter(component.getLSMComponentFilter(), filterTuples,
                        NoOpOperationCallback.INSTANCE);
                getFilterManager().writeFilter(component.getLSMComponentFilter(), component.getMetadataHolder());
            }
            flushingComponent.getMetadata().copy(component.getMetadata());
            abort = false;
            componentBulkLoader.end();
        } finally {
            try {
                if (rTreeTupleSorter != null) {
                    rTreeTupleSorter.destroy();
                }
            } finally {
                try {
                    if (bTreeTupleSorter != null) {
                        bTreeTupleSorter.destroy();
                    }
                } finally {
                    if (abort && componentBulkLoader != null) {
                        componentBulkLoader.abort();
                    }
                }
            }
        }
        return component;
    }

    private boolean scanAndSort(RTreeSearchCursor scanCursor, TreeTupleSorter tupleSorter) throws HyracksDataException {
        boolean isEmpty = true;
        while (scanCursor.hasNext()) {
            isEmpty = false;
            scanCursor.next();
            tupleSorter.insertTupleEntry(scanCursor.getPageId(), scanCursor.getTupleOffset());
        }
        return isEmpty;
    }

    private boolean scanAndSort(BTreeRangeSearchCursor scanCursor, TreeTupleSorter tupleSorter)
            throws HyracksDataException {
        boolean isEmpty = true;
        while (scanCursor.hasNext()) {
            isEmpty = false;
            scanCursor.next();
            tupleSorter.insertTupleEntry(scanCursor.getPageId(), scanCursor.getTupleOffset());
        }
        return isEmpty;
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        MergeOperation mergeOp = (MergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        search(opCtx, cursor, rtreeSearchPred);

        // Bulk load the tuples from all on-disk RTrees into the new RTree.
        ILSMDiskComponent component = createDiskComponent(componentFactory, mergeOp.getTarget(), null, null, true);

        ILSMDiskComponentBulkLoader componentBulkLoader = component.createBulkLoader(operation, 1.0f, false, 0L, false,
                false, false, pageWriteCallbackFactory.createPageWriteCallback());
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                componentBulkLoader.add(frameTuple);
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
            getFilterManager().updateFilter(component.getLSMComponentFilter(), filterTuples,
                    NoOpOperationCallback.INSTANCE);
            getFilterManager().writeFilter(component.getLSMComponentFilter(), component.getMetadataHolder());
        }

        componentBulkLoader.end();

        return component;
    }

    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) {
        LSMRTreeOpContext opCtx = createOpContext(iap);
        return new LSMTreeIndexAccessor(getHarness(), opCtx, cursorFactory);
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        ILSMIndexAccessor accessor = new LSMTreeIndexAccessor(getHarness(), opCtx, cursorFactory);
        return new LSMRTreeFlushOperation(accessor, componentFileRefs.getInsertIndexFileReference(), null, null,
                callback, getIndexIdentifier());
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException {
        boolean returnDeletedTuples = false;
        List<ILSMComponent> mergingComponents = opCtx.getComponentHolder();
        if (mergingComponents.get(mergingComponents.size() - 1) != diskComponents.get(diskComponents.size() - 1)) {
            returnDeletedTuples = true;
        }
        IIndexCursorStats stats = new IndexCursorStats();
        LSMRTreeWithAntiMatterTuplesSearchCursor cursor =
                new LSMRTreeWithAntiMatterTuplesSearchCursor(opCtx, returnDeletedTuples, stats);
        ILSMIndexAccessor accessor = new LSMTreeIndexAccessor(getHarness(), opCtx, cursorFactory);
        return new LSMRTreeMergeOperation(accessor, cursor, stats, mergeFileRefs.getInsertIndexFileReference(), null,
                null, callback, getIndexIdentifier());
    }
}
