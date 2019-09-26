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

package org.apache.hyracks.storage.am.lsm.btree.impls;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBloomFilterDiskComponent;
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
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.IndexCursorStats;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.util.trace.ITracer;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

    private static final ICursorFactory cursorFactory = LSMBTreeSearchCursor::new;
    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory insertLeafFrameFactory;
    protected final ITreeIndexFrameFactory deleteLeafFrameFactory;
    protected final IBinaryComparatorFactory[] cmpFactories;
    private final boolean updateAware;

    private final boolean needKeyDupCheck;

    // Primary and Primary Key LSMBTree has a Bloomfilter, but Secondary one doesn't have.
    private final boolean hasBloomFilter;

    public LSMBTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, boolean hasBloomFilter, int[] btreeFields, int[] filterFields, boolean durable,
            boolean updateAware, ITracer tracer) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, diskBufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy,
                opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, componentFactory,
                bulkLoadComponentFactory, filterFrameFactory, filterManager, filterFields, durable, filterHelper,
                btreeFields, tracer);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.updateAware = updateAware;
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            LSMBTreeMemoryComponent mutableComponent = new LSMBTreeMemoryComponent(this,
                    new BTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache), interiorFrameFactory,
                            insertLeafFrameFactory, cmpFactories, fieldCount,
                            ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_" + i)),
                    virtualBufferCache, filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
        this.needKeyDupCheck = needKeyDupCheck;
        this.hasBloomFilter = hasBloomFilter;
    }

    // Without memory components
    public LSMBTree(IIOManager ioManager, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, IBufferCache bufferCache, ILSMIndexFileManager fileManager,
            ILSMDiskComponentFactory componentFactory, ILSMDiskComponentFactory bulkLoadComponentFactory,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean needKeyDupCheck, boolean durable, ITracer tracer) throws HyracksDataException {
        super(ioManager, bufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler,
                ioOpCallbackFactory, pageWriteCallbackFactory, componentFactory, bulkLoadComponentFactory, durable,
                tracer);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.needKeyDupCheck = needKeyDupCheck;
        this.hasBloomFilter = true;
        this.updateAware = false;
    }

    @Override
    public boolean isPrimaryIndex() {
        return needKeyDupCheck;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
        } else {
            indexTuple = tuple;
        }

        switch (ctx.getOperation()) {
            case PHYSICALDELETE:
                ctx.getCurrentMutableBTreeAccessor().delete(indexTuple);
                break;
            case INSERT:
                insert(indexTuple, ctx);
                break;
            default:
                ctx.getCurrentMutableBTreeAccessor().upsert(indexTuple);
                break;
        }
        updateFilter(ctx, tuple);
    }

    private boolean insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException {
        LSMBTreePointSearchCursor searchCursor = ctx.getInsertSearchCursor();
        IIndexCursor memCursor = ctx.getMemCursor();
        RangePredicate predicate = (RangePredicate) ctx.getSearchPredicate();
        predicate.setHighKey(tuple);
        predicate.setLowKey(tuple);
        if (needKeyDupCheck) {
            // first check the inmemory component
            boolean found;
            ctx.getCurrentMutableBTreeAccessor().search(memCursor, predicate);
            try {
                found = memCursor.hasNext();
                if (found) {
                    memCursor.next();
                    LSMBTreeTupleReference lsmbtreeTuple = (LSMBTreeTupleReference) memCursor.getTuple();
                    if (!lsmbtreeTuple.isAntimatter()) {
                        throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
                    }
                }
            } finally {
                memCursor.close();
            }
            if (found) {
                ctx.getCurrentMutableBTreeAccessor().upsertIfConditionElseInsert(tuple,
                        AntimatterAwareTupleAcceptor.INSTANCE);
                return true;
            }

            // TODO: Can we just remove the above code that search the mutable
            // component and do it together with the search call below? i.e. instead
            // of passing false to the lsmHarness.search(), we pass true to include
            // the mutable component?
            // the key was not in the inmemory component, so check the disk
            // components

            // This is a hack to avoid searching the current active mutable component twice. It is critical to add it back once the search is over.
            ILSMComponent firstComponent = ctx.getComponentHolder().remove(0);
            search(ctx, searchCursor, predicate);
            try {
                if (searchCursor.hasNext()) {
                    throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
                }
            } finally {
                searchCursor.close();
                // Add the current active mutable component back
                ctx.getComponentHolder().add(0, firstComponent);
            }
        }
        ctx.getCurrentMutableBTreeAccessor().upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);
        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        cursor.open(ctx.getSearchInitialState(), pred);
    }

    @Override
    public void scanDiskComponents(ILSMIndexOperationContext ictx, IIndexCursor cursor) throws HyracksDataException {
        if (!isPrimaryIndex()) {
            throw HyracksDataException.create(ErrorCode.DISK_COMPONENT_SCAN_NOT_ALLOWED_FOR_SECONDARY_INDEX);
        }
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        MultiComparator comp = MultiComparator.create(getComparatorFactories());
        ISearchPredicate pred = new RangePredicate(null, null, true, true, comp, comp);
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        ctx.getSearchInitialState().setDiskComponentScan(true);
        ((LSMBTreeSearchCursor) cursor).open(ctx.getSearchInitialState(), pred);
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeFlushOperation flushOp = (LSMBTreeFlushOperation) operation;
        LSMBTreeMemoryComponent flushingComponent = (LSMBTreeMemoryComponent) flushOp.getFlushingComponent();
        IIndexAccessor accessor = flushingComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ILSMDiskComponent component;
        ILSMDiskComponentBulkLoader componentBulkLoader;
        try {
            RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
            long numElements = 0L;
            if (hasBloomFilter) {
                //count elements in btree for creating Bloomfilter
                IIndexCursor countingCursor = ((BTreeAccessor) accessor).createCountingSearchCursor();
                accessor.search(countingCursor, nullPred);
                try {
                    while (countingCursor.hasNext()) {
                        countingCursor.next();
                        ITupleReference countTuple = countingCursor.getTuple();
                        numElements =
                                IntegerPointable.getInteger(countTuple.getFieldData(0), countTuple.getFieldStart(0));
                    }
                } finally {
                    try {
                        countingCursor.close();
                    } finally {
                        countingCursor.destroy();
                    }
                }
            }
            component = createDiskComponent(componentFactory, flushOp.getTarget(), null, flushOp.getBloomFilterTarget(),
                    true);
            componentBulkLoader = component.createBulkLoader(operation, 1.0f, false, numElements, false, false, false,
                    pageWriteCallbackFactory.createPageWriteCallback());
            IIndexCursor scanCursor = accessor.createSearchCursor(false);
            accessor.search(scanCursor, nullPred);
            try {
                while (scanCursor.hasNext()) {
                    scanCursor.next();
                    // we can safely throw away updated tuples in secondary BTree components, because they correspond to
                    // deleted tuples
                    if (updateAware && ((LSMBTreeTupleReference) scanCursor.getTuple()).isUpdated()) {
                        continue;
                    }
                    componentBulkLoader.add(scanCursor.getTuple());
                }
            } finally {
                try {
                    scanCursor.close();
                } finally {
                    scanCursor.destroy();
                }
            }
        } finally {
            accessor.destroy();
        }
        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            getFilterManager().updateFilter(component.getLSMComponentFilter(), filterTuples,
                    NoOpOperationCallback.INSTANCE);
            getFilterManager().writeFilter(component.getLSMComponentFilter(), component.getMetadataHolder());
        }
        // Write metadata from memory component to disk
        // Q. what about the merge operation? how do we resolve conflicts
        // A. Through providing an appropriate ILSMIOOperationCallback
        // Must not reset the metadata before the flush is completed
        // Use the copy of the metadata in the opContext
        // TODO This code should be in the callback and not in the index
        flushingComponent.getMetadata().copy(component.getMetadata());

        componentBulkLoader.end();

        return component;
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ILSMDiskComponent mergedComponent;
        ILSMDiskComponentBulkLoader componentBulkLoader = null;
        try {
            try {
                RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
                search(mergeOp.getAccessor().getOpContext(), cursor, rangePred);
                try {
                    List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();
                    long numElements = getNumberOfElements(mergedComponents);
                    mergedComponent = createDiskComponent(componentFactory, mergeOp.getTarget(), null,
                            mergeOp.getBloomFilterTarget(), true);
                    IPageWriteCallback pageWriteCallback = pageWriteCallbackFactory.createPageWriteCallback();
                    componentBulkLoader = mergedComponent.createBulkLoader(operation, 1.0f, false, numElements, false,
                            false, false, pageWriteCallback);
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
        } catch (Throwable e) { // NOSONAR.. As per the contract, we should either abort or end
            try {
                if (componentBulkLoader != null) {
                    componentBulkLoader.abort();
                }
            } catch (Throwable th) { // NOSONAR Don't lose the root failure
                e.addSuppressed(th);
            }
            throw e;
        }
        componentBulkLoader.end();
        return mergedComponent;
    }

    private long getNumberOfElements(List<ILSMComponent> mergedComponents) throws HyracksDataException {
        long numElements = 0L;
        if (hasBloomFilter) {
            //count elements in btree for creating Bloomfilter
            for (int i = 0; i < mergedComponents.size(); ++i) {
                numElements += ((AbstractLSMWithBloomFilterDiskComponent) mergedComponents.get(i)).getBloomFilter()
                        .getNumElements();
            }
        }
        return numElements;
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback) {
        ILSMIndexAccessor accessor = createAccessor(opCtx);
        return new LSMBTreeFlushOperation(accessor, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), callback, getIndexIdentifier());
    }

    @Override
    public LSMBTreeOpContext createOpContext(IIndexAccessParameters iap) {
        int numBloomFilterKeyFields = hasBloomFilter
                ? ((LSMBTreeWithBloomFilterDiskComponentFactory) componentFactory).getBloomFilterKeyFields().length : 0;
        return new LSMBTreeOpContext(this, memoryComponents, insertLeafFrameFactory, deleteLeafFrameFactory,
                (IExtendedModificationOperationCallback) iap.getModificationCallback(),
                iap.getSearchOperationCallback(), numBloomFilterKeyFields, getTreeFields(), getFilterFields(),
                getHarness(), getFilterCmpFactories(), tracer);
    }

    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return createAccessor(createOpContext(iap));
    }

    public ILSMIndexAccessor createAccessor(AbstractLSMIndexOperationContext opCtx) {
        return new LSMTreeIndexAccessor(getHarness(), opCtx, cursorFactory);
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getInteriorFrameFactory();
    }

    @Override
    public int getFieldCount() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFieldCount();
    }

    @Override
    public int getFileId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getFileId();
    }

    @Override
    public IPageManager getPageManager() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getPageManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getLeafFrameFactory();
    }

    @Override
    public int getRootPageId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getIndex().getRootPageId();
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException {
        BTree firstBTree = (BTree) firstComponent.getIndex();
        BTree lastBTree = (BTree) lastComponent.getIndex();
        FileReference firstFile = firstBTree.getFileReference();
        FileReference lastFile = lastBTree.getFileReference();
        return fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) {
        boolean returnDeletedTuples = false;
        ILSMIndexAccessor accessor = createAccessor(opCtx);
        List<ILSMComponent> mergingComponents = opCtx.getComponentHolder();
        if (mergingComponents.get(mergingComponents.size() - 1) != diskComponents.get(diskComponents.size() - 1)) {
            returnDeletedTuples = true;
        }
        IIndexCursorStats stats = new IndexCursorStats();
        LSMBTreeRangeSearchCursor cursor = new LSMBTreeRangeSearchCursor(opCtx, returnDeletedTuples, stats);
        return new LSMBTreeMergeOperation(accessor, cursor, stats, mergeFileRefs.getInsertIndexFileReference(),
                mergeFileRefs.getBloomFilterFileReference(), callback, getIndexIdentifier());
    }
}
