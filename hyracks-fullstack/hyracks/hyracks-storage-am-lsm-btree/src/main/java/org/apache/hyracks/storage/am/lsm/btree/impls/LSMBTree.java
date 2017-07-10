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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

    private static final ICursorFactory cursorFactory = opCtx -> new LSMBTreeSearchCursor(opCtx);
    // For creating BTree's used in flush and merge.
    protected final LSMBTreeDiskComponentFactory componentFactory;
    // For creating BTree's used in bulk load. Different from diskBTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    protected final LSMBTreeDiskComponentFactory bulkLoadComponentFactory;

    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory insertLeafFrameFactory;
    protected final ITreeIndexFrameFactory deleteLeafFrameFactory;
    protected final IBinaryComparatorFactory[] cmpFactories;

    private final boolean needKeyDupCheck;

    // Primary LSMBTree has a Bloomfilter, but Secondary one doesn't have.
    private final boolean hasBloomFilter;

    public LSMBTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMIndexFileManager fileManager,
            TreeIndexFactory<BTree> diskBTreeFactory, TreeIndexFactory<BTree> bulkLoadBTreeFactory,
            BloomFilterFactory bloomFilterFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, boolean needKeyDupCheck, int[] btreeFields, int[] filterFields,
            boolean durable) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, diskBTreeFactory.getBufferCache(), fileManager,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallback, filterFrameFactory,
                filterManager, filterFields, durable, filterHelper, btreeFields);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            LSMBTreeMemoryComponent mutableComponent = new LSMBTreeMemoryComponent(
                    new BTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache), interiorFrameFactory,
                            insertLeafFrameFactory, cmpFactories, fieldCount,
                            ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_" + i)),
                    virtualBufferCache, i == 0 ? true : false,
                    filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
        componentFactory = new LSMBTreeDiskComponentFactory(diskBTreeFactory, bloomFilterFactory, filterHelper);
        bulkLoadComponentFactory =
                new LSMBTreeDiskComponentFactory(bulkLoadBTreeFactory, bloomFilterFactory, filterHelper);
        this.needKeyDupCheck = needKeyDupCheck;
        this.hasBloomFilter = needKeyDupCheck;
    }

    // Without memory components
    public LSMBTree(IIOManager ioManager, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMIndexFileManager fileManager,
            TreeIndexFactory<BTree> diskBTreeFactory, TreeIndexFactory<BTree> bulkLoadBTreeFactory,
            BloomFilterFactory bloomFilterFactory, double bloomFilterFalsePositiveRate,
            IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback, boolean needKeyDupCheck,
            boolean durable) {
        super(ioManager, diskBTreeFactory.getBufferCache(), fileManager, bloomFilterFalsePositiveRate, mergePolicy,
                opTracker, ioScheduler, ioOpCallback, durable);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        this.needKeyDupCheck = needKeyDupCheck;
        this.hasBloomFilter = true;
        componentFactory = new LSMBTreeDiskComponentFactory(diskBTreeFactory, bloomFilterFactory, null);
        bulkLoadComponentFactory = new LSMBTreeDiskComponentFactory(bulkLoadBTreeFactory, bloomFilterFactory, null);
    }

    public boolean hasBloomFilter() {
        return hasBloomFilter;
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
    protected ILSMDiskComponent loadComponent(LSMComponentFileReferences lsmComonentFileReferences)
            throws HyracksDataException {
        return createDiskComponent(componentFactory, lsmComonentFileReferences.getInsertIndexFileReference(),
                lsmComonentFileReferences.getBloomFilterFileReference(), false);
    }

    @Override
    protected void destroyDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
        component.getBTree().destroy();
        if (hasBloomFilter) {
            component.getBloomFilter().destroy();
        }
    }

    @Override
    protected void clearDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
        if (hasBloomFilter) {
            component.getBloomFilter().deactivate();
        }
        component.getBTree().deactivate();
        if (hasBloomFilter) {
            component.getBloomFilter().destroy();
        }
        component.getBTree().destroy();
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
            ctx.getCurrentMutableBTreeAccessor().getOpContext().resetNonIndexFieldsTuple(tuple);
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
            ctx.getCurrentMutableBTreeAccessor().search(memCursor, predicate);
            try {
                if (memCursor.hasNext()) {
                    memCursor.next();
                    LSMBTreeTupleReference lsmbtreeTuple = (LSMBTreeTupleReference) memCursor.getTuple();
                    if (!lsmbtreeTuple.isAntimatter()) {
                        throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
                    } else {
                        memCursor.close();
                        ctx.getCurrentMutableBTreeAccessor().upsertIfConditionElseInsert(tuple,
                                AntimatterAwareTupleAcceptor.INSTANCE);
                        return true;
                    }
                }
            } finally {
                memCursor.close();
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
        ((LSMBTreeSearchCursor) cursor).scan(ctx.getSearchInitialState(), pred);
    }

    @Override
    public ILSMDiskComponent flush(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeFlushOperation flushOp = (LSMBTreeFlushOperation) operation;
        LSMBTreeMemoryComponent flushingComponent = (LSMBTreeMemoryComponent) flushOp.getFlushingComponent();
        IIndexAccessor accessor = flushingComponent.getBTree().createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

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
                    numElements = IntegerPointable.getInteger(countTuple.getFieldData(0), countTuple.getFieldStart(0));
                }
            } finally {
                countingCursor.close();
            }
        }

        LSMBTreeDiskComponent component =
                createDiskComponent(componentFactory, flushOp.getTarget(), flushOp.getBloomFilterTarget(), true);

        ILSMDiskComponentBulkLoader componentBulkLoader =
                createComponentBulkLoader(component, 1.0f, false, numElements, false, false, false);

        IIndexCursor scanCursor = accessor.createSearchCursor(false);
        accessor.search(scanCursor, nullPred);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                componentBulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
        }

        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            getFilterManager().updateFilter(component.getLSMComponentFilter(), filterTuples);
            getFilterManager().writeFilter(component.getLSMComponentFilter(), component.getBTree());
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
    public ILSMDiskComponent merge(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        opCtx.getComponentHolder().addAll(mergeOp.getMergingComponents());
        search(opCtx, cursor, rangePred);
        List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();

        long numElements = 0L;
        if (hasBloomFilter) {
            //count elements in btree for creating Bloomfilter
            for (int i = 0; i < mergedComponents.size(); ++i) {
                numElements += ((LSMBTreeDiskComponent) mergedComponents.get(i)).getBloomFilter().getNumElements();
            }
        }
        LSMBTreeDiskComponent mergedComponent =
                createDiskComponent(componentFactory, mergeOp.getTarget(), mergeOp.getBloomFilterTarget(), true);

        ILSMDiskComponentBulkLoader componentBulkLoader =
                createComponentBulkLoader(mergedComponent, 1.0f, false, numElements, false, false, false);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                componentBulkLoader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }
        if (mergedComponent.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
            }
            getFilterManager().updateFilter(mergedComponent.getLSMComponentFilter(), filterTuples);
            getFilterManager().writeFilter(mergedComponent.getLSMComponentFilter(), mergedComponent.getBTree());
        }

        componentBulkLoader.end();

        return mergedComponent;
    }

    protected LSMBTreeDiskComponent createDiskComponent(LSMBTreeDiskComponentFactory factory,
            FileReference btreeFileRef, FileReference bloomFilterFileRef, boolean createComponent)
            throws HyracksDataException {
        // Create new BTree instance.
        LSMBTreeDiskComponent component =
                factory.createComponent(new LSMComponentFileReferences(btreeFileRef, null, bloomFilterFileRef));
        // BTree will be closed during cleanup of merge().
        if (createComponent) {
            component.getBTree().create();
        }
        component.getBTree().activate();
        if (hasBloomFilter) {
            if (createComponent) {
                component.getBloomFilter().create();
            }
            component.getBloomFilter().activate();
        }
        if (component.getLSMComponentFilter() != null && !createComponent) {
            getFilterManager().readFilter(component.getLSMComponentFilter(), component.getBTree());
        }
        return component;
    }

    @Override
    public ILSMDiskComponentBulkLoader createComponentBulkLoader(ILSMDiskComponent component, float fillFactor,
            boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter,
            boolean cleanupEmptyComponent) throws HyracksDataException {
        BloomFilterSpecification bloomFilterSpec = null;
        if (hasBloomFilter) {
            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
            bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement, bloomFilterFalsePositiveRate);
        }

        if (withFilter && filterFields != null) {
            return new LSMBTreeDiskComponentBulkLoader((LSMBTreeDiskComponent) component, bloomFilterSpec, fillFactor,
                    verifyInput, numElementsHint, checkIfEmptyIndex, cleanupEmptyComponent, filterManager, treeFields,
                    filterFields, MultiComparator.create(component.getLSMComponentFilter().getFilterCmpFactories()));
        } else {
            return new LSMBTreeDiskComponentBulkLoader((LSMBTreeDiskComponent) component, bloomFilterSpec, fillFactor,
                    verifyInput, numElementsHint, checkIfEmptyIndex, cleanupEmptyComponent);
        }

    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        return new LSMBTreeBulkLoader(this, fillLevel, verifyInput, numElementsHint);
    }

    @Override
    public ILSMDiskComponent createBulkLoadTarget() throws HyracksDataException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), true);
    }

    @Override
    public void markAsValid(ILSMDiskComponent lsmComponent) throws HyracksDataException {
        // The order of forcing the dirty page to be flushed is critical. The
        // bloom filter must be always done first.
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) lsmComponent;
        if (hasBloomFilter) {
            markAsValidInternal(component.getBTree().getBufferCache(), component.getBloomFilter());
        }
        markAsValidInternal(component.getBTree());
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            ILSMMemoryComponent flushingComponent, LSMComponentFileReferences componentFileRefs,
            ILSMIOOperationCallback callback) {
        ILSMIndexAccessor accessor = createAccessor(opCtx);
        return new LSMBTreeFlushOperation(accessor, flushingComponent, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir().getAbsolutePath());
    }

    @Override
    public LSMBTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        int numBloomFilterKeyFields = hasBloomFilter ? componentFactory.getBloomFilterKeyFields().length : 0;
        return new LSMBTreeOpContext(memoryComponents, insertLeafFrameFactory, deleteLeafFrameFactory,
                modificationCallback, searchCallback, numBloomFilterKeyFields, getTreeFields(), getFilterFields(),
                getLsmHarness(), getFilterCmpFactories());
    }

    @Override
    public ILSMIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return createAccessor(createOpContext(modificationCallback, searchCallback));
    }

    public ILSMIndexAccessor createAccessor(AbstractLSMIndexOperationContext opCtx) {
        return new LSMTreeIndexAccessor(getLsmHarness(), opCtx, cursorFactory);
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getInteriorFrameFactory();
    }

    @Override
    public int getFieldCount() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getFieldCount();
    }

    @Override
    public int getFileId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getFileId();
    }

    @Override
    public IPageManager getPageManager() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getPageManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getLeafFrameFactory();
    }

    @Override
    public int getRootPageId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getRootPageId();
    }

    @Override
    protected long getMemoryComponentSize(ILSMMemoryComponent c) {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
        IBufferCache virtualBufferCache = mutableComponent.getBTree().getBufferCache();
        return virtualBufferCache.getNumPages() * (long) virtualBufferCache.getPageSize();
    }

    @Override
    public String toString() {
        return "LSMBTree [" + fileManager.getBaseDir() + "]";
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles(ILSMComponent lsmComponent) {
        Set<String> files = new HashSet<>();
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) lsmComponent;
        files.add(component.getBTree().getFileReference().getFile().getAbsolutePath());
        if (hasBloomFilter) {
            files.add(component.getBloomFilter().getFileReference().getFile().getAbsolutePath());
        }
        return files;
    }

    @Override
    protected void clearMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
        mutableComponent.getBTree().clear();
        mutableComponent.reset();
    }

    @Override
    protected void validateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
        mutableComponent.getBTree().validate();
    }

    @Override
    protected void validateDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        BTree btree = ((LSMBTreeDiskComponent) c).getBTree();
        btree.validate();
    }

    @Override
    protected void deactivateDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
        component.getBTree().deactivate();
        component.getBTree().purge();
        if (hasBloomFilter) {
            component.getBloomFilter().deactivate();
            component.getBloomFilter().purge();
        }
    }

    @Override
    protected void deactivateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
        mutableComponent.getBTree().deactivate();
        mutableComponent.getBTree().destroy();
        ((IVirtualBufferCache) mutableComponent.getBTree().getBufferCache()).close();
    }

    @Override
    protected void allocateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
        ((IVirtualBufferCache) mutableComponent.getBTree().getBufferCache()).open();
        mutableComponent.getBTree().create();
        mutableComponent.getBTree().activate();
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException {
        BTree firstBTree = ((LSMBTreeDiskComponent) firstComponent).getBTree();
        BTree lastBTree = ((LSMBTreeDiskComponent) lastComponent).getBTree();
        FileReference firstFile = firstBTree.getFileReference();
        FileReference lastFile = lastBTree.getFileReference();
        return fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            List<ILSMComponent> mergingComponents, LSMComponentFileReferences mergeFileRefs,
            ILSMIOOperationCallback callback) {
        boolean returnDeletedTuples = false;
        ILSMIndexAccessor accessor = createAccessor(opCtx);
        if (mergingComponents.get(mergingComponents.size() - 1) != diskComponents.get(diskComponents.size() - 1)) {
            returnDeletedTuples = true;
        }
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor(opCtx, returnDeletedTuples);
        return new LSMBTreeMergeOperation(accessor, mergingComponents, cursor,
                mergeFileRefs.getInsertIndexFileReference(), mergeFileRefs.getBloomFilterFileReference(), callback,
                fileManager.getBaseDir().getAbsolutePath());
    }
}
