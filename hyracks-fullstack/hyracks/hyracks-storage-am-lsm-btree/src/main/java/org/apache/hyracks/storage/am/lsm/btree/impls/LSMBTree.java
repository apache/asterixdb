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
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import org.apache.hyracks.storage.am.common.impls.AbstractSearchPredicate;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualMetaDataPageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

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
    private final int[] btreeFields;
    // Primary LSMBTree has a Bloomfilter, but Secondary one doesn't have. 
    private final boolean hasBloomFilter;

    public LSMBTree(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory insertLeafFrameFactory, ITreeIndexFrameFactory deleteLeafFrameFactory,
            ILSMIndexFileManager fileManager, TreeIndexFactory<BTree> diskBTreeFactory,
            TreeIndexFactory<BTree> bulkLoadBTreeFactory, BloomFilterFactory bloomFilterFactory,
            ILSMComponentFilterFactory filterFactory, ILSMComponentFilterFrameFactory filterFrameFactory,
            LSMComponentFilterManager filterManager, double bloomFilterFalsePositiveRate,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, boolean needKeyDupCheck, int[] btreeFields, int[] filterFields,
            boolean durable) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, diskBTreeFactory.getBufferCache(), fileManager, diskFileMapProvider,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallback, filterFrameFactory,
                filterManager, filterFields, durable);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            LSMBTreeMemoryComponent mutableComponent = new LSMBTreeMemoryComponent(
                    new BTree(virtualBufferCache, virtualBufferCache.getFileMapProvider(),
                            new VirtualMetaDataPageManager(virtualBufferCache.getNumPages()), interiorFrameFactory,
                            insertLeafFrameFactory, cmpFactories, fieldCount,
                            ioManager.getFileRef(fileManager.getBaseDir() + "_virtual_" + i, false)),
                    virtualBufferCache, i == 0 ? true : false,
                    filterFactory == null ? null : filterFactory.createLSMComponentFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
        componentFactory = new LSMBTreeDiskComponentFactory(diskBTreeFactory, bloomFilterFactory, filterFactory);
        bulkLoadComponentFactory =
                new LSMBTreeDiskComponentFactory(bulkLoadBTreeFactory, bloomFilterFactory, filterFactory);
        this.needKeyDupCheck = needKeyDupCheck;
        this.btreeFields = btreeFields;
        this.hasBloomFilter = needKeyDupCheck;
    }

    // Without memory components
    public LSMBTree(IIOManager ioManager,
            ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMIndexFileManager fileManager,
            TreeIndexFactory<BTree> diskBTreeFactory, TreeIndexFactory<BTree> bulkLoadBTreeFactory,
            BloomFilterFactory bloomFilterFactory, double bloomFilterFalsePositiveRate,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, boolean needKeyDupCheck, boolean durable) {
        super(ioManager, diskBTreeFactory.getBufferCache(), fileManager, diskFileMapProvider,
                bloomFilterFalsePositiveRate,
                mergePolicy, opTracker, ioScheduler, ioOpCallback, durable);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        componentFactory = new LSMBTreeDiskComponentFactory(diskBTreeFactory, bloomFilterFactory, null);
        bulkLoadComponentFactory = new LSMBTreeDiskComponentFactory(bulkLoadBTreeFactory, bloomFilterFactory, null);
        this.needKeyDupCheck = needKeyDupCheck;
        this.btreeFields = null;
        //TODO remove BloomFilter from external dataset's secondary LSMBTree index
        this.hasBloomFilter = true;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        fileManager.deleteDirs();
        fileManager.createDirs();
        diskComponents.clear();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }
        List<ILSMComponent> immutableComponents = diskComponents;
        immutableComponents.clear();
        List<LSMComponentFileReferences> validFileReferences;
        try {
            validFileReferences = fileManager.cleanupAndGetValidFiles();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            LSMBTreeDiskComponent component;
            try {
                component =
                        createDiskComponent(componentFactory, lsmComonentFileReference.getInsertIndexFileReference(),
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
        if (!isActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }

        if (flushOnExit) {
            BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(ioOpCallback);
            ILSMIndexAccessor accessor = createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            accessor.scheduleFlush(cb);
            try {
                cb.waitForIO();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }

        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
            component.getBTree().deactivateCloseHandle();
            if (hasBloomFilter) {
                component.getBloomFilter().deactivate();
            }
        }
        deallocateMemoryComponents();
        isActivated = false;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        deactivate(true);
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }

        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
            component.getBTree().destroy();
            if (hasBloomFilter) {
                component.getBloomFilter().destroy();
            }
        }
        for (ILSMComponent c : memoryComponents) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
            mutableComponent.getBTree().destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }

        clearMemoryComponents();
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
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
        immutableComponents.clear();
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) throws HyracksDataException {
        List<ILSMComponent> immutableComponents = diskComponents;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        int cmc = currentMutableComponentId.get();
        ctx.setCurrentMutableComponentId(cmc);
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case UPDATE:
            case PHYSICALDELETE:
            case FLUSH:
            case DELETE:
                operationalComponents.add(memoryComponents.get(cmc));
                break;
            case INSERT:
            case UPSERT:
                addOperationalMutableComponents(operationalComponents);
                operationalComponents.addAll(immutableComponents);
                break;
            case SEARCH:
                if (memoryComponentsAllocated) {
                    addOperationalMutableComponents(operationalComponents);
                }
                if (filterManager != null) {
                    for (ILSMComponent c : immutableComponents) {
                        if (c.getLSMComponentFilter().satisfy(
                                ((AbstractSearchPredicate) ctx.getSearchPredicate()).getMinFilterTuple(),
                                ((AbstractSearchPredicate) ctx.getSearchPredicate()).getMaxFilterTuple(),
                                ((LSMBTreeOpContext) ctx).filterCmp)) {
                            operationalComponents.add(c);
                        }
                    }
                } else {
                    operationalComponents.addAll(immutableComponents);
                }

                break;
            case MERGE:
                operationalComponents.addAll(ctx.getComponentsToBeMerged());
                break;
            case FULL_MERGE:
                operationalComponents.addAll(immutableComponents);
                break;
            case REPLICATE:
                operationalComponents.addAll(ctx.getComponentsToBeReplicated());
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;

        ITupleReference indexTuple;
        if (ctx.indexTuple != null) {
            ctx.indexTuple.reset(tuple);
            indexTuple = ctx.indexTuple;
        } else {
            indexTuple = tuple;
        }

        switch (ctx.getOperation()) {
            case PHYSICALDELETE:
                ctx.currentMutableBTreeAccessor.delete(indexTuple);
                break;
            case INSERT:
                insert(indexTuple, ctx);
                break;
            default:
                ctx.currentMutableBTreeAccessor.upsert(indexTuple);
                break;
        }
        if (ctx.filterTuple != null) {
            ctx.filterTuple.reset(tuple);
            memoryComponents.get(currentMutableComponentId.get()).getLSMComponentFilter().update(ctx.filterTuple,
                    ctx.filterCmp);
        }
    }

    private boolean insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, IndexException {
        LSMBTreePointSearchCursor searchCursor = ctx.insertSearchCursor;
        IIndexCursor memCursor = ctx.memCursor;
        RangePredicate predicate = (RangePredicate) ctx.getSearchPredicate();
        predicate.setHighKey(tuple);
        predicate.setLowKey(tuple);
        if (needKeyDupCheck) {
            // first check the inmemory component
            ctx.currentMutableBTreeAccessor.search(memCursor, predicate);
            try {
                if (memCursor.hasNext()) {
                    memCursor.next();
                    LSMBTreeTupleReference lsmbtreeTuple = (LSMBTreeTupleReference) memCursor.getTuple();
                    if (!lsmbtreeTuple.isAntimatter()) {
                        throw new TreeIndexDuplicateKeyException("Failed to insert key since key already exists.");
                    } else {
                        memCursor.close();
                        ctx.currentMutableBTreeAccessor.upsertIfConditionElseInsert(tuple,
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
                    throw new TreeIndexDuplicateKeyException("Failed to insert key since key already exists.");
                }
            } finally {
                searchCursor.close();
                // Add the current active mutable component back
                ctx.getComponentHolder().add(0, firstComponent);
            }
        }
        ctx.currentMutableBTreeAccessor.upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);
        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        ctx.searchInitialState.reset(pred, operationalComponents);
        cursor.open(ctx.searchInitialState, pred);
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        LSMBTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        assert ctx.getComponentHolder().size() == 1;
        opCtx.setOperation(IndexOperation.FLUSH);
        opCtx.getComponentHolder().add(flushingComponent);
        ILSMIndexAccessorInternal flushAccessor = new LSMBTreeAccessor(lsmHarness, opCtx);
        ioScheduler.scheduleOperation(new LSMBTreeFlushOperation(flushAccessor, flushingComponent,
                componentFileRefs.getInsertIndexFileReference(), componentFileRefs.getBloomFilterFileReference(),
                callback, fileManager.getBaseDir(), flushingComponent.getMostRecentMarkerLSN()));
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMBTreeFlushOperation flushOp = (LSMBTreeFlushOperation) operation;
        LSMBTreeMemoryComponent flushingComponent = (LSMBTreeMemoryComponent) flushOp.getFlushingComponent();
        IIndexAccessor accessor = flushingComponent.getBTree().createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        long numElements = 0L;
        BloomFilterSpecification bloomFilterSpec = null;
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

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
            bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement, bloomFilterFalsePositiveRate);
        }

        LSMBTreeDiskComponent component = createDiskComponent(componentFactory, flushOp.getBTreeFlushTarget(),
                flushOp.getBloomFilterFlushTarget(), true);
        IIndexBulkLoader bulkLoader = component.getBTree().createBulkLoader(1.0f, false, numElements, false, true);
        IIndexBulkLoader builder = null;
        if (hasBloomFilter) {
            builder = component.getBloomFilter().createBuilder(numElements, bloomFilterSpec.getNumHashes(),
                    bloomFilterSpec.getNumBucketsPerElements());
        }

        IIndexCursor scanCursor = accessor.createSearchCursor(false);
        accessor.search(scanCursor, nullPred);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                if (hasBloomFilter) {
                    builder.add(scanCursor.getTuple());
                }
                bulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
            if (hasBloomFilter) {
                builder.end();
            }
        }

        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            filterManager.updateFilterInfo(component.getLSMComponentFilter(), filterTuples);
            filterManager.writeFilterInfo(component.getLSMComponentFilter(), component.getBTree());
        }
        component.setMostRecentMarkerLSN(flushOp.getPrevMarkerLSN());
        bulkLoader.end();

        return component;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        LSMBTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        opCtx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        boolean returnDeletedTuples = false;
        if (ctx.getComponentHolder().get(ctx.getComponentHolder().size() - 1) != diskComponents
                .get(diskComponents.size() - 1)) {
            returnDeletedTuples = true;
        }
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor(opCtx, returnDeletedTuples);
        BTree firstBTree = ((LSMBTreeDiskComponent) mergingComponents.get(0)).getBTree();
        BTree lastBTree = ((LSMBTreeDiskComponent) mergingComponents.get(mergingComponents.size() - 1)).getBTree();
        FileReference firstFile = firstBTree.getFileReference();
        FileReference lastFile = lastBTree.getFileReference();
        LSMComponentFileReferences relMergeFileRefs =
                fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
        ILSMIndexAccessorInternal accessor = new LSMBTreeAccessor(lsmHarness, opCtx);
        ioScheduler.scheduleOperation(new LSMBTreeMergeOperation(accessor, mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getBloomFilterFileReference(),
                callback, fileManager.getBaseDir()));
    }

    @Override
    public ILSMComponent merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        ITreeIndexCursor cursor = mergeOp.getCursor();
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        opCtx.getComponentHolder().addAll(mergeOp.getMergingComponents());
        search(opCtx, cursor, rangePred);
        List<ILSMComponent> mergedComponents = mergeOp.getMergingComponents();

        long numElements = 0L;
        BloomFilterSpecification bloomFilterSpec = null;
        if (hasBloomFilter) {
            //count elements in btree for creating Bloomfilter
            for (int i = 0; i < mergedComponents.size(); ++i) {
                numElements += ((LSMBTreeDiskComponent) mergedComponents.get(i)).getBloomFilter().getNumElements();
            }
            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
            bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement, bloomFilterFalsePositiveRate);
        }
        LSMBTreeDiskComponent mergedComponent = createDiskComponent(componentFactory, mergeOp.getBTreeMergeTarget(),
                mergeOp.getBloomFilterMergeTarget(), true);

        IIndexBulkLoader bulkLoader =
                mergedComponent.getBTree().createBulkLoader(1.0f, false, numElements, false, true);
        IIndexBulkLoader builder = null;
        if (hasBloomFilter) {
            builder = mergedComponent.getBloomFilter().createBuilder(numElements, bloomFilterSpec.getNumHashes(),
                    bloomFilterSpec.getNumBucketsPerElements());
        }
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                if (hasBloomFilter) {
                    builder.add(frameTuple);
                }
                bulkLoader.add(frameTuple);
            }
        } finally {
            cursor.close();
            if (hasBloomFilter) {
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
            filterManager.writeFilterInfo(mergedComponent.getLSMComponentFilter(), mergedComponent.getBTree());
        }

        mergedComponent
                .setMostRecentMarkerLSN(mergedComponents.get(mergedComponents.size() - 1).getMostRecentMarkerLSN());
        bulkLoader.end();

        return mergedComponent;
    }

    protected LSMBTreeDiskComponent createDiskComponent(LSMBTreeDiskComponentFactory factory,
            FileReference btreeFileRef, FileReference bloomFilterFileRef, boolean createComponent)
            throws HyracksDataException, IndexException {
        // Create new BTree instance.
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) factory
                .createLSMComponentInstance(new LSMComponentFileReferences(btreeFileRef, null, bloomFilterFileRef));
        // BTree will be closed during cleanup of merge().
        if (!createComponent) {
            component.getBTree().activate();
        }
        if (hasBloomFilter) {
            component.getBloomFilter().activate();
        }
        if (component.getLSMComponentFilter() != null && !createComponent) {
            filterManager.readFilterInfo(component.getLSMComponentFilter(), component.getBTree());
        }

        if (!createComponent) {
            component.readMostRecentMarkerLSN(component.getBTree());
        }

        return component;
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMBTreeBulkLoader(fillLevel, verifyInput, numElementsHint, checkIfEmptyIndex);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    protected ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), true);
    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        // The order of forcing the dirty page to be flushed is critical. The
        // bloom filter must be always done first.
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) lsmComponent;
        if (hasBloomFilter) {
            markAsValidInternal(component.getBTree().getBufferCache(), component.getBloomFilter());
        }
        markAsValidInternal(component.getBTree());
    }

    public class LSMBTreeBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final BTreeBulkLoader bulkLoader;
        private final IIndexBulkLoader builder;
        private boolean cleanedUpArtifacts = false;
        private boolean isEmptyComponent = true;
        private boolean endedBloomFilterLoad = false;
        public final PermutingTupleReference indexTuple;
        public final PermutingTupleReference filterTuple;
        public final MultiComparator filterCmp;

        public LSMBTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex) throws TreeIndexException, HyracksDataException {
            if (checkIfEmptyIndex && !isEmptyIndex()) {
                throw new TreeIndexException("Cannot load an index that is not empty");
            }
            try {
                component = createBulkLoadTarget();
            } catch (HyracksDataException | IndexException e) {
                throw new TreeIndexException(e);
            }
            bulkLoader = (BTreeBulkLoader) ((LSMBTreeDiskComponent) component).getBTree().createBulkLoader(fillFactor,
                    verifyInput, numElementsHint, false, true);

            if (hasBloomFilter) {
                int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
                BloomFilterSpecification bloomFilterSpec =
                        BloomCalculations.computeBloomSpec(maxBucketsPerElement, bloomFilterFalsePositiveRate);
                builder = ((LSMBTreeDiskComponent) component).getBloomFilter().createBuilder(numElementsHint,
                        bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
            } else {
                builder = null;
            }

            if (filterFields != null) {
                indexTuple = new PermutingTupleReference(btreeFields);
                filterCmp = MultiComparator.create(component.getLSMComponentFilter().getFilterCmpFactories());
                filterTuple = new PermutingTupleReference(filterFields);
            } else {
                indexTuple = null;
                filterCmp = null;
                filterTuple = null;
            }
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                ITupleReference t;
                if (indexTuple != null) {
                    indexTuple.reset(tuple);
                    t = indexTuple;
                } else {
                    t = tuple;
                }

                bulkLoader.add(t);
                if (hasBloomFilter) {
                    builder.add(t);
                }

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

        protected void cleanupArtifacts() throws HyracksDataException {
            if (!cleanedUpArtifacts) {
                cleanedUpArtifacts = true;
                if (hasBloomFilter && !endedBloomFilterLoad) {
                    builder.abort();
                    endedBloomFilterLoad = true;
                }
                ((LSMBTreeDiskComponent) component).getBTree().deactivate();
                ((LSMBTreeDiskComponent) component).getBTree().destroy();
                if (hasBloomFilter) {
                    ((LSMBTreeDiskComponent) component).getBloomFilter().deactivate();
                    ((LSMBTreeDiskComponent) component).getBloomFilter().destroy();
                }
            }
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            if (!cleanedUpArtifacts) {
                if (hasBloomFilter && !endedBloomFilterLoad) {
                    builder.end();
                    endedBloomFilterLoad = true;
                }

                if (component.getLSMComponentFilter() != null) {
                    filterManager.writeFilterInfo(component.getLSMComponentFilter(),
                            ((LSMBTreeDiskComponent) component).getBTree());
                }
                component.setMostRecentMarkerLSN(-1L);
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

            if (builder != null) {
                builder.abort();
            }

        }
    }

    public LSMBTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        int numBloomFilterKeyFields = hasBloomFilter ? componentFactory.getBloomFilterKeyFields().length : 0;
        return new LSMBTreeOpContext(memoryComponents, insertLeafFrameFactory, deleteLeafFrameFactory,
                modificationCallback, searchCallback, numBloomFilterKeyFields, btreeFields, filterFields, lsmHarness);
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMBTreeAccessor(lsmHarness, createOpContext(modificationCallback, searchCallback));
    }

    public class LSMBTreeAccessor extends LSMTreeIndexAccessor {
        public LSMBTreeAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public IIndexCursor createSearchCursor(boolean exclusive) {
            return new LSMBTreeSearchCursor(ctx);
        }

        public MultiComparator getMultiComparator() {
            LSMBTreeOpContext concreteCtx = (LSMBTreeOpContext) ctx;
            return concreteCtx.cmp;
        }
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
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
    public IMetaDataPageManager getMetaManager() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getMetaManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getLeafFrameFactory();
    }

    @Override
    public long getMemoryAllocationSize() {
        long size = 0;
        for (ILSMComponent c : memoryComponents) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
            IBufferCache virtualBufferCache = mutableComponent.getBTree().getBufferCache();
            size += virtualBufferCache.getNumPages() * virtualBufferCache.getPageSize();
        }
        return size;
    }

    @Override
    public int getRootPageId() {
        LSMBTreeMemoryComponent mutableComponent =
                (LSMBTreeMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getRootPageId();
    }

    @Override
    public void validate() throws HyracksDataException {
        validateMemoryComponents();
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            BTree btree = ((LSMBTreeDiskComponent) c).getBTree();
            btree.validate();
        }
    }

    @Override
    public String toString() {
        return "LSMBTree [" + fileManager.getBaseDir() + "]";
    }

    @Override
    public boolean isPrimaryIndex() {
        return needKeyDupCheck;
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, boolean appendOnly) throws IndexException {
        try {
            return new LSMBTreeBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
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
    public synchronized void allocateMemoryComponents() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to allocate memory components since the index is not active");
        }
        if (memoryComponentsAllocated) {
            return;
        }
        long markerLSN = -1L;
        if (!diskComponents.isEmpty()) {
            markerLSN = diskComponents.get(diskComponents.size() - 1).getMostRecentMarkerLSN();
        } else {
            // Needed in case a marker was added before any record
            if (memoryComponents != null && !memoryComponents.isEmpty()) {
                markerLSN = memoryComponents.get(0).getMostRecentMarkerLSN();
            }
        }
        for (ILSMComponent c : memoryComponents) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
            ((IVirtualBufferCache) mutableComponent.getBTree().getBufferCache()).open();
            mutableComponent.getBTree().create();
            mutableComponent.getBTree().activate();
            mutableComponent.setMostRecentMarkerLSN(markerLSN);
        }
        memoryComponentsAllocated = true;
    }

    private void addOperationalMutableComponents(List<ILSMComponent> operationalComponents) {
        int cmc = currentMutableComponentId.get();
        int numMutableComponents = memoryComponents.size();
        for (int i = 0; i < numMutableComponents - 1; i++) {
            ILSMComponent c = memoryComponents.get((cmc + i + 1) % numMutableComponents);
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
            if (mutableComponent.isReadable()) {
                // Make sure newest components are added first
                operationalComponents.add(0, mutableComponent);
            }
        }
        // The current mutable component is always added
        operationalComponents.add(0, memoryComponents.get(cmc));
    }

    private synchronized void clearMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMComponent c : memoryComponents) {
                LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
                mutableComponent.getBTree().clear();
                mutableComponent.reset();
            }
        }
    }

    private synchronized void validateMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMComponent c : memoryComponents) {
                LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
                mutableComponent.getBTree().validate();
            }
        }
    }

    private synchronized void deallocateMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMComponent c : memoryComponents) {
                LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
                mutableComponent.getBTree().deactivate();
                mutableComponent.getBTree().destroy();
                ((IVirtualBufferCache) mutableComponent.getBTree().getBufferCache()).close();
            }
            memoryComponentsAllocated = false;
        }
    }

    public synchronized long getMostRecentMarkerLSN() throws HyracksDataException {
        if (!isPrimaryIndex()) {
            throw new HyracksDataException("Markers are only supported for primary indexes");
        }
        LSMBTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        opCtx.setOperation(IndexOperation.SEARCH);
        getOperationalComponents(opCtx);
        return !opCtx.getComponentHolder().isEmpty() ? opCtx.getComponentHolder().get(0).getMostRecentMarkerLSN() : -1L;
    }
}
