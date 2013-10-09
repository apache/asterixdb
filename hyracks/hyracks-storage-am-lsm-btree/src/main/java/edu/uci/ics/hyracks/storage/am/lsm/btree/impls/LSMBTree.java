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

package edu.uci.ics.hyracks.storage.am.lsm.btree.impls;

import java.io.File;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

    // For creating BTree's used in flush and merge.
    private final LSMBTreeDiskComponentFactory componentFactory;
    // For creating BTree's used in bulk load. Different from diskBTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final LSMBTreeDiskComponentFactory bulkLoadComponentFactory;

    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory insertLeafFrameFactory;
    private final ITreeIndexFrameFactory deleteLeafFrameFactory;
    private final IBinaryComparatorFactory[] cmpFactories;

    public LSMBTree(List<IVirtualBufferCache> virtualBufferCaches, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory insertLeafFrameFactory, ITreeIndexFrameFactory deleteLeafFrameFactory,
            ILSMIndexFileManager fileManager, TreeIndexFactory<BTree> diskBTreeFactory,
            TreeIndexFactory<BTree> bulkLoadBTreeFactory, BloomFilterFactory bloomFilterFactory,
            double bloomFilterFalsePositiveRate, IFileMapProvider diskFileMapProvider, int fieldCount,
            IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback) {
        super(virtualBufferCaches, diskBTreeFactory.getBufferCache(), fileManager, diskFileMapProvider,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallback);
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            LSMBTreeMemoryComponent mutableComponent = new LSMBTreeMemoryComponent(new BTree(virtualBufferCache,
                    virtualBufferCache.getFileMapProvider(), new VirtualFreePageManager(
                            virtualBufferCache.getNumPages()), interiorFrameFactory, insertLeafFrameFactory,
                    cmpFactories, fieldCount, new FileReference(new File(fileManager.getBaseDir() + "_virtual_" + i))),
                    virtualBufferCache, i == 0 ? true : false);
            memoryComponents.add(mutableComponent);
            ++i;
        }

        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        componentFactory = new LSMBTreeDiskComponentFactory(diskBTreeFactory, bloomFilterFactory);
        bulkLoadComponentFactory = new LSMBTreeDiskComponentFactory(bulkLoadBTreeFactory, bloomFilterFactory);
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
        for (ILSMComponent c : memoryComponents) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
            ((IVirtualBufferCache) mutableComponent.getBTree().getBufferCache()).open();
            mutableComponent.getBTree().create();
            mutableComponent.getBTree().activate();
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
                component = createDiskComponent(componentFactory,
                        lsmComonentFileReference.getInsertIndexFileReference(),
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
            BTree btree = component.getBTree();
            BloomFilter bloomFilter = component.getBloomFilter();
            btree.deactivate();
            bloomFilter.deactivate();
        }
        for (ILSMComponent c : memoryComponents) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
            mutableComponent.getBTree().deactivate();
            mutableComponent.getBTree().destroy();
            ((IVirtualBufferCache) mutableComponent.getBTree().getBufferCache()).close();
        }
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
            component.getBloomFilter().destroy();
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

        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : memoryComponents) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
            mutableComponent.getBTree().clear();
            mutableComponent.reset();
        }
        for (ILSMComponent c : immutableComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
            component.getBloomFilter().deactivate();
            component.getBTree().deactivate();
            component.getBloomFilter().destroy();
            component.getBTree().destroy();
        }
        immutableComponents.clear();
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) {
        List<ILSMComponent> immutableComponents = diskComponents;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        int cmc = currentMutableComponentId.get();
        ctx.setCurrentMutableComponentId(cmc);
        int numMutableComponents = memoryComponents.size();
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case UPDATE:
            case UPSERT:
            case PHYSICALDELETE:
            case FLUSH:
            case DELETE:
                operationalComponents.add(memoryComponents.get(cmc));
                break;
            case SEARCH:
            case INSERT:

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
                operationalComponents.addAll(immutableComponents);
                break;
            case MERGE:
                operationalComponents.addAll(ctx.getComponentsToBeMerged());
                break;
            case FULL_MERGE:
                operationalComponents.addAll(immutableComponents);
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        switch (ctx.getOperation()) {
            case PHYSICALDELETE:
                ctx.currentMutableBTreeAccessor.delete(tuple);
                break;
            case INSERT:
                insert(tuple, ctx);
                break;
            default:
                ctx.currentMutableBTreeAccessor.upsert(tuple);
                break;
        }
    }

    private boolean insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, IndexException {
        ILSMComponent c = ctx.getComponentHolder().get(0);
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
        MultiComparator comparator = MultiComparator.createIgnoreFieldLength(mutableComponent.getBTree()
                .getComparatorFactories());
        LSMBTreePointSearchCursor searchCursor = new LSMBTreePointSearchCursor(ctx);
        IIndexCursor memCursor = new BTreeRangeSearchCursor(ctx.currentMutableBTreeOpCtx.leafFrame, false);
        RangePredicate predicate = new RangePredicate(tuple, tuple, true, true, comparator, comparator);

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

        ctx.currentMutableBTreeAccessor.upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);
        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();

        LSMBTreeCursorInitialState initialState = new LSMBTreeCursorInitialState(insertLeafFrameFactory, ctx.cmp,
                ctx.bloomFilterCmp, lsmHarness, pred, ctx.searchCallback, operationalComponents);
        cursor.open(initialState, pred);
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
        ioScheduler.scheduleOperation(new LSMBTreeFlushOperation(flushAccessor, flushingComponent, componentFileRefs
                .getInsertIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), callback, fileManager
                .getBaseDir()));
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMBTreeFlushOperation flushOp = (LSMBTreeFlushOperation) operation;
        LSMBTreeMemoryComponent flushingComponent = (LSMBTreeMemoryComponent) flushOp.getFlushingComponent();
        IIndexAccessor accessor = flushingComponent.getBTree().createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        IIndexCursor countingCursor = ((BTreeAccessor) accessor).createCountingSearchCursor();
        accessor.search(countingCursor, nullPred);
        long numElements = 0L;
        try {
            while (countingCursor.hasNext()) {
                countingCursor.next();
                ITupleReference countTuple = countingCursor.getTuple();
                numElements = IntegerSerializerDeserializer.getInt(countTuple.getFieldData(0),
                        countTuple.getFieldStart(0));
            }
        } finally {
            countingCursor.close();
        }

        int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                bloomFilterFalsePositiveRate);

        LSMBTreeDiskComponent component = createDiskComponent(componentFactory, flushOp.getBTreeFlushTarget(),
                flushOp.getBloomFilterFlushTarget(), true);
        IIndexBulkLoader bulkLoader = component.getBTree().createBulkLoader(1.0f, false, numElements, false);
        IIndexBulkLoader builder = component.getBloomFilter().createBuilder(numElements,
                bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());

        IIndexCursor scanCursor = accessor.createSearchCursor();
        accessor.search(scanCursor, nullPred);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                builder.add(scanCursor.getTuple());
                bulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
            builder.end();
        }
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
        if (ctx.getComponentHolder().get(ctx.getComponentHolder().size() - 1) != diskComponents.get(diskComponents
                .size() - 1)) {
            returnDeletedTuples = true;
        }
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor(opCtx, returnDeletedTuples);
        BTree firstBTree = (BTree) ((LSMBTreeDiskComponent) mergingComponents.get(0)).getBTree();
        BTree lastBTree = (BTree) ((LSMBTreeDiskComponent) mergingComponents.get(mergingComponents.size() - 1))
                .getBTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstBTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastBTree.getFileId());
        LSMComponentFileReferences relMergeFileRefs = fileManager.getRelMergeFileReference(firstFile.getFile()
                .getName(), lastFile.getFile().getName());
        ILSMIndexAccessorInternal accessor = new LSMBTreeAccessor(lsmHarness, opCtx);
        ioScheduler.scheduleOperation(new LSMBTreeMergeOperation(accessor, mergingComponents, cursor, relMergeFileRefs
                .getInsertIndexFileReference(), relMergeFileRefs.getBloomFilterFileReference(), callback, fileManager
                .getBaseDir()));
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
        for (int i = 0; i < mergedComponents.size(); ++i) {
            numElements += ((LSMBTreeDiskComponent) mergedComponents.get(i)).getBloomFilter().getNumElements();
        }

        int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                bloomFilterFalsePositiveRate);
        LSMBTreeDiskComponent mergedComponent = createDiskComponent(componentFactory, mergeOp.getBTreeMergeTarget(),
                mergeOp.getBloomFilterMergeTarget(), true);

        IIndexBulkLoader bulkLoader = mergedComponent.getBTree().createBulkLoader(1.0f, false, numElements, false);
        IIndexBulkLoader builder = mergedComponent.getBloomFilter().createBuilder(numElements,
                bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                builder.add(frameTuple);
                bulkLoader.add(frameTuple);
            }
        } finally {
            cursor.close();
            builder.end();
        }
        bulkLoader.end();
        return mergedComponent;
    }

    private LSMBTreeDiskComponent createDiskComponent(LSMBTreeDiskComponentFactory factory, FileReference btreeFileRef,
            FileReference bloomFilterFileRef, boolean createComponent) throws HyracksDataException, IndexException {
        // Create new BTree instance.
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) factory
                .createLSMComponentInstance(new LSMComponentFileReferences(btreeFileRef, null, bloomFilterFileRef));
        if (createComponent) {
            component.getBTree().create();
            component.getBloomFilter().create();
        }
        // BTree will be closed during cleanup of merge().
        component.getBTree().activate();
        component.getBloomFilter().activate();
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

    private ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), true);
    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        // The order of forcing the dirty page to be flushed is critical. The
        // bloom filter must be always done first.
        LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) lsmComponent;
        // Flush the bloom filter first.
        int fileId = component.getBloomFilter().getFileId();
        IBufferCache bufferCache = component.getBTree().getBufferCache();
        int startPage = 0;
        int maxPage = component.getBloomFilter().getNumPages();
        forceFlushDirtyPages(bufferCache, fileId, startPage, maxPage);
        forceFlushDirtyPages(component.getBTree());
        markAsValidInternal(component.getBTree());
    }

    public class LSMBTreeBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final BTreeBulkLoader bulkLoader;
        private final IIndexBulkLoader builder;
        private boolean cleanedUpArtifacts = false;
        private boolean isEmptyComponent = true;
        private boolean endedBloomFilterLoad = false;

        public LSMBTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex)
                throws TreeIndexException, HyracksDataException {
            if (checkIfEmptyIndex && !isEmptyIndex()) {
                throw new TreeIndexException("Cannot load an index that is not empty");
            }
            try {
                component = createBulkLoadTarget();
            } catch (HyracksDataException | IndexException e) {
                throw new TreeIndexException(e);
            }
            bulkLoader = (BTreeBulkLoader) ((LSMBTreeDiskComponent) component).getBTree().createBulkLoader(fillFactor,
                    verifyInput, numElementsHint, false);

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            builder = ((LSMBTreeDiskComponent) component).getBloomFilter().createBuilder(numElementsHint,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                bulkLoader.add(tuple);
                builder.add(tuple);
            } catch (IndexException | HyracksDataException | RuntimeException e) {
                cleanupArtifacts();
                throw e;
            }
            if (isEmptyComponent) {
                isEmptyComponent = false;
            }
        }

        protected void cleanupArtifacts() throws HyracksDataException, IndexException {
            if (!cleanedUpArtifacts) {
                cleanedUpArtifacts = true;
                // We make sure to end the bloom filter load to release latches.
                if (!endedBloomFilterLoad) {
                    builder.end();
                    endedBloomFilterLoad = true;
                }
                ((LSMBTreeDiskComponent) component).getBTree().deactivate();
                ((LSMBTreeDiskComponent) component).getBTree().destroy();
                ((LSMBTreeDiskComponent) component).getBloomFilter().deactivate();
                ((LSMBTreeDiskComponent) component).getBloomFilter().destroy();
            }
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            if (!cleanedUpArtifacts) {
                if (!endedBloomFilterLoad) {
                    builder.end();
                    endedBloomFilterLoad = true;
                }
                bulkLoader.end();
                if (isEmptyComponent) {
                    cleanupArtifacts();
                } else {
                    lsmHarness.addBulkLoadedComponent(component);
                }
            }
        }
    }

    public LSMBTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMBTreeOpContext(memoryComponents, insertLeafFrameFactory, deleteLeafFrameFactory,
                modificationCallback, searchCallback, componentFactory.getBloomFilterKeyFields().length);
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
        public IIndexCursor createSearchCursor() {
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

    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getInteriorFrameFactory();
    }

    @Override
    public int getFieldCount() {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getFieldCount();
    }

    @Override
    public int getFileId() {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getFileId();
    }

    @Override
    public IFreePageManager getFreePageManager() {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getFreePageManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
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
        LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getBTree().getRootPageId();
    }

    @Override
    public void validate() throws HyracksDataException {
        for (ILSMComponent c : memoryComponents) {
            LSMBTreeMemoryComponent mutableComponent = (LSMBTreeMemoryComponent) c;
            mutableComponent.getBTree().validate();
        }
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            BTree btree = (BTree) ((LSMBTreeDiskComponent) c).getBTree();
            btree.validate();
        }
    }

    @Override
    public String toString() {
        return "LSMBTree [" + fileManager.getBaseDir() + "]";
    }
}
