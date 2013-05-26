/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
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
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.btree.tuples.LSMBTreeTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMBTree extends AbstractLSMIndex implements ITreeIndex {

    // In-memory components.
    private final LSMBTreeMutableComponent mutableComponent;

    // For creating BTree's used in flush and merge.
    private final LSMBTreeImmutableComponentFactory componentFactory;
    // For creating BTree's used in bulk load. Different from diskBTreeFactory
    // because it should have a different tuple writer in it's leaf frames.
    private final LSMBTreeImmutableComponentFactory bulkLoadComponentFactory;

    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory insertLeafFrameFactory;
    private final ITreeIndexFrameFactory deleteLeafFrameFactory;
    private final IBinaryComparatorFactory[] cmpFactories;

    public LSMBTree(IVirtualBufferCache virtualBufferCache, IInMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMIndexFileManager fileManager,
            TreeIndexFactory<BTree> diskBTreeFactory, TreeIndexFactory<BTree> bulkLoadBTreeFactory,
            BloomFilterFactory bloomFilterFactory, double bloomFilterFalsePositiveRate,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackProvider ioOpCallbackProvider) {
        super(memFreePageManager, diskBTreeFactory.getBufferCache(), fileManager, diskFileMapProvider,
                bloomFilterFalsePositiveRate, mergePolicy, opTrackerFactory, ioScheduler, ioOpCallbackProvider);
        mutableComponent = new LSMBTreeMutableComponent(new BTree(virtualBufferCache,
                virtualBufferCache.getFileMapProvider(), memFreePageManager, interiorFrameFactory,
                insertLeafFrameFactory, cmpFactories, fieldCount, new FileReference(new File("membtree"))),
                memFreePageManager);
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.cmpFactories = cmpFactories;
        componentFactory = new LSMBTreeImmutableComponentFactory(diskBTreeFactory, bloomFilterFactory);
        bulkLoadComponentFactory = new LSMBTreeImmutableComponentFactory(bulkLoadBTreeFactory, bloomFilterFactory);
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        fileManager.deleteDirs();
        fileManager.createDirs();
        componentsRef.get().clear();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActivated) {
            return;
        }

        ((IVirtualBufferCache) mutableComponent.getBTree().getBufferCache()).open();
        mutableComponent.getBTree().create();
        mutableComponent.getBTree().activate();
        List<ILSMComponent> immutableComponents = componentsRef.get();
        immutableComponents.clear();
        List<LSMComponentFileReferences> validFileReferences;
        try {
            validFileReferences = fileManager.cleanupAndGetValidFiles();
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
        for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
            LSMBTreeImmutableComponent component;
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
            return;
        }

        if (flushOnExit) {
            BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(
                    ioOpCallbackProvider.getIOOperationCallback(this));
            ILSMIndexAccessor accessor = createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            accessor.scheduleFlush(cb);
            try {
                cb.waitForIO();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }

        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            LSMBTreeImmutableComponent component = (LSMBTreeImmutableComponent) c;
            BTree btree = component.getBTree();
            BloomFilter bloomFilter = component.getBloomFilter();
            btree.deactivate();
            bloomFilter.deactivate();
        }
        mutableComponent.getBTree().deactivate();
        mutableComponent.getBTree().destroy();
        ((IVirtualBufferCache) mutableComponent.getBTree().getBufferCache()).close();
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

        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            LSMBTreeImmutableComponent component = (LSMBTreeImmutableComponent) c;
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
        }
        mutableComponent.getBTree().destroy();
        fileManager.deleteDirs();
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }

        List<ILSMComponent> immutableComponents = componentsRef.get();
        mutableComponent.getBTree().clear();
        for (ILSMComponent c : immutableComponents) {
            LSMBTreeImmutableComponent component = (LSMBTreeImmutableComponent) c;
            component.getBloomFilter().deactivate();
            component.getBTree().deactivate();
            component.getBloomFilter().destroy();
            component.getBTree().destroy();
        }
        immutableComponents.clear();
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) {
        List<ILSMComponent> immutableComponents = componentsRef.get();
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case UPDATE:
            case UPSERT:
            case PHYSICALDELETE:
            case FLUSH:
            case DELETE:
                operationalComponents.add(mutableComponent);
                break;
            case SEARCH:
            case INSERT:
                operationalComponents.add(mutableComponent);
                operationalComponents.addAll(immutableComponents);
                break;
            case MERGE:
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
                ctx.memBTreeAccessor.delete(tuple);
                break;
            case INSERT:
                insert(tuple, ctx);
                break;
            default:
                ctx.memBTreeAccessor.upsert(tuple);
                break;
        }
        mutableComponent.setIsModified();
    }

    private boolean insert(ITupleReference tuple, LSMBTreeOpContext ctx) throws HyracksDataException, IndexException {
        MultiComparator comparator = MultiComparator.createIgnoreFieldLength(mutableComponent.getBTree()
                .getComparatorFactories());
        LSMBTreePointSearchCursor searchCursor = new LSMBTreePointSearchCursor(ctx);
        IIndexCursor memCursor = new BTreeRangeSearchCursor(ctx.memBTreeOpCtx.leafFrame, false);
        RangePredicate predicate = new RangePredicate(tuple, tuple, true, true, comparator, comparator);

        // first check the inmemory component
        ctx.memBTreeAccessor.search(memCursor, predicate);
        try {
            if (memCursor.hasNext()) {
                memCursor.next();
                LSMBTreeTupleReference lsmbtreeTuple = (LSMBTreeTupleReference) memCursor.getTuple();
                if (!lsmbtreeTuple.isAntimatter()) {
                    throw new BTreeDuplicateKeyException("Failed to insert key since key already exists.");
                } else {
                    memCursor.close();
                    ctx.memBTreeAccessor.upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);
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
        search(ctx, searchCursor, predicate);
        try {
            if (searchCursor.hasNext()) {
                throw new BTreeDuplicateKeyException("Failed to insert key since key already exists.");
            }
        } finally {
            searchCursor.close();
        }
        ctx.memBTreeAccessor.upsertIfConditionElseInsert(tuple, AntimatterAwareTupleAcceptor.INSTANCE);

        return true;
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        LSMBTreeOpContext ctx = (LSMBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        int numBTrees = operationalComponents.size();
        assert numBTrees > 0;

        boolean includeMutableComponent = operationalComponents.get(0) == mutableComponent;
        LSMBTreeCursorInitialState initialState = new LSMBTreeCursorInitialState(numBTrees, insertLeafFrameFactory,
                ctx.cmp, ctx.bloomFilterCmp, includeMutableComponent, lsmHarness, ctx.memBTreeAccessor, pred,
                ctx.searchCallback, operationalComponents);
        cursor.open(initialState, pred);
    }

    @Override
    public boolean scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        if (!mutableComponent.isModified()) {
            return false;
        }
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        LSMBTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        assert ctx.getComponentHolder().size() == 1;
        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
        opCtx.setOperation(IndexOperation.FLUSH);
        opCtx.getComponentHolder().add(flushingComponent);
        ILSMIndexAccessorInternal flushAccessor = new LSMBTreeAccessor(lsmHarness, opCtx);
        ioScheduler.scheduleOperation(new LSMBTreeFlushOperation(flushAccessor, flushingComponent, componentFileRefs
                .getInsertIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), callback));
        return true;
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMBTreeFlushOperation flushOp = (LSMBTreeFlushOperation) operation;
        LSMBTreeMutableComponent flushingComponent = (LSMBTreeMutableComponent) flushOp.getFlushingComponent();
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

        LSMBTreeImmutableComponent component = createDiskComponent(componentFactory, flushOp.getBTreeFlushTarget(),
                flushOp.getBloomFilterFlushTarget(), true);
        IIndexBulkLoader bulkLoader = component.getBTree().createBulkLoader(1.0f, false, numElements);
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

    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        LSMBTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        opCtx.getComponentHolder().addAll(mergingComponents);
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor(opCtx);
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        search(opCtx, cursor, rangePred);

        opCtx.setOperation(IndexOperation.MERGE);
        BTree firstBTree = (BTree) ((LSMBTreeImmutableComponent) mergingComponents.get(0)).getBTree();
        BTree lastBTree = (BTree) ((LSMBTreeImmutableComponent) mergingComponents.get(mergingComponents.size() - 1))
                .getBTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstBTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastBTree.getFileId());
        LSMComponentFileReferences relMergeFileRefs = fileManager.getRelMergeFileReference(firstFile.getFile()
                .getName(), lastFile.getFile().getName());
        ILSMIndexAccessorInternal accessor = new LSMBTreeAccessor(lsmHarness, opCtx);
        ioScheduler.scheduleOperation(new LSMBTreeMergeOperation(accessor, mergingComponents, cursor, relMergeFileRefs
                .getInsertIndexFileReference(), relMergeFileRefs.getBloomFilterFileReference(), callback));
    }

    @Override
    public ILSMComponent merge(List<ILSMComponent> mergedComponents, ILSMIOOperation operation)
            throws HyracksDataException, IndexException {
        LSMBTreeMergeOperation mergeOp = (LSMBTreeMergeOperation) operation;
        ITreeIndexCursor cursor = mergeOp.getCursor();
        mergedComponents.addAll(mergeOp.getMergingComponents());

        long numElements = 0L;
        for (int i = 0; i < mergedComponents.size(); ++i) {
            numElements += ((LSMBTreeImmutableComponent) mergedComponents.get(i)).getBloomFilter().getNumElements();
        }

        int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                bloomFilterFalsePositiveRate);
        LSMBTreeImmutableComponent mergedComponent = createDiskComponent(componentFactory,
                mergeOp.getBTreeMergeTarget(), mergeOp.getBloomFilterMergeTarget(), true);

        IIndexBulkLoader bulkLoader = mergedComponent.getBTree().createBulkLoader(1.0f, false, numElements);
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

    private LSMBTreeImmutableComponent createDiskComponent(LSMBTreeImmutableComponentFactory factory,
            FileReference btreeFileRef, FileReference bloomFilterFileRef, boolean createComponent)
            throws HyracksDataException, IndexException {
        // Create new BTree instance.
        LSMBTreeImmutableComponent component = (LSMBTreeImmutableComponent) factory
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
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws TreeIndexException {
        try {
            return new LSMBTreeBulkLoader(fillLevel, verifyInput, numElementsHint);
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
        LSMBTreeImmutableComponent component = (LSMBTreeImmutableComponent) lsmComponent;
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
        private boolean endHasBeenCalled = false;

        public LSMBTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint)
                throws TreeIndexException, HyracksDataException {
            try {
                component = createBulkLoadTarget();
            } catch (HyracksDataException e) {
                throw new TreeIndexException(e);
            } catch (IndexException e) {
                throw new TreeIndexException(e);
            }
            bulkLoader = (BTreeBulkLoader) ((LSMBTreeImmutableComponent) component).getBTree().createBulkLoader(
                    fillFactor, verifyInput, numElementsHint);

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            builder = ((LSMBTreeImmutableComponent) component).getBloomFilter().createBuilder(numElementsHint,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                bulkLoader.add(tuple);
                builder.add(tuple);
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

        protected void handleException() throws HyracksDataException, IndexException {
            if (!endHasBeenCalled) {
                builder.end();
            }
            ((LSMBTreeImmutableComponent) component).getBTree().deactivate();
            ((LSMBTreeImmutableComponent) component).getBTree().destroy();
            ((LSMBTreeImmutableComponent) component).getBloomFilter().deactivate();
            ((LSMBTreeImmutableComponent) component).getBloomFilter().destroy();
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            bulkLoader.end();
            builder.end();
            endHasBeenCalled = true;
            lsmHarness.addBulkLoadedComponent(component);
        }

    }

    public LSMBTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMBTreeOpContext(mutableComponent.getBTree(), insertLeafFrameFactory, deleteLeafFrameFactory,
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
        return mutableComponent.getBTree().getInteriorFrameFactory();
    }

    @Override
    public int getFieldCount() {
        return mutableComponent.getBTree().getFieldCount();
    }

    @Override
    public int getFileId() {
        return mutableComponent.getBTree().getFileId();
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return mutableComponent.getBTree().getFreePageManager();
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return mutableComponent.getBTree().getLeafFrameFactory();
    }

    @Override
    public long getMemoryAllocationSize() {
        IBufferCache virtualBufferCache = mutableComponent.getBTree().getBufferCache();
        return virtualBufferCache.getNumPages() * virtualBufferCache.getPageSize();
    }

    @Override
    public int getRootPageId() {
        return mutableComponent.getBTree().getRootPageId();
    }

    public boolean isEmptyIndex() throws HyracksDataException {
        List<ILSMComponent> immutableComponents = componentsRef.get();
        return immutableComponents.isEmpty()
                && mutableComponent.getBTree().isEmptyTree(
                        mutableComponent.getBTree().getInteriorFrameFactory().createFrame());
    }

    @Override
    public void validate() throws HyracksDataException {
        mutableComponent.getBTree().validate();
        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            BTree btree = (BTree) ((LSMBTreeImmutableComponent) c).getBTree();
            btree.validate();
        }
    }

    @Override
    public String toString() {
        return "LSMBTree [" + fileManager.getBaseDir() + "]";
    }
}
