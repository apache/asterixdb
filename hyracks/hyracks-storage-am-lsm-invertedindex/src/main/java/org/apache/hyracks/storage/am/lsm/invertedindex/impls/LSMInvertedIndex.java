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
package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.ICursorInitialState;
import org.apache.hyracks.storage.am.common.api.IIndexAccessor;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.IVirtualFreePageManager;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import org.apache.hyracks.storage.am.common.impls.AbstractSearchPredicate;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
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
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndex extends AbstractLSMIndex implements IInvertedIndex {

    protected final IBinaryTokenizerFactory tokenizerFactory;

    // On-disk components.
    // For creating inverted indexes in flush and merge.
    protected final ILSMComponentFactory componentFactory;

    // Type traits and comparators for tokens and inverted-list elements.
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    private final int[] invertedIndexFields;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;

    public LSMInvertedIndex(List<IVirtualBufferCache> virtualBufferCaches,
            OnDiskInvertedIndexFactory diskInvIndexFactory, BTreeFactory deletedKeysBTreeFactory,
            BloomFilterFactory bloomFilterFactory, ILSMComponentFilterFactory filterFactory,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, ILSMIndexFileManager fileManager,
            IFileMapProvider diskFileMapProvider, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, int[] invertedIndexFields, int[] filterFields,
            int[] filterFieldsForNonBulkLoadOps, int[] invertedIndexFieldsForNonBulkLoadOps, boolean durable)
            throws IndexException {
        super(virtualBufferCaches, diskInvIndexFactory.getBufferCache(), fileManager, diskFileMapProvider,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallback, filterFrameFactory,
                filterManager, filterFields, durable);

        this.tokenizerFactory = tokenizerFactory;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.invertedIndexFields = invertedIndexFields;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;

        componentFactory = new LSMInvertedIndexDiskComponentFactory(diskInvIndexFactory, deletedKeysBTreeFactory,
                bloomFilterFactory, filterFactory);

        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            InMemoryInvertedIndex memInvIndex = createInMemoryInvertedIndex(virtualBufferCache,
                    new VirtualFreePageManager(virtualBufferCache.getNumPages()), i);
            BTree deleteKeysBTree = BTreeUtils.createBTree(virtualBufferCache, new VirtualFreePageManager(
                    virtualBufferCache.getNumPages()), virtualBufferCache.getFileMapProvider(), invListTypeTraits,
                    invListCmpFactories, BTreeLeafFrameType.REGULAR_NSM,
                    new FileReference(new File(fileManager.getBaseDir() + "_virtual_del_" + i)));
            LSMInvertedIndexMemoryComponent mutableComponent = new LSMInvertedIndexMemoryComponent(memInvIndex,
                    deleteKeysBTree, virtualBufferCache, i == 0 ? true : false, filterFactory == null ? null
                            : filterFactory.createLSMComponentFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
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

        try {
            List<ILSMComponent> immutableComponents = diskComponents;
            immutableComponents.clear();
            List<LSMComponentFileReferences> validFileReferences = fileManager.cleanupAndGetValidFiles();
            for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
                LSMInvertedIndexDiskComponent component;
                try {
                    component = createDiskInvIndexComponent(componentFactory,
                            lsmComonentFileReference.getInsertIndexFileReference(),
                            lsmComonentFileReference.getDeleteIndexFileReference(),
                            lsmComonentFileReference.getBloomFilterFileReference(), false);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
                immutableComponents.add(component);
            }
            isActivated = true;
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }

        clearMemoryComponents();
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            LSMInvertedIndexDiskComponent component = (LSMInvertedIndexDiskComponent) c;
            component.getBloomFilter().deactivate();
            component.getInvIndex().deactivate();
            component.getDeletedKeysBTree().deactivate();
            component.getBloomFilter().destroy();
            component.getInvIndex().destroy();
            component.getDeletedKeysBTree().destroy();
        }
        immutableComponents.clear();
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
            LSMInvertedIndexDiskComponent component = (LSMInvertedIndexDiskComponent) c;
            component.getBloomFilter().deactivate();
            component.getInvIndex().deactivate();
            component.getDeletedKeysBTree().deactivate();
        }
        deallocateMemoryComponents();
        isActivated = false;
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        deactivate(true);
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }

        for (ILSMComponent c : memoryComponents) {
            LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) c;
            mutableComponent.getInvIndex().destroy();
            mutableComponent.getDeletedKeysBTree().destroy();
        }
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            LSMInvertedIndexDiskComponent component = (LSMInvertedIndexDiskComponent) c;
            component.getInvIndex().destroy();
            component.getDeletedKeysBTree().destroy();
            component.getBloomFilter().destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) throws HyracksDataException {
        List<ILSMComponent> immutableComponents = diskComponents;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        int cmc = currentMutableComponentId.get();
        ctx.setCurrentMutableComponentId(cmc);
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case FLUSH:
            case DELETE:
            case INSERT:
                operationalComponents.add(memoryComponents.get(cmc));
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
                                ((LSMInvertedIndexOpContext) ctx).filterCmp)) {
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

    /**
     * The keys in the in-memory deleted-keys BTree only refer to on-disk components.
     * We delete documents from the in-memory inverted index by deleting its entries directly,
     * while still adding the deleted key to the deleted-keys BTree.
     * Otherwise, inserts would have to remove keys from the in-memory deleted-keys BTree which
     * may cause incorrect behavior (lost deletes) in the following pathological case:
     * Insert doc 1, flush, delete doc 1, insert doc 1
     * After the sequence above doc 1 will now appear twice because the delete of the on-disk doc 1 has been lost.
     * Insert:
     * - Insert document into in-memory inverted index.
     * Delete:
     * - Delete document from in-memory inverted index (ignore if it does not exist).
     * - Insert key into deleted-keys BTree.
     */
    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) ictx;
        // TODO: This is a hack to support logging properly in ASTERIX.
        // The proper undo operations are only dependent on the after image so
        // it is correct to say we found nothing (null) as the before image (at least
        // in the perspective of ASTERIX). The semantics for the operation callbacks
        // are violated here (and they are somewhat unclear in the first place as to
        // what they should be for an inverted index).

        ITupleReference indexTuple;
        if (ctx.indexTuple != null) {
            ctx.indexTuple.reset(tuple);
            indexTuple = ctx.indexTuple;
        } else {
            indexTuple = tuple;
        }

        ctx.modificationCallback.before(indexTuple);
        ctx.modificationCallback.found(null, indexTuple);
        switch (ctx.getOperation()) {
            case INSERT: {
                // Insert into the in-memory inverted index.
                ctx.currentMutableInvIndexAccessors.insert(indexTuple);
                break;
            }
            case DELETE: {
                // First remove all entries in the in-memory inverted index (if any).
                ctx.currentMutableInvIndexAccessors.delete(indexTuple);
                // Insert key into the deleted-keys BTree.
                ctx.keysOnlyTuple.reset(indexTuple);
                try {
                    ctx.currentDeletedKeysBTreeAccessors.insert(ctx.keysOnlyTuple);
                } catch (TreeIndexDuplicateKeyException e) {
                    // Key has already been deleted.
                }
                break;
            }
            default: {
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
            }
        }
        if (ctx.filterTuple != null) {
            ctx.filterTuple.reset(tuple);
            memoryComponents.get(currentMutableComponentId.get()).getLSMComponentFilter()
                    .update(ctx.filterTuple, ctx.filterCmp);
        }
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();
        int numComponents = operationalComponents.size();
        boolean includeMutableComponent = false;
        ArrayList<IIndexAccessor> indexAccessors = new ArrayList<IIndexAccessor>(numComponents);
        ArrayList<IIndexAccessor> deletedKeysBTreeAccessors = new ArrayList<IIndexAccessor>(numComponents);

        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                IIndexAccessor invIndexAccessor = ((LSMInvertedIndexMemoryComponent) component).getInvIndex()
                        .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                indexAccessors.add(invIndexAccessor);
                IIndexAccessor deletedKeysAccessor = ((LSMInvertedIndexMemoryComponent) component)
                        .getDeletedKeysBTree().createAccessor(NoOpOperationCallback.INSTANCE,
                                NoOpOperationCallback.INSTANCE);
                deletedKeysBTreeAccessors.add(deletedKeysAccessor);
            } else {
                IIndexAccessor invIndexAccessor = ((LSMInvertedIndexDiskComponent) component).getInvIndex()
                        .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                indexAccessors.add(invIndexAccessor);
                IIndexAccessor deletedKeysAccessor = ((LSMInvertedIndexDiskComponent) component).getDeletedKeysBTree()
                        .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
                deletedKeysBTreeAccessors.add(deletedKeysAccessor);
            }
        }

        ICursorInitialState initState = createCursorInitialState(pred, ictx, includeMutableComponent, indexAccessors,
                deletedKeysBTreeAccessors, operationalComponents);
        cursor.open(initState, pred);
    }

    private ICursorInitialState createCursorInitialState(ISearchPredicate pred, IIndexOperationContext ictx,
            boolean includeMutableComponent, ArrayList<IIndexAccessor> indexAccessors,
            ArrayList<IIndexAccessor> deletedKeysBTreeAccessors, List<ILSMComponent> operationalComponents) {
        ICursorInitialState initState = null;
        PermutingTupleReference keysOnlyTuple = createKeysOnlyTupleReference();
        MultiComparator keyCmp = MultiComparator.create(invListCmpFactories);

        // TODO: This check is not pretty, but it does the job. Come up with something more OO in the future.
        // Distinguish between regular searches and range searches (mostly used in merges).
        if (pred instanceof InvertedIndexSearchPredicate) {
            initState = new LSMInvertedIndexSearchCursorInitialState(keyCmp, keysOnlyTuple, indexAccessors,
                    deletedKeysBTreeAccessors,
                    ((LSMInvertedIndexMemoryComponent) memoryComponents.get(currentMutableComponentId.get()))
                            .getDeletedKeysBTree().getLeafFrameFactory(), ictx, includeMutableComponent, lsmHarness,
                    operationalComponents);
        } else {
            LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) memoryComponents
                    .get(currentMutableComponentId.get());
            InMemoryInvertedIndex memInvIndex = (InMemoryInvertedIndex) mutableComponent.getInvIndex();
            MultiComparator tokensAndKeysCmp = MultiComparator.create(memInvIndex.getBTree().getComparatorFactories());
            initState = new LSMInvertedIndexRangeSearchCursorInitialState(tokensAndKeysCmp, keyCmp, keysOnlyTuple,
                    ((LSMInvertedIndexMemoryComponent) memoryComponents.get(currentMutableComponentId.get()))
                            .getDeletedKeysBTree().getLeafFrameFactory(), includeMutableComponent, lsmHarness,
                    indexAccessors, deletedKeysBTreeAccessors, pred, operationalComponents);
        }
        return initState;
    }

    /**
     * Returns a permuting tuple reference that projects away the document field(s) of a tuple, only leaving the key fields.
     */
    private PermutingTupleReference createKeysOnlyTupleReference() {
        // Project away token fields.
        int[] keyFieldPermutation = new int[invListTypeTraits.length];
        int numTokenFields = tokenTypeTraits.length;
        for (int i = 0; i < invListTypeTraits.length; i++) {
            keyFieldPermutation[i] = numTokenFields + i;
        }
        return new PermutingTupleReference(keyFieldPermutation);
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        LSMInvertedIndexOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        opCtx.setOperation(IndexOperation.FLUSH);
        opCtx.getComponentHolder().add(flushingComponent);
        ioScheduler.scheduleOperation(new LSMInvertedIndexFlushOperation(
                new LSMInvertedIndexAccessor(lsmHarness, opCtx), flushingComponent, componentFileRefs
                        .getInsertIndexFileReference(), componentFileRefs.getDeleteIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir()));
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMInvertedIndexFlushOperation flushOp = (LSMInvertedIndexFlushOperation) operation;

        // Create an inverted index instance to be bulk loaded.
        LSMInvertedIndexDiskComponent component = createDiskInvIndexComponent(componentFactory,
                flushOp.getDictBTreeFlushTarget(), flushOp.getDeletedKeysBTreeFlushTarget(),
                flushOp.getBloomFilterFlushTarget(), true);
        IInvertedIndex diskInvertedIndex = component.getInvIndex();

        // Create a scan cursor on the BTree underlying the in-memory inverted index.
        LSMInvertedIndexMemoryComponent flushingComponent = (LSMInvertedIndexMemoryComponent) flushOp
                .getFlushingComponent();
        InMemoryInvertedIndexAccessor memInvIndexAccessor = (InMemoryInvertedIndexAccessor) flushingComponent
                .getInvIndex().createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        BTreeAccessor memBTreeAccessor = memInvIndexAccessor.getBTreeAccessor();
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        IIndexCursor scanCursor = memBTreeAccessor.createSearchCursor(false);
        memBTreeAccessor.search(scanCursor, nullPred);

        // Bulk load the disk inverted index from the in-memory inverted index.
        IIndexBulkLoader invIndexBulkLoader = diskInvertedIndex.createBulkLoader(1.0f, false, 0L, false);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                invIndexBulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
        }
        invIndexBulkLoader.end();

        IIndexAccessor deletedKeysBTreeAccessor = flushingComponent.getDeletedKeysBTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IIndexCursor btreeCountingCursor = ((BTreeAccessor) deletedKeysBTreeAccessor).createCountingSearchCursor();
        deletedKeysBTreeAccessor.search(btreeCountingCursor, nullPred);
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

        if (numBTreeTuples > 0) {
            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numBTreeTuples);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);

            // Create an BTree instance for the deleted keys.
            BTree diskDeletedKeysBTree = component.getDeletedKeysBTree();

            // Create a scan cursor on the deleted keys BTree underlying the in-memory inverted index.
            IIndexCursor deletedKeysScanCursor = deletedKeysBTreeAccessor.createSearchCursor(false);
            deletedKeysBTreeAccessor.search(deletedKeysScanCursor, nullPred);

            // Bulk load the deleted-keys BTree.
            IIndexBulkLoader deletedKeysBTreeBulkLoader = diskDeletedKeysBTree.createBulkLoader(1.0f, false, 0L, false);
            IIndexBulkLoader builder = component.getBloomFilter().createBuilder(numBTreeTuples,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());

            try {
                while (deletedKeysScanCursor.hasNext()) {
                    deletedKeysScanCursor.next();
                    deletedKeysBTreeBulkLoader.add(deletedKeysScanCursor.getTuple());
                    builder.add(deletedKeysScanCursor.getTuple());
                }
            } finally {
                deletedKeysScanCursor.close();
                builder.end();
            }
            deletedKeysBTreeBulkLoader.end();
        }

        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<ITupleReference>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            filterManager.updateFilterInfo(component.getLSMComponentFilter(), filterTuples);
            filterManager.writeFilterInfo(component.getLSMComponentFilter(),
                    ((OnDiskInvertedIndex) component.getInvIndex()).getBTree());
        }

        return component;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        LSMInvertedIndexOpContext ictx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        ictx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        IIndexCursor cursor = new LSMInvertedIndexRangeSearchCursor(ictx);

        LSMInvertedIndexDiskComponent firstComponent = (LSMInvertedIndexDiskComponent) mergingComponents.get(0);
        OnDiskInvertedIndex firstInvIndex = (OnDiskInvertedIndex) firstComponent.getInvIndex();
        String firstFileName = firstInvIndex.getBTree().getFileReference().getFile().getName();

        LSMInvertedIndexDiskComponent lastComponent = (LSMInvertedIndexDiskComponent) mergingComponents
                .get(mergingComponents.size() - 1);
        OnDiskInvertedIndex lastInvIndex = (OnDiskInvertedIndex) lastComponent.getInvIndex();
        String lastFileName = lastInvIndex.getBTree().getFileReference().getFile().getName();

        LSMComponentFileReferences relMergeFileRefs = fileManager.getRelMergeFileReference(firstFileName, lastFileName);
        ILSMIndexAccessorInternal accessor = new LSMInvertedIndexAccessor(lsmHarness, ictx);
        ioScheduler.scheduleOperation(new LSMInvertedIndexMergeOperation(accessor, mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getDeleteIndexFileReference(),
                relMergeFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir()));
    }

    @Override
    public ILSMComponent merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMInvertedIndexMergeOperation mergeOp = (LSMInvertedIndexMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();

        RangePredicate mergePred = new RangePredicate(null, null, true, true, null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        opCtx.getComponentHolder().addAll(mergeOp.getMergingComponents());
        // Scan diskInvertedIndexes ignoring the memoryInvertedIndex.
        search(opCtx, cursor, mergePred);

        // Create an inverted index instance.
        LSMInvertedIndexDiskComponent component = createDiskInvIndexComponent(componentFactory,
                mergeOp.getDictBTreeMergeTarget(), mergeOp.getDeletedKeysBTreeMergeTarget(),
                mergeOp.getBloomFilterMergeTarget(), true);

        IInvertedIndex mergedDiskInvertedIndex = component.getInvIndex();

        // In case we must keep the deleted-keys BTrees, then they must be merged *before* merging the inverted indexes so that
        // lsmHarness.endSearch() is called once when the inverted indexes have been merged.
        if (mergeOp.getMergingComponents().get(mergeOp.getMergingComponents().size() - 1) != diskComponents
                .get(diskComponents.size() - 1)) {
            // Keep the deleted tuples since the oldest disk component is not included in the merge operation

            LSMInvertedIndexDeletedKeysBTreeMergeCursor btreeCursor = new LSMInvertedIndexDeletedKeysBTreeMergeCursor(
                    opCtx);
            search(opCtx, btreeCursor, mergePred);

            BTree btree = component.getDeletedKeysBTree();
            IIndexBulkLoader btreeBulkLoader = btree.createBulkLoader(1.0f, true, 0L, false);

            long numElements = 0L;
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                numElements += ((LSMInvertedIndexDiskComponent) mergeOp.getMergingComponents().get(i)).getBloomFilter()
                        .getNumElements();
            }

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            IIndexBulkLoader builder = component.getBloomFilter().createBuilder(numElements,
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
            btreeBulkLoader.end();
        }

        IIndexBulkLoader invIndexBulkLoader = mergedDiskInvertedIndex.createBulkLoader(1.0f, true, 0L, false);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                invIndexBulkLoader.add(tuple);
            }
        } finally {
            cursor.close();
        }
        invIndexBulkLoader.end();

        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<ITupleReference>();
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple());
                filterTuples.add(mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple());
            }
            filterManager.updateFilterInfo(component.getLSMComponentFilter(), filterTuples);
            filterManager.writeFilterInfo(component.getLSMComponentFilter(),
                    ((OnDiskInvertedIndex) component.getInvIndex()).getBTree());
        }

        return component;
    }

    private ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskInvIndexComponent(componentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), true);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws IndexException {
        try {
            return new LSMInvertedIndexBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
        } catch (HyracksDataException e) {
            throw new IndexException(e);
        }
    }

    public class LSMInvertedIndexBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final IIndexBulkLoader invIndexBulkLoader;
        private boolean cleanedUpArtifacts = false;
        private boolean isEmptyComponent = true;
        public final PermutingTupleReference indexTuple;
        public final PermutingTupleReference filterTuple;
        public final MultiComparator filterCmp;

        public LSMInvertedIndexBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex) throws IndexException, HyracksDataException {
            if (checkIfEmptyIndex && !isEmptyIndex()) {
                throw new IndexException("Cannot load an index that is not empty");
            }
            // Note that by using a flush target file name, we state that the
            // new bulk loaded tree is "newer" than any other merged tree.
            try {
                component = createBulkLoadTarget();
            } catch (HyracksDataException | IndexException e) {
                throw new IndexException(e);
            }
            invIndexBulkLoader = ((LSMInvertedIndexDiskComponent) component).getInvIndex().createBulkLoader(fillFactor,
                    verifyInput, numElementsHint, false);

            if (filterFields != null) {
                indexTuple = new PermutingTupleReference(invertedIndexFields);
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

                invIndexBulkLoader.add(t);

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
                ((LSMInvertedIndexDiskComponent) component).getInvIndex().deactivate();
                ((LSMInvertedIndexDiskComponent) component).getInvIndex().destroy();
                ((LSMInvertedIndexDiskComponent) component).getDeletedKeysBTree().deactivate();
                ((LSMInvertedIndexDiskComponent) component).getDeletedKeysBTree().destroy();
                ((LSMInvertedIndexDiskComponent) component).getBloomFilter().deactivate();
                ((LSMInvertedIndexDiskComponent) component).getBloomFilter().destroy();
            }
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            if (!cleanedUpArtifacts) {
                invIndexBulkLoader.end();

                if (component.getLSMComponentFilter() != null) {
                    filterManager.writeFilterInfo(component.getLSMComponentFilter(),
                            (((OnDiskInvertedIndex) ((LSMInvertedIndexDiskComponent) component).getInvIndex())
                                    .getBTree()));
                }

                if (isEmptyComponent) {
                    cleanupArtifacts();
                } else {
                    lsmHarness.addBulkLoadedComponent(component);
                }
            }
        }
    }

    protected InMemoryInvertedIndex createInMemoryInvertedIndex(IVirtualBufferCache virtualBufferCache,
            IVirtualFreePageManager virtualFreePageManager, int id) throws IndexException {
        return InvertedIndexUtils.createInMemoryBTreeInvertedindex(virtualBufferCache, virtualFreePageManager,
                invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                new FileReference(new File(fileManager.getBaseDir() + "_virtual_vocab_" + id)));
    }

    protected LSMInvertedIndexDiskComponent createDiskInvIndexComponent(ILSMComponentFactory factory,
            FileReference dictBTreeFileRef, FileReference btreeFileRef, FileReference bloomFilterFileRef, boolean create)
            throws HyracksDataException, IndexException {
        LSMInvertedIndexDiskComponent component = (LSMInvertedIndexDiskComponent) factory
                .createLSMComponentInstance(new LSMComponentFileReferences(dictBTreeFileRef, btreeFileRef,
                        bloomFilterFileRef));
        if (create) {
            component.getInvIndex().create();
            component.getDeletedKeysBTree().create();
            component.getBloomFilter().create();
        }
        // Will be closed during cleanup of merge().
        component.getInvIndex().activate();
        component.getDeletedKeysBTree().activate();
        component.getBloomFilter().activate();
        if (component.getLSMComponentFilter() != null) {
            filterManager.readFilterInfo(component.getLSMComponentFilter(),
                    ((OnDiskInvertedIndex) component.getInvIndex()).getBTree());
        }
        return component;
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException {
        return new LSMInvertedIndexAccessor(lsmHarness, createOpContext(modificationCallback, searchCallback));
    }

    private LSMInvertedIndexOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException {
        return new LSMInvertedIndexOpContext(memoryComponents, modificationCallback, searchCallback,
                invertedIndexFieldsForNonBulkLoadOps, filterFieldsForNonBulkLoadOps);
    }

    @Override
    public IInvertedListCursor createInvertedListCursor() {
        throw new UnsupportedOperationException("Cannot create inverted list cursor on lsm inverted index.");
    }

    @Override
    public void openInvertedListCursor(IInvertedListCursor listCursor, ITupleReference searchKey,
            IIndexOperationContext ictx) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("Cannot open inverted list cursor on lsm inverted index.");
    }

    @Override
    public ITypeTraits[] getInvListTypeTraits() {
        return invListTypeTraits;
    }

    @Override
    public IBinaryComparatorFactory[] getInvListCmpFactories() {
        return invListCmpFactories;
    }

    @Override
    public long getMemoryAllocationSize() {
        long size = 0;
        for (ILSMComponent c : memoryComponents) {
            LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) c;
            IBufferCache virtualBufferCache = mutableComponent.getInvIndex().getBufferCache();
            size += virtualBufferCache.getNumPages() * virtualBufferCache.getPageSize();
        }
        return size;
    }

    @Override
    public ITypeTraits[] getTokenTypeTraits() {
        return tokenTypeTraits;
    }

    @Override
    public IBinaryComparatorFactory[] getTokenCmpFactories() {
        return tokenCmpFactories;
    }

    public IBinaryTokenizerFactory getTokenizerFactory() {
        return tokenizerFactory;
    }

    protected void forceFlushInvListsFileDirtyPages(OnDiskInvertedIndex invIndex) throws HyracksDataException {
        int fileId = invIndex.getInvListsFileId();
        IBufferCache bufferCache = invIndex.getBufferCache();
        int startPageId = 0;
        int maxPageId = invIndex.getInvListsMaxPageId();
        forceFlushDirtyPages(bufferCache, fileId, startPageId, maxPageId);
    }

    @Override
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        LSMInvertedIndexDiskComponent invIndexComponent = (LSMInvertedIndexDiskComponent) lsmComponent;
        OnDiskInvertedIndex invIndex = (OnDiskInvertedIndex) invIndexComponent.getInvIndex();
        // Flush the bloom filter first.
        int fileId = invIndexComponent.getBloomFilter().getFileId();
        IBufferCache bufferCache = invIndex.getBufferCache();
        int startPage = 0;
        int maxPage = invIndexComponent.getBloomFilter().getNumPages();
        forceFlushDirtyPages(bufferCache, fileId, startPage, maxPage);

        // Flush inverted index second.
        forceFlushDirtyPages(invIndex.getBTree());
        forceFlushInvListsFileDirtyPages(invIndex);
        markAsValidInternal(invIndex.getBTree());

        // Flush deleted keys BTree.
        forceFlushDirtyPages(invIndexComponent.getDeletedKeysBTree());
        markAsValidInternal(invIndexComponent.getDeletedKeysBTree());
    }

    @Override
    public void validate() throws HyracksDataException {
        validateMemoryComponents();
        List<ILSMComponent> immutableComponents = diskComponents;
        for (ILSMComponent c : immutableComponents) {
            LSMInvertedIndexDiskComponent component = (LSMInvertedIndexDiskComponent) c;
            component.getInvIndex().validate();
            component.getDeletedKeysBTree().validate();
        }
    }

    @Override
    public String toString() {
        return "LSMInvertedIndex [" + fileManager.getBaseDir() + "]";
    }

    @Override
    public boolean hasMemoryComponents() {
        return true;
    }

    @Override
    public boolean isPrimaryIndex() {
        return false;
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles(ILSMComponent lsmComponent) {
        Set<String> files = new HashSet<String>();

        LSMInvertedIndexDiskComponent invIndexComponent = (LSMInvertedIndexDiskComponent) lsmComponent;
        OnDiskInvertedIndex invIndex = (OnDiskInvertedIndex) invIndexComponent.getInvIndex();

        files.add(invIndex.getInvListsFile().toString());
        files.add(invIndex.getBTree().toString());
        files.add(invIndexComponent.getBloomFilter().getFileReference().toString());
        files.add(invIndexComponent.getDeletedKeysBTree().getFileReference().toString());

        return files;
    }

    @Override
    public synchronized void allocateMemoryComponents() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to allocate memory components since the index is not active.");
        }
        if (memoryComponentsAllocated) {
            return;
        }
        for (ILSMComponent c : memoryComponents) {
            LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) c;
            ((IVirtualBufferCache) mutableComponent.getInvIndex().getBufferCache()).open();
            mutableComponent.getInvIndex().create();
            mutableComponent.getInvIndex().activate();
            mutableComponent.getDeletedKeysBTree().create();
            mutableComponent.getDeletedKeysBTree().activate();
        }
        memoryComponentsAllocated = true;
    }

    private void addOperationalMutableComponents(List<ILSMComponent> operationalComponents) {
        int cmc = currentMutableComponentId.get();
        int numMutableComponents = memoryComponents.size();
        for (int i = 0; i < numMutableComponents - 1; i++) {
            ILSMComponent c = memoryComponents.get((cmc + i + 1) % numMutableComponents);
            LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) c;
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
                LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) c;
                mutableComponent.getInvIndex().clear();
                mutableComponent.getDeletedKeysBTree().clear();
                mutableComponent.reset();
            }
        }
    }

    private synchronized void validateMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMComponent c : memoryComponents) {
                LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) c;
                mutableComponent.getInvIndex().validate();
                mutableComponent.getDeletedKeysBTree().validate();
            }
        }
    }

    private synchronized void deallocateMemoryComponents() throws HyracksDataException {
        if (memoryComponentsAllocated) {
            for (ILSMComponent c : memoryComponents) {
                LSMInvertedIndexMemoryComponent mutableComponent = (LSMInvertedIndexMemoryComponent) c;
                mutableComponent.getInvIndex().deactivate();
                mutableComponent.getDeletedKeysBTree().deactivate();
                mutableComponent.getInvIndex().destroy();
                mutableComponent.getDeletedKeysBTree().destroy();
                ((IVirtualBufferCache) mutableComponent.getInvIndex().getBufferCache()).close();
            }
            memoryComponentsAllocated = false;
        }
    }
}