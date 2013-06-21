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
package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IVirtualFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
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
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndex extends AbstractLSMIndex implements IInvertedIndex {

    // In-memory components.
    protected final LSMInvertedIndexMutableComponent mutableComponent;
    protected final IVirtualFreePageManager virtualFreePageManager;
    protected final IBinaryTokenizerFactory tokenizerFactory;

    // On-disk components.
    // For creating inverted indexes in flush and merge.
    protected final ILSMComponentFactory componentFactory;

    // Type traits and comparators for tokens and inverted-list elements.
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;

    public LSMInvertedIndex(IVirtualBufferCache virtualBufferCache, OnDiskInvertedIndexFactory diskInvIndexFactory,
            BTreeFactory deletedKeysBTreeFactory, BloomFilterFactory bloomFilterFactory,
            double bloomFilterFalsePositiveRate, ILSMIndexFileManager fileManager,
            IFileMapProvider diskFileMapProvider, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackProvider ioOpCallbackProvider) throws IndexException {
        super(virtualBufferCache, diskInvIndexFactory.getBufferCache(), fileManager, diskFileMapProvider,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallbackProvider);
        this.virtualFreePageManager = new VirtualFreePageManager(virtualBufferCache.getNumPages());
        this.tokenizerFactory = tokenizerFactory;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        // Create in-memory component.
        InMemoryInvertedIndex memInvIndex = createInMemoryInvertedIndex(virtualBufferCache);
        BTree deleteKeysBTree = BTreeUtils.createBTree(virtualBufferCache, new VirtualFreePageManager(
                virtualBufferCache.getNumPages()), ((IVirtualBufferCache) virtualBufferCache).getFileMapProvider(),
                invListTypeTraits, invListCmpFactories, BTreeLeafFrameType.REGULAR_NSM, new FileReference(new File(
                        fileManager.getBaseDir() + "_virtual_del")));
        mutableComponent = new LSMInvertedIndexMutableComponent(memInvIndex, deleteKeysBTree, virtualBufferCache);
        componentFactory = new LSMInvertedIndexComponentFactory(diskInvIndexFactory, deletedKeysBTreeFactory,
                bloomFilterFactory);
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
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }

        try {
            List<ILSMComponent> immutableComponents = componentsRef.get();
            ((IVirtualBufferCache) mutableComponent.getInvIndex().getBufferCache()).open();
            mutableComponent.getInvIndex().create();
            mutableComponent.getInvIndex().activate();
            mutableComponent.getDeletedKeysBTree().create();
            mutableComponent.getDeletedKeysBTree().activate();
            immutableComponents.clear();
            List<LSMComponentFileReferences> validFileReferences = fileManager.cleanupAndGetValidFiles();
            for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
                LSMInvertedIndexImmutableComponent component;
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
            // TODO: Maybe we can make activate throw an index exception?
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        List<ILSMComponent> immutableComponents = componentsRef.get();
        mutableComponent.getInvIndex().clear();
        mutableComponent.getDeletedKeysBTree().clear();
        mutableComponent.reset();
        for (ILSMComponent c : immutableComponents) {
            LSMInvertedIndexImmutableComponent component = (LSMInvertedIndexImmutableComponent) c;
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

        isActivated = false;
        if (flushOnExit) {
            BlockingIOOperationCallbackWrapper blockingCallBack = new BlockingIOOperationCallbackWrapper(
                    ioOpCallbackProvider.getIOOperationCallback(this));
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFlush(blockingCallBack);
            try {
                blockingCallBack.waitForIO();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }

        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            LSMInvertedIndexImmutableComponent component = (LSMInvertedIndexImmutableComponent) c;
            component.getBloomFilter().deactivate();
            component.getInvIndex().deactivate();
            component.getDeletedKeysBTree().deactivate();
        }
        mutableComponent.getInvIndex().deactivate();
        mutableComponent.getDeletedKeysBTree().deactivate();
        mutableComponent.getInvIndex().destroy();
        mutableComponent.getDeletedKeysBTree().destroy();
        ((IVirtualBufferCache) mutableComponent.getInvIndex().getBufferCache()).close();
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

        mutableComponent.getInvIndex().destroy();
        mutableComponent.getDeletedKeysBTree().destroy();
        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            LSMInvertedIndexImmutableComponent component = (LSMInvertedIndexImmutableComponent) c;
            component.getInvIndex().destroy();
            component.getDeletedKeysBTree().destroy();
            component.getBloomFilter().destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) {
        List<ILSMComponent> immutableComponents = componentsRef.get();
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case FLUSH:
            case DELETE:
            case INSERT:
                operationalComponents.add(mutableComponent);
                break;
            case SEARCH:
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
        ctx.modificationCallback.before(tuple);
        ctx.modificationCallback.found(null, tuple);
        switch (ctx.getOperation()) {
            case INSERT: {
                // Insert into the in-memory inverted index.                
                ctx.memInvIndexAccessor.insert(tuple);
                break;
            }
            case DELETE: {
                // First remove all entries in the in-memory inverted index (if any).
                ctx.memInvIndexAccessor.delete(tuple);
                // Insert key into the deleted-keys BTree.
                ctx.keysOnlyTuple.reset(tuple);
                try {
                    ctx.deletedKeysBTreeAccessor.insert(ctx.keysOnlyTuple);
                } catch (BTreeDuplicateKeyException e) {
                    // Key has already been deleted.
                }
                break;
            }
            default: {
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
            }
        }
        mutableComponent.setIsModified();
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();
        int numComponents = operationalComponents.size();
        assert numComponents > 0;
        boolean includeMutableComponent = operationalComponents.get(0) == mutableComponent;
        ArrayList<IIndexAccessor> indexAccessors = new ArrayList<IIndexAccessor>(numComponents);
        ArrayList<IIndexAccessor> deletedKeysBTreeAccessors = new ArrayList<IIndexAccessor>(numComponents);
        if (includeMutableComponent) {
            IIndexAccessor invIndexAccessor = mutableComponent.getInvIndex().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            indexAccessors.add(invIndexAccessor);
            IIndexAccessor deletedKeysAccessor = mutableComponent.getDeletedKeysBTree().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            deletedKeysBTreeAccessors.add(deletedKeysAccessor);
        }

        for (int i = includeMutableComponent ? 1 : 0; i < operationalComponents.size(); i++) {
            LSMInvertedIndexImmutableComponent component = (LSMInvertedIndexImmutableComponent) operationalComponents
                    .get(i);
            IIndexAccessor invIndexAccessor = component.getInvIndex().createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            indexAccessors.add(invIndexAccessor);
            IIndexAccessor deletedKeysAccessor = component.getDeletedKeysBTree().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            deletedKeysBTreeAccessors.add(deletedKeysAccessor);
        }

        ICursorInitialState initState = createCursorInitialState(pred, ictx, includeMutableComponent, indexAccessors,
                deletedKeysBTreeAccessors);
        cursor.open(initState, pred);
    }

    private ICursorInitialState createCursorInitialState(ISearchPredicate pred, IIndexOperationContext ictx,
            boolean includeMutableComponent, ArrayList<IIndexAccessor> indexAccessors,
            ArrayList<IIndexAccessor> deletedKeysBTreeAccessors) {
        List<ILSMComponent> immutableComponents = componentsRef.get();
        ICursorInitialState initState = null;
        PermutingTupleReference keysOnlyTuple = createKeysOnlyTupleReference();
        MultiComparator keyCmp = MultiComparator.createIgnoreFieldLength(invListCmpFactories);
        List<ILSMComponent> operationalComponents = new ArrayList<ILSMComponent>();
        if (includeMutableComponent) {
            operationalComponents.add(mutableComponent);
        }
        operationalComponents.addAll(immutableComponents);

        // TODO: This check is not pretty, but it does the job. Come up with something more OO in the future.
        // Distinguish between regular searches and range searches (mostly used in merges).
        if (pred instanceof InvertedIndexSearchPredicate) {
            initState = new LSMInvertedIndexSearchCursorInitialState(keyCmp, keysOnlyTuple, indexAccessors,
                    deletedKeysBTreeAccessors, mutableComponent.getDeletedKeysBTree().getLeafFrameFactory(), ictx,
                    includeMutableComponent, lsmHarness, operationalComponents);
        } else {
            InMemoryInvertedIndex memInvIndex = (InMemoryInvertedIndex) mutableComponent.getInvIndex();
            MultiComparator tokensAndKeysCmp = MultiComparator.create(memInvIndex.getBTree().getComparatorFactories());
            initState = new LSMInvertedIndexRangeSearchCursorInitialState(tokensAndKeysCmp, keyCmp, keysOnlyTuple,
                    mutableComponent.getDeletedKeysBTree().getLeafFrameFactory(), includeMutableComponent, lsmHarness,
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
    public boolean scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        if (!mutableComponent.isModified()) {
            return false;
        }
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        LSMInvertedIndexOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        ILSMComponent flushingComponent = ctx.getComponentHolder().get(0);
        opCtx.setOperation(IndexOperation.FLUSH);
        opCtx.getComponentHolder().add(flushingComponent);
        ioScheduler.scheduleOperation(new LSMInvertedIndexFlushOperation(new LSMInvertedIndexAccessor(this, lsmHarness,
                fileManager, opCtx), mutableComponent, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(),
                callback));
        return true;
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMInvertedIndexFlushOperation flushOp = (LSMInvertedIndexFlushOperation) operation;

        // Create an inverted index instance to be bulk loaded.
        LSMInvertedIndexImmutableComponent component = createDiskInvIndexComponent(componentFactory,
                flushOp.getDictBTreeFlushTarget(), flushOp.getDeletedKeysBTreeFlushTarget(),
                flushOp.getBloomFilterFlushTarget(), true);
        IInvertedIndex diskInvertedIndex = component.getInvIndex();

        // Create a scan cursor on the BTree underlying the in-memory inverted index.
        LSMInvertedIndexMutableComponent flushingComponent = flushOp.getFlushingComponent();
        InMemoryInvertedIndexAccessor memInvIndexAccessor = (InMemoryInvertedIndexAccessor) flushingComponent
                .getInvIndex().createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        BTreeAccessor memBTreeAccessor = memInvIndexAccessor.getBTreeAccessor();
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        IIndexCursor scanCursor = memBTreeAccessor.createSearchCursor();
        memBTreeAccessor.search(scanCursor, nullPred);

        // Bulk load the disk inverted index from the in-memory inverted index.
        IIndexBulkLoader invIndexBulkLoader = diskInvertedIndex.createBulkLoader(1.0f, false, 0L);
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
                numBTreeTuples = IntegerSerializerDeserializer.getInt(countTuple.getFieldData(0),
                        countTuple.getFieldStart(0));
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
            IIndexCursor deletedKeysScanCursor = deletedKeysBTreeAccessor.createSearchCursor();
            deletedKeysBTreeAccessor.search(deletedKeysScanCursor, nullPred);

            // Bulk load the deleted-keys BTree.
            IIndexBulkLoader deletedKeysBTreeBulkLoader = diskDeletedKeysBTree.createBulkLoader(1.0f, false, 0L);
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

        return component;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        LSMInvertedIndexOpContext ictx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        ictx.getComponentHolder().addAll(mergingComponents);
        IIndexCursor cursor = new LSMInvertedIndexRangeSearchCursor(ictx);
        RangePredicate mergePred = new RangePredicate(null, null, true, true, null, null);

        // Scan diskInvertedIndexes ignoring the memoryInvertedIndex.
        search(ictx, cursor, mergePred);

        ictx.setOperation(IndexOperation.MERGE);
        LSMInvertedIndexImmutableComponent firstComponent = (LSMInvertedIndexImmutableComponent) mergingComponents
                .get(0);
        OnDiskInvertedIndex firstInvIndex = (OnDiskInvertedIndex) firstComponent.getInvIndex();
        String firstFileName = firstInvIndex.getBTree().getFileReference().getFile().getName();

        LSMInvertedIndexImmutableComponent lastComponent = (LSMInvertedIndexImmutableComponent) mergingComponents
                .get(mergingComponents.size() - 1);
        OnDiskInvertedIndex lastInvIndex = (OnDiskInvertedIndex) lastComponent.getInvIndex();
        String lastFileName = lastInvIndex.getBTree().getFileReference().getFile().getName();

        LSMComponentFileReferences relMergeFileRefs = fileManager.getRelMergeFileReference(firstFileName, lastFileName);
        ILSMIndexAccessorInternal accessor = new LSMInvertedIndexAccessor(this, lsmHarness, fileManager, ictx);
        ioScheduler.scheduleOperation(new LSMInvertedIndexMergeOperation(accessor, mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getDeleteIndexFileReference(),
                relMergeFileRefs.getBloomFilterFileReference(), callback));
    }

    @Override
    public ILSMComponent merge(List<ILSMComponent> mergedComponents, ILSMIOOperation operation)
            throws HyracksDataException, IndexException {
        LSMInvertedIndexMergeOperation mergeOp = (LSMInvertedIndexMergeOperation) operation;

        // Create an inverted index instance.
        LSMInvertedIndexImmutableComponent component = createDiskInvIndexComponent(componentFactory,
                mergeOp.getDictBTreeMergeTarget(), mergeOp.getDeletedKeysBTreeMergeTarget(),
                mergeOp.getBloomFilterMergeTarget(), true);

        IInvertedIndex mergedDiskInvertedIndex = component.getInvIndex();
        IIndexCursor cursor = mergeOp.getCursor();
        IIndexBulkLoader invIndexBulkLoader = mergedDiskInvertedIndex.createBulkLoader(1.0f, true, 0L);
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

        // Add the merged components for cleanup.
        mergedComponents.addAll(mergeOp.getMergingComponents());

        return component;
    }

    private ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskInvIndexComponent(componentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), true);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint)
            throws IndexException {
        try {
            return new LSMInvertedIndexBulkLoader(fillFactor, verifyInput, numElementsHint);
        } catch (HyracksDataException e) {
            throw new IndexException(e);
        }
    }

    public boolean isEmptyIndex() throws HyracksDataException {
        return componentsRef.get().isEmpty() && !mutableComponent.isModified();
    }

    public class LSMInvertedIndexBulkLoader implements IIndexBulkLoader {
        private final ILSMComponent component;
        private final IIndexBulkLoader invIndexBulkLoader;
        private boolean exceptionCaught = false;

        public LSMInvertedIndexBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint)
                throws IndexException, HyracksDataException {
            if (!isEmptyIndex()) {
                throw new IndexException("Cannot load an index that is not empty");
            }
            // Note that by using a flush target file name, we state that the
            // new bulk loaded tree is "newer" than any other merged tree.
            try {
                component = createBulkLoadTarget();
            } catch (HyracksDataException e) {
                throw new IndexException(e);
            } catch (IndexException e) {
                throw new IndexException(e);
            }
            invIndexBulkLoader = ((LSMInvertedIndexImmutableComponent) component).getInvIndex().createBulkLoader(
                    fillFactor, verifyInput, numElementsHint);
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                invIndexBulkLoader.add(tuple);
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

        protected void handleException() throws HyracksDataException {
            exceptionCaught = true;
            ((LSMInvertedIndexImmutableComponent) component).getInvIndex().deactivate();
            ((LSMInvertedIndexImmutableComponent) component).getInvIndex().destroy();
            ((LSMInvertedIndexImmutableComponent) component).getDeletedKeysBTree().deactivate();
            ((LSMInvertedIndexImmutableComponent) component).getDeletedKeysBTree().destroy();
            ((LSMInvertedIndexImmutableComponent) component).getBloomFilter().deactivate();
            ((LSMInvertedIndexImmutableComponent) component).getBloomFilter().destroy();
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            if (!exceptionCaught) {
                invIndexBulkLoader.end();
            }
            lsmHarness.addBulkLoadedComponent(component);
        }
    }

    protected InMemoryInvertedIndex createInMemoryInvertedIndex(IVirtualBufferCache virtualBufferCache)
            throws IndexException {
        return InvertedIndexUtils.createInMemoryBTreeInvertedindex(virtualBufferCache, virtualFreePageManager,
                invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                new FileReference(new File(fileManager.getBaseDir() + "_virtual_vocab")));
    }

    protected LSMInvertedIndexImmutableComponent createDiskInvIndexComponent(ILSMComponentFactory factory,
            FileReference dictBTreeFileRef, FileReference btreeFileRef, FileReference bloomFilterFileRef, boolean create)
            throws HyracksDataException, IndexException {
        LSMInvertedIndexImmutableComponent component = (LSMInvertedIndexImmutableComponent) factory
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
        return component;
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMInvertedIndexAccessor(this, lsmHarness, fileManager, createOpContext(modificationCallback,
                searchCallback));
    }

    private LSMInvertedIndexOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMInvertedIndexOpContext(mutableComponent.getInvIndex(), mutableComponent.getDeletedKeysBTree(),
                modificationCallback, searchCallback);
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
        IBufferCache virtualBufferCache = mutableComponent.getInvIndex().getBufferCache();
        return virtualBufferCache.getNumPages() * virtualBufferCache.getPageSize();
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
        LSMInvertedIndexImmutableComponent invIndexComponent = (LSMInvertedIndexImmutableComponent) lsmComponent;
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
        mutableComponent.getInvIndex().validate();
        mutableComponent.getDeletedKeysBTree().validate();
        List<ILSMComponent> immutableComponents = componentsRef.get();
        for (ILSMComponent c : immutableComponents) {
            LSMInvertedIndexImmutableComponent component = (LSMInvertedIndexImmutableComponent) c;
            component.getInvIndex().validate();
            component.getDeletedKeysBTree().validate();
        }
    }

    @Override
    public String toString() {
        return "LSMInvertedIndex [" + fileManager.getBaseDir() + "]";
    }
}
