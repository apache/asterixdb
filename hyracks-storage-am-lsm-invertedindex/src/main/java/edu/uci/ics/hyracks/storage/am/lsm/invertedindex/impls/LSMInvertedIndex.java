/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IInMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingTupleReference;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IInMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexInternal;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls.LSMInvertedIndexFileManager.LSMInvertedIndexFileNameComponent;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndexFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMInvertedIndex implements ILSMIndexInternal, IInvertedIndex {
    public class LSMInvertedIndexComponent {
        private final IInvertedIndex invIndex;
        private final BTree deletedKeysBTree;

        LSMInvertedIndexComponent(IInvertedIndex invIndex, BTree deletedKeysBTree) {
            this.invIndex = invIndex;
            this.deletedKeysBTree = deletedKeysBTree;
        }

        public IInvertedIndex getInvIndex() {
            return invIndex;
        }

        public BTree getDeletedKeysBTree() {
            return deletedKeysBTree;
        }
    }

    private final ILSMHarness lsmHarness;

    // In-memory components.
    private final LSMInvertedIndexComponent memComponent;
    private final IInMemoryFreePageManager memFreePageManager;
    private final IBinaryTokenizerFactory tokenizerFactory;

    // On-disk components.
    private final ILSMIndexFileManager fileManager;
    // For creating inverted indexes in flush and merge.
    private final OnDiskInvertedIndexFactory diskInvIndexFactory;
    // For creating deleted-keys BTrees in flush and merge.
    private final BTreeFactory deletedKeysBTreeFactory;
    private final IBufferCache diskBufferCache;
    // List of LSMInvertedIndexComponent instances. Using Object for better sharing via
    // ILSMIndex + LSMHarness.
    private final LinkedList<Object> diskComponents;
    // Helps to guarantees physical consistency of LSM components.
    private final ILSMComponentFinalizer componentFinalizer;

    // Type traits and comparators for tokens and inverted-list elements.
    private final ITypeTraits[] invListTypeTraits;
    private final IBinaryComparatorFactory[] invListCmpFactories;
    private final ITypeTraits[] tokenTypeTraits;
    private final IBinaryComparatorFactory[] tokenCmpFactories;

    private boolean isActivated;

    public LSMInvertedIndex(IInMemoryBufferCache memBufferCache, IInMemoryFreePageManager memFreePageManager,
            OnDiskInvertedIndexFactory diskInvIndexFactory, BTreeFactory deletedKeysBTreeFactory,
            ILSMIndexFileManager fileManager, IFileMapProvider diskFileMapProvider, ITypeTraits[] invListTypeTraits,
            IBinaryComparatorFactory[] invListCmpFactories, ITypeTraits[] tokenTypeTraits,
            IBinaryComparatorFactory[] tokenCmpFactories, IBinaryTokenizerFactory tokenizerFactory,
            ILSMFlushController flushController, ILSMMergePolicy mergePolicy, ILSMOperationTrackerFactory opTrackerFactory,
            ILSMIOOperationScheduler ioScheduler) throws IndexException {
        InMemoryInvertedIndex memInvIndex = InvertedIndexUtils.createInMemoryBTreeInvertedindex(memBufferCache,
                memFreePageManager, invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories,
                tokenizerFactory);
        BTree deleteKeysBTree = BTreeUtils.createBTree(memBufferCache, memFreePageManager,
                ((InMemoryBufferCache) memBufferCache).getFileMapProvider(), invListTypeTraits, invListCmpFactories,
                BTreeLeafFrameType.REGULAR_NSM, new FileReference(new File("membtree")));
        memComponent = new LSMInvertedIndexComponent(memInvIndex, deleteKeysBTree);
        this.memFreePageManager = memFreePageManager;
        this.tokenizerFactory = tokenizerFactory;
        this.fileManager = fileManager;
        this.diskInvIndexFactory = diskInvIndexFactory;
        this.deletedKeysBTreeFactory = deletedKeysBTreeFactory;
        this.diskBufferCache = diskInvIndexFactory.getBufferCache();
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        ILSMOperationTracker opTracker = opTrackerFactory.createOperationTracker(this);
        this.lsmHarness = new LSMHarness(this, flushController, mergePolicy, opTracker, ioScheduler);
        this.componentFinalizer = new LSMInvertedIndexComponentFinalizer(diskFileMapProvider);
        diskComponents = new LinkedList<Object>();
        isActivated = false;
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
            return;
        }
        try {
            ((InMemoryBufferCache) memComponent.getInvIndex().getBufferCache()).open();
            memComponent.getInvIndex().create();
            memComponent.getInvIndex().activate();
            memComponent.getDeletedKeysBTree().create();
            memComponent.getDeletedKeysBTree().activate();
            List<Object> validFileNames = fileManager.cleanupAndGetValidFiles(componentFinalizer);
            for (Object o : validFileNames) {
                LSMInvertedIndexFileNameComponent component = (LSMInvertedIndexFileNameComponent) o;
                FileReference dictBTreeFile = new FileReference(new File(component.getDictBTreeFileName()));
                FileReference deletedKeysBTreeFile = new FileReference(
                        new File(component.getDeletedKeysBTreeFileName()));
                IInvertedIndex invIndex = createDiskInvIndex(diskInvIndexFactory, dictBTreeFile, false);
                BTree deletedKeysBTree = (BTree) createDiskTree(deletedKeysBTreeFactory, deletedKeysBTreeFile, false);
                LSMInvertedIndexComponent diskComponent = new LSMInvertedIndexComponent(invIndex, deletedKeysBTree);
                diskComponents.add(diskComponent);
            }
            isActivated = true;
            // TODO: Maybe we can make activate throw an index exception?
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }
    }

    protected IInvertedIndex createDiskInvIndex(OnDiskInvertedIndexFactory invIndexFactory,
            FileReference dictBTreeFileRef, boolean create) throws HyracksDataException, IndexException {
        IInvertedIndex invIndex = invIndexFactory.createIndexInstance(dictBTreeFileRef);
        if (create) {
            invIndex.create();
        }
        // Will be closed during cleanup of merge().
        invIndex.activate();
        return invIndex;
    }

    protected ITreeIndex createDiskTree(BTreeFactory btreeFactory, FileReference btreeFileRef, boolean create)
            throws HyracksDataException, IndexException {
        ITreeIndex btree = btreeFactory.createIndexInstance(btreeFileRef);
        if (create) {
            btree.create();
        }
        // Will be closed during cleanup of merge().
        btree.activate();
        return btree;
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        resetInMemoryComponent();
        for (Object o : diskComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            component.getInvIndex().deactivate();
            component.getDeletedKeysBTree().deactivate();
            component.getInvIndex().destroy();
            component.getDeletedKeysBTree().destroy();
        }
        diskComponents.clear();
    }

    @Override
    public synchronized void deactivate() throws HyracksDataException {
        if (!isActivated) {
            return;
        }

        isActivated = false;

        BlockingIOOperationCallback blockingCallBack = new BlockingIOOperationCallback();
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        lsmHarness.getIOScheduler().scheduleOperation(accessor.createFlushOperation(blockingCallBack));
        try {
            blockingCallBack.waitForIO();
        } catch (InterruptedException e) {
            throw new HyracksDataException(e);
        }

        for (Object o : diskComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            component.getInvIndex().deactivate();
            component.getDeletedKeysBTree().deactivate();
        }
        memComponent.getInvIndex().deactivate();
        memComponent.getDeletedKeysBTree().deactivate();
        memComponent.getInvIndex().destroy();
        memComponent.getDeletedKeysBTree().destroy();
        ((InMemoryBufferCache) memComponent.getInvIndex().getBufferCache()).close();
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }

        memComponent.getInvIndex().destroy();
        memComponent.getDeletedKeysBTree().destroy();
        for (Object o : diskComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            component.getInvIndex().destroy();
            component.getDeletedKeysBTree().destroy();
        }
        fileManager.deleteDirs();
    }

    @Override
    public void validate() throws HyracksDataException {
        memComponent.getInvIndex().validate();
        memComponent.getDeletedKeysBTree().validate();
        for (Object o : diskComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            component.getInvIndex().validate();
            component.getDeletedKeysBTree().validate();
        }
    }

    @Override
    public IIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMInvertedIndexAccessor(this, lsmHarness, fileManager, createOpContext(modificationCallback,
                searchCallback));
    }

    private LSMInvertedIndexOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMInvertedIndexOpContext(memComponent.getInvIndex(), memComponent.getDeletedKeysBTree(),
                modificationCallback, searchCallback);
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput) throws IndexException {
        return new LSMInvertedIndexBulkLoader(fillFactor, verifyInput);
    }

    /**
     * The keys in the in-memory deleted-keys BTree only refer to on-disk components.
     * We delete documents from the in-memory inverted index by deleting its entries directly,
     * and do NOT add the deleted key to the deleted-keys BTree.
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
    public void insertUpdateOrDelete(ITupleReference tuple, IIndexOperationContext ictx) throws HyracksDataException,
            IndexException {
        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) ictx;
        ctx.modificationCallback.before(tuple);
        switch (ctx.getOperation()) {
            case INSERT: {
                // Insert into the in-memory inverted index.                
                ctx.memInvIndexAccessor.insert(tuple);
                break;
            }
            case DELETE: {
                ctx.modificationCallback.before(tuple);
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
    }

    @Override
    public void search(IIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred,
            IIndexOperationContext ictx, boolean includeMemComponent, AtomicInteger searcherRefCount)
            throws HyracksDataException, IndexException {
        int numComponents = (includeMemComponent) ? diskComponents.size() : diskComponents.size() + 1;
        ArrayList<IIndexAccessor> indexAccessors = new ArrayList<IIndexAccessor>(numComponents);
        ArrayList<IIndexAccessor> deletedKeysBTreeAccessors = new ArrayList<IIndexAccessor>(numComponents);
        if (includeMemComponent) {
            IIndexAccessor invIndexAccessor = memComponent.getInvIndex().createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            indexAccessors.add(invIndexAccessor);
            IIndexAccessor deletedKeysAccessor = memComponent.getDeletedKeysBTree().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            deletedKeysBTreeAccessors.add(deletedKeysAccessor);
        }
        for (int i = 0; i < diskComponents.size(); i++) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) diskComponents.get(i);
            IIndexAccessor invIndexAccessor = component.getInvIndex().createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            indexAccessors.add(invIndexAccessor);
            IIndexAccessor deletedKeysAccessor = component.getDeletedKeysBTree().createAccessor(
                    NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            deletedKeysBTreeAccessors.add(deletedKeysAccessor);
        }
        ICursorInitialState initState = createCursorInitialState(pred, ictx, includeMemComponent, searcherRefCount,
                indexAccessors, deletedKeysBTreeAccessors);
        cursor.open(initState, pred);
    }

    private ICursorInitialState createCursorInitialState(ISearchPredicate pred, IIndexOperationContext ictx,
            boolean includeMemComponent, AtomicInteger searcherRefCount, ArrayList<IIndexAccessor> indexAccessors,
            ArrayList<IIndexAccessor> deletedKeysBTreeAccessors) {
        ICursorInitialState initState = null;
        PermutingTupleReference keysOnlyTuple = createKeysOnlyTupleReference();
        MultiComparator keyCmp = MultiComparator.create(invListCmpFactories);
        // TODO: This check is not pretty, but it does the job. Come up with something more OO in the future.
        // Distinguish between regular searches and range searches (mostly used in merges).
        if (pred instanceof InvertedIndexSearchPredicate) {
            initState = new LSMInvertedIndexSearchCursorInitialState(keyCmp, keysOnlyTuple, indexAccessors,
                    deletedKeysBTreeAccessors, ictx, includeMemComponent, searcherRefCount, lsmHarness);
        } else {
            InMemoryInvertedIndex memInvIndex = (InMemoryInvertedIndex) memComponent.getInvIndex();
            MultiComparator tokensAndKeysCmp = MultiComparator.create(memInvIndex.getBTree().getComparatorFactories());
            initState = new LSMInvertedIndexRangeSearchCursorInitialState(tokensAndKeysCmp, keyCmp, keysOnlyTuple,
                    includeMemComponent, searcherRefCount, lsmHarness, indexAccessors, deletedKeysBTreeAccessors, pred);
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
    public Object merge(List<Object> mergedComponents, ILSMIOOperation operation) throws HyracksDataException,
            IndexException {
        LSMInvertedIndexMergeOperation mergeOp = (LSMInvertedIndexMergeOperation) operation;

        // Create an inverted index instance.
        IInvertedIndex mergedDiskInvertedIndex = createDiskInvIndex(diskInvIndexFactory,
                mergeOp.getDictBTreeMergeTarget(), true);
        IIndexCursor cursor = mergeOp.getCursor();
        IIndexBulkLoader invIndexBulkLoader = mergedDiskInvertedIndex.createBulkLoader(1.0f, true);
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

        // Create an empty deleted keys BTree (do nothing with the returned index).
        BTree deletedKeysBTree = (BTree) createDiskTree(deletedKeysBTreeFactory,
                mergeOp.getDeletedKeysBTreeMergeTarget(), true);

        // Add the merged components for cleanup.
        mergedComponents.addAll(mergeOp.getMergingComponents());

        return new LSMInvertedIndexComponent(mergedDiskInvertedIndex, deletedKeysBTree);
    }

    @Override
    public ILSMIOOperation createMergeOperation(ILSMIOOperationCallback callback) throws HyracksDataException,
            IndexException {
        LSMInvertedIndexOpContext ctx = createOpContext(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        ctx.setOperation(IndexOperation.SEARCH);
        IIndexCursor cursor = new LSMInvertedIndexRangeSearchCursor(ctx);
        RangePredicate mergePred = new RangePredicate(null, null, true, true, null, null);

        // Scan diskInvertedIndexes ignoring the memoryInvertedIndex.
        List<Object> mergingComponents = lsmHarness.search(cursor, mergePred, ctx, false);
        if (mergingComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        LSMInvertedIndexComponent firstComponent = (LSMInvertedIndexComponent) mergingComponents.get(0);
        OnDiskInvertedIndex firstInvIndex = (OnDiskInvertedIndex) firstComponent.getInvIndex();
        String firstFileName = firstInvIndex.getBTree().getFileReference().getFile().getName();

        LSMInvertedIndexComponent lastComponent = (LSMInvertedIndexComponent) mergingComponents.get(mergingComponents
                .size() - 1);
        OnDiskInvertedIndex lastInvIndex = (OnDiskInvertedIndex) lastComponent.getInvIndex();
        String lastFileName = lastInvIndex.getBTree().getFileReference().getFile().getName();

        LSMInvertedIndexFileNameComponent fileNameComponent = (LSMInvertedIndexFileNameComponent) fileManager
                .getRelMergeFileName(firstFileName, lastFileName);
        FileReference dictBTreeFileRef = fileManager.createMergeFile(fileNameComponent.getDictBTreeFileName());
        FileReference deletedKeysBTreeFileRef = fileManager.createMergeFile(fileNameComponent
                .getDeletedKeysBTreeFileName());

        LSMInvertedIndexMergeOperation mergeOp = new LSMInvertedIndexMergeOperation(this, mergingComponents, cursor,
                dictBTreeFileRef, deletedKeysBTreeFileRef, callback);

        return mergeOp;
    }

    @Override
    public void addMergedComponent(Object newComponent, List<Object> mergedComponents) {
        diskComponents.removeAll(mergedComponents);
        diskComponents.addLast(newComponent);
    }

    @Override
    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException {
        for (Object o : mergedComponents) {
            LSMInvertedIndexComponent component = (LSMInvertedIndexComponent) o;
            BTree oldDeletedKeysBTree = component.getDeletedKeysBTree();
            oldDeletedKeysBTree.deactivate();
            oldDeletedKeysBTree.destroy();
            IInvertedIndex oldInvIndex = component.getInvIndex();
            oldInvIndex.deactivate();
            oldInvIndex.destroy();
        }
    }

    @Override
    public Object flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMInvertedIndexFlushOperation flushOp = (LSMInvertedIndexFlushOperation) operation;

        // Create an inverted index instance to be bulk loaded.
        IInvertedIndex diskInvertedIndex = createDiskInvIndex(diskInvIndexFactory, flushOp.getDictBTreeFlushTarget(),
                true);

        // Create a scan cursor on the BTree underlying the in-memory inverted index.
        InMemoryInvertedIndexAccessor memInvIndexAccessor = (InMemoryInvertedIndexAccessor) memComponent.getInvIndex()
                .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        BTreeAccessor memBTreeAccessor = memInvIndexAccessor.getBTreeAccessor();
        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
        IIndexCursor scanCursor = memBTreeAccessor.createSearchCursor();
        memBTreeAccessor.search(scanCursor, nullPred);

        // Bulk load the disk inverted index from the in-memory inverted index.
        IIndexBulkLoader invIndexBulkLoader = diskInvertedIndex.createBulkLoader(1.0f, false);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                invIndexBulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
        }
        invIndexBulkLoader.end();

        // Create an BTree instance for the deleted keys.
        BTree diskDeletedKeysBTree = (BTree) createDiskTree(deletedKeysBTreeFactory,
                flushOp.getDeletedKeysBTreeFlushTarget(), true);

        // Create a scan cursor on the deleted keys BTree underlying the in-memory inverted index.
        IIndexAccessor deletedKeysBTreeAccessor = memComponent.getDeletedKeysBTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        IIndexCursor deletedKeysScanCursor = deletedKeysBTreeAccessor.createSearchCursor();
        deletedKeysBTreeAccessor.search(deletedKeysScanCursor, nullPred);

        // Bulk load the deleted-keys BTree.
        IIndexBulkLoader deletedKeysBTreeBulkLoader = diskDeletedKeysBTree.createBulkLoader(1.0f, false);
        try {
            while (deletedKeysScanCursor.hasNext()) {
                deletedKeysScanCursor.next();
                deletedKeysBTreeBulkLoader.add(deletedKeysScanCursor.getTuple());
            }
        } finally {
            deletedKeysScanCursor.close();
        }
        deletedKeysBTreeBulkLoader.end();

        return new LSMInvertedIndexComponent(diskInvertedIndex, diskDeletedKeysBTree);
    }

    @Override
    public void addFlushedComponent(Object index) {
        diskComponents.addFirst(index);
    }

    public class LSMInvertedIndexBulkLoader implements IIndexBulkLoader {
        private final IInvertedIndex invIndex;
        private final BTree deletedKeysBTree;
        private final IIndexBulkLoader invIndexBulkLoader;

        public LSMInvertedIndexBulkLoader(float fillFactor, boolean verifyInput) throws IndexException {
            // Note that by using a flush target file name, we state that the
            // new bulk loaded tree is "newer" than any other merged tree.
            try {
                LSMInvertedIndexFileNameComponent fileNameComponent = (LSMInvertedIndexFileNameComponent) fileManager
                        .getRelFlushFileName();
                FileReference dictBTreeFileRef = fileManager.createFlushFile(fileNameComponent.getDictBTreeFileName());
                invIndex = createDiskInvIndex(diskInvIndexFactory, dictBTreeFileRef, true);
                // Create an empty deleted-keys BTree.
                FileReference deletedKeysBTreeFile = fileManager.createFlushFile(fileNameComponent
                        .getDeletedKeysBTreeFileName());
                deletedKeysBTree = (BTree) createDiskTree(deletedKeysBTreeFactory, deletedKeysBTreeFile, true);
            } catch (HyracksDataException e) {
                throw new TreeIndexException(e);
            }
            invIndexBulkLoader = invIndex.createBulkLoader(fillFactor, verifyInput);
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
            invIndex.deactivate();
            invIndex.destroy();
            deletedKeysBTree.deactivate();
            deletedKeysBTree.destroy();
        }

        @Override
        public void end() throws IndexException, HyracksDataException {
            invIndexBulkLoader.end();
            LSMInvertedIndexComponent diskComponent = new LSMInvertedIndexComponent(invIndex, deletedKeysBTree);
            lsmHarness.addBulkLoadedComponent(diskComponent);
        }
    }

    @Override
    public void resetInMemoryComponent() throws HyracksDataException {
        memFreePageManager.reset();
        memComponent.getInvIndex().clear();
        memComponent.getDeletedKeysBTree().clear();
    }

    @Override
    public long getMemoryAllocationSize() {
        InMemoryBufferCache memBufferCache = (InMemoryBufferCache) memComponent.getInvIndex().getBufferCache();
        return memBufferCache.getNumPages() * memBufferCache.getPageSize();
    }

    @Override
    public IInMemoryFreePageManager getInMemoryFreePageManager() {
        return memFreePageManager;
    }

    @Override
    public List<Object> getDiskComponents() {
        return diskComponents;
    }

    @Override
    public ILSMComponentFinalizer getComponentFinalizer() {
        return componentFinalizer;
    }

    @Override
    public ILSMFlushController getFlushController() {
        return lsmHarness.getFlushController();
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return lsmHarness.getOperationTracker();
    }

    @Override
    public ILSMIOOperationScheduler getIOScheduler() {
        return lsmHarness.getIOScheduler();
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
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
}
