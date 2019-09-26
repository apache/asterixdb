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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.AbstractLSMWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
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
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.ExternalIndexHarness;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LoadOperation;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.IndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.util.trace.ITracer;

public class ExternalBTreeWithBuddy extends AbstractLSMIndex implements ITreeIndex, ITwoPCIndex {

    private final IBinaryComparatorFactory[] btreeCmpFactories;
    private final IBinaryComparatorFactory[] buddyBtreeCmpFactories;
    private final int[] buddyBTreeFields;

    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory btreeLeafFrameFactory;
    private final ITreeIndexFrameFactory buddyBtreeLeafFrameFactory;

    // A second disk component list that will be used when a transaction is
    // committed and will be seen by subsequent accessors
    private final List<ILSMDiskComponent> secondDiskComponents;
    private int version = 0;

    public ExternalBTreeWithBuddy(IIOManager ioManager, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ITreeIndexFrameFactory buddyBtreeLeafFrameFactory,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            IBinaryComparatorFactory[] btreeCmpFactories, IBinaryComparatorFactory[] buddyBtreeCmpFactories,
            int[] buddyBTreeFields, boolean durable, ITracer tracer) throws HyracksDataException {
        super(ioManager, diskBufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy, opTracker,
                ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, componentFactory, bulkLoadComponentFactory,
                durable, tracer);
        this.btreeCmpFactories = btreeCmpFactories;
        this.buddyBtreeCmpFactories = buddyBtreeCmpFactories;
        this.buddyBTreeFields = buddyBTreeFields;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.buddyBtreeLeafFrameFactory = buddyBtreeLeafFrameFactory;
        this.secondDiskComponents = new LinkedList<>();
    }

    @Override
    public void create() throws HyracksDataException {
        super.create();
        secondDiskComponents.clear();
    }

    @Override
    public void activate() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }
        if (diskComponents.size() == 0 && secondDiskComponents.size() == 0) {
            //First time activation
            List<LSMComponentFileReferences> validFileReferences;
            validFileReferences = fileManager.cleanupAndGetValidFiles();
            for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
                ILSMDiskComponent component;
                component =
                        createDiskComponent(componentFactory, lsmComonentFileReference.getInsertIndexFileReference(),
                                lsmComonentFileReference.getDeleteIndexFileReference(),
                                lsmComonentFileReference.getBloomFilterFileReference(), false);
                diskComponents.add(component);
                secondDiskComponents.add(component);
            }
            ((ExternalIndexHarness) getHarness()).indexFirstTimeActivated();
        } else {
            // This index has been opened before or is brand new with no
            // components. It should also maintain the version pointer
            for (ILSMDiskComponent c : diskComponents) {
                c.activate(false);
            }
            for (ILSMDiskComponent c : secondDiskComponents) {
                // Only activate non shared components
                if (!diskComponents.contains(c)) {
                    c.activate(false);
                }
            }
        }
        isActive = true;
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        ((ExternalIndexHarness) getHarness()).indexClear();
        for (ILSMDiskComponent c : diskComponents) {
            c.deactivateAndDestroy();
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }
        for (ILSMDiskComponent c : secondDiskComponents) {
            c.deactivateAndDestroy();
        }
        diskComponents.clear();
        secondDiskComponents.clear();
        version = 0;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }
        for (ILSMDiskComponent c : diskComponents) {
            c.destroy();
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }
        for (ILSMDiskComponent c : secondDiskComponents) {
            c.destroy();
        }
        diskComponents.clear();
        secondDiskComponents.clear();
        fileManager.deleteDirs();
        version = 0;
    }

    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) throws HyracksDataException {
        return new LSMTreeIndexAccessor(getHarness(), createOpContext(iap.getSearchOperationCallback(), version),
                ctx -> new LSMBTreeWithBuddySearchCursor(ctx, buddyBTreeFields));
    }

    // The subsume merged components is overridden to account for:
    // Maintaining two versions of the index
    @Override
    public void subsumeMergedComponents(ILSMDiskComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        List<ILSMDiskComponent> newerList;
        List<ILSMDiskComponent> olderList;
        if (version == 0) {
            newerList = diskComponents;
            olderList = secondDiskComponents;
        } else {
            newerList = secondDiskComponents;
            olderList = diskComponents;
        }

        // Check if merge will affect the older list
        if (olderList.containsAll(mergedComponents)) {
            int swapIndex = olderList.indexOf(mergedComponents.get(0));
            olderList.removeAll(mergedComponents);
            olderList.add(swapIndex, newComponent);
        }

        // The new list will always have all the merged components
        int swapIndex = newerList.indexOf(mergedComponents.get(0));
        newerList.removeAll(mergedComponents);
        newerList.add(swapIndex, newComponent);
    }

    // For initial load
    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        return new LSMTwoPCBTreeWithBuddyBulkLoader(fillLevel, verifyInput, 0, false, parameters);
    }

    // For transaction bulk load <- could consolidate with the above method ->
    @Override
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        return new LSMTwoPCBTreeWithBuddyBulkLoader(fillLevel, verifyInput, numElementsHint, true, parameters);
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException("tuple modify not supported in LSM-Disk-Only-BTree");
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        ExternalBTreeWithBuddyOpContext ctx = (ExternalBTreeWithBuddyOpContext) ictx;
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();
        ctx.getSearchInitialState().setOperationalComponents(operationalComponents);
        cursor.open(ctx.getSearchInitialState(), pred);
    }

    @Override
    public ILSMIOOperation createFlushOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.FLUSH_NOT_SUPPORTED_IN_EXTERNAL_INDEX);
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.FLUSH_NOT_SUPPORTED_IN_EXTERNAL_INDEX);
    }

    protected LSMComponentFileReferences getMergeTargetFileName(List<ILSMComponent> mergingDiskComponents)
            throws HyracksDataException {
        BTree lastTree = ((LSMBTreeWithBuddyDiskComponent) mergingDiskComponents.get(0)).getIndex();
        BTree firstTree = ((LSMBTreeWithBuddyDiskComponent) mergingDiskComponents.get(mergingDiskComponents.size() - 1))
                .getIndex();
        FileReference firstFile = firstTree.getFileReference();
        FileReference lastFile = lastTree.getFileReference();
        LSMComponentFileReferences fileRefs =
                fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
        return fileRefs;
    }

    @Override
    public LSMBTreeWithBuddyMergeOperation createMergeOperation(ILSMIndexOperationContext ctx)
            throws HyracksDataException {
        ILSMIndexOperationContext bctx = createOpContext(NoOpOperationCallback.INSTANCE, 0);
        bctx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        IIndexCursorStats stats = new IndexCursorStats();
        LSMBTreeWithBuddySortedCursor cursor = new LSMBTreeWithBuddySortedCursor(bctx, buddyBTreeFields, stats);
        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        ILSMIndexAccessor accessor = new LSMTreeIndexAccessor(getHarness(), bctx,
                opCtx -> new LSMBTreeWithBuddySearchCursor(opCtx, buddyBTreeFields));

        // Since we have two lists of components, to tell whether we need to
        // keep deleted tuples, we need to know
        // which list to check against and we need to synchronize for this
        boolean keepDeleteTuples = false;
        if (version == 0) {
            keepDeleteTuples = mergingComponents.get(mergingComponents.size() - 1) != diskComponents
                    .get(diskComponents.size() - 1);
        } else {
            keepDeleteTuples = mergingComponents.get(mergingComponents.size() - 1) != secondDiskComponents
                    .get(secondDiskComponents.size() - 1);
        }

        LSMBTreeWithBuddyMergeOperation mergeOp = new LSMBTreeWithBuddyMergeOperation(accessor, cursor, stats,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getDeleteIndexFileReference(),
                relMergeFileRefs.getBloomFilterFileReference(), ioOpCallback,
                fileManager.getBaseDir().getAbsolutePath(), keepDeleteTuples);
        ioOpCallback.scheduled(mergeOp);
        return mergeOp;

    }

    // This method creates the appropriate opContext for the targeted version
    public ExternalBTreeWithBuddyOpContext createOpContext(ISearchOperationCallback searchCallback, int targetVersion) {
        return new ExternalBTreeWithBuddyOpContext(this, btreeCmpFactories, buddyBtreeCmpFactories, searchCallback,
                targetVersion, getHarness(), btreeInteriorFrameFactory, btreeLeafFrameFactory,
                buddyBtreeLeafFrameFactory, tracer);
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeWithBuddyMergeOperation mergeOp = (LSMBTreeWithBuddyMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate btreeSearchPred = new RangePredicate(null, null, true, true, null, null);
        ILSMIndexOperationContext opCtx = ((LSMBTreeWithBuddySortedCursor) cursor).getOpCtx();
        search(opCtx, cursor, btreeSearchPred);

        ILSMDiskComponent mergedComponent = createDiskComponent(componentFactory, mergeOp.getTarget(),
                mergeOp.getBuddyBTreeTarget(), mergeOp.getBloomFilterTarget(), true);

        ILSMDiskComponentBulkLoader componentBulkLoader;

        // In case we must keep the deleted-keys BuddyBTrees, then they must be
        // merged *before* merging the b-trees so that
        // lsmHarness.endSearch() is called once when the b-trees have been
        // merged.

        if (mergeOp.isKeepDeletedTuples()) {
            // Keep the deleted tuples since the oldest disk component is not
            // included in the merge operation
            LSMBuddyBTreeMergeCursor buddyBtreeCursor = new LSMBuddyBTreeMergeCursor(opCtx, mergeOp.getCursorStats());
            search(opCtx, buddyBtreeCursor, btreeSearchPred);

            long numElements = 0L;
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                numElements += ((AbstractLSMWithBloomFilterDiskComponent) mergeOp.getMergingComponents().get(i))
                        .getBloomFilter().getNumElements();
            }
            componentBulkLoader = mergedComponent.createBulkLoader(operation, 1.0f, false, numElements, false, false,
                    false, pageWriteCallbackFactory.createPageWriteCallback());
            try {
                while (buddyBtreeCursor.hasNext()) {
                    buddyBtreeCursor.next();
                    ITupleReference tuple = buddyBtreeCursor.getTuple();
                    componentBulkLoader.delete(tuple);
                }
            } finally {
                buddyBtreeCursor.close();
            }
        } else {
            componentBulkLoader = mergedComponent.createBulkLoader(operation, 1.0f, false, 0L, false, false, false,
                    pageWriteCallbackFactory.createPageWriteCallback());
        }

        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                componentBulkLoader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }
        componentBulkLoader.end();
        return mergedComponent;
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) {
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        List<ILSMDiskComponent> immutableComponents;
        // Identify current list in case of a merge
        if (version == 0) {
            immutableComponents = diskComponents;
        } else {
            immutableComponents = secondDiskComponents;
        }

        ExternalBTreeWithBuddyOpContext opCtx = (ExternalBTreeWithBuddyOpContext) ctx;
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case SEARCH:
                if (opCtx.getTargetIndexVersion() == 0) {
                    operationalComponents.addAll(diskComponents);
                } else {
                    operationalComponents.addAll(secondDiskComponents);
                }
                break;
            case MERGE:
                operationalComponents.addAll(ctx.getComponentsToBeMerged());
                break;
            case FULL_MERGE:
                operationalComponents.addAll(immutableComponents);
            case REPLICATE:
                operationalComponents.addAll(ctx.getComponentsToBeReplicated());
                break;
            case FLUSH:
                // Do nothing. this is left here even though the index never
                // performs flushes because a flush is triggered by
                // dataset lifecycle manager when closing an index. Having no
                // components is a no operation
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
    }

    // This function is used when a new component is to be committed -- is
    // called by the harness.
    @Override
    public void commitTransactionDiskComponent(ILSMDiskComponent newComponent) throws HyracksDataException {

        // determine which list is the new one and flip the pointer
        List<ILSMDiskComponent> newerList;
        List<ILSMDiskComponent> olderList;
        if (version == 0) {
            newerList = diskComponents;
            olderList = secondDiskComponents;
            version = 1;
        } else {
            newerList = secondDiskComponents;
            olderList = diskComponents;
            version = 0;
        }
        // Remove components from list
        olderList.clear();
        // Add components
        olderList.addAll(newerList);
        if (newComponent != null) {
            // Add it to the list
            olderList.add(0, newComponent);
        }
    }

    @Override
    public void deactivate(boolean flushOnExit) throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }
        // Even though, we deactivate the index, we don't exit components or
        // modify any of the lists to make sure they
        // are there if the index was opened again
        for (ILSMDiskComponent c : diskComponents) {
            c.deactivateAndPurge();
        }
        for (ILSMDiskComponent c : secondDiskComponents) {
            // Only deactivate non shared components
            if (!diskComponents.contains(c)) {
                c.deactivateAndPurge();
            }
        }
        isActive = false;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return btreeLeafFrameFactory;
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return btreeInteriorFrameFactory;
    }

    @Override
    public IMetadataPageManager getPageManager() {
        // This method should never be called for disk only indexes
        return null;
    }

    @Override
    public int getFieldCount() {
        return btreeCmpFactories.length;
    }

    @Override
    public int getRootPageId() {
        // This method should never be called for this index
        return 0;
    }

    @Override
    public int getFileId() {
        // This method should never be called for this index
        return 0;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return btreeCmpFactories;
    }

    // even though the index doesn't support record level modification, the
    // accessor will try to do it
    // we could throw the exception here but we don't. it will eventually be
    // thrown by the index itself

    // The bulk loader used for both initial loading and transaction
    // modifications
    public class LSMTwoPCBTreeWithBuddyBulkLoader implements IIndexBulkLoader, ITwoPCIndexBulkLoader {
        private final ILSMDiskComponent component;
        private final LoadOperation loadOp;
        private final ILSMDiskComponentBulkLoader componentBulkLoader;
        private final boolean isTransaction;

        public LSMTwoPCBTreeWithBuddyBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean isTransaction, Map<String, Object> parameters) throws HyracksDataException {
            this.isTransaction = isTransaction;
            // Create the appropriate target
            LSMComponentFileReferences componentFileRefs;
            if (isTransaction) {
                try {
                    componentFileRefs = fileManager.getNewTransactionFileReference();
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
                component =
                        createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                                componentFileRefs.getDeleteIndexFileReference(),
                                componentFileRefs.getBloomFilterFileReference(), true);
            } else {
                componentFileRefs = fileManager.getRelFlushFileReference();
                component =
                        createDiskComponent(bulkLoadComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                                componentFileRefs.getDeleteIndexFileReference(),
                                componentFileRefs.getBloomFilterFileReference(), true);
            }

            loadOp = new LoadOperation(componentFileRefs, ioOpCallback, getIndexIdentifier(), parameters);
            loadOp.setNewComponent(component);
            ioOpCallback.scheduled(loadOp);
            ioOpCallback.beforeOperation(loadOp);
            componentBulkLoader = component.createBulkLoader(loadOp, fillFactor, verifyInput, numElementsHint, false,
                    true, false, pageWriteCallbackFactory.createPageWriteCallback());
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            componentBulkLoader.add(tuple);
        }

        @Override
        public void end() throws HyracksDataException {
            try {
                ioOpCallback.afterOperation(loadOp);
                componentBulkLoader.end();
                if (component.getComponentSize() > 0) {
                    if (isTransaction) {
                        // Since this is a transaction component, validate and
                        // deactivate. it could later be added or deleted
                        try {
                            component.markAsValid(durable, loadOp);
                        } finally {
                            ioOpCallback.afterFinalize(loadOp);
                        }
                        component.deactivate();
                    } else {
                        ioOpCallback.afterFinalize(loadOp);
                        getHarness().addBulkLoadedComponent(loadOp);
                    }
                }
            } finally {
                ioOpCallback.completed(loadOp);
            }
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            componentBulkLoader.delete(tuple);
        }

        @Override
        public void abort() throws HyracksDataException {
            try {
                try {
                    componentBulkLoader.abort();
                } finally {
                    ioOpCallback.afterFinalize(loadOp);
                }
            } finally {
                ioOpCallback.completed(loadOp);
            }
        }

        @Override
        public void writeFailed(ICachedPage page, Throwable failure) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasFailed() {
            return componentBulkLoader.hasFailed();
        }

        @Override
        public Throwable getFailure() {
            return componentBulkLoader.getFailure();
        }

        @Override
        public void force() throws HyracksDataException {
            componentBulkLoader.force();
        }
    }

    @Override
    public ILSMIndexAccessor createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion)
            throws HyracksDataException {
        return new LSMTreeIndexAccessor(getHarness(), createOpContext(searchCallback, targetIndexVersion),
                ctx -> new LSMBTreeWithBuddySearchCursor(ctx, buddyBTreeFields));
    }

    // This function in an instance of this index is only used after a bulk load
    // is successful
    // it will therefore add the component to the first list and enter it.
    @Override
    public void addDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        if (version == 0) {
            diskComponents.add(0, c);
        } else if (version == 1) {
            secondDiskComponents.add(0, c);
        }
    }

    @Override
    public int getCurrentVersion() {
        return version;
    }

    @Override
    public void setCurrentVersion(int version) {
        this.version = version;
    }

    @Override
    public List<ILSMDiskComponent> getFirstComponentList() {
        return diskComponents;
    }

    @Override
    public List<ILSMDiskComponent> getSecondComponentList() {
        return secondDiskComponents;
    }

    @Override
    public void commitTransaction() throws HyracksDataException {
        LSMComponentFileReferences componentFileRefrences = fileManager.getTransactionFileReferenceForCommit();
        ILSMDiskComponent component = null;
        if (componentFileRefrences != null) {
            component = createDiskComponent(componentFactory, componentFileRefrences.getInsertIndexFileReference(),
                    componentFileRefrences.getDeleteIndexFileReference(),
                    componentFileRefrences.getBloomFilterFileReference(), false);
        }
        ((ExternalIndexHarness) getHarness()).addTransactionComponents(component);
    }

    @Override
    public void abortTransaction() throws HyracksDataException {
        fileManager.deleteTransactionFiles();
    }

    @Override
    public void recoverTransaction() throws HyracksDataException {
        fileManager.recoverTransaction();
    }

    @Override
    public boolean isPrimaryIndex() {
        return false;
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException {
        return null;
    }

    @Override
    protected AbstractLSMIndexOperationContext createOpContext(IIndexAccessParameters iap) {
        return null;
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        return null;
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException {
        return null;
    }
}
