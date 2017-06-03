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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.ExternalIndexHarness;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class ExternalBTreeWithBuddy extends AbstractLSMIndex implements ITreeIndex, ITwoPCIndex {

    // For creating merge disk components
    private final LSMBTreeWithBuddyDiskComponentFactory componentFactory;

    private final LSMBTreeWithBuddyDiskComponentFactory bulkComponentFactory;

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
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager,
            TreeIndexFactory<BTree> bulkLoadBTreeFactory, TreeIndexFactory<BTree> copyBtreeFactory,
            TreeIndexFactory<BTree> buddyBtreeFactory, BloomFilterFactory bloomFilterFactory,
            IFileMapProvider diskFileMapProvider, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            IBinaryComparatorFactory[] btreeCmpFactories, IBinaryComparatorFactory[] buddyBtreeCmpFactories,
            int[] buddyBTreeFields, boolean durable) {
        super(ioManager, diskBufferCache, fileManager, diskFileMapProvider, bloomFilterFalsePositiveRate, mergePolicy,
                opTracker, ioScheduler, ioOpCallback, durable);
        this.btreeCmpFactories = btreeCmpFactories;
        this.buddyBtreeCmpFactories = buddyBtreeCmpFactories;
        this.buddyBTreeFields = buddyBTreeFields;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.buddyBtreeLeafFrameFactory = buddyBtreeLeafFrameFactory;
        this.componentFactory =
                new LSMBTreeWithBuddyDiskComponentFactory(copyBtreeFactory, buddyBtreeFactory, bloomFilterFactory);
        this.bulkComponentFactory =
                new LSMBTreeWithBuddyDiskComponentFactory(bulkLoadBTreeFactory, buddyBtreeFactory, bloomFilterFactory);
        this.secondDiskComponents = new LinkedList<>();
    }

    @Override
    public void create() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }
        fileManager.deleteDirs();
        fileManager.createDirs();
        diskComponents.clear();
        secondDiskComponents.clear();
    }

    @Override
    protected ILSMDiskComponent loadComponent(LSMComponentFileReferences refs) throws HyracksDataException {
        return null;
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
                LSMBTreeWithBuddyDiskComponent component;
                component =
                        createDiskComponent(componentFactory, lsmComonentFileReference.getInsertIndexFileReference(),
                                lsmComonentFileReference.getDeleteIndexFileReference(),
                                lsmComonentFileReference.getBloomFilterFileReference(), false);
                diskComponents.add(component);
                secondDiskComponents.add(component);
            }
            ((ExternalIndexHarness) getLsmHarness()).indexFirstTimeActivated();
        } else {
            // This index has been opened before or is brand new with no
            // components. It should also maintain the version pointer
            for (ILSMComponent c : diskComponents) {
                LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
                BTree btree = component.getBTree();
                BTree buddyBtree = component.getBuddyBTree();
                BloomFilter bloomFilter = component.getBloomFilter();
                btree.activate();
                buddyBtree.activate();
                bloomFilter.activate();
            }
            for (ILSMComponent c : secondDiskComponents) {
                // Only activate non shared components
                if (!diskComponents.contains(c)) {
                    LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
                    BTree btree = component.getBTree();
                    BTree buddyBtree = component.getBuddyBTree();
                    BloomFilter bloomFilter = component.getBloomFilter();
                    btree.activate();
                    buddyBtree.activate();
                    bloomFilter.activate();
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
        ((ExternalIndexHarness) getLsmHarness()).indexClear();
        for (ILSMDiskComponent c : diskComponents) {
            clearDiskComponent(c);
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }
        for (ILSMDiskComponent c : secondDiskComponents) {
            clearDiskComponent(c);
        }
        diskComponents.clear();
        secondDiskComponents.clear();
        version = 0;
    }

    @Override
    protected void clearDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
        component.getBTree().deactivate();
        component.getBuddyBTree().deactivate();
        component.getBloomFilter().deactivate();
        component.getBTree().destroy();
        component.getBloomFilter().destroy();
        component.getBuddyBTree().destroy();
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }
        for (ILSMDiskComponent c : diskComponents) {
            destroyDiskComponent(c);
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }
        for (ILSMDiskComponent c : secondDiskComponents) {
            destroyDiskComponent(c);
        }
        diskComponents.clear();
        secondDiskComponents.clear();
        fileManager.deleteDirs();
        version = 0;
    }

    @Override
    protected void destroyDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
        component.getBTree().destroy();
        component.getBuddyBTree().destroy();
        component.getBloomFilter().destroy();
    }

    @Override
    public ILSMIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException {
        return new LSMTreeIndexAccessor(getLsmHarness(), createOpContext(searchCallback, version),
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

    @Override
    public ILSMDiskComponentBulkLoader createComponentBulkLoader(ILSMDiskComponent component, float fillFactor,
            boolean verifyInput, long numElementsHint, boolean checkIfEmptyIndex, boolean withFilter)
            throws HyracksDataException {
        BloomFilterSpecification bloomFilterSpec = null;
        if (numElementsHint > 0) {
            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
            bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement, bloomFilterFalsePositiveRate);
        }
        if (withFilter && filterFields != null) {
            return new LSMBTreeWithBuddyDiskComponentBulkLoader((LSMBTreeWithBuddyDiskComponent) component,
                    bloomFilterSpec, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, filterManager,
                    treeFields, filterFields,
                    MultiComparator.create(component.getLSMComponentFilter().getFilterCmpFactories()));
        } else {
            return new LSMBTreeWithBuddyDiskComponentBulkLoader((LSMBTreeWithBuddyDiskComponent) component,
                    bloomFilterSpec, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
        }
    }

    // For initial load
    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        return new LSMTwoPCBTreeWithBuddyBulkLoader(fillLevel, verifyInput, 0, false);
    }

    // For transaction bulk load <- could consolidate with the above method ->
    @Override
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        return new LSMTwoPCBTreeWithBuddyBulkLoader(fillLevel, verifyInput, numElementsHint, true);
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
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.FLUSH_NOT_SUPPORTED_IN_EXTERNAL_INDEX);
    }

    @Override
    public ILSMDiskComponent flush(ILSMIOOperation operation) throws HyracksDataException {
        throw HyracksDataException.create(ErrorCode.FLUSH_NOT_SUPPORTED_IN_EXTERNAL_INDEX);
    }

    protected LSMComponentFileReferences getMergeTargetFileName(List<ILSMComponent> mergingDiskComponents)
            throws HyracksDataException {
        BTree firstTree = ((LSMBTreeWithBuddyDiskComponent) mergingDiskComponents.get(0)).getBTree();
        BTree lastTree = ((LSMBTreeWithBuddyDiskComponent) mergingDiskComponents.get(mergingDiskComponents.size() - 1))
                .getBTree();
        FileReference firstFile = firstTree.getFileReference();
        FileReference lastFile = lastTree.getFileReference();
        LSMComponentFileReferences fileRefs =
                fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
        return fileRefs;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        ILSMIndexOperationContext bctx = createOpContext(NoOpOperationCallback.INSTANCE, 0);
        bctx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        ITreeIndexCursor cursor = new LSMBTreeWithBuddySortedCursor(bctx, buddyBTreeFields);
        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        ILSMIndexAccessor accessor = new LSMTreeIndexAccessor(getLsmHarness(), bctx,
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

        ioScheduler.scheduleOperation(new LSMBTreeWithBuddyMergeOperation(accessor, mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getDeleteIndexFileReference(),
                relMergeFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir(), keepDeleteTuples));
    }

    // This method creates the appropriate opContext for the targeted version
    public ExternalBTreeWithBuddyOpContext createOpContext(ISearchOperationCallback searchCallback, int targetVersion) {
        return new ExternalBTreeWithBuddyOpContext(btreeCmpFactories, buddyBtreeCmpFactories, searchCallback,
                targetVersion, getLsmHarness(), btreeInteriorFrameFactory, btreeLeafFrameFactory,
                buddyBtreeLeafFrameFactory);
    }

    @Override
    public ILSMDiskComponent merge(ILSMIOOperation operation) throws HyracksDataException {
        LSMBTreeWithBuddyMergeOperation mergeOp = (LSMBTreeWithBuddyMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate btreeSearchPred = new RangePredicate(null, null, true, true, null, null);
        ILSMIndexOperationContext opCtx = ((LSMBTreeWithBuddySortedCursor) cursor).getOpCtx();
        opCtx.getComponentHolder().addAll(mergeOp.getMergingComponents());
        search(opCtx, cursor, btreeSearchPred);

        LSMBTreeWithBuddyDiskComponent mergedComponent = createDiskComponent(componentFactory, mergeOp.getTarget(),
                mergeOp.getBuddyBTreeTarget(), mergeOp.getBloomFilterTarget(), true);

        IIndexBulkLoader componentBulkLoader;

        // In case we must keep the deleted-keys BuddyBTrees, then they must be
        // merged *before* merging the b-trees so that
        // lsmHarness.endSearch() is called once when the b-trees have been
        // merged.

        if (mergeOp.isKeepDeletedTuples()) {
            // Keep the deleted tuples since the oldest disk component is not
            // included in the merge operation
            LSMBuddyBTreeMergeCursor buddyBtreeCursor = new LSMBuddyBTreeMergeCursor(opCtx);
            search(opCtx, buddyBtreeCursor, btreeSearchPred);

            long numElements = 0L;
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                numElements += ((LSMBTreeWithBuddyDiskComponent) mergeOp.getMergingComponents().get(i)).getBloomFilter()
                        .getNumElements();
            }

            componentBulkLoader = createComponentBulkLoader(mergedComponent, 1.0f, false, numElements, false, false);

            try {
                while (buddyBtreeCursor.hasNext()) {
                    buddyBtreeCursor.next();
                    ITupleReference tuple = buddyBtreeCursor.getTuple();
                    ((LSMBTreeWithBuddyDiskComponentBulkLoader) componentBulkLoader).delete(tuple);
                }
            } finally {
                buddyBtreeCursor.close();
            }
        } else {
            componentBulkLoader = createComponentBulkLoader(mergedComponent, 1.0f, false, 0L, false, false);
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

    @Override
    public void markAsValid(ILSMDiskComponent lsmComponent) throws HyracksDataException {
        LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) lsmComponent;
        // Flush the bloom filter first.
        markAsValidInternal(component.getBTree().getBufferCache(), component.getBloomFilter());
        markAsValidInternal(component.getBTree());
        markAsValidInternal(component.getBuddyBTree());
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
        if (flushOnExit) {
            ioOpCallback.afterFinalize(LSMOperationType.FLUSH, null);
        }
        // Even though, we deactivate the index, we don't exit components or
        // modify any of the lists to make sure they
        // are there if the index was opened again
        for (ILSMDiskComponent c : diskComponents) {
            deactivateDiskComponent(c);
        }
        for (ILSMDiskComponent c : secondDiskComponents) {
            // Only deactivate non shared components
            if (!diskComponents.contains(c)) {
                deactivateDiskComponent(c);
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

    private LSMBTreeWithBuddyDiskComponent createDiskComponent(ILSMDiskComponentFactory factory,
            FileReference insertFileRef, FileReference deleteFileRef, FileReference bloomFilterFileRef,
            boolean createComponent) throws HyracksDataException {
        // Create new instance.
        LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) factory
                .createComponent(new LSMComponentFileReferences(insertFileRef, deleteFileRef, bloomFilterFileRef));
        if (createComponent) {
            component.getBTree().create();
            component.getBuddyBTree().create();
            component.getBloomFilter().create();
        }
        component.getBTree().activate();
        component.getBuddyBTree().activate();
        component.getBloomFilter().activate();
        return component;
    }

    // even though the index doesn't support record level modification, the
    // accessor will try to do it
    // we could throw the exception here but we don't. it will eventually be
    // thrown by the index itself

    // The bulk loader used for both initial loading and transaction
    // modifications
    public class LSMTwoPCBTreeWithBuddyBulkLoader implements IIndexBulkLoader, ITwoPCIndexBulkLoader {
        private final ILSMDiskComponent component;
        private final IIndexBulkLoader componentBulkLoader;
        private final boolean isTransaction;

        public LSMTwoPCBTreeWithBuddyBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean isTransaction) throws HyracksDataException {
            this.isTransaction = isTransaction;
            // Create the appropriate target
            if (isTransaction) {
                component = createTransactionTarget();
            } else {
                component = createBulkLoadTarget();
            }

            componentBulkLoader =
                    createComponentBulkLoader(component, fillFactor, verifyInput, numElementsHint, false, true);
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            componentBulkLoader.add(tuple);
        }

        @Override
        public void end() throws HyracksDataException {
            componentBulkLoader.end();
            if (component.getComponentSize() > 0) {
                if (isTransaction) {
                    // Since this is a transaction component, validate and
                    // deactivate. it could later be added or deleted
                    markAsValid(component);
                    BTree btree = ((LSMBTreeWithBuddyDiskComponent) component).getBTree();
                    BTree buddyBtree = ((LSMBTreeWithBuddyDiskComponent) component).getBuddyBTree();
                    BloomFilter bloomFilter = ((LSMBTreeWithBuddyDiskComponent) component).getBloomFilter();
                    btree.deactivate();
                    buddyBtree.deactivate();
                    bloomFilter.deactivate();
                } else {
                    getLsmHarness().addBulkLoadedComponent(component);
                }
            }
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            ((LSMBTreeWithBuddyDiskComponentBulkLoader) componentBulkLoader).delete(tuple);
        }

        @Override
        public void abort() {
            try {
                componentBulkLoader.abort();
            } catch (Exception e) {
            }
        }

        // This method is used to create a target for a bulk modify operation. This
        // component must then eventually be either committed or deleted
        private ILSMDiskComponent createTransactionTarget() throws HyracksDataException {
            LSMComponentFileReferences componentFileRefs;
            try {
                componentFileRefs = fileManager.getNewTransactionFileReference();
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            return createDiskComponent(bulkComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                    componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(),
                    true);
        }
    }

    protected ILSMDiskComponent createBulkLoadTarget() throws HyracksDataException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), true);
    }

    @Override
    public ILSMIndexAccessor createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion)
            throws HyracksDataException {
        return new LSMTreeIndexAccessor(getLsmHarness(), createOpContext(searchCallback, targetIndexVersion),
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
        LSMBTreeWithBuddyDiskComponent component = null;
        if (componentFileRefrences != null) {
            component = createDiskComponent(componentFactory, componentFileRefrences.getInsertIndexFileReference(),
                    componentFileRefrences.getDeleteIndexFileReference(),
                    componentFileRefrences.getBloomFilterFileReference(), false);
        }
        ((ExternalIndexHarness) getLsmHarness()).addTransactionComponents(component);
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
    public boolean hasMemoryComponents() {
        return false;
    }

    @Override
    public boolean isPrimaryIndex() {
        return false;
    }

    @Override
    public Set<String> getLSMComponentPhysicalFiles(ILSMComponent lsmComponent) {
        Set<String> files = new HashSet<>();
        LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) lsmComponent;
        files.add(component.getBTree().getFileReference().getFile().getAbsolutePath());
        files.add(component.getBuddyBTree().getFileReference().getFile().getAbsolutePath());
        files.add(component.getBloomFilter().getFileReference().getFile().getAbsolutePath());
        return files;
    }

    @Override
    protected void deactivateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        //do nothing since external index never use memory components
    }

    @Override
    protected void deactivateDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
        BTree btree = component.getBTree();
        BTree buddyBtree = component.getBuddyBTree();
        BloomFilter bloomFilter = component.getBloomFilter();
        btree.deactivateCloseHandle();
        buddyBtree.deactivateCloseHandle();
        bloomFilter.deactivate();
    }

    @Override
    protected void destroyMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        //do nothing since external index never use memory components
    }

    @Override
    protected void clearMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        //do nothing since external index never use memory components
    }

    @Override
    protected long getMemoryComponentSize(ILSMMemoryComponent c) {
        return 0;
    }

    @Override
    protected void validateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for LSM B-Trees with Buddy B-Tree.");
    }

    @Override
    protected void validateDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for LSM B-Trees with Buddy B-Tree.");
    }

    @Override
    protected void allocateMemoryComponent(ILSMMemoryComponent c) throws HyracksDataException {
        //do nothing since external index never use memory components
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException {
        return null;
    }

    @Override
    protected AbstractLSMIndexOperationContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return null;
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            ILSMMemoryComponent flushingComponent, LSMComponentFileReferences componentFileRefs,
            ILSMIOOperationCallback callback) throws HyracksDataException {
        return null;
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            List<ILSMComponent> mergingComponents, LSMComponentFileReferences mergeFileRefs,
            ILSMIOOperationCallback callback) throws HyracksDataException {
        return null;
    }
}
