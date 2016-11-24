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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
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
import org.apache.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import org.apache.hyracks.storage.am.lsm.common.impls.ExternalIndexHarness;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
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
    private final List<ILSMComponent> secondDiskComponents;
    private int version = -1;

    public ExternalBTreeWithBuddy(IIOManager ioManager, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ITreeIndexFrameFactory buddyBtreeLeafFrameFactory,
            IBufferCache diskBufferCache, ILSMIndexFileManager fileManager,
            TreeIndexFactory<BTree> bulkLoadBTreeFactory, TreeIndexFactory<BTree> copyBtreeFactory,
            TreeIndexFactory<BTree> buddyBtreeFactory, BloomFilterFactory bloomFilterFactory,
            IFileMapProvider diskFileMapProvider, double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            IBinaryComparatorFactory[] btreeCmpFactories, IBinaryComparatorFactory[] buddyBtreeCmpFactories,
            int[] buddyBTreeFields, int version, boolean durable) {
        super(ioManager, diskBufferCache, fileManager, diskFileMapProvider, bloomFilterFalsePositiveRate, mergePolicy,
                opTracker,
                ioScheduler, ioOpCallback, durable);
        this.btreeCmpFactories = btreeCmpFactories;
        this.buddyBtreeCmpFactories = buddyBtreeCmpFactories;
        this.buddyBTreeFields = buddyBTreeFields;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.buddyBtreeLeafFrameFactory = buddyBtreeLeafFrameFactory;
        this.componentFactory = new LSMBTreeWithBuddyDiskComponentFactory(copyBtreeFactory, buddyBtreeFactory,
                bloomFilterFactory);
        this.bulkComponentFactory = new LSMBTreeWithBuddyDiskComponentFactory(bulkLoadBTreeFactory, buddyBtreeFactory,
                bloomFilterFactory);
        this.secondDiskComponents = new LinkedList<>();
        this.version = version;
    }

    @Override
    public void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }
        fileManager.deleteDirs();
        fileManager.createDirs();
        diskComponents.clear();
        secondDiskComponents.clear();
    }

    @Override
    public void activate() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }

        if (diskComponents.size() == 0 && secondDiskComponents.size() == 0) {
            //First time activation
            List<LSMComponentFileReferences> validFileReferences;
            try {
                validFileReferences = fileManager.cleanupAndGetValidFiles();
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
            for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
                LSMBTreeWithBuddyDiskComponent component;
                try {
                    component = createDiskComponent(componentFactory,
                            lsmComonentFileReference.getInsertIndexFileReference(),
                            lsmComonentFileReference.getDeleteIndexFileReference(),
                            lsmComonentFileReference.getBloomFilterFileReference(), false);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
                diskComponents.add(component);
                secondDiskComponents.add(component);
            }
            ((ExternalIndexHarness) lsmHarness).indexFirstTimeActivated();
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
        isActivated = true;
    }

    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        ((ExternalIndexHarness) lsmHarness).indexClear();

        for (ILSMComponent c : diskComponents) {
            LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
            component.getBTree().deactivate();
            component.getBuddyBTree().deactivate();
            component.getBloomFilter().deactivate();
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
            component.getBuddyBTree().destroy();
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }

        for (ILSMComponent c : secondDiskComponents) {
            LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
            component.getBTree().deactivate();
            component.getBloomFilter().deactivate();
            component.getBuddyBTree().deactivate();
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
            component.getBuddyBTree().destroy();
        }

        diskComponents.clear();
        secondDiskComponents.clear();
        version = -1;
    }

    @Override
    public void deactivate() throws HyracksDataException {
        deactivate(true);
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }
        for (ILSMComponent c : diskComponents) {
            LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
            component.getBTree().destroy();
            component.getBuddyBTree().destroy();
            component.getBloomFilter().destroy();
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }
        for (ILSMComponent c : secondDiskComponents) {
            LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
            component.getBTree().destroy();
            component.getBuddyBTree().destroy();
            component.getBloomFilter().destroy();
        }
        diskComponents.clear();
        secondDiskComponents.clear();
        fileManager.deleteDirs();
        version = -1;
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException {
        return new LSMBTreeWithBuddyAccessor(lsmHarness, createOpContext(searchCallback, version));
    }

    @Override
    public void validate() throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for LSM B-Trees with Buddy B-Tree.");
    }

    @Override
    public long getMemoryAllocationSize() {
        return 0;
    }

    // The subsume merged components is overridden to account for:
    // Maintaining two versions of the index
    @Override
    public void subsumeMergedComponents(ILSMComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        List<ILSMComponent> newerList;
        List<ILSMComponent> olderList;
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
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMTwoPCBTreeWithBuddyBulkLoader(fillLevel, verifyInput, 0, checkIfEmptyIndex, false);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    // For transaction bulk load <- could consolidate with the above method ->
    @Override
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMTwoPCBTreeWithBuddyBulkLoader(fillLevel, verifyInput, numElementsHint, checkIfEmptyIndex,
                    true);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("tuple modify not supported in LSM-Disk-Only-BTree");
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        ExternalBTreeWithBuddyOpContext ctx = (ExternalBTreeWithBuddyOpContext) ictx;
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();
        ctx.searchInitialState.setOperationalComponents(operationalComponents);
        cursor.open(ctx.searchInitialState, pred);
    }

    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-BTree");
    }

    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-BTree");
    }

    protected LSMComponentFileReferences getMergeTargetFileName(List<ILSMComponent> mergingDiskComponents)
            throws HyracksDataException {
        BTree firstTree = ((LSMBTreeWithBuddyDiskComponent) mergingDiskComponents.get(0)).getBTree();
        BTree lastTree = ((LSMBTreeWithBuddyDiskComponent) mergingDiskComponents.get(mergingDiskComponents.size() - 1))
                .getBTree();
        FileReference firstFile = firstTree.getFileReference();
        FileReference lastFile = lastTree.getFileReference();
        LSMComponentFileReferences fileRefs = fileManager.getRelMergeFileReference(firstFile.getFile().getName(),
                lastFile.getFile().getName());
        return fileRefs;
    }

    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        ILSMIndexOperationContext bctx = createOpContext(NoOpOperationCallback.INSTANCE, 0);
        bctx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        ITreeIndexCursor cursor = new LSMBTreeWithBuddySortedCursor(bctx, buddyBTreeFields);
        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        ILSMIndexAccessorInternal accessor = new LSMBTreeWithBuddyAccessor(lsmHarness, bctx);

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
                targetVersion, lsmHarness, btreeInteriorFrameFactory, btreeLeafFrameFactory,
                buddyBtreeLeafFrameFactory);
    }

    @Override
    public ILSMComponent merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMBTreeWithBuddyMergeOperation mergeOp = (LSMBTreeWithBuddyMergeOperation) operation;
        ITreeIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate btreeSearchPred = new RangePredicate(null, null, true, true, null, null);
        ILSMIndexOperationContext opCtx = ((LSMBTreeWithBuddySortedCursor) cursor).getOpCtx();
        opCtx.getComponentHolder().addAll(mergeOp.getMergingComponents());
        search(opCtx, cursor, btreeSearchPred);

        LSMBTreeWithBuddyDiskComponent mergedComponent = createDiskComponent(componentFactory,
                mergeOp.getBTreeMergeTarget(), mergeOp.getBuddyBTreeMergeTarget(), mergeOp.getBloomFilterMergeTarget(),
                true);

        // In case we must keep the deleted-keys BuddyBTrees, then they must be
        // merged *before* merging the b-trees so that
        // lsmHarness.endSearch() is called once when the b-trees have been
        // merged.

        if (mergeOp.isKeepDeletedTuples()) {
            // Keep the deleted tuples since the oldest disk component is not
            // included in the merge operation
            LSMBuddyBTreeMergeCursor buddyBtreeCursor = new LSMBuddyBTreeMergeCursor(opCtx);
            search(opCtx, buddyBtreeCursor, btreeSearchPred);

            BTree buddyBtree = mergedComponent.getBuddyBTree();
            IIndexBulkLoader buddyBtreeBulkLoader = buddyBtree.createBulkLoader(1.0f, true, 0L, false);

            long numElements = 0L;
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                numElements += ((LSMBTreeWithBuddyDiskComponent) mergeOp.getMergingComponents().get(i)).getBloomFilter()
                        .getNumElements();
            }

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            IIndexBulkLoader builder = mergedComponent.getBloomFilter().createBuilder(numElements,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());

            try {
                while (buddyBtreeCursor.hasNext()) {
                    buddyBtreeCursor.next();
                    ITupleReference tuple = buddyBtreeCursor.getTuple();
                    buddyBtreeBulkLoader.add(tuple);
                    builder.add(tuple);
                }
            } finally {
                buddyBtreeCursor.close();
                builder.end();
            }
            buddyBtreeBulkLoader.end();
        }

        IIndexBulkLoader bulkLoader = mergedComponent.getBTree().createBulkLoader(1.0f, false, 0L, false);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                bulkLoader.add(frameTuple);
            }
        } finally {
            cursor.close();
        }
        bulkLoader.end();
        return mergedComponent;
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) {
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        List<ILSMComponent> immutableComponents;
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
    public void markAsValid(ILSMComponent lsmComponent) throws HyracksDataException {
        LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) lsmComponent;
        // Flush the bloom filter first.
        markAsValidInternal(component.getBTree().getBufferCache(), component.getBloomFilter());
        markAsValidInternal(component.getBTree());
        markAsValidInternal(component.getBuddyBTree());
    }

    // This function is used when a new component is to be committed -- is
    // called by the harness.
    @Override
    public void commitTransactionDiskComponent(ILSMComponent newComponent) throws HyracksDataException {

        // determine which list is the new one and flip the pointer
        List<ILSMComponent> newerList;
        List<ILSMComponent> olderList;
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
        if (!isActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }

        if (flushOnExit) {
            BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(ioOpCallback);
            cb.afterFinalize(LSMOperationType.FLUSH, null);
        }
        // Even though, we deactivate the index, we don't exit components or
        // modify any of the lists to make sure they
        // are there if the index was opened again

        for (ILSMComponent c : diskComponents) {
            LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
            BTree btree = component.getBTree();
            BTree buddyBtree = component.getBuddyBTree();
            BloomFilter bloomFilter = component.getBloomFilter();
            btree.deactivateCloseHandle();
            buddyBtree.deactivateCloseHandle();
            bloomFilter.deactivate();
        }
        for (ILSMComponent c : secondDiskComponents) {
            // Only deactivate non shared components
            if (!diskComponents.contains(c)) {
                LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) c;
                BTree btree = component.getBTree();
                BTree buddyBtree = component.getBuddyBTree();
                BloomFilter bloomFilter = component.getBloomFilter();
                btree.deactivateCloseHandle();
                buddyBtree.deactivateCloseHandle();
                bloomFilter.deactivate();
            }
        }
        isActivated = false;
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
    public IMetaDataPageManager getMetaManager() {
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

    private LSMBTreeWithBuddyDiskComponent createDiskComponent(ILSMComponentFactory factory,
            FileReference insertFileRef, FileReference deleteFileRef, FileReference bloomFilterFileRef,
            boolean createComponent) throws HyracksDataException, IndexException {
        // Create new instance.
        LSMBTreeWithBuddyDiskComponent component = (LSMBTreeWithBuddyDiskComponent) factory.createLSMComponentInstance(
                new LSMComponentFileReferences(insertFileRef, deleteFileRef, bloomFilterFileRef));
        if (createComponent) {
            component.getBloomFilter().create();
        } else {
            component.getBTree().activate();
            component.getBuddyBTree().activate();
        }
        component.getBloomFilter().activate();
        return component;
    }

    // even though the index doesn't support record level modification, the
    // accessor will try to do it
    // we could throw the exception here but we don't. it will eventually be
    // thrown by the index itself
    public class LSMBTreeWithBuddyAccessor extends LSMTreeIndexAccessor {
        public LSMBTreeWithBuddyAccessor(ILSMHarness lsmHarness, ILSMIndexOperationContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor(boolean exclusive) {
            return new LSMBTreeWithBuddySearchCursor(ctx, buddyBTreeFields);
        }

        public MultiComparator getBTreeMultiComparator() {
            ExternalBTreeWithBuddyOpContext concreteCtx = (ExternalBTreeWithBuddyOpContext) ctx;
            return concreteCtx.getBTreeMultiComparator();
        }

        public MultiComparator getBodyBTreeMultiComparator() {
            ExternalBTreeWithBuddyOpContext concreteCtx = (ExternalBTreeWithBuddyOpContext) ctx;
            return concreteCtx.getBuddyBTreeMultiComparator();
        }
    }

    // The bulk loader used for both initial loading and transaction
    // modifications
    public class LSMTwoPCBTreeWithBuddyBulkLoader implements IIndexBulkLoader, ITwoPCIndexBulkLoader {
        private final ILSMComponent component;
        private final BTreeBulkLoader btreeBulkLoader;
        private final BTreeBulkLoader buddyBtreeBulkLoader;
        private final IIndexBulkLoader builder;
        private boolean cleanedUpArtifacts = false;
        private boolean isEmptyComponent = true;
        private boolean endedBloomFilterLoad = false;
        private final boolean isTransaction;

        public LSMTwoPCBTreeWithBuddyBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex, boolean isTransaction) throws TreeIndexException, HyracksDataException {
            this.isTransaction = isTransaction;
            // Create the appropriate target
            if (isTransaction) {
                try {
                    component = createTransactionTarget();
                } catch (HyracksDataException | IndexException e) {
                    throw new TreeIndexException(e);
                }
            } else {
                if (checkIfEmptyIndex && !isEmptyIndex()) {
                    throw new TreeIndexException("Cannot load an index that is not empty");
                }
                try {
                    component = createBulkLoadTarget();
                } catch (HyracksDataException | IndexException e) {
                    throw new TreeIndexException(e);
                }
            }

            // Create the three loaders
            btreeBulkLoader = (BTreeBulkLoader) ((LSMBTreeWithBuddyDiskComponent) component).getBTree()
                    .createBulkLoader(fillFactor, verifyInput, numElementsHint, false, true);
            buddyBtreeBulkLoader = (BTreeBulkLoader) ((LSMBTreeWithBuddyDiskComponent) component).getBuddyBTree()
                    .createBulkLoader(fillFactor, verifyInput, numElementsHint, false, true);
            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            builder = ((LSMBTreeWithBuddyDiskComponent) component).getBloomFilter().createBuilder(numElementsHint,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                btreeBulkLoader.add(tuple);
            } catch (IndexException | HyracksDataException | RuntimeException e) {
                cleanupArtifacts();
                throw e;
            }
            if (isEmptyComponent) {
                isEmptyComponent = false;
            }
        }

        // This is made public in case of a failure, it is better to delete all
        // created artifacts.
        public void cleanupArtifacts() throws HyracksDataException {
            if (!cleanedUpArtifacts) {
                cleanedUpArtifacts = true;
                try {
                    ((LSMBTreeWithBuddyDiskComponent) component).getBTree().deactivate();
                } catch (Exception e) {

                }
                ((LSMBTreeWithBuddyDiskComponent) component).getBTree().destroy();
                try {
                    ((LSMBTreeWithBuddyDiskComponent) component).getBuddyBTree().deactivate();
                } catch (Exception e) {

                }
                ((LSMBTreeWithBuddyDiskComponent) component).getBuddyBTree().destroy();
                try {
                    ((LSMBTreeWithBuddyDiskComponent) component).getBloomFilter().deactivate();
                } catch (Exception e) {

                }
                ((LSMBTreeWithBuddyDiskComponent) component).getBloomFilter().destroy();
            }
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            if (!cleanedUpArtifacts) {
                if (!endedBloomFilterLoad) {
                    builder.end();
                    endedBloomFilterLoad = true;
                }
                btreeBulkLoader.end();
                buddyBtreeBulkLoader.end();
                if (isEmptyComponent) {
                    cleanupArtifacts();
                } else if (isTransaction) {
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
                    lsmHarness.addBulkLoadedComponent(component);
                }
            }
        }

        @Override
        public void delete(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                buddyBtreeBulkLoader.add(tuple);
                builder.add(tuple);
            } catch (IndexException | HyracksDataException | RuntimeException e) {
                cleanupArtifacts();
                throw e;
            }
            if (isEmptyComponent) {
                isEmptyComponent = false;
            }
        }

        @Override
        public void abort() {
            try {
                cleanupArtifacts();
            } catch (Exception e) {
            }
        }
    }

    protected ILSMComponent createBulkLoadTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs = fileManager.getRelFlushFileReference();
        return createDiskComponent(bulkComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), true);
    }

    // This method is used to create a target for a bulk modify operation. This
    // component must then eventually be either committed or deleted
    private ILSMComponent createTransactionTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs;
        try {
            componentFileRefs = fileManager.getNewTransactionFileReference();
        } catch (IOException e) {
            throw new HyracksDataException("Failed to create transaction components", e);
        }
        return createDiskComponent(bulkComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), true);
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion)
            throws HyracksDataException {
        return new LSMBTreeWithBuddyAccessor(lsmHarness, createOpContext(searchCallback, targetIndexVersion));
    }

    // This function in an instance of this index is only used after a bulk load
    // is successful
    // it will therefore add the component to the first list and enter it.
    @Override
    public void addComponent(ILSMComponent c) throws HyracksDataException {
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
    public List<ILSMComponent> getFirstComponentList() {
        return diskComponents;
    }

    @Override
    public List<ILSMComponent> getSecondComponentList() {
        return secondDiskComponents;
    }

    @Override
    public void commitTransaction() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefrences = fileManager.getTransactionFileReferenceForCommit();
        LSMBTreeWithBuddyDiskComponent component = null;
        if (componentFileRefrences != null) {
            component = createDiskComponent(componentFactory, componentFileRefrences.getInsertIndexFileReference(),
                    componentFileRefrences.getDeleteIndexFileReference(),
                    componentFileRefrences.getBloomFilterFileReference(), false);
        }
        ((ExternalIndexHarness) lsmHarness).addTransactionComponents(component);
    }

    @Override
    public void abortTransaction() throws TreeIndexException {
        try {
            fileManager.deleteTransactionFiles();
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    @Override
    public void recoverTransaction() throws TreeIndexException {
        try {
            fileManager.recoverTransaction();
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
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
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, boolean appendOnly) throws IndexException {
        if (!appendOnly) {
            throw new IndexException("LSM Indices do not support in-place inserts");
        } else {
            return createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex);
        }
    }

    @Override
    public void allocateMemoryComponents() throws HyracksDataException {
        //do nothing since external index never use memory components
    }
}
