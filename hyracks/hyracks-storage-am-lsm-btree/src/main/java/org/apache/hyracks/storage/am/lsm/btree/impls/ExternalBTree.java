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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import org.apache.hyracks.storage.am.common.api.IFreePageManager;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.tuples.LSMBTreeRefrencingTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessorInternal;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import org.apache.hyracks.storage.am.lsm.common.impls.ExternalIndexHarness;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

/**
 * This is an lsm b-tree that does not have memory component and is modified
 * only by bulk loading and addition of disk components as of this point, it is
 * intended for use with external dataset indexes only.
 * 
 * @author alamouda
 */
public class ExternalBTree extends LSMBTree implements ITwoPCIndex {

    // This component factory has to be different since it uses different tuple
    // writer in it's leaf frames to support inserting both
    // regular and delete tuples
    private final LSMBTreeDiskComponentFactory transactionComponentFactory;
    // A second disk component list that will be used when a transaction is
    // committed and will be seen by subsequent accessors
    private final List<ILSMComponent> secondDiskComponents;
    // A pointer that points to the current most recent list (either
    // diskComponents = 0, or secondDiskComponents = 1). It starts with -1 to
    // indicate first time activation
    private int version = -1;

    private final ITreeIndexFrameFactory interiorFrameFactory;

    public ExternalBTree(ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ILSMIndexFileManager fileManager,
            TreeIndexFactory<BTree> diskBTreeFactory, TreeIndexFactory<BTree> bulkLoadBTreeFactory,
            BloomFilterFactory bloomFilterFactory, double bloomFilterFalsePositiveRate,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, TreeIndexFactory<BTree> transactionBTreeFactory, int version,
            boolean durable) {
        super(interiorFrameFactory, insertLeafFrameFactory, deleteLeafFrameFactory, fileManager, diskBTreeFactory,
                bulkLoadBTreeFactory, bloomFilterFactory, bloomFilterFalsePositiveRate, diskFileMapProvider,
                fieldCount, cmpFactories, mergePolicy, opTracker, ioScheduler, ioOpCallback, false, durable);
        this.transactionComponentFactory = new LSMBTreeDiskComponentFactory(transactionBTreeFactory,
                bloomFilterFactory, null);
        this.secondDiskComponents = new LinkedList<ILSMComponent>();
        this.interiorFrameFactory = interiorFrameFactory;
        this.version = version;
    }

    // This method is used to create a target for a bulk modify operation. This
    // component must then be either committed or deleted
    private ILSMComponent createTransactionTarget() throws HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefs;
        try {
            componentFileRefs = fileManager.getNewTransactionFileReference();
        } catch (IOException e) {
            throw new HyracksDataException("Failed to create transaction components", e);
        }
        return createDiskComponent(transactionComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), true);
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

    // This method is used by the merge policy when it needs to check if a merge
    // is needed.
    // It only needs to return the newer list
    @Override
    public List<ILSMComponent> getImmutableComponents() {
        if (version == 0) {
            return diskComponents;
        } else {
            return secondDiskComponents;
        }
    }

    // The only reason to override the following method is that it uses a different context object
    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        ExternalBTreeOpContext ctx = (ExternalBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        LSMBTreeCursorInitialState initialState = new LSMBTreeCursorInitialState(insertLeafFrameFactory, ctx.cmp,
                ctx.bloomFilterCmp, lsmHarness, pred, ctx.searchCallback, operationalComponents);
        cursor.open(initialState, pred);
    }

    // The only reason to override the following method is that it uses a different context object
    // in addition, determining whether or not to keep deleted tuples is different here
    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        ExternalBTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE, -1);
        opCtx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        boolean returnDeletedTuples = false;
        if (version == 0) {
            if (ctx.getComponentHolder().get(ctx.getComponentHolder().size() - 1) != diskComponents.get(diskComponents
                    .size() - 1)) {
                returnDeletedTuples = true;
            }
        } else {
            if (ctx.getComponentHolder().get(ctx.getComponentHolder().size() - 1) != secondDiskComponents
                    .get(secondDiskComponents.size() - 1)) {
                returnDeletedTuples = true;
            }
        }
        ITreeIndexCursor cursor = new LSMBTreeRangeSearchCursor(opCtx, returnDeletedTuples);
        BTree firstBTree = ((LSMBTreeDiskComponent) mergingComponents.get(0)).getBTree();
        BTree lastBTree = ((LSMBTreeDiskComponent) mergingComponents.get(mergingComponents.size() - 1)).getBTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstBTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastBTree.getFileId());
        LSMComponentFileReferences relMergeFileRefs = fileManager.getRelMergeFileReference(firstFile.getFile()
                .getName(), lastFile.getFile().getName());
        ILSMIndexAccessorInternal accessor = new LSMBTreeAccessor(lsmHarness, opCtx);
        ioScheduler.scheduleOperation(new LSMBTreeMergeOperation(accessor, mergingComponents, cursor, relMergeFileRefs
                .getInsertIndexFileReference(), relMergeFileRefs.getBloomFilterFileReference(), callback, fileManager
                .getBaseDir()));
    }

    // This function should only be used when a transaction fail. it doesn't
    // take any parameters since there can only be
    // a single transaction and hence a single transaction component on disk
    public void deleteTransactionComponent() throws HyracksDataException {
        fileManager.deleteTransactionFiles();
    }

    // This function in an instance of this index is only used after a bulk load
    // is successful
    @Override
    public void addComponent(ILSMComponent c) throws HyracksDataException {
        if (version == 0) {
            diskComponents.add(0, c);
        } else if (version == 1) {
            secondDiskComponents.add(0, c);
        }
    }

    // This function is used when a new component is to be committed.
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
    public synchronized void activate() throws HyracksDataException {
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
                LSMBTreeDiskComponent component;
                try {
                    component = createDiskComponent(componentFactory,
                            lsmComonentFileReference.getInsertIndexFileReference(),
                            lsmComonentFileReference.getBloomFilterFileReference(), false);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
                diskComponents.add(component);
                secondDiskComponents.add(component);
            }
            ((ExternalIndexHarness) lsmHarness).indexFirstTimeActivated();
        } else {
            // This index has been opened before
            for (ILSMComponent c : diskComponents) {
                LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
                BTree btree = component.getBTree();
                BloomFilter bloomFilter = component.getBloomFilter();
                btree.activate();
                bloomFilter.activate();
            }
            for (ILSMComponent c : secondDiskComponents) {
                // Only activate non shared components
                if (!diskComponents.contains(c)) {
                    LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
                    BTree btree = component.getBTree();
                    BloomFilter bloomFilter = component.getBloomFilter();
                    btree.activate();
                    bloomFilter.activate();
                }
            }
        }
        isActivated = true;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        super.create();
        secondDiskComponents.clear();
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
        for (ILSMComponent c : diskComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
            BTree btree = component.getBTree();
            BloomFilter bloomFilter = component.getBloomFilter();
            btree.deactivate();
            bloomFilter.deactivate();
        }
        for (ILSMComponent c : secondDiskComponents) {
            // Only deactivate non shared components (So components are not de-activated twice)
            if (!diskComponents.contains(c)) {
                LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
                BTree btree = component.getBTree();
                BloomFilter bloomFilter = component.getBloomFilter();
                btree.deactivate();
                bloomFilter.deactivate();
            }
        }
        isActivated = false;
    }

    // The clear method is not used anywhere in AsterixDB! we override it anyway
    // to exit components first and deal with the two lists
    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        ((ExternalIndexHarness) lsmHarness).indexClear();

        for (ILSMComponent c : diskComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
            component.getBloomFilter().deactivate();
            component.getBTree().deactivate();
            component.getBloomFilter().destroy();
            component.getBTree().destroy();
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }

        for (ILSMComponent c : secondDiskComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
            component.getBloomFilter().deactivate();
            component.getBTree().deactivate();
            component.getBloomFilter().destroy();
            component.getBTree().destroy();
        }
        diskComponents.clear();
        secondDiskComponents.clear();
        version = -1;
    }

    @Override
    public void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }
        for (ILSMComponent c : diskComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }
        for (ILSMComponent c : secondDiskComponents) {
            LSMBTreeDiskComponent component = (LSMBTreeDiskComponent) c;
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
        }
        diskComponents.clear();
        secondDiskComponents.clear();
        fileManager.deleteDirs();
        version = -1;
    }

    @Override
    public void validate() throws HyracksDataException {
        for (ILSMComponent c : diskComponents) {
            BTree btree = ((LSMBTreeDiskComponent) c).getBTree();
            btree.validate();
        }
        for (ILSMComponent c : secondDiskComponents) {
            if (!diskComponents.contains(c)) {
                BTree btree = ((LSMBTreeDiskComponent) c).getBTree();
                btree.validate();
            }
        }
    }

    // Not supported
    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("tuple modify not supported in LSM-Disk-Only-BTree");
    }

    // Not supported
    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-BTree");
    }

    // Not supported
    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-BTree");
    }

    // Only support search and merge operations
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
        ExternalBTreeOpContext opCtx = (ExternalBTreeOpContext) ctx;
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
                break;
            case REPLICATE:
                operationalComponents.addAll(ctx.getComponentsToBeReplicated());
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
    }

    // For initial load
    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMTwoPCBTreeBulkLoader(fillLevel, verifyInput, numElementsHint, checkIfEmptyIndex, false);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    // For transaction bulk load <- could consolidate with the above method ->
    @Override
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMTwoPCBTreeBulkLoader(fillLevel, verifyInput, numElementsHint, checkIfEmptyIndex, true);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    // The bulk loader used for both initial loading and transaction
    // modifications
    public class LSMTwoPCBTreeBulkLoader implements IIndexBulkLoader, ITwoPCIndexBulkLoader {
        private final ILSMComponent component;
        private final BTreeBulkLoader bulkLoader;
        private final IIndexBulkLoader builder;
        private boolean cleanedUpArtifacts = false;
        private boolean isEmptyComponent = true;
        private boolean endedBloomFilterLoad = false;
        private final boolean isTransaction;
        private final ITreeIndexTupleWriterFactory frameTupleWriterFactory;

        public LSMTwoPCBTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
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

            frameTupleWriterFactory = ((LSMBTreeDiskComponent) component).getBTree().getLeafFrameFactory()
                    .getTupleWriterFactory();
            bulkLoader = (BTreeBulkLoader) ((LSMBTreeDiskComponent) component).getBTree().createBulkLoader(fillFactor,
                    verifyInput, numElementsHint, false);

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            builder = ((LSMBTreeDiskComponent) component).getBloomFilter().createBuilder(numElementsHint,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        }

        // It is expected that the mode was set to insert operation before
        // calling add
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

        // This is made public in case of a failure, it is better to delete all
        // created artifacts.
        public void cleanupArtifacts() throws HyracksDataException, IndexException {
            if (!cleanedUpArtifacts) {
                cleanedUpArtifacts = true;
                // We make sure to end the bloom filter load to release latches.
                if (!endedBloomFilterLoad) {
                    builder.end();
                    endedBloomFilterLoad = true;
                }
                try {
                    ((LSMBTreeDiskComponent) component).getBTree().deactivate();
                } catch (HyracksDataException e) {
                    // Do nothing.. this could've bee
                }
                ((LSMBTreeDiskComponent) component).getBTree().destroy();
                try {
                    ((LSMBTreeDiskComponent) component).getBloomFilter().deactivate();
                } catch (HyracksDataException e) {
                    // Do nothing.. this could've bee
                }
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
                } else if (isTransaction) {
                    // Since this is a transaction component, validate and
                    // deactivate. it could later be added or deleted
                    markAsValid(component);
                    BTree btree = ((LSMBTreeDiskComponent) component).getBTree();
                    BloomFilter bloomFilter = ((LSMBTreeDiskComponent) component).getBloomFilter();
                    btree.deactivate();
                    bloomFilter.deactivate();
                } else {
                    lsmHarness.addBulkLoadedComponent(component);
                }
            }
        }

        // It is expected that the mode was set to delete operation before
        // calling delete
        @Override
        public void delete(ITupleReference tuple) throws IndexException, HyracksDataException {
            ((LSMBTreeRefrencingTupleWriterFactory) frameTupleWriterFactory).setMode(IndexOperation.DELETE);
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
            ((LSMBTreeRefrencingTupleWriterFactory) frameTupleWriterFactory).setMode(IndexOperation.INSERT);
        }

        @Override
        public void abort() {
            try {
                cleanupArtifacts();
            } catch (Exception e) {
                // Do nothing
            }
        }
    }

    @Override
    public String toString() {
        return "LSMTwoPCBTree [" + fileManager.getBaseDir() + "]";
    }

    // The accessor for disk only indexes don't use modification callback and always carry the target index version with them
    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMBTreeAccessor(lsmHarness, createOpContext(searchCallback, version));
    }

    // This method creates the appropriate opContext for the targeted version
    public ExternalBTreeOpContext createOpContext(ISearchOperationCallback searchCallback, int targetVersion) {
        return new ExternalBTreeOpContext(insertLeafFrameFactory, deleteLeafFrameFactory, searchCallback,
                componentFactory.getBloomFilterKeyFields().length, cmpFactories, targetVersion);
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion)
            throws HyracksDataException {
        return new LSMBTreeAccessor(lsmHarness, createOpContext(searchCallback, targetIndexVersion));
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    @Override
    public int getFieldCount() {
        return cmpFactories.length;
    }

    @Override
    public int getFileId() {
        return -1;
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return null;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return insertLeafFrameFactory;
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
    public void commitTransaction() throws TreeIndexException, HyracksDataException, IndexException {
        LSMComponentFileReferences componentFileRefrences = fileManager.getTransactionFileReferenceForCommit();
        LSMBTreeDiskComponent component = null;
        if (componentFileRefrences != null) {
            component = createDiskComponent(componentFactory, componentFileRefrences.getInsertIndexFileReference(),
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
}
