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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import org.apache.hyracks.storage.am.lsm.common.impls.ExternalIndexHarness;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;

/**
 * This is an lsm b-tree that does not have memory component and is modified
 * only by bulk loading and addition of disk components as of this point, it is
 * intended for use with external dataset indexes only.
 *
 * @author alamouda
 */
public class ExternalBTree extends LSMBTree implements ITwoPCIndex {

    private static final ICursorFactory cursorFactory = opCtx -> new LSMBTreeSearchCursor(opCtx);
    // This component factory has to be different since it uses different tuple
    // writer in it's leaf frames to support inserting both
    // regular and delete tuples
    private final LSMBTreeDiskComponentFactory transactionComponentFactory;
    // A second disk component list that will be used when a transaction is
    // committed and will be seen by subsequent accessors
    private final List<ILSMDiskComponent> secondDiskComponents;
    // A pointer that points to the current most recent list (either
    // diskComponents = 0, or secondDiskComponents = 1). It starts with -1 to
    // indicate first time activation
    private int version = 0;

    private final ITreeIndexFrameFactory interiorFrameFactory;

    //TODO remove BloomFilter from external dataset's secondary LSMBTree index
    public ExternalBTree(IIOManager ioManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory insertLeafFrameFactory, ITreeIndexFrameFactory deleteLeafFrameFactory,
            ILSMIndexFileManager fileManager, TreeIndexFactory<BTree> diskBTreeFactory,
            TreeIndexFactory<BTree> bulkLoadBTreeFactory, BloomFilterFactory bloomFilterFactory,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            TreeIndexFactory<BTree> transactionBTreeFactory, boolean durable) {
        super(ioManager, insertLeafFrameFactory, deleteLeafFrameFactory, fileManager, diskBTreeFactory,
                bulkLoadBTreeFactory, bloomFilterFactory, bloomFilterFalsePositiveRate, cmpFactories, mergePolicy,
                opTracker, ioScheduler, ioOpCallback, false, durable);
        this.transactionComponentFactory =
                new LSMBTreeDiskComponentFactory(transactionBTreeFactory, bloomFilterFactory, null);
        this.secondDiskComponents = new LinkedList<>();
        this.interiorFrameFactory = interiorFrameFactory;
    }

    @Override
    public ExternalIndexHarness getLsmHarness() {
        return (ExternalIndexHarness) super.getLsmHarness();
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

    // This method is used by the merge policy when it needs to check if a merge
    // is needed.
    // It only needs to return the newer list
    @Override
    public List<ILSMDiskComponent> getDiskComponents() {
        if (version == 0) {
            return diskComponents;
        } else if (version == 1) {
            return secondDiskComponents;
        } else {
            return Collections.emptyList();
        }
    }

    // The only reason to override the following method is that it uses a different context object
    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        ExternalBTreeOpContext ctx = (ExternalBTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        ctx.getSearchInitialState().reset(pred, operationalComponents);
        cursor.open(ctx.getSearchInitialState(), pred);
    }

    // This method creates the appropriate opContext for the targeted version
    public ExternalBTreeOpContext createOpContext(ISearchOperationCallback searchCallback, int targetVersion) {
        return new ExternalBTreeOpContext(insertLeafFrameFactory, deleteLeafFrameFactory, searchCallback,
                componentFactory.getBloomFilterKeyFields().length, cmpFactories, targetVersion, getLsmHarness());
    }

    // The only reason to override the following method is that it uses a different context object
    // in addition, determining whether or not to keep deleted tuples is different here
    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        ExternalBTreeOpContext opCtx = createOpContext(NoOpOperationCallback.INSTANCE, -1);
        opCtx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        boolean returnDeletedTuples = false;
        if (version == 0) {
            if (ctx.getComponentHolder().get(ctx.getComponentHolder().size() - 1) != diskComponents
                    .get(diskComponents.size() - 1)) {
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
        FileReference firstFile = firstBTree.getFileReference();
        FileReference lastFile = lastBTree.getFileReference();
        LSMComponentFileReferences relMergeFileRefs =
                fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
        ILSMIndexAccessor accessor = new LSMTreeIndexAccessor(getLsmHarness(), opCtx, cursorFactory);
        ioScheduler.scheduleOperation(new LSMBTreeMergeOperation(accessor, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getBloomFilterFileReference(),
                callback, fileManager.getBaseDir().getAbsolutePath()));
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
    public void addDiskComponent(ILSMDiskComponent c) throws HyracksDataException {
        if (version == 0) {
            diskComponents.add(0, c);
        } else if (version == 1) {
            secondDiskComponents.add(0, c);
        }
    }

    // This function is used when a new component is to be committed.
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
    public synchronized void activate() throws HyracksDataException {
        if (isActive) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }
        if (diskComponents.size() == 0 && secondDiskComponents.size() == 0) {
            //First time activation
            List<LSMComponentFileReferences> validFileReferences;
            validFileReferences = fileManager.cleanupAndGetValidFiles();
            for (LSMComponentFileReferences lsmComonentFileReference : validFileReferences) {
                LSMBTreeDiskComponent component;
                component =
                        createDiskComponent(componentFactory, lsmComonentFileReference.getInsertIndexFileReference(),
                                lsmComonentFileReference.getBloomFilterFileReference(), false);
                diskComponents.add(component);
                secondDiskComponents.add(component);
            }
            getLsmHarness().indexFirstTimeActivated();
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
        isActive = true;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        super.create();
        secondDiskComponents.clear();
    }

    @Override
    public void deactivate(boolean flushOnExit) throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }
        if (flushOnExit) {
            BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(ioOpCallback);
            cb.afterFinalize(LSMOperationType.FLUSH, null);
        }
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

    // The clear method is not used anywhere in AsterixDB! we override it anyway
    // to exit components first and deal with the two lists
    @Override
    public void clear() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        getLsmHarness().indexClear();

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
        version = 0;
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

    // Not supported
    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
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
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-BTree");
    }

    // Only support search and merge operations
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
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        return new LSMTwoPCBTreeBulkLoader(fillLevel, verifyInput, numElementsHint, false);
    }

    // For transaction bulk load <- could consolidate with the above method ->
    @Override
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        return new LSMTwoPCBTreeBulkLoader(fillLevel, verifyInput, numElementsHint, true);
    }

    // The bulk loader used for both initial loading and transaction
    // modifications
    public class LSMTwoPCBTreeBulkLoader implements IIndexBulkLoader, ITwoPCIndexBulkLoader {
        private final ILSMDiskComponent component;
        private final ILSMDiskComponentBulkLoader componentBulkLoader;

        private final boolean isTransaction;

        public LSMTwoPCBTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean isTransaction) throws HyracksDataException {
            this.isTransaction = isTransaction;
            // Create the appropriate target
            if (isTransaction) {
                component = createTransactionTarget();
            } else {
                component = createBulkLoadTarget();
            }

            componentBulkLoader =
                    createComponentBulkLoader(component, fillFactor, verifyInput, numElementsHint, false, true, true);
        }

        // It is expected that the mode was set to insert operation before
        // calling add
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
                    component.markAsValid(durable);
                    BTree btree = ((LSMBTreeDiskComponent) component).getBTree();
                    BloomFilter bloomFilter = ((LSMBTreeDiskComponent) component).getBloomFilter();
                    btree.deactivate();
                    bloomFilter.deactivate();
                } else {
                    getLsmHarness().addBulkLoadedComponent(component);
                }
            }
        }

        // It is expected that the mode was set to delete operation before
        // calling delete
        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            componentBulkLoader.delete(tuple);
        }

        @Override
        public void abort() {
            try {
                componentBulkLoader.abort();
            } catch (Exception e) {
                // Do nothing
            }
        }

        // This method is used to create a target for a bulk modify operation. This
        // component must then be either committed or deleted
        private ILSMDiskComponent createTransactionTarget() throws HyracksDataException {
            LSMComponentFileReferences componentFileRefs;
            try {
                componentFileRefs = fileManager.getNewTransactionFileReference();
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            return createDiskComponent(transactionComponentFactory, componentFileRefs.getInsertIndexFileReference(),
                    componentFileRefs.getBloomFilterFileReference(), true);
        }
    }

    // The accessor for disk only indexes don't use modification callback and always carry the target index version with them
    @Override
    public ILSMIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        ExternalBTreeOpContext opCtx = createOpContext(searchCallback, version);
        return new LSMTreeIndexAccessor(getLsmHarness(), opCtx, cursorFactory);
    }

    @Override
    public ILSMIndexAccessor createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion)
            throws HyracksDataException {
        ExternalBTreeOpContext opCtx = createOpContext(searchCallback, targetIndexVersion);
        return new LSMTreeIndexAccessor(getLsmHarness(), opCtx, cursorFactory);
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
    public IMetadataPageManager getPageManager() {
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
        LSMBTreeDiskComponent component = null;
        if (componentFileRefrences != null) {
            component = createDiskComponent(componentFactory, componentFileRefrences.getInsertIndexFileReference(),
                    componentFileRefrences.getBloomFilterFileReference(), false);
        }
        getLsmHarness().addTransactionComponents(component);
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
}
