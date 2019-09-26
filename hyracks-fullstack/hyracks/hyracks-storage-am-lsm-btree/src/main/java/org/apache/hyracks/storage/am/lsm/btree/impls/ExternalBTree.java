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
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.ExternalIndexHarness;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor.ICursorFactory;
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
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.util.trace.ITracer;

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
    private final ILSMDiskComponentFactory transactionComponentFactory;
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
            IBufferCache bufferCache, ILSMIndexFileManager fileManager, ILSMDiskComponentFactory componentFactory,
            ILSMDiskComponentFactory bulkLoadComponentFactory, ILSMDiskComponentFactory transactionComponentFactory,
            double bloomFilterFalsePositiveRate, IBinaryComparatorFactory[] cmpFactories, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            boolean durable, ITracer tracer) throws HyracksDataException {
        super(ioManager, insertLeafFrameFactory, deleteLeafFrameFactory, bufferCache, fileManager, componentFactory,
                bulkLoadComponentFactory, bloomFilterFalsePositiveRate, cmpFactories, mergePolicy, opTracker,
                ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, false, durable, tracer);
        this.transactionComponentFactory = transactionComponentFactory;
        this.secondDiskComponents = new LinkedList<>();
        this.interiorFrameFactory = interiorFrameFactory;
    }

    @Override
    public ExternalIndexHarness getHarness() {
        return (ExternalIndexHarness) super.getHarness();
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
        return new ExternalBTreeOpContext(this, insertLeafFrameFactory, deleteLeafFrameFactory, searchCallback,
                ((LSMBTreeWithBloomFilterDiskComponentFactory) componentFactory).getBloomFilterKeyFields().length,
                cmpFactories, targetVersion, getHarness(), tracer);
    }

    // The only reason to override the following method is that it uses a different context object
    // in addition, determining whether or not to keep deleted tuples is different here
    @Override
    public LSMBTreeMergeOperation createMergeOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
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
        IIndexCursorStats stats = new IndexCursorStats();
        LSMBTreeRangeSearchCursor cursor = new LSMBTreeRangeSearchCursor(opCtx, returnDeletedTuples, stats);
        BTree lastBTree = ((LSMBTreeDiskComponent) mergingComponents.get(0)).getIndex();
        BTree firstBTree = ((LSMBTreeDiskComponent) mergingComponents.get(mergingComponents.size() - 1)).getIndex();
        FileReference firstFile = firstBTree.getFileReference();
        FileReference lastFile = lastBTree.getFileReference();
        LSMComponentFileReferences relMergeFileRefs =
                fileManager.getRelMergeFileReference(firstFile.getFile().getName(), lastFile.getFile().getName());
        ILSMIndexAccessor accessor = new LSMTreeIndexAccessor(getHarness(), opCtx, cursorFactory);
        LSMBTreeMergeOperation mergeOp = new LSMBTreeMergeOperation(accessor, cursor, stats,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getBloomFilterFileReference(),
                ioOpCallback, fileManager.getBaseDir().getAbsolutePath());
        ioOpCallback.scheduled(mergeOp);
        return mergeOp;
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
            for (LSMComponentFileReferences lsmComponentFileReferences : validFileReferences) {
                ILSMDiskComponent component =
                        createDiskComponent(componentFactory, lsmComponentFileReferences.getInsertIndexFileReference(),
                                null, lsmComponentFileReferences.getBloomFilterFileReference(), false);
                diskComponents.add(component);
                secondDiskComponents.add(component);
            }
            getHarness().indexFirstTimeActivated();
        } else {
            // This index has been opened before
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
    public synchronized void create() throws HyracksDataException {
        super.create();
        secondDiskComponents.clear();
    }

    @Override
    public void deactivate(boolean flushOnExit) throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }
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

    // The clear method is not used anywhere in AsterixDB! we override it anyway
    // to exit components first and deal with the two lists
    @Override
    public void clear() throws HyracksDataException {
        if (!isActive) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        getHarness().indexClear();

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

    // Not supported
    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException("tuple modify not supported in LSM-Disk-Only-BTree");
    }

    // Not supported
    @Override
    public ILSMIOOperation createFlushOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
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
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        return new LSMTwoPCBTreeBulkLoader(fillLevel, verifyInput, numElementsHint, false, parameters);
    }

    // For transaction bulk load <- could consolidate with the above method ->
    @Override
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        return new LSMTwoPCBTreeBulkLoader(fillLevel, verifyInput, numElementsHint, true, parameters);
    }

    // The bulk loader used for both initial loading and transaction
    // modifications
    public class LSMTwoPCBTreeBulkLoader implements IIndexBulkLoader, ITwoPCIndexBulkLoader {
        private final ILSMDiskComponent component;
        private final ILSMDiskComponentBulkLoader componentBulkLoader;
        private final LoadOperation loadOp;

        private final boolean isTransaction;

        public LSMTwoPCBTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
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
                component = createDiskComponent(transactionComponentFactory,
                        componentFileRefs.getInsertIndexFileReference(), null,
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
            IPageWriteCallback pageWriteCallback = pageWriteCallbackFactory.createPageWriteCallback();
            componentBulkLoader = component.createBulkLoader(loadOp, fillFactor, verifyInput, numElementsHint, false,
                    true, true, pageWriteCallback);
        }

        // It is expected that the mode was set to insert operation before
        // calling add
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

        // It is expected that the mode was set to delete operation before
        // calling delete
        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            componentBulkLoader.delete(tuple);
        }

        @Override
        public void abort() throws HyracksDataException {
            try {
                componentBulkLoader.abort();
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

    // The accessor for disk only indexes don't use modification callback and always carry the target index version with them
    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) {
        ExternalBTreeOpContext opCtx = createOpContext(iap.getSearchOperationCallback(), version);
        return new LSMTreeIndexAccessor(getHarness(), opCtx, cursorFactory);
    }

    @Override
    public ILSMIndexAccessor createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion)
            throws HyracksDataException {
        ExternalBTreeOpContext opCtx = createOpContext(searchCallback, targetIndexVersion);
        return new LSMTreeIndexAccessor(getHarness(), opCtx, cursorFactory);
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
        LSMComponentFileReferences componentFileReferences = fileManager.getTransactionFileReferenceForCommit();
        ILSMDiskComponent component = null;
        if (componentFileReferences != null) {
            component = createDiskComponent(componentFactory, componentFileReferences.getInsertIndexFileReference(),
                    null, componentFileReferences.getBloomFilterFileReference(), false);
        }
        getHarness().addTransactionComponents(component);
    }

    @Override
    public void abortTransaction() throws HyracksDataException {
        fileManager.deleteTransactionFiles();
    }

    @Override
    public void recoverTransaction() throws HyracksDataException {
        fileManager.recoverTransaction();
    }
}
