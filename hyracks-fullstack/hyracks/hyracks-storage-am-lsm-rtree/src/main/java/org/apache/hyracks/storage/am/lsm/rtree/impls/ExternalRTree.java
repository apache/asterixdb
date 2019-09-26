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
package org.apache.hyracks.storage.am.lsm.rtree.impls;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
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
import org.apache.hyracks.storage.am.lsm.common.impls.LoadOperation;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
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
 * This is an lsm r-tree that does not have memory component and is modified
 * only by bulk loading and addition of disk components as of this point, it is
 * intended for use with external dataset indexes only.
 */
public class ExternalRTree extends LSMRTree implements ITwoPCIndex {

    // A second disk component list that will be used when a transaction is
    // committed and will be seen by subsequent accessors
    private final List<ILSMDiskComponent> secondDiskComponents;
    // A pointer that points to the current most recent list (either
    // diskComponents = 0, or secondDiskComponents = 1). It starts with -1 to
    // indicate first time activation
    private int version = 0;
    private final int fieldCount;

    public ExternalRTree(IIOManager ioManager, ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, IBufferCache diskBufferCache,
            ILSMIndexFileManager fileNameManager, ILSMDiskComponentFactory componentFactory,
            double bloomFilterFalsePositiveRate, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackFactory ioOpCallbackFactory, ILSMPageWriteCallbackFactory pageWriteCallbackFactory,
            int[] buddyBTreeFields, boolean durable, boolean isPointMBR, ITracer tracer) throws HyracksDataException {
        super(ioManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, btreeInteriorFrameFactory,
                btreeLeafFrameFactory, diskBufferCache, fileNameManager, componentFactory, bloomFilterFalsePositiveRate,
                rtreeCmpFactories, btreeCmpFactories, linearizer, comparatorFields, linearizerArray, mergePolicy,
                opTracker, ioScheduler, ioOpCallbackFactory, pageWriteCallbackFactory, buddyBTreeFields, durable,
                isPointMBR, tracer);
        this.secondDiskComponents = new LinkedList<>();
        this.fieldCount = fieldCount;
    }

    @Override
    public ExternalIndexHarness getHarness() {
        return (ExternalIndexHarness) super.getHarness();
    }

    // The subsume merged components is overridden to account for:
    // 1. the number of readers of components
    // 2. maintaining two versions of the index
    @Override
    public void subsumeMergedComponents(ILSMDiskComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        // determine which list is the new one
        List<ILSMDiskComponent> newerList;
        List<ILSMDiskComponent> olderList;
        if (version == 0) {
            newerList = diskComponents;
            olderList = secondDiskComponents;
        } else {
            newerList = secondDiskComponents;
            olderList = diskComponents;
        }
        // check if merge will affect the older list
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
        } else {
            return secondDiskComponents;
        }
    }

    // This function should only be used when a transaction fail. it doesn't
    // take any parameters since there can only be
    // a single transaction and hence a single transaction component on disk
    public void deleteTransactionComponent() throws HyracksDataException {
        fileManager.deleteTransactionFiles();
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
                ILSMDiskComponent component;
                component =
                        createDiskComponent(componentFactory, lsmComonentFileReference.getInsertIndexFileReference(),
                                lsmComonentFileReference.getDeleteIndexFileReference(),
                                lsmComonentFileReference.getBloomFilterFileReference(), false);
                diskComponents.add(component);
                secondDiskComponents.add(component);
            }
            getHarness().indexFirstTimeActivated();
        } else {
            // This index has been opened before or is brand new with no components
            // components. It should also maintain the version pointer
            for (ILSMComponent c : diskComponents) {
                LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
                component.activate(false);
            }
            for (ILSMComponent c : secondDiskComponents) {
                // Only activate non shared components
                if (!diskComponents.contains(c)) {
                    LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
                    component.activate(false);
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

    // we override this method because this index uses a different opcontext
    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        ExternalRTreeOpContext ctx = (ExternalRTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();
        ctx.getInitialState().setOperationalComponents(operationalComponents);
        cursor.open(ctx.getInitialState(), pred);
    }

    // The only reason for overriding the merge method is the way to determine
    // the need to keep deleted tuples
    // This can be done in a better way by creating a method boolean
    // keepDeletedTuples(mergedComponents);
    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMRTreeSortedCursor) cursor).getOpCtx();
        search(opCtx, cursor, rtreeSearchPred);

        LSMRTreeDiskComponent mergedComponent = (LSMRTreeDiskComponent) createDiskComponent(componentFactory,
                mergeOp.getTarget(), mergeOp.getBTreeTarget(), mergeOp.getBloomFilterTarget(), true);

        // In case we must keep the deleted-keys BTrees, then they must be
        // merged *before* merging the r-trees so that
        // lsmHarness.endSearch() is called once when the r-trees have been
        // merged.
        boolean keepDeleteTuples = false;
        if (version == 0) {
            keepDeleteTuples = mergeOp.getMergingComponents()
                    .get(mergeOp.getMergingComponents().size() - 1) != diskComponents.get(diskComponents.size() - 1);
        } else {
            keepDeleteTuples = mergeOp.getMergingComponents()
                    .get(mergeOp.getMergingComponents().size() - 1) != secondDiskComponents
                            .get(secondDiskComponents.size() - 1);
        }
        IPageWriteCallback pageWriteCallback = pageWriteCallbackFactory.createPageWriteCallback();
        if (keepDeleteTuples) {
            // Keep the deleted tuples since the oldest disk component is not
            // included in the merge operation

            LSMRTreeDeletedKeysBTreeMergeCursor btreeCursor =
                    new LSMRTreeDeletedKeysBTreeMergeCursor(opCtx, mergeOp.getCursorStats());
            search(opCtx, btreeCursor, rtreeSearchPred);

            BTree btree = mergedComponent.getBuddyIndex();
            IIndexBulkLoader btreeBulkLoader = btree.createBulkLoader(1.0f, true, 0L, false, pageWriteCallback);

            long numElements = 0L;
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                numElements += ((LSMRTreeDiskComponent) mergeOp.getMergingComponents().get(i)).getBloomFilter()
                        .getNumElements();
            }

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
            BloomFilterSpecification bloomFilterSpec =
                    BloomCalculations.computeBloomSpec(maxBucketsPerElement, bloomFilterFalsePositiveRate);
            IIndexBulkLoader builder = mergedComponent.getBloomFilter().createBuilder(numElements,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements(), pageWriteCallback);

            try {
                while (btreeCursor.hasNext()) {
                    btreeCursor.next();
                    ITupleReference tuple = btreeCursor.getTuple();
                    btreeBulkLoader.add(tuple);
                    builder.add(tuple);
                }
            } finally {
                btreeCursor.destroy();
                builder.end();
            }
            btreeBulkLoader.end();
        }

        IIndexBulkLoader bulkLoader =
                mergedComponent.getIndex().createBulkLoader(1.0f, false, 0L, false, pageWriteCallback);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                bulkLoader.add(frameTuple);
            }
        } finally {
            cursor.destroy();
        }
        bulkLoader.end();
        return mergedComponent;
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
            if (!diskComponents.contains(c)) {
                c.deactivateAndPurge();
            }
        }
        isActive = false;
    }

    // The clear method is not used anywhere in AsterixDB! we override it anyway
    // to exit components first and clear the two lists
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

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException("tuple modify not supported in LSM-Disk-Only-RTree");
    }

    @Override
    public ILSMIOOperation createFlushOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-RTree");
    }

    // Not supported
    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-RTree");
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
        ExternalRTreeOpContext opCtx = (ExternalRTreeOpContext) ctx;
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

    // For initial load
    @Override
    public IIndexBulkLoader createBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        return new LSMTwoPCRTreeBulkLoader(fillLevel, verifyInput, 0, false, parameters);
    }

    // For transaction bulk load <- could consolidate with the above method ->
    @Override
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            Map<String, Object> parameters) throws HyracksDataException {
        return new LSMTwoPCRTreeBulkLoader(fillLevel, verifyInput, numElementsHint, true, parameters);
    }

    // The bulk loader used for both initial loading and transaction
    // modifications
    public class LSMTwoPCRTreeBulkLoader implements IIndexBulkLoader, ITwoPCIndexBulkLoader {
        private final ILSMDiskComponent component;
        private final boolean isTransaction;
        private final LoadOperation loadOp;
        private final ILSMDiskComponentBulkLoader componentBulkLoader;

        public LSMTwoPCRTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
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
                component = createDiskComponent(componentFactory, componentFileRefs.getInsertIndexFileReference(),
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
            return loadOp.hasFailed();
        }

        @Override
        public Throwable getFailure() {
            return loadOp.getFailure();
        }

        @Override
        public void force() throws HyracksDataException {
            componentBulkLoader.force();
        }
    }

    // The only change the the schedule merge is the method used to create the
    // opCtx. first line <- in schedule merge, we->
    @Override
    public ILSMIOOperation createMergeOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
        ILSMIndexOperationContext rctx = createOpContext(NoOpOperationCallback.INSTANCE, -1);
        rctx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        IIndexCursorStats stats = new IndexCursorStats();
        LSMRTreeSortedCursor cursor = new LSMRTreeSortedCursor(rctx, linearizer, buddyBTreeFields, stats);
        LSMComponentFileReferences relMergeFileRefs =
                getMergeFileReferences((ILSMDiskComponent) mergingComponents.get(mergingComponents.size() - 1),
                        (ILSMDiskComponent) mergingComponents.get(0));
        ILSMIndexAccessor accessor = new LSMRTreeAccessor(getHarness(), rctx, buddyBTreeFields);
        // create the merge operation.
        LSMRTreeMergeOperation mergeOp =
                new LSMRTreeMergeOperation(accessor, cursor, stats, relMergeFileRefs.getInsertIndexFileReference(),
                        relMergeFileRefs.getDeleteIndexFileReference(), relMergeFileRefs.getBloomFilterFileReference(),
                        ioOpCallback, fileManager.getBaseDir().getAbsolutePath());
        ioOpCallback.scheduled(mergeOp);
        return mergeOp;
    }

    @Override
    public ILSMIndexAccessor createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion)
            throws HyracksDataException {
        return new LSMRTreeAccessor(getHarness(), createOpContext(searchCallback, targetIndexVersion),
                buddyBTreeFields);
    }

    // This method creates the appropriate opContext for the targeted version
    public ExternalRTreeOpContext createOpContext(ISearchOperationCallback searchCallback, int targetVersion) {
        return new ExternalRTreeOpContext(this, rtreeCmpFactories, btreeCmpFactories, searchCallback, targetVersion,
                getHarness(), comparatorFields, linearizerArray, rtreeLeafFrameFactory, rtreeInteriorFrameFactory,
                btreeLeafFrameFactory, tracer);
    }

    // The accessor for disk only indexes don't use modification callback and
    // always carry the target index version with them
    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) {
        return new LSMRTreeAccessor(getHarness(), createOpContext(iap.getSearchOperationCallback(), version),
                buddyBTreeFields);
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

    @Override
    public int getFieldCount() {
        return fieldCount;
    }
}
