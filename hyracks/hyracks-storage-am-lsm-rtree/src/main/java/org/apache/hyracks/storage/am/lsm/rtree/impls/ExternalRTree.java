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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterFactory;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITwoPCIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
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
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

/**
 * This is an lsm r-tree that does not have memory component and is modified
 * only by bulk loading and addition of disk components as of this point, it is
 * intended for use with external dataset indexes only.
 * 
 * @author alamouda
 */
public class ExternalRTree extends LSMRTree implements ITwoPCIndex {

    // A second disk component list that will be used when a transaction is
    // committed and will be seen by subsequent accessors
    private final List<ILSMComponent> secondDiskComponents;
    // A pointer that points to the current most recent list (either
    // diskComponents = 0, or secondDiskComponents = 1). It starts with -1 to
    // indicate first time activation
    private int version = -1;
    private final int fieldCount;

    public ExternalRTree(ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ILSMIndexFileManager fileNameManager,
            TreeIndexFactory<RTree> diskRTreeFactory, TreeIndexFactory<BTree> diskBTreeFactory,
            BloomFilterFactory bloomFilterFactory, double bloomFilterFalsePositiveRate,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMMergePolicy mergePolicy,
            ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback,
            int[] buddyBTreeFields, int version, boolean durable) {
        super(rtreeInteriorFrameFactory, rtreeLeafFrameFactory, btreeInteriorFrameFactory, btreeLeafFrameFactory,
                fileNameManager, diskRTreeFactory, diskBTreeFactory, bloomFilterFactory, bloomFilterFalsePositiveRate,
                diskFileMapProvider, fieldCount, rtreeCmpFactories, btreeCmpFactories, linearizer, comparatorFields,
                linearizerArray, mergePolicy, opTracker, ioScheduler, ioOpCallback, buddyBTreeFields, durable);
        this.secondDiskComponents = new LinkedList<ILSMComponent>();
        this.version = version;
        this.fieldCount = fieldCount;
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
        return createDiskComponent(componentFactory, componentFileRefs.getInsertIndexFileReference(),
                componentFileRefs.getDeleteIndexFileReference(), componentFileRefs.getBloomFilterFileReference(), true);
    }

    // The subsume merged components is overridden to account for:
    // 1. the number of readers of components
    // 2. maintaining two versions of the index
    @Override
    public void subsumeMergedComponents(ILSMComponent newComponent, List<ILSMComponent> mergedComponents)
            throws HyracksDataException {
        // determine which list is the new one
        List<ILSMComponent> newerList;
        List<ILSMComponent> olderList;
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
    public List<ILSMComponent> getImmutableComponents() {
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
                LSMRTreeDiskComponent component;
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
            // This index has been opened before or is brand new with no components
            // components. It should also maintain the version pointer
            for (ILSMComponent c : diskComponents) {
                LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
                RTree rtree = component.getRTree();
                BTree btree = component.getBTree();
                BloomFilter bloomFilter = component.getBloomFilter();
                rtree.activate();
                btree.activate();
                bloomFilter.activate();
            }
            for (ILSMComponent c : secondDiskComponents) {
                // Only activate non shared components
                if (!diskComponents.contains(c)) {
                    LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
                    RTree rtree = component.getRTree();
                    BTree btree = component.getBTree();
                    BloomFilter bloomFilter = component.getBloomFilter();
                    rtree.activate();
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

    // we override this method because this index uses a different opcontext
    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        ExternalRTreeOpContext ctx = (ExternalRTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();

        LSMRTreeCursorInitialState initialState = new LSMRTreeCursorInitialState(rtreeLeafFrameFactory,
                rtreeInteriorFrameFactory, btreeLeafFrameFactory, ctx.getBTreeMultiComparator(), lsmHarness,
                comparatorFields, linearizerArray, ctx.searchCallback, operationalComponents);

        cursor.open(initialState, pred);
    }

    // The only reason for overriding the merge method is the way to determine
    // the need to keep deleted tuples
    // This can be done in a better way by creating a method boolean
    // keepDeletedTuples(mergedComponents);
    @Override
    public ILSMComponent merge(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        LSMRTreeMergeOperation mergeOp = (LSMRTreeMergeOperation) operation;
        ITreeIndexCursor cursor = mergeOp.getCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        ILSMIndexOperationContext opCtx = ((LSMRTreeSortedCursor) cursor).getOpCtx();
        opCtx.getComponentHolder().addAll(mergeOp.getMergingComponents());
        search(opCtx, cursor, rtreeSearchPred);

        LSMRTreeDiskComponent mergedComponent = createDiskComponent(componentFactory, mergeOp.getRTreeMergeTarget(),
                mergeOp.getBTreeMergeTarget(), mergeOp.getBloomFilterMergeTarget(), true);

        // In case we must keep the deleted-keys BTrees, then they must be
        // merged *before* merging the r-trees so that
        // lsmHarness.endSearch() is called once when the r-trees have been
        // merged.
        if (mergeOp.isKeepDeletedTuples()) {
            // Keep the deleted tuples since the oldest disk component is not
            // included in the merge operation

            LSMRTreeDeletedKeysBTreeMergeCursor btreeCursor = new LSMRTreeDeletedKeysBTreeMergeCursor(opCtx);
            search(opCtx, btreeCursor, rtreeSearchPred);

            BTree btree = mergedComponent.getBTree();
            IIndexBulkLoader btreeBulkLoader = btree.createBulkLoader(1.0f, true, 0L, false);

            long numElements = 0L;
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                numElements += ((LSMRTreeDiskComponent) mergeOp.getMergingComponents().get(i)).getBloomFilter()
                        .getNumElements();
            }

            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            IIndexBulkLoader builder = mergedComponent.getBloomFilter().createBuilder(numElements,
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

        IIndexBulkLoader bulkLoader = mergedComponent.getRTree().createBulkLoader(1.0f, false, 0L, false);
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
    public void deactivate(boolean flushOnExit) throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }

        if (flushOnExit) {
            BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(ioOpCallback);
            cb.afterFinalize(LSMOperationType.FLUSH, null);
        }

        for (ILSMComponent c : diskComponents) {
            LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
            RTree rtree = component.getRTree();
            BTree btree = component.getBTree();
            BloomFilter bloomFilter = component.getBloomFilter();
            rtree.deactivate();
            btree.deactivate();
            bloomFilter.deactivate();
        }
        for (ILSMComponent c : secondDiskComponents) {
            // Only deactivate non shared components
            if (!diskComponents.contains(c)) {
                LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
                RTree rtree = component.getRTree();
                BTree btree = component.getBTree();
                BloomFilter bloomFilter = component.getBloomFilter();
                rtree.deactivate();
                btree.deactivate();
                bloomFilter.deactivate();
            }
        }
        isActivated = false;
    }

    // The clear method is not used anywhere in AsterixDB! we override it anyway
    // to exit components first and clear the two lists
    @Override
    public void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }
        ((ExternalIndexHarness) lsmHarness).indexClear();

        for (ILSMComponent c : diskComponents) {
            LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
            component.getRTree().deactivate();
            component.getBloomFilter().deactivate();
            component.getBTree().deactivate();
            component.getRTree().destroy();
            component.getBloomFilter().destroy();
            component.getBTree().destroy();
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }

        for (ILSMComponent c : secondDiskComponents) {
            LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
            component.getRTree().deactivate();
            component.getBloomFilter().deactivate();
            component.getBTree().deactivate();
            component.getRTree().destroy();
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
            LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
            component.getRTree().destroy();
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
            // Remove from second list to avoid destroying twice
            secondDiskComponents.remove(c);
        }
        for (ILSMComponent c : secondDiskComponents) {
            LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) c;
            component.getRTree().destroy();
            component.getBTree().destroy();
            component.getBloomFilter().destroy();
        }
        diskComponents.clear();
        secondDiskComponents.clear();
        fileManager.deleteDirs();
        version = -1;
    }

    // Not supported
    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("tuple modify not supported in LSM-Disk-Only-RTree");
    }

    // Not supported
    @Override
    public void scheduleFlush(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-RTree");
    }

    // Not supported
    @Override
    public ILSMComponent flush(ILSMIOOperation operation) throws HyracksDataException, IndexException {
        throw new UnsupportedOperationException("flush not supported in LSM-Disk-Only-RTree");
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
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMTwoPCRTreeBulkLoader(fillLevel, verifyInput, 0, checkIfEmptyIndex, false);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    // For transaction bulk load <- could consolidate with the above method ->
    @Override
    public IIndexBulkLoader createTransactionBulkLoader(float fillLevel, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new LSMTwoPCRTreeBulkLoader(fillLevel, verifyInput, numElementsHint, checkIfEmptyIndex, true);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    // The bulk loader used for both initial loading and transaction
    // modifications
    public class LSMTwoPCRTreeBulkLoader implements IIndexBulkLoader, ITwoPCIndexBulkLoader {
        private final ILSMComponent component;
        private final IIndexBulkLoader rtreeBulkLoader;
        private final BTreeBulkLoader btreeBulkLoader;
        private final IIndexBulkLoader builder;
        private boolean cleanedUpArtifacts = false;
        private boolean isEmptyComponent = true;
        private boolean endedBloomFilterLoad = false;
        private final boolean isTransaction;

        public LSMTwoPCRTreeBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
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
            rtreeBulkLoader = ((LSMRTreeDiskComponent) component).getRTree().createBulkLoader(fillFactor, verifyInput,
                    numElementsHint, false);
            btreeBulkLoader = (BTreeBulkLoader) ((LSMRTreeDiskComponent) component).getBTree().createBulkLoader(
                    fillFactor, verifyInput, numElementsHint, false);
            int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElementsHint);
            BloomFilterSpecification bloomFilterSpec = BloomCalculations.computeBloomSpec(maxBucketsPerElement,
                    bloomFilterFalsePositiveRate);
            builder = ((LSMRTreeDiskComponent) component).getBloomFilter().createBuilder(numElementsHint,
                    bloomFilterSpec.getNumHashes(), bloomFilterSpec.getNumBucketsPerElements());
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                rtreeBulkLoader.add(tuple);
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
                    ((LSMRTreeDiskComponent) component).getRTree().deactivate();
                } catch (Exception e) {

                }
                ((LSMRTreeDiskComponent) component).getRTree().destroy();
                try {
                    ((LSMRTreeDiskComponent) component).getBTree().deactivate();
                } catch (Exception e) {

                }
                ((LSMRTreeDiskComponent) component).getBTree().destroy();
                try {
                    ((LSMRTreeDiskComponent) component).getBloomFilter().deactivate();
                } catch (Exception e) {

                }
                ((LSMRTreeDiskComponent) component).getBloomFilter().destroy();
            }
        }

        @Override
        public void end() throws HyracksDataException, IndexException {
            if (!cleanedUpArtifacts) {
                if (!endedBloomFilterLoad) {
                    builder.end();
                    endedBloomFilterLoad = true;
                }
                rtreeBulkLoader.end();
                btreeBulkLoader.end();
                if (isEmptyComponent) {
                    cleanupArtifacts();
                } else if (isTransaction) {
                    // Since this is a transaction component, validate and
                    // deactivate. it could later be added or deleted
                    markAsValid(component);
                    RTree rtree = ((LSMRTreeDiskComponent) component).getRTree();
                    BTree btree = ((LSMRTreeDiskComponent) component).getBTree();
                    BloomFilter bloomFilter = ((LSMRTreeDiskComponent) component).getBloomFilter();
                    rtree.deactivate();
                    btree.deactivate();
                    bloomFilter.deactivate();
                } else {
                    lsmHarness.addBulkLoadedComponent(component);
                }
            }
        }

        @Override
        public void delete(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                btreeBulkLoader.add(tuple);
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

    @Override
    public String toString() {
        return "LSMTwoPCRTree [" + fileManager.getBaseDir() + "]";
    }

    // The only change the the schedule merge is the method used to create the
    // opCtx. first line <- in schedule merge, we->
    @Override
    public void scheduleMerge(ILSMIndexOperationContext ctx, ILSMIOOperationCallback callback)
            throws HyracksDataException, IndexException {
        ILSMIndexOperationContext rctx = createOpContext(NoOpOperationCallback.INSTANCE, -1);
        rctx.setOperation(IndexOperation.MERGE);
        List<ILSMComponent> mergingComponents = ctx.getComponentHolder();
        ITreeIndexCursor cursor = new LSMRTreeSortedCursor(rctx, linearizer, buddyBTreeFields);
        LSMComponentFileReferences relMergeFileRefs = getMergeTargetFileName(mergingComponents);
        ILSMIndexAccessorInternal accessor = new LSMRTreeAccessor(lsmHarness, rctx);
        // create the merge operation.
        LSMRTreeMergeOperation mergeOp = new LSMRTreeMergeOperation(accessor, mergingComponents, cursor,
                relMergeFileRefs.getInsertIndexFileReference(), relMergeFileRefs.getDeleteIndexFileReference(),
                relMergeFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir());
        // set the keepDeletedTuples flag
        boolean keepDeleteTuples = false;
        if (version == 0) {
            keepDeleteTuples = mergeOp.getMergingComponents().get(mergeOp.getMergingComponents().size() - 1) != diskComponents
                    .get(diskComponents.size() - 1);
        } else {
            keepDeleteTuples = mergeOp.getMergingComponents().get(mergeOp.getMergingComponents().size() - 1) != secondDiskComponents
                    .get(secondDiskComponents.size() - 1);
        }
        mergeOp.setKeepDeletedTuples(keepDeleteTuples);

        ioScheduler.scheduleOperation(mergeOp);
    }

    @Override
    public ILSMIndexAccessorInternal createAccessor(ISearchOperationCallback searchCallback, int targetIndexVersion)
            throws HyracksDataException {
        return new LSMRTreeAccessor(lsmHarness, createOpContext(searchCallback, targetIndexVersion));
    }

    // This method creates the appropriate opContext for the targeted version
    public ExternalRTreeOpContext createOpContext(ISearchOperationCallback searchCallback, int targetVersion) {
        return new ExternalRTreeOpContext(rtreeCmpFactories, btreeCmpFactories, searchCallback, targetVersion);
    }

    // The accessor for disk only indexes don't use modification callback and
    // always carry the target index version with them
    @Override
    public ILSMIndexAccessorInternal createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new LSMRTreeAccessor(lsmHarness, createOpContext(searchCallback, version));
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
        LSMRTreeDiskComponent component = null;
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
    public int getFieldCount() {
        return fieldCount;
    }
}
