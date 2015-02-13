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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.io.File;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.impls.AbstractSearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BlockingIOOperationCallbackWrapper;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractLSMRTree extends AbstractLSMIndex implements ITreeIndex {

    protected final ILinearizeComparatorFactory linearizer;
    protected final int[] comparatorFields;
    protected final IBinaryComparatorFactory[] linearizerArray;

    // On-disk components.
    // For creating RTree's used in flush and merge.
    protected final ILSMComponentFactory componentFactory;

    protected IBinaryComparatorFactory[] btreeCmpFactories;
    protected IBinaryComparatorFactory[] rtreeCmpFactories;

    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory rtreeLeafFrameFactory;
    protected final ITreeIndexFrameFactory btreeLeafFrameFactory;
    protected final int[] rtreeFields;

    public AbstractLSMRTree(List<IVirtualBufferCache> virtualBufferCaches,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMIndexFileManager fileManager, ILSMComponentFactory componentFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallback ioOpCallback, ILSMComponentFilterFactory filterFactory,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            int[] rtreeFields, int[] filterFields) {
        super(virtualBufferCaches, componentFactory.getBufferCache(), fileManager, diskFileMapProvider,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallback, filterFrameFactory,
                filterManager, filterFields);
        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            RTree memRTree = new RTree(virtualBufferCache,
                    ((IVirtualBufferCache) virtualBufferCache).getFileMapProvider(), new VirtualFreePageManager(
                            virtualBufferCache.getNumPages()), rtreeInteriorFrameFactory, rtreeLeafFrameFactory,
                    rtreeCmpFactories, fieldCount, new FileReference(new File(fileManager.getBaseDir() + "_virtual_r_"
                            + i)));
            BTree memBTree = new BTree(virtualBufferCache,
                    ((IVirtualBufferCache) virtualBufferCache).getFileMapProvider(), new VirtualFreePageManager(
                            virtualBufferCache.getNumPages()), btreeInteriorFrameFactory, btreeLeafFrameFactory,
                    btreeCmpFactories, btreeCmpFactories.length, new FileReference(new File(fileManager.getBaseDir()
                            + "_virtual_b_" + i)));
            LSMRTreeMemoryComponent mutableComponent = new LSMRTreeMemoryComponent(memRTree, memBTree,
                    virtualBufferCache, i == 0 ? true : false, filterFactory == null ? null
                            : filterFactory.createLSMComponentFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }

        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.componentFactory = componentFactory;
        this.btreeCmpFactories = btreeCmpFactories;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.linearizer = linearizer;
        this.comparatorFields = comparatorFields;
        this.linearizerArray = linearizerArray;
        this.rtreeFields = rtreeFields;
    }

    /*
     * For External indexes with no memory components
     */
    public AbstractLSMRTree(ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ILSMIndexFileManager fileManager,
            ILSMComponentFactory componentFactory, IFileMapProvider diskFileMapProvider, int fieldCount,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            ILinearizeComparatorFactory linearizer, int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray,
            double bloomFilterFalsePositiveRate, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallback ioOpCallback) {
        super(componentFactory.getBufferCache(), fileManager, diskFileMapProvider, bloomFilterFalsePositiveRate,
                mergePolicy, opTracker, ioScheduler, ioOpCallback);
        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.componentFactory = componentFactory;
        this.btreeCmpFactories = btreeCmpFactories;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.linearizer = linearizer;
        this.comparatorFields = comparatorFields;
        this.linearizerArray = linearizerArray;
        this.rtreeFields = null;
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
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }

        for (ILSMComponent c : memoryComponents) {
            LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) c;
            ((IVirtualBufferCache) mutableComponent.getRTree().getBufferCache()).open();
            mutableComponent.getRTree().create();
            mutableComponent.getBTree().create();
            mutableComponent.getRTree().activate();
            mutableComponent.getBTree().activate();
        }
    }

    @Override
    public synchronized void deactivate(boolean flushOnExit) throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }

        if (flushOnExit) {
            BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(ioOpCallback);
            ILSMIndexAccessor accessor = createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            accessor.scheduleFlush(cb);
            try {
                cb.waitForIO();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }

        for (ILSMComponent c : memoryComponents) {
            LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) c;
            mutableComponent.getRTree().deactivate();
            mutableComponent.getBTree().deactivate();
            mutableComponent.getRTree().destroy();
            mutableComponent.getBTree().destroy();
            ((IVirtualBufferCache) mutableComponent.getRTree().getBufferCache()).close();
        }
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to destroy the index since it is activated.");
        }
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to clear the index since it is not activated.");
        }

        for (ILSMComponent c : memoryComponents) {
            LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) c;
            mutableComponent.getRTree().clear();
            mutableComponent.getBTree().clear();
            mutableComponent.reset();
        }
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) throws HyracksDataException {
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        List<ILSMComponent> immutableComponents = diskComponents;
        int cmc = currentMutableComponentId.get();
        ctx.setCurrentMutableComponentId(cmc);
        int numMutableComponents = memoryComponents.size();
        operationalComponents.clear();
        switch (ctx.getOperation()) {
            case INSERT:
            case DELETE:
            case FLUSH:
                operationalComponents.add(memoryComponents.get(cmc));
                break;
            case SEARCH:
                for (int i = 0; i < numMutableComponents - 1; i++) {
                    ILSMComponent c = memoryComponents.get((cmc + i + 1) % numMutableComponents);
                    LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) c;
                    if (mutableComponent.isReadable()) {
                        // Make sure newest components are added first
                        operationalComponents.add(0, mutableComponent);
                    }
                }
                // The current mutable component is always added
                operationalComponents.add(0, memoryComponents.get(cmc));

                if (filterManager != null) {
                    for (ILSMComponent c : immutableComponents) {
                        if (c.getLSMComponentFilter().satisfy(
                                ((AbstractSearchPredicate) ctx.getSearchPredicate()).getMinFilterTuple(),
                                ((AbstractSearchPredicate) ctx.getSearchPredicate()).getMaxFilterTuple(),
                                ((LSMRTreeOpContext) ctx).filterCmp)) {
                            operationalComponents.add(c);
                        }
                    }
                } else {
                    operationalComponents.addAll(immutableComponents);
                }

                break;
            case MERGE:
                operationalComponents.addAll(ctx.getComponentsToBeMerged());
                break;
            case FULL_MERGE:
                operationalComponents.addAll(immutableComponents);
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();

        LSMRTreeCursorInitialState initialState = new LSMRTreeCursorInitialState(rtreeLeafFrameFactory,
                rtreeInteriorFrameFactory, btreeLeafFrameFactory, ctx.getBTreeMultiComparator(), lsmHarness,
                comparatorFields, linearizerArray, ctx.searchCallback, operationalComponents);

        cursor.open(initialState, pred);
    }

    protected LSMComponentFileReferences getMergeTargetFileName(List<ILSMComponent> mergingDiskComponents)
            throws HyracksDataException {
        RTree firstTree = ((LSMRTreeDiskComponent) mergingDiskComponents.get(0)).getRTree();
        RTree lastTree = ((LSMRTreeDiskComponent) mergingDiskComponents.get(mergingDiskComponents.size() - 1))
                .getRTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastTree.getFileId());
        LSMComponentFileReferences fileRefs = fileManager.getRelMergeFileReference(firstFile.getFile().getName(),
                lastFile.getFile().getName());
        return fileRefs;
    }

    protected LSMRTreeDiskComponent createDiskComponent(ILSMComponentFactory factory, FileReference insertFileRef,
            FileReference deleteFileRef, FileReference bloomFilterFileRef, boolean createComponent)
            throws HyracksDataException, IndexException {
        // Create new tree instance.
        LSMRTreeDiskComponent component = (LSMRTreeDiskComponent) factory
                .createLSMComponentInstance(new LSMComponentFileReferences(insertFileRef, deleteFileRef,
                        bloomFilterFileRef));
        if (createComponent) {
            component.getRTree().create();
            if (component.getBTree() != null) {
                component.getBTree().create();
                component.getBloomFilter().create();
            }
        }
        // Tree will be closed during cleanup of merge().
        component.getRTree().activate();
        if (component.getBTree() != null) {
            component.getBTree().activate();
            component.getBloomFilter().activate();
        }
        if (component.getLSMComponentFilter() != null) {
            filterManager.readFilterInfo(component.getLSMComponentFilter(), component.getRTree());
        }
        return component;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getRTree().getLeafFrameFactory();
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getRTree().getInteriorFrameFactory();
    }

    @Override
    public IFreePageManager getFreePageManager() {
        LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getRTree().getFreePageManager();
    }

    @Override
    public int getFieldCount() {
        LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getRTree().getFieldCount();
    }

    @Override
    public int getRootPageId() {
        LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getRTree().getRootPageId();
    }

    @Override
    public int getFileId() {
        LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) memoryComponents
                .get(currentMutableComponentId.get());
        return mutableComponent.getRTree().getFileId();
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        if (ctx.getOperation() == IndexOperation.PHYSICALDELETE) {
            throw new UnsupportedOperationException("Physical delete not supported in the LSM-RTree");
        }

        ITupleReference indexTuple;
        if (ctx.indexTuple != null) {
            ctx.indexTuple.reset(tuple);
            indexTuple = ctx.indexTuple;
        } else {
            indexTuple = tuple;
        }

        ctx.modificationCallback.before(indexTuple);
        ctx.modificationCallback.found(null, indexTuple);
        if (ctx.getOperation() == IndexOperation.INSERT) {
            ctx.currentMutableRTreeAccessor.insert(indexTuple);
        } else {
            // First remove all entries in the in-memory rtree (if any).
            ctx.currentMutableRTreeAccessor.delete(indexTuple);
            // Insert key into the deleted-keys BTree.
            try {
                ctx.currentMutableBTreeAccessor.insert(indexTuple);
            } catch (TreeIndexDuplicateKeyException e) {
                // Do nothing, because one delete tuple is enough to indicate
                // that all the corresponding insert tuples are deleted
            }
        }
        if (ctx.filterTuple != null) {
            ctx.filterTuple.reset(tuple);
            memoryComponents.get(currentMutableComponentId.get()).getLSMComponentFilter()
                    .update(ctx.filterTuple, ctx.filterCmp);
        }
    }

    protected LSMRTreeOpContext createOpContext(IModificationOperationCallback modCallback) {
        return new LSMRTreeOpContext(memoryComponents, (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(), btreeLeafFrameFactory,
                btreeInteriorFrameFactory, rtreeCmpFactories, btreeCmpFactories, modCallback,
                NoOpOperationCallback.INSTANCE, rtreeFields, filterFields);
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return rtreeCmpFactories;
    }

    @Override
    public void validate() throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for LSM R-Trees.");
    }

    @Override
    public long getMemoryAllocationSize() {
        long size = 0;
        for (ILSMComponent c : memoryComponents) {
            LSMRTreeMemoryComponent mutableComponent = (LSMRTreeMemoryComponent) c;
            IBufferCache virtualBufferCache = mutableComponent.getRTree().getBufferCache();
            size += virtualBufferCache.getNumPages() * virtualBufferCache.getPageSize();
        }
        return size;
    }

    @Override
    public boolean isPrimaryIndex() {
        return false;
    }

    @Override
    public String toString() {
        return "LSMRTree [" + fileManager.getBaseDir() + "]";
    }
}
