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
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOperationContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IVirtualFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
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
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree.RTreeAccessor;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractLSMRTree extends AbstractLSMIndex implements ITreeIndex {

    protected final ILinearizeComparatorFactory linearizer;
    protected final int[] comparatorFields;
    protected final IBinaryComparatorFactory[] linearizerArray;

    // In-memory components.
    protected final LSMRTreeMutableComponent mutableComponent;
    protected final IVirtualBufferCache virtualBufferCache;
    protected final IVirtualFreePageManager virtualFreePageManager;

    protected TreeTupleSorter rTreeTupleSorter;

    // On-disk components.
    // For creating RTree's used in flush and merge.
    protected final ILSMComponentFactory componentFactory;

    private IBinaryComparatorFactory[] btreeCmpFactories;
    private IBinaryComparatorFactory[] rtreeCmpFactories;

    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory rtreeLeafFrameFactory;
    protected final ITreeIndexFrameFactory btreeLeafFrameFactory;

    public AbstractLSMRTree(IVirtualBufferCache virtualBufferCache, ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, ILSMIndexFileManager fileManager,
            TreeIndexFactory<RTree> diskRTreeFactory, ILSMComponentFactory componentFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, double bloomFilterFalsePositiveRate,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOOperationScheduler ioScheduler,
            ILSMIOOperationCallbackProvider ioOpCallbackProvider) {
        super(virtualBufferCache, diskRTreeFactory.getBufferCache(), fileManager, diskFileMapProvider,
                bloomFilterFalsePositiveRate, mergePolicy, opTracker, ioScheduler, ioOpCallbackProvider);
        virtualFreePageManager = new VirtualFreePageManager(virtualBufferCache.getNumPages());
        RTree memRTree = new RTree(virtualBufferCache, ((IVirtualBufferCache) virtualBufferCache).getFileMapProvider(),
                virtualFreePageManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, rtreeCmpFactories,
                fieldCount, new FileReference(new File(fileManager.getBaseDir() + "_virtual_r")));
        BTree memBTree = new BTree(virtualBufferCache, ((IVirtualBufferCache) virtualBufferCache).getFileMapProvider(),
                new VirtualFreePageManager(virtualBufferCache.getNumPages()), btreeInteriorFrameFactory,
                btreeLeafFrameFactory, btreeCmpFactories, fieldCount, new FileReference(new File(
                        fileManager.getBaseDir() + "_virtual_b")));
        mutableComponent = new LSMRTreeMutableComponent(memRTree, memBTree, virtualBufferCache);
        this.virtualBufferCache = virtualBufferCache;
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
        rTreeTupleSorter = null;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to create the index since it is activated.");
        }

        fileManager.deleteDirs();
        fileManager.createDirs();
        componentsRef.get().clear();
    }

    @Override
    public synchronized void activate() throws HyracksDataException {
        if (isActivated) {
            throw new HyracksDataException("Failed to activate the index since it is already activated.");
        }

        ((IVirtualBufferCache) mutableComponent.getRTree().getBufferCache()).open();
        mutableComponent.getRTree().create();
        mutableComponent.getBTree().create();
        mutableComponent.getRTree().activate();
        mutableComponent.getBTree().activate();
    }

    @Override
    public synchronized void deactivate(boolean flushOnExit) throws HyracksDataException {
        if (!isActivated) {
            throw new HyracksDataException("Failed to deactivate the index since it is already deactivated.");
        }

        if (flushOnExit) {
            BlockingIOOperationCallbackWrapper cb = new BlockingIOOperationCallbackWrapper(
                    ioOpCallbackProvider.getIOOperationCallback(this));
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFlush(cb);
            try {
                cb.waitForIO();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }

        mutableComponent.getRTree().deactivate();
        mutableComponent.getBTree().deactivate();
        mutableComponent.getRTree().destroy();
        mutableComponent.getBTree().destroy();
        ((IVirtualBufferCache) mutableComponent.getRTree().getBufferCache()).close();
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

        mutableComponent.getRTree().clear();
        mutableComponent.getBTree().clear();
    }

    @Override
    public void getOperationalComponents(ILSMIndexOperationContext ctx) {
        List<ILSMComponent> operationalComponents = ctx.getComponentHolder();
        operationalComponents.clear();
        List<ILSMComponent> immutableComponents = componentsRef.get();
        switch (ctx.getOperation()) {
            case INSERT:
            case DELETE:
            case FLUSH:
                operationalComponents.add(mutableComponent);
                break;
            case SEARCH:
                operationalComponents.add(mutableComponent);
                operationalComponents.addAll(immutableComponents);
                break;
            case MERGE:
                operationalComponents.addAll(immutableComponents);
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
    }

    protected LSMComponentFileReferences getMergeTargetFileName(List<ILSMComponent> mergingDiskComponents)
            throws HyracksDataException {
        RTree firstTree = ((LSMRTreeImmutableComponent) mergingDiskComponents.get(0)).getRTree();
        RTree lastTree = ((LSMRTreeImmutableComponent) mergingDiskComponents.get(mergingDiskComponents.size() - 1))
                .getRTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastTree.getFileId());
        LSMComponentFileReferences fileRefs = fileManager.getRelMergeFileReference(firstFile.getFile().getName(),
                lastFile.getFile().getName());
        return fileRefs;
    }

    protected LSMRTreeImmutableComponent createDiskComponent(ILSMComponentFactory factory, FileReference insertFileRef,
            FileReference deleteFileRef, FileReference bloomFilterFileRef, boolean createComponent)
            throws HyracksDataException, IndexException {
        // Create new tree instance.
        LSMRTreeImmutableComponent component = (LSMRTreeImmutableComponent) factory
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
        return component;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return mutableComponent.getRTree().getLeafFrameFactory();
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return mutableComponent.getRTree().getInteriorFrameFactory();
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return mutableComponent.getRTree().getFreePageManager();
    }

    @Override
    public int getFieldCount() {
        return mutableComponent.getRTree().getFieldCount();
    }

    @Override
    public int getRootPageId() {
        return mutableComponent.getRTree().getRootPageId();
    }

    @Override
    public int getFileId() {
        return mutableComponent.getRTree().getFileId();
    }

    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException, IndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        if (ctx.getOperation() == IndexOperation.PHYSICALDELETE) {
            throw new UnsupportedOperationException("Physical delete not supported in the LSM-RTree");
        }

        ctx.modificationCallback.before(tuple);
        ctx.modificationCallback.found(null, tuple);
        if (ctx.getOperation() == IndexOperation.INSERT) {
            // Before each insert, we must check whether there exist a killer
            // tuple in the memBTree. If we find a killer tuple, we must truly
            // delete the existing tuple from the BTree, and then insert it to
            // memRTree. Otherwise, the old killer tuple will kill the newly
            // added RTree tuple.
            RangePredicate btreeRangePredicate = new RangePredicate(tuple, tuple, true, true,
                    ctx.getBTreeMultiComparator(), ctx.getBTreeMultiComparator());
            ITreeIndexCursor cursor = ctx.memBTreeAccessor.createSearchCursor();
            ctx.memBTreeAccessor.search(cursor, btreeRangePredicate);
            boolean foundTupleInMemoryBTree = false;
            try {
                if (cursor.hasNext()) {
                    foundTupleInMemoryBTree = true;
                }
            } finally {
                cursor.close();
            }
            if (foundTupleInMemoryBTree) {
                try {
                    ctx.memBTreeAccessor.delete(tuple);
                } catch (BTreeNonExistentKeyException e) {
                    // Tuple has been deleted in the meantime. Do nothing.
                    // This normally shouldn't happen if we are dealing with
                    // good citizens since LSMRTree is used as a secondary
                    // index and a tuple shouldn't be deleted twice without
                    // insert between them.
                }
            } else {
                ctx.memRTreeAccessor.insert(tuple);
            }

        } else {
            try {
                ctx.memBTreeAccessor.insert(tuple);
            } catch (BTreeDuplicateKeyException e) {
                // Do nothing, because one delete tuple is enough to indicate
                // that all the corresponding insert tuples are deleted
            }
        }
        mutableComponent.setIsModified();
    }

    protected LSMRTreeOpContext createOpContext(IModificationOperationCallback modCallback) {
        RTreeAccessor rtreeAccessor = (RTree.RTreeAccessor) mutableComponent.getRTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        BTreeAccessor btreeAccessor = (BTree.BTreeAccessor) mutableComponent.getBTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);

        return new LSMRTreeOpContext(rtreeAccessor, (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(), virtualFreePageManager
                        .getMetaDataFrameFactory().createFrame(), 4, btreeAccessor, btreeLeafFrameFactory,
                btreeInteriorFrameFactory, virtualFreePageManager.getMetaDataFrameFactory().createFrame(),
                rtreeCmpFactories, btreeCmpFactories, modCallback, NoOpOperationCallback.INSTANCE);
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return rtreeCmpFactories;
    }

    public boolean isEmptyIndex() throws HyracksDataException {
        return componentsRef.get().isEmpty()
                && mutableComponent.getBTree().isEmptyTree(
                        mutableComponent.getBTree().getInteriorFrameFactory().createFrame())
                && mutableComponent.getRTree().isEmptyTree(
                        mutableComponent.getRTree().getInteriorFrameFactory().createFrame());
    }

    @Override
    public void validate() throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for LSM R-Trees.");
    }

    @Override
    public long getMemoryAllocationSize() {
        IBufferCache virtualBufferCache = mutableComponent.getRTree().getBufferCache();
        return virtualBufferCache.getNumPages() * virtualBufferCache.getPageSize();
    }

    @Override
    public String toString() {
        return "LSMRTree [" + fileManager.getBaseDir() + "]";
    }
}
