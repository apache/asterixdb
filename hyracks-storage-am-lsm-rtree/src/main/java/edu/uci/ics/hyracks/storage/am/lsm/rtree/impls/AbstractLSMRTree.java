/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryBufferCache;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeFactory;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public abstract class AbstractLSMRTree implements ILSMIndex, ITreeIndex {

    public class LSMRTreeComponent {
        private final RTree rtree;
        private final BTree btree;

        LSMRTreeComponent(RTree rtree, BTree btree) {
            this.rtree = rtree;
            this.btree = btree;
        }

        public RTree getRTree() {
            return rtree;
        }

        public BTree getBTree() {
            return btree;
        }
    }

    protected final LSMHarness lsmHarness;

    protected final ILinearizeComparatorFactory linearizer;
    protected final int[] comparatorFields;
    protected final IBinaryComparatorFactory[] linearizerArray;

    // In-memory components.
    protected final LSMRTreeComponent memComponent;
    protected final InMemoryFreePageManager memFreePageManager;
    protected FileReference memRtreeFile = new FileReference(new File("memrtree"));
    protected FileReference memBtreeFile = new FileReference(new File("membtree"));

    // This is used to estimate number of tuples in the memory RTree and BTree
    // for efficient memory allocation in the sort operation prior to flushing
    protected int memRTreeTuples = 0;
    protected int memBTreeTuples = 0;
    protected TreeTupleSorter rTreeTupleSorter = null;

    // On-disk components.
    protected final ILSMFileManager fileManager;
    protected final IBufferCache diskBufferCache;
    protected final IFileMapProvider diskFileMapProvider;
    // For creating RTree's used in flush and merge.
    protected final TreeFactory<RTree> diskRTreeFactory;
    // List of LSMRTreeComponent instances. Using Object for better sharing via
    // ILSMTree + LSMHarness.
    protected final LinkedList<Object> diskComponents = new LinkedList<Object>();
    // Helps to guarantees physical consistency of LSM components.
    protected final ILSMComponentFinalizer componentFinalizer;

    private IBinaryComparatorFactory[] btreeCmpFactories;
    private IBinaryComparatorFactory[] rtreeCmpFactories;

    // Common for in-memory and on-disk components.
    protected final ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    protected final ITreeIndexFrameFactory rtreeLeafFrameFactory;
    protected final ITreeIndexFrameFactory btreeLeafFrameFactory;

    private boolean isOpen = false;

    public AbstractLSMRTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMFileManager fileManager, TreeFactory<RTree> diskRTreeFactory, IFileMapProvider diskFileMapProvider,
            ILSMComponentFinalizer componentFinalizer, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, ILinearizeComparatorFactory linearizer,
            int[] comparatorFields, IBinaryComparatorFactory[] linearizerArray, ILSMFlushController flushController,
            ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker, ILSMIOScheduler ioScheduler) {
        RTree memRTree = new RTree(memBufferCache, ((InMemoryBufferCache) memBufferCache).getFileMapProvider(),
                memFreePageManager, rtreeInteriorFrameFactory, rtreeLeafFrameFactory, rtreeCmpFactories, fieldCount,
                memBtreeFile);
        BTree memBTree = new BTree(memBufferCache, ((InMemoryBufferCache) memBufferCache).getFileMapProvider(),
                memFreePageManager, btreeInteriorFrameFactory, btreeLeafFrameFactory, btreeCmpFactories, fieldCount,
                memRtreeFile);
        memComponent = new LSMRTreeComponent(memRTree, memBTree);
        this.memFreePageManager = memFreePageManager;
        this.diskBufferCache = diskRTreeFactory.getBufferCache();
        this.diskFileMapProvider = diskFileMapProvider;
        this.fileManager = fileManager;
        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.diskRTreeFactory = diskRTreeFactory;
        this.btreeCmpFactories = btreeCmpFactories;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.lsmHarness = new LSMHarness(this, flushController, mergePolicy, opTracker, ioScheduler);
        this.componentFinalizer = componentFinalizer;
        this.linearizer = linearizer;
        this.comparatorFields = comparatorFields;
        this.linearizerArray = linearizerArray;
    }

    @Override
    public synchronized void create() throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to create since index is already open.");
        }

        fileManager.deleteDirs();
        fileManager.createDirs();
        memComponent.getRTree().create();
        memComponent.getBTree().create();
    }

    @Override
    public synchronized void open() throws HyracksDataException {
        if (isOpen) {
            return;
        }

        memComponent.getRTree().open();
        memComponent.getBTree().open();
        isOpen = true;
    }

    @Override
    public synchronized void close() throws HyracksDataException {
        if (!isOpen) {
            return;
        }

        isOpen = false;

        memComponent.getRTree().close();
        memComponent.getBTree().close();
    }

    @Override
    public synchronized void destroy() throws HyracksDataException {
        if (isOpen) {
            throw new HyracksDataException("Failed to destroy since index is already open.");
        }

        memComponent.getRTree().close();
        memComponent.getBTree().close();
        fileManager.deleteDirs();
    }

    @Override
    public synchronized void clear() throws HyracksDataException {
        if (!isOpen) {
            throw new HyracksDataException("Failed to clear since index is not open.");
        }

        memComponent.getRTree().clear();
        memComponent.getBTree().clear();
    }

    @SuppressWarnings("rawtypes")
    protected ITreeIndex createDiskTree(TreeFactory diskTreeFactory, FileReference fileRef, boolean createTree)
            throws HyracksDataException {
        // Create new tree instance.
        ITreeIndex diskTree = diskTreeFactory.createIndexInstance(fileRef);
        if (createTree) {
            diskTree.create();
        }
        // Tree will be closed during cleanup of merge().
        diskTree.open();
        return diskTree;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return memComponent.getRTree().getLeafFrameFactory();
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return memComponent.getRTree().getInteriorFrameFactory();
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return memComponent.getRTree().getFreePageManager();
    }

    @Override
    public int getFieldCount() {
        return memComponent.getRTree().getFieldCount();
    }

    @Override
    public int getRootPageId() {
        return memComponent.getRTree().getRootPageId();
    }

    @Override
    public IndexType getIndexType() {
        return memComponent.getRTree().getIndexType();
    }

    @Override
    public int getFileId() {
        return memComponent.getRTree().getFileId();
    }

    public boolean insertUpdateOrDelete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException,
            TreeIndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        if (ctx.getIndexOp() == IndexOp.PHYSICALDELETE) {
            throw new UnsupportedOperationException("Physical delete not yet supported in LSM R-tree");
        }

        if (ctx.getIndexOp() == IndexOp.INSERT) {
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
                    memBTreeTuples--;
                } catch (BTreeNonExistentKeyException e) {
                    // Tuple has been deleted in the meantime. Do nothing.
                    // This normally shouldn't happen if we are dealing with
                    // good citizens since LSMRTree is used as a secondary
                    // index and a tuple shouldn't be deleted twice without
                    // insert between them.
                }
            } else {
                ctx.memRTreeAccessor.insert(tuple);
                memRTreeTuples++;
            }

        } else {
            try {
                ctx.memBTreeAccessor.insert(tuple);
                memBTreeTuples++;
            } catch (BTreeDuplicateKeyException e) {
                // Do nothing, because one delete tuple is enough to indicate
                // that all the corresponding insert tuples are deleted
            }
        }
        return true;
    }

    @Override
    public void addMergedComponent(Object newComponent, List<Object> mergedComponents) {
        diskComponents.removeAll(mergedComponents);
        diskComponents.addLast(newComponent);
    }

    @Override
    public void addFlushedComponent(Object index) {
        diskComponents.addFirst(index);
    }

    @Override
    public InMemoryFreePageManager getInMemoryFreePageManager() {
        return memFreePageManager;
    }

    @Override
    public void resetInMemoryComponent() throws HyracksDataException {
        memFreePageManager.reset();
        memComponent.getRTree().clear();
        memComponent.getBTree().clear();
        memRTreeTuples = 0;
        memBTreeTuples = 0;
    }

    @Override
    public List<Object> getDiskComponents() {
        return diskComponents;
    }

    protected LSMRTreeOpContext createOpContext() {
        return new LSMRTreeOpContext((RTree.RTreeAccessor) memComponent.getRTree().createAccessor(
                NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(), memFreePageManager
                        .getMetaDataFrameFactory().createFrame(), 8, (BTree.BTreeAccessor) memComponent.getBTree()
                        .createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE),
                btreeLeafFrameFactory, btreeInteriorFrameFactory, memFreePageManager.getMetaDataFrameFactory()
                        .createFrame(), rtreeCmpFactories, btreeCmpFactories, null, null);
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return rtreeCmpFactories;
    }

    @Override
    public IBufferCache getBufferCache() {
        return diskBufferCache;
    }

    @Override
    public ILSMComponentFinalizer getComponentFinalizer() {
        return componentFinalizer;
    }

    @Override
    public ILSMFlushController getFlushController() {
        return lsmHarness.getFlushController();
    }

    @Override
    public ILSMOperationTracker getOperationTracker() {
        return lsmHarness.getOperationTracker();
    }

    @Override
    public ILSMIOScheduler getIOScheduler() {
        return lsmHarness.getIOScheduler();
    }
}
