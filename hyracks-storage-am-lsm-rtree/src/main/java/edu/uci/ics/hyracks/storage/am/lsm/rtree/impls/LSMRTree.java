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
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFinalizer;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMHarness;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.LSMRTreeFileManager.LSMRTreeFileNameComponent;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTree implements ILSMTree {

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

    private final LSMHarness lsmHarness;

    // In-memory components.
    private final LSMRTreeComponent memComponent;
    protected final InMemoryFreePageManager memFreePageManager;
    private final static int MEM_RTREE_FILE_ID = 0;
    private final static int MEM_BTREE_FILE_ID = 1;

    // On-disk components.
    private final ILSMFileManager fileManager;
    protected final IBufferCache diskBufferCache;
    protected final IFileMapProvider diskFileMapProvider;
    // For creating RTree's used in flush and merge.
    private final RTreeFactory diskRTreeFactory;
    // For creating BTree's used in flush and merge.
    private final BTreeFactory diskBTreeFactory;
    // List of LSMRTreeComponent instances. Using Object for better sharing via
    // ILSMTree + LSMHarness.
    private final LinkedList<Object> diskComponents = new LinkedList<Object>();
    // Helps to guarantees physical consistency of LSM components.
    private final ILSMComponentFinalizer componentFinalizer;
    
    private IBinaryComparatorFactory[] btreeCmpFactories;
    private IBinaryComparatorFactory[] rtreeCmpFactories;

    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory rtreeLeafFrameFactory;
    private final ITreeIndexFrameFactory btreeLeafFrameFactory;

    public LSMRTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMFileManager fileManager, RTreeFactory diskRTreeFactory, BTreeFactory diskBTreeFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories) {
        RTree memRTree = new RTree(memBufferCache, fieldCount, rtreeCmpFactories, memFreePageManager,
                rtreeInteriorFrameFactory, rtreeLeafFrameFactory);
        BTree memBTree = new BTree(memBufferCache, fieldCount, btreeCmpFactories, memFreePageManager,
                btreeInteriorFrameFactory, btreeLeafFrameFactory);
        memComponent = new LSMRTreeComponent(memRTree, memBTree);
        this.memFreePageManager = memFreePageManager;
        this.diskBufferCache = diskBTreeFactory.getBufferCache();
        this.diskFileMapProvider = diskFileMapProvider;
        this.diskBTreeFactory = diskBTreeFactory;
        this.fileManager = fileManager;
        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.diskRTreeFactory = diskRTreeFactory;
        this.btreeCmpFactories = btreeCmpFactories;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.lsmHarness = new LSMHarness(this);
        componentFinalizer = new LSMRTreeComponentFinalizer();
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        memComponent.getRTree().create(MEM_RTREE_FILE_ID);
        memComponent.getBTree().create(MEM_BTREE_FILE_ID);
        fileManager.createDirs();
    }

    /**
     * Opens LSMRTree, cleaning up invalid files from base dir, and registering
     * all valid files as on-disk RTrees and BTrees.
     * 
     * @param indexFileId
     *            Dummy file id.
     * @throws HyracksDataException
     */
    @Override
    public void open(int indexFileId) throws HyracksDataException {
        memComponent.getRTree().open(MEM_RTREE_FILE_ID);
        memComponent.getBTree().open(MEM_BTREE_FILE_ID);
        List<Object> validFileNames = fileManager.cleanupAndGetValidFiles();
        for (Object o : validFileNames) {
            LSMRTreeFileNameComponent component = (LSMRTreeFileNameComponent) o;            
            FileReference rtreeFile = new FileReference(new File(component.getRTreeFileName()));
            FileReference btreeFile = new FileReference(new File(component.getBTreeFileName()));
            RTree rtree = (RTree) createDiskTree(diskRTreeFactory, rtreeFile, false);
            BTree btree = (BTree) createDiskTree(diskBTreeFactory, btreeFile, false);
            LSMRTreeComponent diskComponent = new LSMRTreeComponent(rtree, btree);
            diskComponents.add(diskComponent);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        for (Object o : diskComponents) {
            LSMRTreeComponent diskComponent = (LSMRTreeComponent) o;
            RTree rtree = diskComponent.getRTree();
            BTree btree = diskComponent.getBTree();
            diskBufferCache.closeFile(rtree.getFileId());
            diskBufferCache.deleteFile(rtree.getFileId(), false);
            rtree.close();
            diskBufferCache.closeFile(btree.getFileId());
            diskBufferCache.deleteFile(btree.getFileId(), false);
            btree.close();
        }
        diskComponents.clear();
        memComponent.getRTree().close();
        memComponent.getBTree().close();
    }

    private LSMRTreeFileNameComponent getMergeTargetFileName(List<Object> mergingDiskTrees) throws HyracksDataException {        
        RTree firstTree = ((LSMRTreeComponent) mergingDiskTrees.get(0)).getRTree();
        RTree lastTree = ((LSMRTreeComponent) mergingDiskTrees.get(mergingDiskTrees.size() - 1)).getRTree();
        FileReference firstFile = diskFileMapProvider.lookupFileName(firstTree.getFileId());
        FileReference lastFile = diskFileMapProvider.lookupFileName(lastTree.getFileId());
        LSMRTreeFileNameComponent component = (LSMRTreeFileNameComponent) ((LSMRTreeFileManager) fileManager)
                .getRelMergeFileName(firstFile.getFile().getName(), lastFile.getFile().getName());
        return component;
    }

    protected ITreeIndex createDiskTree(TreeFactory diskTreeFactory, FileReference fileRef, boolean createTree)
            throws HyracksDataException {
        // File will be deleted during cleanup of merge().
        diskBufferCache.createFile(fileRef);
        int diskTreeFileId = diskFileMapProvider.lookupFileId(fileRef);
        // File will be closed during cleanup of merge().
        diskBufferCache.openFile(diskTreeFileId);
        // Create new tree instance.
        ITreeIndex diskTree = diskTreeFactory.createIndexInstance(diskTreeFileId);
        if (createTree) {
            diskTree.create(diskTreeFileId);
        }
        // Tree will be closed during cleanup of merge().
        diskTree.open(diskTreeFileId);
        return diskTree;
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException {
    	// Note that by using a flush target file name, we state that the new
        // bulk loaded tree is "newer" than any other merged tree.
    	LSMRTreeFileNameComponent fileNames = (LSMRTreeFileNameComponent) fileManager.getRelFlushFileName();
    	FileReference rtreeFile = fileManager.createFlushFile(fileNames.getRTreeFileName());
        RTree diskRTree = (RTree) createDiskTree(diskRTreeFactory, rtreeFile, true);
        // For each RTree, we require to have a buddy BTree. thus, we create an
        // empty BTree.
        FileReference btreeFile = fileManager.createFlushFile(fileNames.getBTreeFileName());
        BTree diskBTree = (BTree) createDiskTree(diskBTreeFactory, btreeFile, true);        
        LSMRTreeBulkLoadContext bulkLoadCtx = new LSMRTreeBulkLoadContext(diskRTree, diskBTree);
        bulkLoadCtx.beginBulkLoad(fillFactor);
        return bulkLoadCtx;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        LSMRTreeBulkLoadContext bulkLoadCtx = (LSMRTreeBulkLoadContext) ictx;
        bulkLoadCtx.getRTree().bulkLoadAddTuple(tuple, bulkLoadCtx.getBulkLoadCtx());
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        LSMRTreeBulkLoadContext bulkLoadCtx = (LSMRTreeBulkLoadContext) ictx;
        bulkLoadCtx.getRTree().endBulkLoad(bulkLoadCtx.getBulkLoadCtx());
        LSMRTreeComponent diskComponent = new LSMRTreeComponent(bulkLoadCtx.getRTree(), bulkLoadCtx.getBTree());
        lsmHarness.addBulkLoadedComponent(diskComponent);
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
            ITupleReference tupleCopy = null;
            try {
                if (cursor.hasNext()) {
                    cursor.next();
                    tupleCopy = TupleUtils.copyTuple(cursor.getTuple());
                }
            } finally {
                cursor.close();
            }
            if (tupleCopy != null) {
                ctx.memRTreeAccessor.insert(tupleCopy);
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
        return true;
    }

    public void search(ITreeIndexCursor cursor, List<Object> diskComponents, ISearchPredicate pred,
            IIndexOpContext ictx, boolean includeMemComponent, AtomicInteger searcherRefCount)
            throws HyracksDataException, TreeIndexException {
        LSMRTreeOpContext ctx = (LSMRTreeOpContext) ictx;
        int numDiskTrees = diskComponents.size();
        int numTrees = (includeMemComponent) ? numDiskTrees + 1 : numDiskTrees;
        ITreeIndexAccessor[] bTreeAccessors = new ITreeIndexAccessor[numTrees];
        int diskBTreeIx = 0;
        if (includeMemComponent) {
            bTreeAccessors[0] = ctx.memBTreeAccessor;
            diskBTreeIx++;
        }

        ListIterator<Object> diskBTreesIter = diskComponents.listIterator();
        while (diskBTreesIter.hasNext()) {
            LSMRTreeComponent component = (LSMRTreeComponent) diskBTreesIter.next();
            BTree diskBTree = component.getBTree();
            bTreeAccessors[diskBTreeIx] = diskBTree.createAccessor();
            diskBTreeIx++;
        }

        LSMRTreeSearchCursor lsmRTreeCursor = (LSMRTreeSearchCursor) cursor;
        LSMRTreeCursorInitialState initialState = new LSMRTreeCursorInitialState(numTrees, rtreeLeafFrameFactory,
                rtreeInteriorFrameFactory, btreeLeafFrameFactory, ctx.getBTreeMultiComparator(), bTreeAccessors,
                searcherRefCount, includeMemComponent, lsmHarness);
        lsmRTreeCursor.open(initialState, pred);

        int cursorIx;
        if (includeMemComponent) {
            ctx.memRTreeAccessor.search(((LSMRTreeSearchCursor) lsmRTreeCursor).getCursor(0), pred);
            cursorIx = 1;
        } else {
            cursorIx = 0;
        }

        // Open cursors of on-disk RTrees
        ITreeIndexAccessor[] diskRTreeAccessors = new ITreeIndexAccessor[numDiskTrees];
        ListIterator<Object> diskRTreesIter = diskComponents.listIterator();

        int diskRTreeIx = 0;
        while (diskRTreesIter.hasNext()) {
            LSMRTreeComponent component = (LSMRTreeComponent) diskRTreesIter.next();
            RTree diskRTree = component.getRTree();
            diskRTreeAccessors[diskRTreeIx] = diskRTree.createAccessor();
            diskRTreeAccessors[diskRTreeIx].search(lsmRTreeCursor.getCursor(cursorIx), pred);
            cursorIx++;
            diskRTreeIx++;
        }
    }

    @Override
    public Object flush() throws HyracksDataException, TreeIndexException {
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.

        // scan the memory RTree
        ITreeIndexAccessor memRTreeAccessor = memComponent.getRTree().createAccessor();
        ITreeIndexCursor rtreeScanCursor = memRTreeAccessor.createSearchCursor();
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);
        LSMRTreeFileNameComponent fileNames = (LSMRTreeFileNameComponent) fileManager.getRelFlushFileName();
        FileReference rtreeFile = fileManager.createFlushFile(fileNames.getRTreeFileName());
        RTree diskRTree = (RTree) createDiskTree(diskRTreeFactory, rtreeFile, true);

        // BulkLoad the tuples from the in-memory tree into the new disk RTree.
        IIndexBulkLoadContext rtreeBulkLoadCtx = diskRTree.beginBulkLoad(1.0f);

        try {
            while (rtreeScanCursor.hasNext()) {
                rtreeScanCursor.next();
                ITupleReference frameTuple = rtreeScanCursor.getTuple();
                diskRTree.bulkLoadAddTuple(frameTuple, rtreeBulkLoadCtx);
            }
        } finally {
            rtreeScanCursor.close();
        }
        diskRTree.endBulkLoad(rtreeBulkLoadCtx);

        // scan the memory BTree
        ITreeIndexAccessor memBTreeAccessor = memComponent.getBTree().createAccessor();
        ITreeIndexCursor btreeScanCursor = memBTreeAccessor.createSearchCursor();
        RangePredicate btreeNullPredicate = new RangePredicate(null, null, true, true, null, null);
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);
        FileReference btreeFile = fileManager.createFlushFile(fileNames.getBTreeFileName());
        BTree diskBTree = (BTree) createDiskTree(diskBTreeFactory, btreeFile, true);

        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoadContext btreeBulkLoadCtx = diskBTree.beginBulkLoad(1.0f);
        try {
            while (btreeScanCursor.hasNext()) {
                btreeScanCursor.next();
                ITupleReference frameTuple = btreeScanCursor.getTuple();
                diskBTree.bulkLoadAddTuple(frameTuple, btreeBulkLoadCtx);
            }
        } finally {
            btreeScanCursor.close();
        }
        diskBTree.endBulkLoad(btreeBulkLoadCtx);
        return new LSMRTreeComponent(diskRTree, diskBTree);
    }

    @Override
    public Object merge(List<Object> mergedComponents) throws HyracksDataException, TreeIndexException {
        // Renaming order is critical because we use assume ordering when we
        // read the file names when we open the tree.
        // The RTree should be renamed before the BTree.

        IIndexOpContext ctx = createOpContext();
        ITreeIndexCursor cursor = new LSMRTreeSearchCursor();
        ISearchPredicate rtreeSearchPred = new SearchPredicate(null, null);
        // Scan the RTrees, ignoring the in-memory RTree.
        List<Object> mergingComponents = lsmHarness.search(cursor, rtreeSearchPred, ctx, false);
        mergedComponents.addAll(mergingComponents);

        // Nothing to merge.
        if (mergedComponents.size() <= 1) {
            cursor.close();
            return null;
        }

        // Bulk load the tuples from all on-disk RTrees into the new RTree.
        LSMRTreeFileNameComponent fileNames = getMergeTargetFileName(mergingComponents);
        FileReference rtreeFile = fileManager.createMergeFile(fileNames.getRTreeFileName());
        FileReference btreeFile = fileManager.createMergeFile(fileNames.getBTreeFileName());
        RTree mergedRTree = (RTree) createDiskTree(diskRTreeFactory, rtreeFile, true);
        BTree mergedBTree = (BTree) createDiskTree(diskBTreeFactory, btreeFile, true);
        
        IIndexBulkLoadContext bulkLoadCtx = mergedRTree.beginBulkLoad(1.0f);
        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference frameTuple = cursor.getTuple();
                mergedRTree.bulkLoadAddTuple(frameTuple, bulkLoadCtx);
            }
        } finally {
            cursor.close();
        }
        mergedRTree.endBulkLoad(bulkLoadCtx);
        
        // Load an empty BTree tree.
        IIndexBulkLoadContext btreeBulkLoadCtx = mergedBTree.beginBulkLoad(1.0f);
        mergedBTree.endBulkLoad(btreeBulkLoadCtx);
        
        return new LSMRTreeComponent(mergedRTree, mergedBTree);
    }

    @Override
    public void addMergedComponent(Object newComponent, List<Object> mergedComponents) {
        diskComponents.removeAll(mergedComponents);
        diskComponents.addLast((LSMRTreeComponent) newComponent);
    }

    @Override
    public void cleanUpAfterMerge(List<Object> mergedComponents) throws HyracksDataException {
        for (Object o : mergedComponents) {
            LSMRTreeComponent component = (LSMRTreeComponent) o;
            BTree oldBTree = component.getBTree();
            FileReference btreeFileRef = diskFileMapProvider.lookupFileName(oldBTree.getFileId());
            diskBufferCache.closeFile(oldBTree.getFileId());
            diskBufferCache.deleteFile(oldBTree.getFileId(), false);
            oldBTree.close();
            btreeFileRef.getFile().delete();
            RTree oldRTree = component.getRTree();
            FileReference rtreeFileRef = diskFileMapProvider.lookupFileName(oldRTree.getFileId());
            diskBufferCache.closeFile(oldRTree.getFileId());
            diskBufferCache.deleteFile(oldRTree.getFileId(), false);
            oldRTree.close();
            rtreeFileRef.getFile().delete();
        }
    }

    @Override
    public void addFlushedComponent(Object index) {
        diskComponents.addFirst((LSMRTreeComponent) index);
    }

    @Override
    public InMemoryFreePageManager getInMemoryFreePageManager() {
        return memFreePageManager;
    }

    @Override
    public void resetInMemoryComponent() throws HyracksDataException {
        memComponent.getRTree().create(MEM_RTREE_FILE_ID);
        memComponent.getBTree().create(MEM_BTREE_FILE_ID);
        memFreePageManager.reset();
    }

    @Override
    public List<Object> getDiskComponents() {
        return diskComponents;
    }

    protected LSMRTreeOpContext createOpContext() {
        return new LSMRTreeOpContext((RTree.RTreeAccessor) memComponent.getRTree().createAccessor(),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(), memFreePageManager
                        .getMetaDataFrameFactory().createFrame(), 8, (BTree.BTreeAccessor) memComponent.getBTree()
                        .createAccessor(), btreeLeafFrameFactory, btreeInteriorFrameFactory, memFreePageManager
                        .getMetaDataFrameFactory().createFrame(), rtreeCmpFactories, btreeCmpFactories);
    }

    @Override
    public ITreeIndexAccessor createAccessor() {
        return new LSMRTreeAccessor(lsmHarness, createOpContext());
    }

    public class LSMRTreeAccessor extends LSMTreeIndexAccessor {
        public LSMRTreeAccessor(LSMHarness lsmHarness, IIndexOpContext ctx) {
            super(lsmHarness, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMRTreeSearchCursor();
        }

        public MultiComparator getMultiComparator() {
            LSMRTreeOpContext concreteCtx = (LSMRTreeOpContext) ctx;
            return concreteCtx.rtreeOpContext.cmp;
        }
    }

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
}
