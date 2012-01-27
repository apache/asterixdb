package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.ListIterator;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFileNameManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMTree;
import edu.uci.ics.hyracks.storage.am.lsm.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMTreeBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeFactory;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class LSMRTree implements ILSMTree {

    // In-memory components.
    private RTree memRTree;
    private final BTree memBTree;
    private final InMemoryFreePageManager memFreePageManager;
    private int rtreeFileId;
    private int btreeFileId;

    // On-disk components.
    private final ILSMFileNameManager fileNameManager;
    private final RTreeFactory diskRTreeFactory;
    private final BTreeFactory diskBTreeFactory;
    private final IBufferCache diskBufferCache;
    private final IFileMapProvider diskFileMapProvider;
    private LinkedList<RTree> onDiskRTrees = new LinkedList<RTree>();
    private LinkedList<RTree> mergedRTrees = new LinkedList<RTree>();
    private LinkedList<BTree> onDiskBTrees = new LinkedList<BTree>();
    private LinkedList<BTree> mergedBTrees = new LinkedList<BTree>();
    private int onDiskRTreeCount;
    private int onDiskBTreeCount;

    // Common for in-memory and on-disk components.
    private final ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory rtreeLeafFrameFactory;
    private final ITreeIndexFrameFactory btreeLeafFrameFactory;
    private final MultiComparator btreeCmp;

    // For dealing with concurrent accesses.
    private int threadRefCount;
    private boolean flushFlag;

    public LSMRTree(IBufferCache memBufferCache, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ILSMFileNameManager fileNameManager, RTreeFactory diskRTreeFactory, BTreeFactory diskBTreeFactory,
            IFileMapProvider diskFileMapProvider, int fieldCount, MultiComparator rtreeCmp, MultiComparator btreeCmp) {

        memRTree = new RTree(memBufferCache, fieldCount, rtreeCmp, memFreePageManager, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory);
        memBTree = new BTree(memBufferCache, fieldCount, btreeCmp, memFreePageManager, btreeInteriorFrameFactory,
                btreeLeafFrameFactory);

        this.memFreePageManager = memFreePageManager;
        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;

        this.diskBufferCache = diskRTreeFactory.getBufferCache();
        this.diskFileMapProvider = diskFileMapProvider;
        this.diskRTreeFactory = diskRTreeFactory;
        this.diskBTreeFactory = diskBTreeFactory;
        this.btreeCmp = btreeCmp;
        this.onDiskRTreeCount = 0;
        this.onDiskBTreeCount = 0;
        this.threadRefCount = 0;
        this.flushFlag = false;
        this.fileNameManager = fileNameManager;
        this.rtreeFileId = 0;
        this.btreeFileId = 1;
    }

    @Override
    public ITreeIndexAccessor createAccessor() {
        return new LSMRTreeAccessor(this);
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException {
        // Note that by using a flush target file name, we state that the new
        // bulk loaded tree is "newer" than any other merged tree.

        String fileName = fileNameManager.getFlushFileName();
        RTree diskRTree = (RTree) createDiskTree(fileName + "-rtree", diskRTreeFactory, true);
        // For each RTree, we require to have a buddy BTree. thus, we create an
        // empty BTree. This can be optimized later.
        createDiskTree(fileName + "-btree", diskBTreeFactory, true);
        LSMTreeBulkLoadContext bulkLoadCtx = new LSMTreeBulkLoadContext(diskRTree);
        bulkLoadCtx.beginBulkLoad(fillFactor);
        return bulkLoadCtx;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        LSMTreeBulkLoadContext bulkLoadCtx = (LSMTreeBulkLoadContext) ictx;
        bulkLoadCtx.getTree().bulkLoadAddTuple(tuple, bulkLoadCtx.getBulkLoadCtx());

    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        LSMTreeBulkLoadContext bulkLoadCtx = (LSMTreeBulkLoadContext) ictx;
        bulkLoadCtx.getTree().endBulkLoad(bulkLoadCtx.getBulkLoadCtx());
        onDiskRTrees.addFirst((RTree) bulkLoadCtx.getTree());

    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IFreePageManager getFreePageManager() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getFieldCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getRootPageId() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public IndexType getIndexType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        memRTree.create(rtreeFileId);
        memBTree.create(btreeFileId);
    }

    /**
     * Opens LSMRTree, assuming a consistent state of the disk-resident
     * components. In particular, registers all files in in base dir of
     * fileNameManager as on-disk RTrees and BTrees.
     * 
     * Example pathological scenario to explain "consistent state assumption":
     * Suppose a merge finished, but before the original files were deleted the
     * system crashes. We are left in a state where we have the original RTrees
     * and BTrees in addition to the merged ones. We assume that prior to
     * calling this method a separate recovery process has ensured the
     * consistent of the disk-resident components.
     * 
     * @param indexFileId
     *            Dummy file id.
     * @throws HyracksDataException
     */
    @Override
    public void open(int indexFileId) throws HyracksDataException {
        memRTree.open(rtreeFileId);
        memBTree.open(btreeFileId);
        File dir = new File(fileNameManager.getBaseDir());
        FilenameFilter rtreeFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".") && !name.endsWith("btree");
            }
        };
        String[] rtreeFiles = dir.list(rtreeFilter);

        FilenameFilter btreeFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return !name.startsWith(".") && !name.endsWith("rtree");
            }
        };
        String[] btreeFiles = dir.list(btreeFilter);

        if (rtreeFiles == null || btreeFiles == null) {
            return;
        }
        
        Comparator<String> fileNameCmp = fileNameManager.getFileNameComparator();
        Arrays.sort(rtreeFiles, fileNameCmp);
        for (String fileName : rtreeFiles) {
            RTree rtree = (RTree) createDiskTree(fileName, diskRTreeFactory, false);
            onDiskRTrees.add(rtree);
        }

        Arrays.sort(btreeFiles, fileNameCmp);
        for (String fileName : btreeFiles) {
            BTree btree = (BTree) createDiskTree(fileName, diskBTreeFactory, false);
            onDiskBTrees.add(btree);
        }
    }

    @Override
    public void close() throws HyracksDataException {
        for (RTree rtree : onDiskRTrees) {
            diskBufferCache.closeFile(rtree.getFileId());
            rtree.close();
        }
        for (BTree btree : onDiskBTrees) {
            diskBufferCache.closeFile(btree.getFileId());
            btree.close();
        }
        onDiskRTrees.clear();
        onDiskBTrees.clear();
        onDiskRTreeCount = 0;
        onDiskBTreeCount = 0;
        memRTree.close();
        memBTree.close();
    }

    private ITreeIndex createDiskTree(String fileName, TreeFactory diskTreeFactory, boolean createTree)
            throws HyracksDataException {
        // Register the new tree file.
        FileReference file = new FileReference(new File(fileName));
        // TODO: Delete the file during cleanup.
        diskBufferCache.createFile(file);
        int diskTreeFileId = diskFileMapProvider.lookupFileId(file);
        // TODO: Close the file during cleanup.
        diskBufferCache.openFile(diskTreeFileId);
        // Create new tree instance.
        ITreeIndex diskTree = diskTreeFactory.createIndexInstance(diskTreeFileId);
        if (createTree) {
            diskTree.create(diskTreeFileId);
        }
        // TODO: Close the tree during cleanup.
        diskTree.open(diskTreeFileId);
        return diskTree;
    }

    @Override
    public void merge() throws Exception {

        // Cursor setting -- almost the same as search, only difference is
        // "no cursor for in-memory tree"
        int numberOfInDiskTrees = onDiskRTrees.size();

        RTreeSearchCursor[] rtreeCursors = new RTreeSearchCursor[numberOfInDiskTrees];
        BTreeRangeSearchCursor[] btreeCursors = new BTreeRangeSearchCursor[numberOfInDiskTrees];

        for (int i = 0; i < numberOfInDiskTrees; i++) {
            rtreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(),
                    (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame());

            btreeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) btreeLeafFrameFactory.createFrame(), false);
        }

        String fileName = fileNameManager.getMergeFileName();
        RTree mergedRTree = (RTree) createDiskTree(fileName + "-rtree", diskRTreeFactory, true);
        BTree mergedBTree = (BTree) createDiskTree(fileName + "-btree", diskBTreeFactory, true);

        // BulkLoad the tuples from the trees into the new merged trees.
        IIndexBulkLoadContext rtreeBulkLoadCtx = mergedRTree.beginBulkLoad(1.0f);
        IIndexBulkLoadContext btreeBulkLoadCtx = mergedBTree.beginBulkLoad(1.0f);

        for (int i = 0; i < numberOfInDiskTrees; i++) {

            // scan the RTrees
            ITreeIndexCursor rtreeScanCursor = new RTreeSearchCursor(
                    (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(),
                    (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame());
            SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);

            ITreeIndexAccessor onDiskRTreeAccessor = onDiskRTrees.get(i).createAccessor();
            onDiskRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);

            try {
                while (rtreeScanCursor.hasNext()) {
                    rtreeScanCursor.next();
                    ITupleReference frameTuple = rtreeScanCursor.getTuple();
                    mergedRTree.bulkLoadAddTuple(frameTuple, rtreeBulkLoadCtx);
                }
            } finally {
                rtreeScanCursor.close();
            }

            // scan the BTrees
            ITreeIndexCursor btreeScanCursor = new BTreeRangeSearchCursor(
                    (IBTreeLeafFrame) btreeLeafFrameFactory.createFrame(), false);
            RangePredicate btreeNullPredicate = new RangePredicate(true, null, null, true, true, null, null);
            ITreeIndexAccessor onDiskBTreeAccessor = onDiskBTrees.get(i).createAccessor();
            onDiskBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);

            try {
                while (btreeScanCursor.hasNext()) {
                    btreeScanCursor.next();
                    ITupleReference frameTuple = btreeScanCursor.getTuple();
                    mergedBTree.bulkLoadAddTuple(frameTuple, btreeBulkLoadCtx);
                }
            } finally {
                btreeScanCursor.close();
            }

        }
        mergedRTree.endBulkLoad(rtreeBulkLoadCtx);
        mergedBTree.endBulkLoad(btreeBulkLoadCtx);

        // TODO: complete the merge code

    }

    @Override
    public void flush() throws HyracksDataException, TreeIndexException {

        // scan the RTree
        ITreeIndexCursor rtreeScanCursor = new RTreeSearchCursor(
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame());
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);

        ITreeIndexAccessor memRTreeAccessor = memRTree.createAccessor();
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);

        String fileName = fileNameManager.getFlushFileName();
        RTree diskRTree = (RTree) createDiskTree(fileName + "-rtree", diskRTreeFactory, true);

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

        // scan the BTree
        ITreeIndexCursor btreeScanCursor = new BTreeRangeSearchCursor(
                (IBTreeLeafFrame) btreeLeafFrameFactory.createFrame(), false);
        RangePredicate btreeNullPredicate = new RangePredicate(true, null, null, true, true, null, null);
        ITreeIndexAccessor memBTreeAccessor = memBTree.createAccessor();
        memBTreeAccessor.search(btreeScanCursor, btreeNullPredicate);

        BTree diskBTree = (BTree) createDiskTree(fileName + "-btree", diskBTreeFactory, true);

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

        resetInMemoryTrees();

        onDiskRTrees.addFirst(diskRTree);
        onDiskBTrees.addFirst(diskBTree);

    }

    public void resetInMemoryTrees() throws HyracksDataException {
        memFreePageManager.reset();
        memRTree.create(rtreeFileId);
        memBTree.create(btreeFileId);
    }

    public void threadEnter() {
        threadRefCount++;
    }

    public void threadExit() throws HyracksDataException, TreeIndexException {
        synchronized (this) {
            threadRefCount--;
            // Check if we've reached or exceeded the maximum number of pages.
            if (!flushFlag && memFreePageManager.isFull()) {
                flushFlag = true;
            }
            // Flush will only be handled by last exiting thread.
            if (flushFlag && threadRefCount == 0) {
                flush();
                flushFlag = false;
            }
        }
    }

    private void insert(ITupleReference tuple, LSMRTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        boolean waitForFlush = false;
        do {
            synchronized (this) {
                if (!flushFlag) {
                    threadEnter();
                    waitForFlush = false;
                }
            }
        } while (waitForFlush == true);
        ctx.memRTreeAccessor.insert(tuple);
        try {
            threadExit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void delete(ITupleReference tuple, LSMRTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        boolean waitForFlush = false;
        do {
            synchronized (this) {
                if (!flushFlag) {
                    threadEnter();
                    waitForFlush = false;
                }
            }
        } while (waitForFlush == true);
        ctx.memBTreeAccessor.insert(tuple);
        try {
            threadExit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate rtreeSearchPred, LSMRTreeOpContext ctx,
            boolean includeMemTree) throws Exception {

        boolean continuePerformOp = false;
        ctx.reset(IndexOp.SEARCH);
        while (continuePerformOp == false) {
            synchronized (this) {
                if (!flushFlag) {
                    threadRefCount++;
                    continuePerformOp = true;
                }
            }
        }

        int numDiskTrees = onDiskRTrees.size();

        ITreeIndexAccessor[] bTreeAccessors;
        int diskBTreeIx = 0;
        if (includeMemTree) {
            bTreeAccessors = new ITreeIndexAccessor[numDiskTrees + 1];
            bTreeAccessors[0] = ctx.memBTreeAccessor;
            diskBTreeIx++;
        } else {
            bTreeAccessors = new ITreeIndexAccessor[numDiskTrees];
        }

        ListIterator<BTree> diskBTreesIter = onDiskBTrees.listIterator();
        while (diskBTreesIter.hasNext()) {
            BTree diskBTree = diskBTreesIter.next();
            bTreeAccessors[diskBTreeIx] = diskBTree.createAccessor();
            diskBTreeIx++;
        }

        LSMRTreeSearchCursor lsmRTreeCursor = (LSMRTreeSearchCursor) cursor;
        LSMRTreeCursorInitialState initialState = new LSMRTreeCursorInitialState(numDiskTrees + 1,
                rtreeLeafFrameFactory, rtreeInteriorFrameFactory, btreeLeafFrameFactory, btreeCmp, bTreeAccessors, this);
        lsmRTreeCursor.open(initialState, rtreeSearchPred);

        int cursorIx = 1;
        if (includeMemTree) {
            ctx.memRTreeAccessor.search(((LSMRTreeSearchCursor) lsmRTreeCursor).getCursor(0), rtreeSearchPred);
            cursorIx = 1;
        } else {
            cursorIx = 0;
        }

        // Open cursors of on-disk RTrees
        ITreeIndexAccessor[] diskRTreeAccessors = new ITreeIndexAccessor[numDiskTrees];
        ListIterator<RTree> diskRTreesIter = onDiskRTrees.listIterator();

        int diskRTreeIx = 0;
        while (diskRTreesIter.hasNext()) {
            RTree diskRTree = diskRTreesIter.next();
            diskRTreeAccessors[diskRTreeIx] = diskRTree.createAccessor();
            diskRTreeAccessors[diskRTreeIx].search(lsmRTreeCursor.getCursor(cursorIx), rtreeSearchPred);
            cursorIx++;
            diskRTreeIx++;
        }

    }

    public LinkedList<BTree> getOnDiskBTrees() {
        return onDiskBTrees;
    }

    private LSMRTreeOpContext createOpContext() {

        return new LSMRTreeOpContext((RTree.RTreeAccessor) memRTree.createAccessor(),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(), memFreePageManager
                        .getMetaDataFrameFactory().createFrame(), 8, (BTree.BTreeAccessor) memBTree.createAccessor(),
                btreeLeafFrameFactory, btreeInteriorFrameFactory, memFreePageManager.getMetaDataFrameFactory()
                        .createFrame(), btreeCmp);
    }

    private class LSMRTreeAccessor implements ITreeIndexAccessor {
        private LSMRTree lsmRTree;
        private LSMRTreeOpContext ctx;

        public LSMRTreeAccessor(LSMRTree lsmRTree) {
            this.lsmRTree = lsmRTree;
            this.ctx = lsmRTree.createOpContext();

        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.INSERT);
            lsmRTree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            throw new UnsupportedOperationException("Update not supported by LSMRTree");
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.DELETE);
            lsmRTree.delete(tuple, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new LSMRTreeSearchCursor();
        }

        @Override
        public void search(ITreeIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                TreeIndexException {
            ctx.reset(IndexOp.SEARCH);
            // TODO: fix exception handling throughout LSM tree.
            try {
                lsmRTree.search(cursor, searchPred, ctx, true);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            throw new UnsupportedOperationException("DiskOrderScan not supported by LSM-RTree.");
        }
        
        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            throw new UnsupportedOperationException("DiskOrderScan not supported by LSM-RTree.");
        }
    }

}
