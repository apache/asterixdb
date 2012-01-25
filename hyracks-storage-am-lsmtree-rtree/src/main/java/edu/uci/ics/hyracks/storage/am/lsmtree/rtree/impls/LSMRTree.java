package edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls;

import java.io.File;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeDuplicateKeyException;
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
import edu.uci.ics.hyracks.storage.am.lsmtree.common.api.ILSMTree;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class LSMRTree implements ILSMTree {

    private final IBufferCache bufferCache;
    private RTree memRTree;
    private BTree memBTree;
    private String rtreeFileName;
    private String btreeFileName;
    private int rtreeFileId;
    private int btreeFileId;
    private boolean created;

    private final InMemoryFreePageManager memFreePageManager;
    private final ITreeIndexFrameFactory rtreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory btreeInteriorFrameFactory;
    private final ITreeIndexFrameFactory rtreeLeafFrameFactory;
    private final ITreeIndexFrameFactory btreeLeafFrameFactory;
    private final MultiComparator rtreeCmp;
    private final MultiComparator btreeCmp;

    // TODO: change to private, it's public only for LSMTreeSearchTest
    public LinkedList<ITreeIndex> inDiskRTreeList;
    public LinkedList<ITreeIndex> inDiskBTreeList;

    private LinkedList<ITreeIndex> mergedInDiskRTreeList;
    private LinkedList<ITreeIndex> mergedInDiskBTreeList;
    private int inDiskTreeCounter;
    private final RTreeFactory rTreeFactory;
    private final BTreeFactory bTreeFactory;
    private final IFileMapManager fileMapManager;
    private int threadRefCount;
    private boolean flushFlag;

    public LSMRTree(IBufferCache rtreeMemCache, IBufferCache bufferCache, int fieldCount, MultiComparator rtreeCmp,
            MultiComparator btreeCmp, InMemoryFreePageManager memFreePageManager,
            ITreeIndexFrameFactory rtreeInteriorFrameFactory, ITreeIndexFrameFactory btreeInteriorFrameFactory,
            ITreeIndexFrameFactory rtreeLeafFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            RTreeFactory rTreeFactory, BTreeFactory bTreeFactory, IFileMapManager fileMapManager) {
        this.bufferCache = bufferCache;
        this.rtreeCmp = rtreeCmp;
        this.btreeCmp = btreeCmp;
        this.rtreeInteriorFrameFactory = rtreeInteriorFrameFactory;
        this.btreeInteriorFrameFactory = btreeInteriorFrameFactory;
        this.rtreeLeafFrameFactory = rtreeLeafFrameFactory;
        this.btreeLeafFrameFactory = btreeLeafFrameFactory;
        this.memFreePageManager = memFreePageManager;
        this.rTreeFactory = rTreeFactory;
        this.bTreeFactory = bTreeFactory;
        this.inDiskRTreeList = new LinkedList<ITreeIndex>();
        this.inDiskBTreeList = new LinkedList<ITreeIndex>();
        this.inDiskTreeCounter = 0;
        this.fileMapManager = fileMapManager;
        this.threadRefCount = 0;
        this.created = false;
        this.flushFlag = false;

        try {
            this.rtreeFileName = this.fileMapManager.lookupFileName(this.rtreeFileId).toString();

            this.btreeFileName = this.rtreeFileName + "-btree";
            FileReference file = new FileReference(new File(this.btreeFileName));
            this.bufferCache.createFile(file);
            this.btreeFileId = fileMapManager.lookupFileId(file);
            bufferCache.openFile(btreeFileId);

        } catch (Exception e) {
            e.printStackTrace();
        }

        memRTree = new RTree(rtreeMemCache, fieldCount, rtreeCmp, memFreePageManager, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory);
        memBTree = new BTree(rtreeMemCache, fieldCount, btreeCmp, memFreePageManager, btreeInteriorFrameFactory,
                btreeLeafFrameFactory);
    }

    @Override
    public ITreeIndexAccessor createAccessor() {
        return new LSMRTreeAccessor(this);
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        // TODO Auto-generated method stub

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
        if (created) {
            return;
        } else {
            rtreeFileId = indexFileId;
            memRTree.create(rtreeFileId);
            memBTree.create(btreeFileId);
            created = true;
        }
    }

    @Override
    public void open(int indexFileId) {
        memRTree.open(rtreeFileId);
        memBTree.open(btreeFileId);
    }

    @Override
    public void close() {
        memRTree.close();
        memBTree.close();
        this.rtreeFileId = -1;

    }

    @Override
    public void merge() throws Exception {

        // Cursor setting -- almost the same as search, only difference is
        // "no cursor for in-memory tree"
        int numberOfInDiskTrees;   
        synchronized (inDiskRTreeList) {
            numberOfInDiskTrees = inDiskRTreeList.size();
        }
        
        RTreeSearchCursor[] rtreeCursors = new RTreeSearchCursor[numberOfInDiskTrees];
        BTreeRangeSearchCursor[] btreeCursors = new BTreeRangeSearchCursor[numberOfInDiskTrees];
        
        for (int i = 0; i < numberOfInDiskTrees; i++) {
            rtreeCursors[i] = new RTreeSearchCursor((IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(),
                    (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame());

            btreeCursors[i] = new BTreeRangeSearchCursor((IBTreeLeafFrame) btreeLeafFrameFactory.createFrame(), false);
        }
        
        // Create a new in-Disk RTree
        // Register the RTree information into system.
        inDiskTreeCounter++;
        FileReference rtreeFile = new FileReference(new File(getNextFileName(rtreeFileName, inDiskTreeCounter)));
        // TODO: Delete the file during cleanup.
        bufferCache.createFile(rtreeFile);
        int newDiskRTreeId = fileMapManager.lookupFileId(rtreeFile);
        // TODO: Close the file during cleanup.
        bufferCache.openFile(newDiskRTreeId);

        // Create new in-Disk RTree.
        RTree mergedRTree = (RTree) rTreeFactory.createIndexInstance(newDiskRTreeId);
        mergedRTree.create(newDiskRTreeId);
        // TODO: Close the RTree during cleanup.
        mergedRTree.open(newDiskRTreeId);

        
        // Create a new in-Disk BTree, which have full fillfactor.
        // Register the BTree information into system.
        FileReference btreeFile = new FileReference(new File(getNextFileName(btreeFileName, inDiskTreeCounter)));
        // TODO: Delete the file during cleanup.
        bufferCache.createFile(btreeFile);
        int newDiskBTreeId = fileMapManager.lookupFileId(btreeFile);
        // TODO: Close the file during cleanup.
        bufferCache.openFile(newDiskBTreeId);

        // Create new in-Disk BTree.
        BTree mergedBTree = (BTree) bTreeFactory.createIndexInstance(newDiskBTreeId);
        mergedBTree.create(newDiskBTreeId);
        // TODO: Close the BTree during cleanup.
        mergedBTree.open(newDiskBTreeId);
        
        // BulkLoad the tuples from the RTree into the new disk RTree.
        IIndexBulkLoadContext rtreeBulkLoadCtx = mergedRTree.beginBulkLoad(1.0f);
        
        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoadContext btreeBulkLoadCtx = mergedBTree.beginBulkLoad(1.0f);
        
        for (int i = 0; i < numberOfInDiskTrees; i++) {
            
            // scan the RTrees
            ITreeIndexCursor rtreeScanCursor = new RTreeSearchCursor(
                    (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(),
                    (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame());
            SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);

            ITreeIndexAccessor onDiskRTreeAccessor = inDiskRTreeList.get(i).createAccessor();
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
            ITreeIndexAccessor onDiskBTreeAccessor = inDiskBTreeList.get(i).createAccessor();
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
    public void flush() throws Exception {
        inDiskTreeCounter++;

        // scan the RTree
        ITreeIndexCursor rtreeScanCursor = new RTreeSearchCursor(
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame());
        SearchPredicate rtreeNullPredicate = new SearchPredicate(null, null);

        ITreeIndexAccessor memRTreeAccessor = memRTree.createAccessor();
        memRTreeAccessor.search(rtreeScanCursor, rtreeNullPredicate);

        // Create a new in-Disk RTree

        // Register the RTree information into system.
        FileReference rtreeFile = new FileReference(new File(getNextFileName(rtreeFileName, inDiskTreeCounter)));
        // TODO: Delete the file during cleanup.
        bufferCache.createFile(rtreeFile);
        int newDiskRTreeId = fileMapManager.lookupFileId(rtreeFile);
        // TODO: Close the file during cleanup.
        bufferCache.openFile(newDiskRTreeId);

        // Create new in-Disk RTree.
        RTree inDiskRTree = (RTree) rTreeFactory.createIndexInstance(newDiskRTreeId);
        inDiskRTree.create(newDiskRTreeId);
        // TODO: Close the RTree during cleanup.
        inDiskRTree.open(newDiskRTreeId);

        // // BulkLoad the tuples from the in-memory tree into the new disk
        // RTree.
        IIndexBulkLoadContext rtreeBulkLoadCtx = inDiskRTree.beginBulkLoad(1.0f);

        try {
            while (rtreeScanCursor.hasNext()) {
                rtreeScanCursor.next();
                ITupleReference frameTuple = rtreeScanCursor.getTuple();
                inDiskRTree.bulkLoadAddTuple(frameTuple, rtreeBulkLoadCtx);
            }
        } finally {
            rtreeScanCursor.close();
        }
        inDiskRTree.endBulkLoad(rtreeBulkLoadCtx);

        // scan the BTree
        ITreeIndexCursor btreeScanCursor = new BTreeRangeSearchCursor(
                (IBTreeLeafFrame) btreeLeafFrameFactory.createFrame(), false);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        ITreeIndexAccessor memBTreeAccessor = memBTree.createAccessor();
        memBTreeAccessor.search(btreeScanCursor, nullPred);

        // Create a new in-Disk BTree, which have full fillfactor.

        // Register the BTree information into system.
        FileReference btreeFile = new FileReference(new File(getNextFileName(btreeFileName, inDiskTreeCounter)));
        // TODO: Delete the file during cleanup.
        bufferCache.createFile(btreeFile);
        int newDiskBTreeId = fileMapManager.lookupFileId(btreeFile);
        // TODO: Close the file during cleanup.
        bufferCache.openFile(newDiskBTreeId);

        // Create new in-Disk BTree.
        BTree inDiskBTree = (BTree) bTreeFactory.createIndexInstance(newDiskBTreeId);
        inDiskBTree.create(newDiskBTreeId);
        // TODO: Close the BTree during cleanup.
        inDiskBTree.open(newDiskBTreeId);

        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoadContext btreeBulkLoadCtx = inDiskBTree.beginBulkLoad(1.0f);
        try {
            while (btreeScanCursor.hasNext()) {
                btreeScanCursor.next();
                ITupleReference frameTuple = btreeScanCursor.getTuple();
                inDiskBTree.bulkLoadAddTuple(frameTuple, btreeBulkLoadCtx);
            }
        } finally {
            btreeScanCursor.close();
        }
        inDiskBTree.endBulkLoad(btreeBulkLoadCtx);

        // After BulkLoading, Clear the in-memTrees
        resetInMemoryTrees();

        synchronized (inDiskRTreeList) {
            inDiskRTreeList.addFirst(inDiskRTree);
        }
        synchronized (inDiskBTreeList) {
            inDiskBTreeList.addFirst(inDiskBTree);
        }
    }

    private static final String getNextFileName(String fileName, int inDiskTreeCounter) {
        return fileName + "-" + Integer.toString(inDiskTreeCounter);
    }

    public void resetInMemoryTrees() throws HyracksDataException {
        ((InMemoryFreePageManager) memFreePageManager).reset();
        memRTree.create(rtreeFileId);
        memBTree.create(btreeFileId);
    }
    
    public void threadEnter() {
        threadRefCount++;
    }
    
    public void threadExit() throws Exception {
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

    private void insert(ITupleReference tuple, LSMTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        boolean waitForFlush = false;
        do {
            synchronized (this) {
                if (!flushFlag) {
                    threadEnter();
                    waitForFlush = false;
                }
            }
        } while (waitForFlush == true);
        ctx.LSMRTreeOpContext.memRTreeAccessor.insert(tuple);
        try {
            threadExit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void delete(ITupleReference tuple, LSMTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        boolean waitForFlush = false;
        do {
            synchronized (this) {
                if (!flushFlag) {
                    threadEnter();
                    waitForFlush = false;
                }
            }
        } while (waitForFlush == true);
        ctx.LSMBTreeOpContext.memBTreeAccessor.insert(tuple);
        try {
            threadExit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate rtreeSearchPred, LSMTreeOpContext ctx)
            throws Exception {

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

        int numberOfInDiskTrees;
        synchronized (inDiskRTreeList) {
            numberOfInDiskTrees = inDiskRTreeList.size();
        }

        LSMRTreeCursorInitialState initialState = new LSMRTreeCursorInitialState(numberOfInDiskTrees + 1,
                rtreeLeafFrameFactory, rtreeInteriorFrameFactory, btreeLeafFrameFactory, btreeCmp,
                ctx.LSMBTreeOpContext.memBTreeAccessor, this);
        cursor.open(initialState, rtreeSearchPred);

        ITreeIndexAccessor[] onDiskRTreeAccessors = new ITreeIndexAccessor[numberOfInDiskTrees];

        for (int i = 0; i < numberOfInDiskTrees; i++) {
            onDiskRTreeAccessors[i] = inDiskRTreeList.get(i).createAccessor();
            onDiskRTreeAccessors[i].search(((LSMRTreeSearchCursor) cursor).getRTreeCursor(i + 1), rtreeSearchPred);
        }

        // in-memory
        ctx.LSMRTreeOpContext.memRTreeAccessor.search(((LSMRTreeSearchCursor) cursor).getRTreeCursor(0),
                rtreeSearchPred);
    }

    public LinkedList<ITreeIndex> getInDiskBTreeList() {
        return inDiskBTreeList;
    }

    private LSMTreeOpContext createOpContext() {

        return new LSMTreeOpContext(new LSMRTreeOpContext((RTree.RTreeAccessor) memRTree.createAccessor(),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(), memFreePageManager
                        .getMetaDataFrameFactory().createFrame(), 8), new LSMBTreeOpContext(
                (BTree.BTreeAccessor) memBTree.createAccessor(), btreeLeafFrameFactory, btreeInteriorFrameFactory,
                memFreePageManager.getMetaDataFrameFactory().createFrame(), btreeCmp));
    }

    private class LSMRTreeAccessor implements ITreeIndexAccessor {
        private LSMRTree lsmRTree;
        private LSMTreeOpContext ctx;

        public LSMRTreeAccessor(LSMRTree lsmRTree) {
            this.lsmRTree = lsmRTree;
            this.ctx = lsmRTree.createOpContext();

        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.LSMRTreeOpContext.reset(IndexOp.INSERT);
            lsmRTree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            throw new UnsupportedOperationException("Update not supported by LSMRTree");
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.LSMBTreeOpContext.reset(IndexOp.INSERT);
            lsmRTree.delete(tuple, ctx);
        }

        @Override
        public void search(ITreeIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                TreeIndexException {
            ctx.reset(IndexOp.SEARCH);
            // TODO: fix exception handling throughout LSM tree.
            try {
                lsmRTree.search(cursor, searchPred, ctx);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            throw new UnsupportedOperationException("DiskOrderScan not supported by LSMRTree");
        }
    }

}

