package edu.uci.ics.hyracks.storage.am.lsmtree.rtree.impls;

import java.io.File;
import java.util.LinkedList;

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
    private final MultiComparator cmp;

    // TODO: change to private, it's public only for LSMTreeSearchTest
    public LinkedList<ITreeIndex> inDiskRTreeList;
    public LinkedList<ITreeIndex> inDiskBTreeList;
    private LinkedList<ITreeIndex> mergedInDiskRTreeList;
    private LinkedList<ITreeIndex> mergedInDiskBTreeList;
    private int inDiskTreeCounter;
    private final RTreeFactory rTreeFactory;
    private final BTreeFactory bTreeFactory;
    private final IFileMapManager fileMapManager;
    private int threadReferenceCounter;
    private boolean flushFlag;

    public LSMRTree(IBufferCache rtreeMemCache, IBufferCache bufferCache, int fieldCount, MultiComparator cmp,
    		InMemoryFreePageManager memFreePageManager, ITreeIndexFrameFactory rtreeInteriorFrameFactory,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory rtreeLeafFrameFactory,
            ITreeIndexFrameFactory btreeLeafFrameFactory, RTreeFactory rTreeFactory, BTreeFactory bTreeFactory,
            IFileMapManager fileMapManager) {
        this.bufferCache = bufferCache;
        this.cmp = cmp;
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
        this.threadReferenceCounter = 0;
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

        memRTree = new RTree(rtreeMemCache, fieldCount, cmp, memFreePageManager, rtreeInteriorFrameFactory,
                rtreeLeafFrameFactory);
        memBTree = new BTree(rtreeMemCache, fieldCount, cmp, memFreePageManager, btreeInteriorFrameFactory,
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
        // TODO Auto-generated method stub

    }

    @Override
    public void flush() throws Exception {
        inDiskTreeCounter++;

        // scan the RTree
        ITreeIndexCursor rtreeScanCursor = new RTreeSearchCursor(
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame());
        SearchPredicate searchPredicate = new SearchPredicate(null, null);

        ITreeIndexAccessor memRTreeAccessor = memRTree.createAccessor();
        memRTreeAccessor.search(rtreeScanCursor, searchPredicate);

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

        int i = 0;
        try {
            while (rtreeScanCursor.hasNext()) {
                rtreeScanCursor.next();
                ITupleReference frameTuple = rtreeScanCursor.getTuple();
                inDiskRTree.bulkLoadAddTuple(frameTuple, rtreeBulkLoadCtx);
                i++;
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

    private void decreaseThreadReferenceCounter() throws Exception {
        synchronized (this) {
            threadReferenceCounter--;
            if (flushFlag == true) {
                if (threadReferenceCounter == 0) {
                    flush();
                    flushFlag = false;
                    return;
                } else if (threadReferenceCounter < 0) {
                    throw new Error("Thread reference counter is below zero. This indicates a programming error!");
                }
            }
        }
    }

    private void insert(ITupleReference tuple, LSMTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        try {
            boolean continuePerformOp = false;
            while (continuePerformOp == false) {
            	synchronized (this) {
            		if (!flushFlag) {
            			threadReferenceCounter++;
            			continuePerformOp = true;
            		}
            	}
            }
            ctx.LSMRTreeOpContext.memRtreeAccessor.insert(tuple);
            decreaseThreadReferenceCounter();
            // Check if we've reached or exceeded the maximum number of pages.
            // Note: It doesn't matter if this inserter or another concurrent
            // inserter caused the overflow.
            // The first inserter reaching this code, should set the flush flag.
            // Also, setting the flush flag multiple times doesn't hurt.
            if (memFreePageManager.isFull()) {
            	synchronized (this) {
                    // If flushFlag is false it means we are the first inserter
                    // to
                    // trigger the flush. If flushFlag is already set to true,
                    // there's no harm in setting it to true again.
                    flushFlag = true;
                    threadReferenceCounter--;
                    if (threadReferenceCounter == 0) {
                        flush();
                        ctx.LSMRTreeOpContext.reset(IndexOp.INSERT);
                        ctx.LSMRTreeOpContext.memRtreeAccessor.insert(tuple);
                        flushFlag = false;
                        return;
                    } else if (threadReferenceCounter < 0) {
                        throw new Error("Thread reference counter is below zero. This indicates a programming error!");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void delete(ITupleReference tuple, LSMTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        try {
        	boolean continuePerformOp = false;
        	while (continuePerformOp == false) {
        		synchronized (this) {
        			if (!flushFlag) {
        				threadReferenceCounter++;
        				continuePerformOp = true;
        			}
        		}
        	}
        	ctx.LSMBTreeOpContext.memBtreeAccessor.insert(tuple);
        	decreaseThreadReferenceCounter();
            // Check if we've reached or exceeded the maximum number of pages.
            // Note: It doesn't matter if this inserter or another concurrent
            // inserter caused the overflow.
            // The first inserter reaching this code, should set the flush flag.
            // Also, setting the flush flag multiple times doesn't hurt.
            if (memFreePageManager.isFull()) {
            	synchronized (this) {
                    // If flushFlag is false it means we are the first inserter
                    // to
                    // trigger the flush. If flushFlag is already set to true,
                    // there's no harm in setting it to true again.
                    flushFlag = true;
                    threadReferenceCounter--;
                    if (threadReferenceCounter == 0) {
                        flush();
                        ctx.LSMBTreeOpContext.reset(IndexOp.INSERT);
                        ctx.LSMBTreeOpContext.memBtreeAccessor.insert(tuple);
                        flushFlag = false;
                        return;
                    } else if (threadReferenceCounter < 0) {
                        throw new Error("Thread reference counter is below zero. This indicates a programming error!");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate searchPred, LSMTreeOpContext ctx) throws Exception {
        // int numberOfInDiskTrees;
        // ListIterator<InDiskTreeInfo> inDiskTreeInfoListIterator;
        // boolean continuePerformOp = false;
        //
        // ctx.reset(IndexOp.SEARCH);
        //
        // while (continuePerformOp == false) {
        // synchronized (this) {
        // if (!flushFlag) {
        // threadReferenceCounter++;
        // continuePerformOp = true;
        // }
        // }
        // }
        //
        // // in-disk
        // synchronized (inDiskTreeInfoList) {
        // numberOfInDiskTrees = inDiskTreeInfoList.size();
        // inDiskTreeInfoListIterator = inDiskTreeInfoList.listIterator();
        // }
        //
        // LSMTreeCursorInitialState initialState = new
        // LSMTreeCursorInitialState(numberOfInDiskTrees + 1,
        // insertLeafFrameFactory, cmp, this);
        // cursor.open(initialState, pred);
        //
        // BTree[] onDiskBtrees = new BTree[numberOfInDiskTrees];
        // ITreeIndexAccessor[] onDiskBtreeAccessors = new
        // ITreeIndexAccessor[numberOfInDiskTrees];
        //
        // for (int i = 0; i < numberOfInDiskTrees; i++) {
        // // get btree instances for in-disk trees
        // if (inDiskTreeInfoListIterator.hasNext()) {
        // onDiskBtrees[i] = ((InDiskTreeInfo)
        // inDiskTreeInfoListIterator.next()).getBTree();
        // } else {
        // throw new HyracksDataException("Cannot find in-disk tree instance");
        // }
        // onDiskBtreeAccessors[i] = onDiskBtrees[i].createAccessor();
        // onDiskBtreeAccessors[i].search(((LSMTreeRangeSearchCursor)
        // cursor).getCursor(i + 1), pred);
        // }
        //
        // // in-memory
        // ctx.memBtreeAccessor.search(((LSMTreeRangeSearchCursor)
        // cursor).getCursor(0), pred);
        //
        // LSMPriorityQueueComparator LSMPriorityQueueCmp = new
        // LSMPriorityQueueComparator(cmp);
        // ((LSMTreeRangeSearchCursor)
        // cursor).initPriorityQueue(numberOfInDiskTrees + 1,
        // LSMPriorityQueueCmp);
    }

    private LSMTreeOpContext createOpContext() {

        return new LSMTreeOpContext(new LSMRTreeOpContext((RTree.RTreeAccessor) memRTree.createAccessor(),
                (IRTreeLeafFrame) rtreeLeafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) rtreeInteriorFrameFactory.createFrame(), memFreePageManager
                        .getMetaDataFrameFactory().createFrame(), 8), new LSMBTreeOpContext(
                (BTree.BTreeAccessor) memBTree.createAccessor(), btreeLeafFrameFactory, btreeInteriorFrameFactory,
                memFreePageManager.getMetaDataFrameFactory().createFrame(), cmp));
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
