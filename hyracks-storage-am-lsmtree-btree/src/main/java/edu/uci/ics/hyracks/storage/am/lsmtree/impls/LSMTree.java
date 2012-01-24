package edu.uci.ics.hyracks.storage.am.lsmtree.impls;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.util.LinkedList;
import java.util.ListIterator;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
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
import edu.uci.ics.hyracks.storage.am.common.api.PageAllocationException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsmtree.tuples.LSMTypeAwareTupleReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class LSMTree implements ITreeIndex {

    private final IBufferCache bufferCache;
    private BTree memBTree;
    private String fileName;
    private int fileId;
    private boolean created;

    private final IFreePageManager memFreePageManager;
    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final ITreeIndexFrameFactory insertLeafFrameFactory;
    private final ITreeIndexFrameFactory deleteLeafFrameFactory;
    private final MultiComparator cmp;

    // TODO: change to private, it's public only for LSMTreeSearchTest
    public LinkedList<InDiskTreeInfo> inDiskTreeInfoList;
    private LinkedList<InDiskTreeInfo> mergedInDiskTreeInfoList;
    private int inDiskTreeCounter;
    private final BTreeFactory bTreeFactory;
    private final IFileMapManager fileMapManager;
    private int threadReferenceCounter;
    private boolean flushFlag;

    public LSMTree(IBufferCache memCache, IBufferCache bufferCache, int fieldCount, MultiComparator cmp,
            IFreePageManager memFreePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory insertLeafFrameFactory, ITreeIndexFrameFactory deleteLeafFrameFactory,
            BTreeFactory bTreeFactory, IFileMapManager fileMapManager) {
        this.bufferCache = bufferCache;
        this.cmp = cmp;
        this.interiorFrameFactory = interiorFrameFactory;
        this.insertLeafFrameFactory = insertLeafFrameFactory;
        this.deleteLeafFrameFactory = deleteLeafFrameFactory;
        this.memFreePageManager = memFreePageManager;
        this.bTreeFactory = bTreeFactory;
        this.inDiskTreeInfoList = new LinkedList<InDiskTreeInfo>();
        this.inDiskTreeCounter = 0;
        this.fileMapManager = fileMapManager;
        this.threadReferenceCounter = 0;
        this.created = false;
        this.flushFlag = false;

        try {
            this.fileName = this.fileMapManager.lookupFileName(this.fileId).toString();
        } catch (Exception e) {
            e.printStackTrace();
        }

        memBTree = new BTree(memCache, fieldCount, cmp, memFreePageManager, interiorFrameFactory,
                insertLeafFrameFactory);
    }

    @Override
    public void create(int indexFileId) throws HyracksDataException {
        if (created) {
            return;
        } else {
            memBTree.create(indexFileId);
            this.fileId = indexFileId;
            created = true;
        }
    }

    @Override
    public void open(int indexFileId) {
        memBTree.open(indexFileId);
        this.fileId = indexFileId;
    }

    @Override
    public void close() {
        memBTree.close();
        this.fileId = -1;
    }

    private void lsmPerformOp(ITupleReference tuple, LSMTreeOpContext ctx) throws Exception {
        boolean continuePerformOp = false;
        try {
            while (continuePerformOp == false) {
                synchronized (this) {
                    if (!flushFlag) {
                        threadReferenceCounter++;
                        continuePerformOp = true;
                    }
                }
            }
            ctx.memBtreeAccessor.insert(tuple);
            decreaseThreadReferenceCounter();
        } catch (BTreeDuplicateKeyException e) {
            ctx.reset(IndexOp.UPDATE);
            // We don't need to deal with a nonexistent key here, because a
            // deleter will actually update the key and it's value, and not
            // delete it from the BTree.
            ctx.memBtreeAccessor.update(tuple);
            decreaseThreadReferenceCounter();
        } catch (PageAllocationException e) {
            synchronized (this) {
                // If flushFlag is false it means we are the first inserter to
                // trigger the flush. If flushFlag is already set to true,
                // there's no harm in setting it to true again.
                flushFlag = true;
                threadReferenceCounter--;
                if (threadReferenceCounter == 0) {
                    flushInMemoryBtree();
                    ctx.reset();
                    ctx.memBtreeAccessor.insert(tuple);
                    flushFlag = false;
                    return;
                } else if (threadReferenceCounter < 0) {
                    throw new Error("Thread reference counter is below zero. This indicates a programming error!");
                }
            }
            lsmPerformOp(tuple, ctx);
            return;
        }
    }

    public void decreaseThreadReferenceCounter() throws Exception {
        synchronized (this) {
            threadReferenceCounter--;
            if (flushFlag == true) {
                if (threadReferenceCounter == 0) {
                    flushInMemoryBtree();
                    flushFlag = false;
                    return;
                } else if (threadReferenceCounter < 0) {
                    throw new Error("Thread reference counter is below zero. This indicates a programming error!");
                }
            }
        }
    }

    private String getNextFileName() {
        int newDiskBTreeId = inDiskTreeCounter++;
        String LSMFileName = new String(this.fileName);
        return LSMFileName.concat("-" + Integer.toString(newDiskBTreeId));
    }

    public void flushInMemoryBtree() throws Exception {
        // read the tuple from InMemoryBtree, and bulkload into the disk

        // scan
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(
                (IBTreeLeafFrame) insertLeafFrameFactory.createFrame(), false);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        ITreeIndexAccessor memBtreeAccessor = memBTree.createAccessor();
        memBtreeAccessor.search(scanCursor, nullPred);

        // Create a new in-Disk BTree, which have full fillfactor.

        // Register the BTree information into system.
        FileReference file = new FileReference(new File(getNextFileName()));
        // TODO: Delete the file during cleanup.
        bufferCache.createFile(file);
        int newDiskBTreeId = fileMapManager.lookupFileId(file);
        // TODO: Close the file during cleanup.
        bufferCache.openFile(newDiskBTreeId);

        // Create new in-Disk Btree.
        BTree inDiskBTree = this.bTreeFactory.createBTreeInstance(newDiskBTreeId);
        inDiskBTree.create(newDiskBTreeId);
        // TODO: Close the BTree during cleanup.
        inDiskBTree.open(newDiskBTreeId);

        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoadContext bulkLoadCtx = inDiskBTree.beginBulkLoad(1.0f);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                inDiskBTree.bulkLoadAddTuple(frameTuple, bulkLoadCtx);
            }
        } finally {
            scanCursor.close();
        }
        inDiskBTree.endBulkLoad(bulkLoadCtx);

        // After BulkLoading, Clear the in-memTree
        resetInMemoryTree();

        InDiskTreeInfo newLinkedListNode = new InDiskTreeInfo(inDiskBTree);
        synchronized (inDiskTreeInfoList) {
            inDiskTreeInfoList.addFirst(newLinkedListNode);
        }
    }

    private void insert(ITupleReference tuple, LSMTreeOpContext ctx) throws HyracksDataException, TreeIndexException,
            PageAllocationException {
        try {
            lsmPerformOp(tuple, ctx);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void delete(ITupleReference tuple, LSMTreeOpContext ctx) throws HyracksDataException, TreeIndexException,
            PageAllocationException {
        try {
            lsmPerformOp(tuple, ctx);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException,
            PageAllocationException {
        return null;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException,
            PageAllocationException {
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
    }

    public void search(ITreeIndexCursor cursor, RangePredicate pred, LSMTreeOpContext ctx) throws Exception {
        int numberOfInDiskTrees;
        ListIterator<InDiskTreeInfo> inDiskTreeInfoListIterator;
        boolean continuePerformOp = false;

        ctx.reset(IndexOp.SEARCH);

        while (continuePerformOp == false) {
            synchronized (this) {
                if (!flushFlag) {
                    threadReferenceCounter++;
                    continuePerformOp = true;
                }
            }
        }

        // in-disk
        synchronized (inDiskTreeInfoList) {
            numberOfInDiskTrees = inDiskTreeInfoList.size();
            inDiskTreeInfoListIterator = inDiskTreeInfoList.listIterator();
        }

        LSMTreeCursorInitialState initialState = new LSMTreeCursorInitialState(numberOfInDiskTrees + 1,
                insertLeafFrameFactory, cmp, this);
        cursor.open(initialState, pred);

        BTree[] onDiskBtrees = new BTree[numberOfInDiskTrees];
        ITreeIndexAccessor[] onDiskBtreeAccessors = new ITreeIndexAccessor[numberOfInDiskTrees];

        for (int i = 0; i < numberOfInDiskTrees; i++) {
            // get btree instances for in-disk trees
            if (inDiskTreeInfoListIterator.hasNext()) {
                onDiskBtrees[i] = ((InDiskTreeInfo) inDiskTreeInfoListIterator.next()).getBTree();
            } else {
                throw new HyracksDataException("Cannot find in-disk tree instance");
            }
            onDiskBtreeAccessors[i] = onDiskBtrees[i].createAccessor();
            onDiskBtreeAccessors[i].search(((LSMTreeRangeSearchCursor) cursor).getCursor(i + 1), pred);
        }

        // in-memory
        ctx.memBtreeAccessor.search(((LSMTreeRangeSearchCursor) cursor).getCursor(0), pred);

        LSMPriorityQueueComparator LSMPriorityQueueCmp = new LSMPriorityQueueComparator(cmp);
        ((LSMTreeRangeSearchCursor) cursor).initPriorityQueue(numberOfInDiskTrees + 1, LSMPriorityQueueCmp);
    }

    public void merge() throws Exception {
        ITreeIndexCursor rangeCursor = new LSMTreeRangeSearchCursor();
        RangePredicate rangePred = new RangePredicate(true, null, null, true, true, null, null);

        // Cursor setting -- almost the same as search, only difference is
        // "no cursor for in-memory tree"
        int numberOfInDiskTrees;
        ListIterator<InDiskTreeInfo> inDiskTreeInfoListIterator;
        boolean continuePerformOp = false;
        while (continuePerformOp == false) {
            synchronized (this) {
                if (!flushFlag) {
                    threadReferenceCounter++;
                    continuePerformOp = true;
                }
            }
        }

        synchronized (inDiskTreeInfoList) {
            numberOfInDiskTrees = inDiskTreeInfoList.size();
            inDiskTreeInfoListIterator = inDiskTreeInfoList.listIterator();
        }

        LSMTreeCursorInitialState initialState = new LSMTreeCursorInitialState(numberOfInDiskTrees,
                insertLeafFrameFactory, cmp, this);
        rangeCursor.open(initialState, rangePred);

        BTree[] onDiskBtrees = new BTree[numberOfInDiskTrees];
        ITreeIndexAccessor[] onDiskBtreeAccessors = new ITreeIndexAccessor[numberOfInDiskTrees];

        for (int i = 0; i < numberOfInDiskTrees; i++) {
            // get btree instances for in-disk trees
            if (inDiskTreeInfoListIterator.hasNext()) {
                onDiskBtrees[i] = ((InDiskTreeInfo) inDiskTreeInfoListIterator.next()).getBTree();
            } else {
                throw new Exception("Cannot find in-disk tree instance");
            }
            onDiskBtreeAccessors[i] = onDiskBtrees[i].createAccessor();
            onDiskBtreeAccessors[i].search(((LSMTreeRangeSearchCursor) rangeCursor).getCursor(i), rangePred);
        }

        LSMPriorityQueueComparator LSMPriorityQueueCmp = new LSMPriorityQueueComparator(cmp);
        ((LSMTreeRangeSearchCursor) rangeCursor).initPriorityQueue(numberOfInDiskTrees, LSMPriorityQueueCmp);
        // End of Cursor setting

        // Create a new Merged BTree, which have full fillfactor.
        // Register the BTree information into system.
        // TODO: change the naming schema for the new tree
        FileReference file = new FileReference(new File(getNextFileName()));
        // TODO: Delete the file during cleanup.
        bufferCache.createFile(file);
        int mergedBTreeId = fileMapManager.lookupFileId(file);
        // TODO: Close the file during cleanup.
        bufferCache.openFile(mergedBTreeId);

        // Create new in-Disk BTree.
        BTree mergedBTree = this.bTreeFactory.createBTreeInstance(mergedBTreeId);
        mergedBTree.create(mergedBTreeId);
        // TODO: Close the BTree during cleanup.
        mergedBTree.open(mergedBTreeId);

        // BulkLoad the tuples from the in-memory tree into the new disk BTree.
        IIndexBulkLoadContext bulkLoadCtx = mergedBTree.beginBulkLoad(1.0f);

        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                mergedBTree.bulkLoadAddTuple(frameTuple, bulkLoadCtx);
            }
        } finally {
            rangeCursor.close();
        }

        mergedBTree.endBulkLoad(bulkLoadCtx);

        InDiskTreeInfo newLinkedListNode = new InDiskTreeInfo(mergedBTree);
        LinkedList<InDiskTreeInfo> tempInDiskTreeInfo;
        synchronized (inDiskTreeInfoList) {
            mergedInDiskTreeInfoList = (LinkedList<InDiskTreeInfo>) inDiskTreeInfoList.clone();
            // Remove the redundant trees, and add the new merged tree in the
            // last off the list
            for (int i = 0; i < numberOfInDiskTrees; i++) {
                mergedInDiskTreeInfoList.removeLast();
            }
            mergedInDiskTreeInfoList.addLast(newLinkedListNode);

            // TODO: to swap the linkedlists
            /*
             * tempInDiskTreeInfo = inDiskTreeInfoList; inDiskTreeInfoList =
             * mergedInDiskTreeInfoList; mergedInDiskTreeInfoList =
             * tempInDiskTreeInfo;
             */
            // TODO: to swap the searchThreadCounters

            // 1. should add the searcherReferenceCounter
            // 2. Wrap the searcherReferenceCounter as Integer object,
            // otherwise, the reference cannot be swapped
            // 3. modify the "decrease counter part" in search(), and let the
            // searcher remember the localReferences

        }
        // TODO: to wake up the cleaning thread
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return memBTree.getLeafFrameFactory();
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return memBTree.getInteriorFrameFactory();
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return memBTree.getFreePageManager();
    }

    @Override
    public int getFieldCount() {
        return memBTree.getFieldCount();
    }

    @Override
    public int getRootPageId() {
        return memBTree.getRootPageId();
    }

    @Override
    public IndexType getIndexType() {
        return memBTree.getIndexType();
    }

    public void resetInMemoryTree() throws HyracksDataException {
        ((InMemoryFreePageManager) memFreePageManager).reset();
        memBTree.create(fileId);
    }

    // This function is just for testing flushInMemoryBtree()
    public void scanDiskTree(int treeIndex) throws Exception {
        BTree onDiskBtree = inDiskTreeInfoList.get(treeIndex).getBTree();
        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(
                (IBTreeLeafFrame) this.insertLeafFrameFactory.createFrame(), false);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        ITreeIndexAccessor onDiskBtreeAccessor = onDiskBtree.createAccessor();
        onDiskBtreeAccessor.search(scanCursor, nullPred);
        try {
            int scanTupleIndex = 0;
            while (scanCursor.hasNext()) {
                scanCursor.hasNext();
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                int numPrintFields = Math.min(frameTuple.getFieldCount(), recDescSers.length);

                for (int i = 0; i < numPrintFields; i++) {
                    ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(i),
                            frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    DataInput dataIn = new DataInputStream(inStream);
                    Object o = recDescSers[i].deserialize(dataIn);

                    if (i == 1)
                        System.out.printf("LSMTree.scanDiskTree(): scanTupleIndex[%d]; Value is [%d]; ",
                                scanTupleIndex, Integer.parseInt(o.toString()));

                }

                if (((LSMTypeAwareTupleReference) frameTuple).isDelete()) {
                    System.out.printf(" DELETE\n");
                } else {
                    System.out.printf(" INSERT\n");
                }
                scanTupleIndex++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }
    }

    // This function is just for testing flushInMemoryBtree()
    public void scanInMemoryTree() throws Exception {
        ISerializerDeserializer[] recDescSers = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor recDesc = new RecordDescriptor(recDescSers);
        ITreeIndexCursor scanCursor = new BTreeRangeSearchCursor(
                (IBTreeLeafFrame) this.insertLeafFrameFactory.createFrame(), false);
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null, null);
        ITreeIndexAccessor inMemBtreeAccessor = memBTree.createAccessor();
        inMemBtreeAccessor.search(scanCursor, nullPred);
        try {
            int scanTupleIndex = 0;
            while (scanCursor.hasNext()) {
                scanCursor.hasNext();
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                int numPrintFields = Math.min(frameTuple.getFieldCount(), recDescSers.length);
                for (int i = 0; i < numPrintFields; i++) {
                    ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(i),
                            frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));
                    DataInput dataIn = new DataInputStream(inStream);
                    Object o = recDescSers[i].deserialize(dataIn);
                    if (i == 1)
                        System.out.printf("LSMTree.scanMemoryTree(): scanTupleIndex[%d]; Value is [%d]; ",
                                scanTupleIndex, Integer.parseInt(o.toString()));
                }
                if (((LSMTypeAwareTupleReference) frameTuple).isDelete()) {
                    System.out.printf(" DELETE\n");
                } else {
                    System.out.printf(" INSERT\n");
                }
                scanTupleIndex++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            scanCursor.close();
        }
    }

    public LSMTreeOpContext createOpContext() {
        return new LSMTreeOpContext((BTree.BTreeAccessor) memBTree.createAccessor(), insertLeafFrameFactory,
                deleteLeafFrameFactory, interiorFrameFactory, memFreePageManager.getMetaDataFrameFactory()
                        .createFrame(), cmp);
    }

    @Override
    public ITreeIndexAccessor createAccessor() {
        return new LSMTreeIndexAccessor(this);
    }

    private class LSMTreeIndexAccessor implements ITreeIndexAccessor {
        private LSMTree lsmTree;
        private LSMTreeOpContext ctx;

        public LSMTreeIndexAccessor(LSMTree lsmTree) {
            this.lsmTree = lsmTree;
            this.ctx = lsmTree.createOpContext();
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException,
                PageAllocationException {
            ctx.reset(IndexOp.INSERT);
            lsmTree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException,
                PageAllocationException {
            throw new UnsupportedOperationException("Update not supported by LSMTree");
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException,
                PageAllocationException {
            ctx.reset(IndexOp.DELETE);
            lsmTree.delete(tuple, ctx);
        }

        @Override
        public void search(ITreeIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                TreeIndexException, PageAllocationException {
            ctx.reset(IndexOp.SEARCH);
            // TODO: fix exception handling throughout LSM tree.
            try {
                lsmTree.search(cursor, (RangePredicate) searchPred, ctx);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            throw new UnsupportedOperationException("DiskOrderScan not supported by LSMTree");
        }
    }
}
