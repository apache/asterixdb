package edu.uci.ics.hyracks.storage.am.rtree.impls;

import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.TreeIndexOp;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.NSMRTreeFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class RTree {

    private boolean created = false;
    private final int rootPage = 1; // the root page never changes

    private final AtomicInteger globalNsn; // Global node sequence number
    private int numOfPages = 1;
    private final ReadWriteLock treeLatch;

    private final IFreePageManager freePageManager;
    private final IBufferCache bufferCache;
    private int fileId;

    private final IRTreeFrameFactory interiorFrameFactory;
    private final IRTreeFrameFactory leafFrameFactory;
    private final MultiComparator interiorCmp;
    private final MultiComparator leafCmp;
    public final int dim;

    public int rootSplits = 0;
    public int[] splitsByLevel = new int[500];
    public long readLatchesAcquired = 0;
    public long readLatchesReleased = 0;
    public long writeLatchesAcquired = 0;
    public long writeLatchesReleased = 0;
    public long pins = 0;
    public long unpins = 0;
    public byte currentLevel = 0;
    public long totalTuplesInserted = 0;

    public RTree(IBufferCache bufferCache, IFreePageManager freePageManager, IRTreeFrameFactory interiorFrameFactory,
            IRTreeFrameFactory leafFrameFactory, MultiComparator interiorCmp, MultiComparator leafCmp, int dim) {
        this.bufferCache = bufferCache;
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.interiorCmp = interiorCmp;
        this.leafCmp = leafCmp;
        this.dim = dim;
        globalNsn = new AtomicInteger();
        this.treeLatch = new ReentrantReadWriteLock(true);
    }

    public synchronized void incrementGlobalNsn() {
        globalNsn.incrementAndGet();
    }

    public synchronized void incrementReadLatchesAcquired() {
        readLatchesAcquired++;
    }
    
    public synchronized void incrementReadLatchesReleased() {
        readLatchesReleased++;
    }
   
    public synchronized void incrementWriteLatchesAcquired() {
        writeLatchesAcquired++;
    }
    
    public synchronized void incrementWriteLatchesReleased() {
        writeLatchesReleased++;
    }
    
    public synchronized void incrementPins() {
        pins++;
    }
    
    public synchronized void incrementUnpins() {
        unpins++;
    }
    
    public synchronized int getGlobalNsn() {
        return globalNsn.get();
    }

    public String printStats() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("\n");
        strBuilder.append("ROOTSPLITS: " + rootSplits + "\n");
        strBuilder.append("SPLITS BY LEVEL\n");
        for (int i = 0; i < currentLevel; i++) {
            strBuilder.append(String.format("%3d ", i) + String.format("%8d ", splitsByLevel[i]) + "\n");
        }
        strBuilder.append(String.format("READ LATCHES:  %10d %10d\n", readLatchesAcquired, readLatchesReleased));
        strBuilder.append(String.format("WRITE LATCHES: %10d %10d\n", writeLatchesAcquired, writeLatchesReleased));
        strBuilder.append(String.format("PINS:          %10d %10d\n", pins, unpins));

        strBuilder.append(String.format("Num of Pages:          %10d\n", numOfPages));

        return strBuilder.toString();
    }

    public void printTree(IRTreeFrame leafFrame, IRTreeFrame interiorFrame, ISerializerDeserializer[] fields)
            throws Exception {
        totalTuplesInserted = 0;
        printTree(rootPage, null, false, leafFrame, interiorFrame, fields);
        System.out.println(totalTuplesInserted);
    }

    public void printTree(int pageId, ICachedPage parent, boolean unpin, IRTreeFrame leafFrame,
            IRTreeFrame interiorFrame, ISerializerDeserializer[] fields) throws Exception {

        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        incrementPins();
        node.acquireReadLatch();
        incrementReadLatchesAcquired();

        try {
            if (parent != null && unpin == true) {
                parent.releaseReadLatch();
                incrementReadLatchesReleased();
                bufferCache.unpin(parent);
                incrementUnpins();
            }

            interiorFrame.setPage(node);
            int level = interiorFrame.getLevel();

            System.out.format("%1d ", level);
            System.out.format("%3d ", pageId);
            for (int i = 0; i < currentLevel - level; i++)
                System.out.format("    ");

            String keyString;
            if (interiorFrame.isLeaf()) {
                leafFrame.setPage(node);
                keyString = leafFrame.printKeys(leafCmp, fields);
            } else {
                keyString = interiorFrame.printKeys(interiorCmp, fields);
            }

            System.out.format(keyString);
            if (!interiorFrame.isLeaf()) {
                ArrayList<Integer> children = ((NSMRTreeFrame) (interiorFrame)).getChildren(interiorCmp);
                for (int i = 0; i < children.size(); i++) {
                    printTree(children.get(i), node, i == children.size() - 1, leafFrame, interiorFrame, fields);
                }
            } else {
                totalTuplesInserted += interiorFrame.getTupleCount();
                node.releaseReadLatch();
                incrementReadLatchesReleased();
                bufferCache.unpin(node);
                incrementUnpins();
            }
        } catch (Exception e) {
            node.releaseReadLatch();
            incrementReadLatchesReleased();
            bufferCache.unpin(node);
            incrementUnpins();
            e.printStackTrace();
        }
    }

    public void create(int fileId, IRTreeFrame leafFrame, ITreeIndexMetaDataFrame metaFrame) throws Exception {

        if (created)
            return;

        treeLatch.writeLock().lock();
        try {
            // check if another thread beat us to it
            if (created)
                return;

            freePageManager.init(metaFrame, rootPage);

            // initialize root page
            ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
            incrementPins();

            rootNode.acquireWriteLatch();
            incrementWriteLatchesAcquired();
            try {
                leafFrame.setPage(rootNode);
                leafFrame.initBuffer((byte) 0);
            } finally {
                rootNode.releaseWriteLatch();
                incrementWriteLatchesReleased();
                bufferCache.unpin(rootNode);
                incrementUnpins();
            }
            currentLevel = 0;

            created = true;
        } finally {
            treeLatch.writeLock().unlock();
        }
    }

    public void open(int fileId) {
        this.fileId = fileId;
    }

    public void close() {
        fileId = -1;
    }

    public RTreeOpContext createOpContext(TreeIndexOp op, IRTreeFrame interiorFrame, IRTreeFrame leafFrame,
            ITreeIndexMetaDataFrame metaFrame, String name) {
        // TODO: figure out better tree-height hint
        return new RTreeOpContext(op, interiorFrame, leafFrame, metaFrame, 8, dim, name);
    }

    public void insert(ITupleReference tuple, RTreeOpContext ctx) throws Exception {
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.splitKey.reset();
        ctx.splitKey.getLeftTuple().setFieldCount(interiorCmp.getFieldCount());
        ctx.splitKey.getRightTuple().setFieldCount(interiorCmp.getFieldCount());
        ctx.interiorFrame.setPageTupleFieldCount(interiorCmp.getFieldCount());
        ctx.leafFrame.setPageTupleFieldCount(leafCmp.getFieldCount());

        ICachedPage leafNode = findLeaf(ctx);

        int pageId = ctx.pathList.getLastPageId();
        ctx.pathList.removeLast();
        System.out.println(ctx.name + " Trying to insert in pageId: " + pageId);
        insertTuple(leafNode, pageId, ctx.getTuple(), ctx, true);

        while (true) {
            if (ctx.splitKey.getLeftPageBuffer() != null) {
                updateParentForInsert(ctx);
            } else {
                break;
            }
        }
        
        leafNode.releaseWriteLatch();
        incrementWriteLatchesReleased();
        bufferCache.unpin(leafNode);
        incrementUnpins();

    }

    public ICachedPage findLeaf(RTreeOpContext ctx) throws Exception {
        int pageId = rootPage;
        boolean writeLatched = false;
        ICachedPage node = null;
        boolean isLeaf = false;
        int pageLsn = 0, parentLsn = 0;

        while (true) {
            if (!writeLatched) {
                node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                incrementPins();
                ctx.interiorFrame.setPage(node);
                isLeaf = ctx.interiorFrame.isLeaf();
                if (isLeaf) {
                    System.out.println(ctx.name + " trying to get 555555555555 write lock: " + pageId);
                    node.acquireWriteLatch();
                    incrementWriteLatchesAcquired();
                    writeLatched = true;
                    System.out.println(ctx.name + " Got write lock: " + pageId);

                    if (!ctx.interiorFrame.isLeaf()) {
                        System.out.println(ctx.name + " Released write lock77777777: " + pageId);
                        node.releaseWriteLatch();
                        incrementWriteLatchesReleased();
                        bufferCache.unpin(node);
                        incrementUnpins();
                        writeLatched = false;
                        continue;
                    }
                } else {
                    // Be optimistic and grab read latch first. We will swap it
                    // to write latch if we need to enlarge the best child
                    // tuple.
                    System.out.println(ctx.name + " trying to get read lock: " + pageId);
                    node.acquireReadLatch();
                    incrementReadLatchesAcquired();
                    System.out.println(ctx.name + " Got read lock: " + pageId);
                }
            }

            if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                // Concurrent split detected, go back to parent and re-choose
                // the best child
                if (writeLatched) {
                    System.out.println(ctx.name + " Released write lock888888888: " + pageId);
                    node.releaseWriteLatch();
                    incrementWriteLatchesReleased();
                    bufferCache.unpin(node);
                    incrementUnpins();
                    writeLatched = false;
                } else {
                    System.out.println(ctx.name + " Released read lock11111111111: " + pageId);
                    node.releaseReadLatch();
                    incrementReadLatchesReleased();
                    bufferCache.unpin(node);
                    incrementUnpins();
                }

                pageId = ctx.pathList.getLastPageId();
                if (pageId != rootPage) {
                    parentLsn = ctx.pathList.getPageLsn(ctx.pathList.size() - 2);
                }
                ctx.pathList.removeLast();
                continue;
            }

            pageLsn = ctx.interiorFrame.getPageLsn();
            ctx.pathList.add(pageId, pageLsn, -1);

            for (int i = 0; i < ctx.pathList.size(); i++) {
                System.out.println(ctx.name + " pageId: " + ctx.pathList.getPageId(i) + " pageLsn: "
                        + ctx.pathList.getPageLsn(i));
            }

            if (!isLeaf) {
                // checkEnlargement must be called *before* getBestChildPageId
                boolean needsEnlargement = ctx.interiorFrame.findBestChild(ctx.getTuple(), ctx.tupleEntries1,
                        interiorCmp);
                int childPageId = ctx.interiorFrame.getBestChildPageId(interiorCmp);

                if (needsEnlargement) {
                    if (!writeLatched) {
                        System.out.println(ctx.name + " Released read lock2222222222222222: " + pageId);
                        node.releaseReadLatch();
                        incrementReadLatchesReleased();
                        // TODO: do we need to un-pin and pin again?
                        bufferCache.unpin(node);
                        incrementUnpins();
                        
                        System.out.println(ctx.name + " trying to get 6666666666666 write lock: " + pageId);
                        node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                        incrementPins();
                        node.acquireWriteLatch();
                        incrementWriteLatchesAcquired();
                        ctx.interiorFrame.setPage(node);
                        writeLatched = true;
                        System.out.println(ctx.name + " Got write lock: " + pageId);

                        if (ctx.interiorFrame.getPageLsn() != pageLsn) {
                            // The page was changed while we unlocked it; thus,
                            // retry (re-choose best child)

                            System.out.println(ctx.name
                                    + "  changed while we unlocked it; thus, retry (re-choose best child): " + pageId);
                            ctx.pathList.removeLast();
                            continue;
                        }
                    }

                    // We don't need to reset the frameTuple because it is
                    // already pointing to the best child
                    ctx.interiorFrame.enlarge(ctx.getTuple(), interiorCmp);

                    System.out.println(ctx.name + " Released write lock999999999: " + pageId);
                    node.releaseWriteLatch();
                    incrementWriteLatchesReleased();
                    bufferCache.unpin(node);
                    incrementUnpins();
                    writeLatched = false;
                } else {
                    if (writeLatched) {
                        System.out.println(ctx.name + " Released write lock1010101010: " + pageId);
                        node.releaseWriteLatch();
                        incrementWriteLatchesReleased();
                        bufferCache.unpin(node);
                        incrementUnpins();
                        writeLatched = false;
                    } else {
                        System.out.println(ctx.name + " Released read lock33333333333333333: " + pageId);
                        node.releaseReadLatch();
                        incrementReadLatchesReleased();
                        bufferCache.unpin(node);
                        incrementUnpins();
                    }
                }
                pageId = childPageId;
                parentLsn = pageLsn;
            } else {
                ctx.leafFrame.setPage(node);
                return node;
            }
        }
    }

    private void insertTuple(ICachedPage node, int pageId, ITupleReference tuple, RTreeOpContext ctx, boolean isLeaf)
            throws Exception {
        FrameOpSpaceStatus spaceStatus;
        if (!isLeaf) {
            spaceStatus = ctx.interiorFrame.hasSpaceInsert(ctx.getTuple(), interiorCmp);
            System.out.println(ctx.name + " Number of tuples: " + ctx.interiorFrame.getTupleCount());
        } else {
            spaceStatus = ctx.leafFrame.hasSpaceInsert(ctx.getTuple(), leafCmp);
            System.out.println(ctx.name + " Number of tuples: " + ctx.leafFrame.getTupleCount());
        }

        switch (spaceStatus) {
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                if (!isLeaf) {
                    ctx.interiorFrame.insert(tuple, interiorCmp);
                    incrementGlobalNsn();
                    ctx.interiorFrame.setPageLsn(getGlobalNsn());
                    System.out.println(ctx.name + " Number of tuples: " + ctx.interiorFrame.getTupleCount());
                } else {
                    ctx.leafFrame.insert(tuple, leafCmp);
                    incrementGlobalNsn();
                    ctx.leafFrame.setPageLsn(getGlobalNsn());
                    System.out.println(ctx.name + " Number of tuples: " + ctx.leafFrame.getTupleCount());
                }
                ctx.splitKey.reset();
                break;
            }

            case SUFFICIENT_SPACE: {
                if (!isLeaf) {
                    ctx.interiorFrame.compact(interiorCmp);
                    ctx.interiorFrame.insert(tuple, interiorCmp);
                    incrementGlobalNsn();
                    ctx.interiorFrame.setPageLsn(getGlobalNsn());
                    System.out.println(ctx.name + " Number of tuples: " + ctx.interiorFrame.getTupleCount());
                } else {
                    ctx.leafFrame.compact(leafCmp);
                    ctx.leafFrame.insert(tuple, leafCmp);
                    incrementGlobalNsn();
                    ctx.leafFrame.setPageLsn(getGlobalNsn());
                    System.out.println(ctx.name + " Number of tuples: " + ctx.leafFrame.getTupleCount());
                }
                ctx.splitKey.reset();
                break;
            }

            case INSUFFICIENT_SPACE: {
                System.out.println(ctx.name + " Split");
                int rightPageId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightPageId), true);
                incrementPins();
                rightNode.acquireWriteLatch();
                incrementWriteLatchesAcquired();
                
                try {
                    IRTreeFrame rightFrame;
                    int ret;
                    numOfPages++; // debug
                    if (!isLeaf) {
                        splitsByLevel[ctx.interiorFrame.getLevel()]++; // debug
                        rightFrame = interiorFrameFactory.getFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) ctx.interiorFrame.getLevel());
                        rightFrame.setPageTupleFieldCount(interiorCmp.getFieldCount());
                        ret = ctx.interiorFrame.split(rightFrame, tuple, interiorCmp, ctx.splitKey, ctx.tupleEntries1,
                                ctx.tupleEntries2, ctx.rec);
                        ctx.interiorFrame.setRightPage(rightPageId);
                        rightFrame.setPageNsn(ctx.interiorFrame.getPageNsn());
                        incrementGlobalNsn();
                        int newNsn = getGlobalNsn();
                        rightFrame.setPageLsn(newNsn);
                        ctx.interiorFrame.setPageNsn(newNsn);
                        ctx.interiorFrame.setPageLsn(newNsn);
                    } else {
                        splitsByLevel[0]++; // debug
                        rightFrame = leafFrameFactory.getFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) 0);
                        rightFrame.setPageTupleFieldCount(leafCmp.getFieldCount());
                        ret = ctx.leafFrame.split(rightFrame, tuple, leafCmp, ctx.splitKey, ctx.tupleEntries1,
                                ctx.tupleEntries2, ctx.rec);
                        ctx.leafFrame.setRightPage(rightPageId);
                        rightFrame.setPageNsn(ctx.leafFrame.getPageNsn());
                        incrementGlobalNsn();
                        int newNsn = getGlobalNsn();
                        rightFrame.setPageLsn(newNsn);
                        ctx.leafFrame.setPageNsn(newNsn);
                        ctx.leafFrame.setPageLsn(newNsn);
                    }
                    if (ret != 0) {
                        ctx.splitKey.reset();
                    } else {
                        ctx.splitKey.setPages(pageId, rightPageId);
                    }
                    // }
                } finally {
                    if (pageId == rootPage) {
                        rootSplits++; // debug
                        splitsByLevel[currentLevel]++;
                        currentLevel++;

                        int newLeftId = freePageManager.getFreePage(ctx.metaFrame);
                        ICachedPage newLeftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeftId),
                                true);
                        incrementPins();
                        newLeftNode.acquireWriteLatch();
                        incrementWriteLatchesAcquired();
                        try {
                            // copy left child to new left child
                            System.arraycopy(node.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0,
                                    newLeftNode.getBuffer().capacity());

                            // initialize new root (leftNode becomes new root)
                            ctx.interiorFrame.setPage(node);
                            ctx.interiorFrame.initBuffer((byte) (ctx.interiorFrame.getLevel() + 1));

                            ctx.splitKey.setLeftPage(newLeftId);

                            ctx.interiorFrame.insert(ctx.splitKey.getLeftTuple(), interiorCmp);
                            ctx.interiorFrame.insert(ctx.splitKey.getRightTuple(), interiorCmp);

                            incrementGlobalNsn();
                            int newNsn = getGlobalNsn();
                            ctx.interiorFrame.setPageLsn(newNsn);
                            ctx.interiorFrame.setPageNsn(newNsn);
                        } finally {
                            newLeftNode.releaseWriteLatch();
                            incrementWriteLatchesReleased();
                            bufferCache.unpin(newLeftNode);
                            incrementUnpins();
                        }

                        ctx.splitKey.reset();
                    }
                    rightNode.releaseWriteLatch();
                    incrementWriteLatchesReleased();
                    bufferCache.unpin(rightNode);
                    incrementUnpins();
                }
                break;
            }
        }
    }
    
    public void updateParentForInsert(RTreeOpContext ctx) throws Exception {
        int parentId = ctx.pathList.getLastPageId();
        ICachedPage parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
        incrementPins();
        parentNode.acquireWriteLatch();
        incrementWriteLatchesAcquired();
        ctx.interiorFrame.setPage(parentNode);
        boolean foundParent = true;

        if (ctx.interiorFrame.getPageLsn() != ctx.pathList.getLastPageLsn()) {
            foundParent = false;
            while (true) {
                if (ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), interiorCmp) != -1) {
                    // found the parent
                    foundParent = true;
                    break;
                }
                int rightPage = ctx.interiorFrame.getRightPage();
                parentNode.releaseWriteLatch();
                incrementWriteLatchesReleased();
                bufferCache.unpin(parentNode);
                incrementUnpins();

                if (rightPage == -1) {
                    break;
                }

                parentId = rightPage;
                parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
                incrementPins();
                parentNode.acquireWriteLatch();
                incrementWriteLatchesAcquired();
                ctx.interiorFrame.setPage(parentNode);
            }
        }
        if (foundParent) {
            ctx.interiorFrame.adjustKey(ctx.splitKey.getLeftTuple(), -1, interiorCmp);
            System.out.println(ctx.name + " Trying to insert from updateParentForInsert in pageId: " + parentId);
            insertTuple(parentNode, parentId, ctx.splitKey.getRightTuple(), ctx, ctx.interiorFrame.isLeaf());
            ctx.pathList.removeLast();

            parentNode.releaseWriteLatch();
            incrementWriteLatchesReleased();
            bufferCache.unpin(parentNode);
            incrementUnpins();
            return;
        }

        // very rare situation when the there is a root split, do an exhaustive
        // breadth-first traversal looking for the parent tuple

        ctx.pathList.clear();
        ctx.traverseList.clear();
        findPath(ctx);
        updateParentForInsert(ctx);
    }

    public void findPath(RTreeOpContext ctx) throws Exception {
        int pageId = rootPage;
        int parentIndex = -1;
        int parentLsn = 0;
        int pageLsn, pageIndex;
        ctx.traverseList.add(pageId, -1, parentIndex);
        while (!ctx.traverseList.isLast()) {
            pageId = ctx.traverseList.getFirstPageId();
            parentIndex = ctx.traverseList.getFirstParentIndex();

            ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            incrementPins();
            node.acquireReadLatch();
            incrementReadLatchesAcquired();
            ctx.interiorFrame.setPage(node);
            pageLsn = ctx.interiorFrame.getPageLsn();
            pageIndex = ctx.traverseList.first();
            ctx.traverseList.setPageLsn(pageIndex, pageLsn);

            ctx.traverseList.moveFirst();

            if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                int rightPage = ctx.interiorFrame.getRightPage();
                if (rightPage != -1) {
                    ctx.traverseList.add(rightPage, -1, parentIndex);
                }
            }
            parentLsn = pageLsn;

            if (ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), ctx.traverseList, pageIndex,
                    interiorCmp) != -1) {
                fillPath(ctx, pageIndex);

                node.releaseReadLatch();
                incrementReadLatchesReleased();
                bufferCache.unpin(node);
                incrementUnpins();
                return;
            }
            node.releaseReadLatch();
            incrementReadLatchesReleased();
            bufferCache.unpin(node);
            incrementUnpins();
        }
    }

    public void fillPath(RTreeOpContext ctx, int pageIndex) throws Exception {
        if (pageIndex != -1) {
            fillPath(ctx, ctx.traverseList.getParentIndex(pageIndex));
            ctx.pathList.add(ctx.traverseList.getPageId(pageIndex), ctx.traverseList.getPageLsn(pageIndex), -1);
        }
    }

    public void delete(ITupleReference tuple, RTreeOpContext ctx) throws Exception {
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.splitKey.reset();
        ctx.splitKey.getLeftTuple().setFieldCount(interiorCmp.getFieldCount());
        ctx.interiorFrame.setPageTupleFieldCount(interiorCmp.getFieldCount());
        ctx.leafFrame.setPageTupleFieldCount(leafCmp.getFieldCount());

        int tupleIndex = findTupleToDelete(ctx);

        if (tupleIndex != -1) {
            int pageId = ctx.pathList.getLastPageId();
            ctx.pathList.removeLast();
            deleteTuple(pageId, tupleIndex, ctx);

            while (true) {
                if (ctx.splitKey.getLeftPageBuffer() != null) {
                    updateParentForDelete(ctx);
                } else {
                    break;
                }
            }

            ctx.leafFrame.getPage().releaseWriteLatch();
            incrementWriteLatchesReleased();
            bufferCache.unpin(ctx.leafFrame.getPage());
            incrementUnpins();
        }
    }

    public void updateParentForDelete(RTreeOpContext ctx) throws Exception {
        int parentId = ctx.pathList.getLastPageId();
        ICachedPage parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
        incrementPins();
        parentNode.acquireWriteLatch();
        incrementWriteLatchesAcquired();
        ctx.interiorFrame.setPage(parentNode);
        boolean foundParent = true;
        int tupleIndex = -1;

        if (ctx.interiorFrame.getPageLsn() != ctx.pathList.getLastPageLsn()) {
            foundParent = false;
            while (true) {
                tupleIndex = ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), interiorCmp);
                if (tupleIndex != -1) {
                    // found the parent
                    foundParent = true;
                    break;
                }
                int rightPage = ctx.interiorFrame.getRightPage();
                parentNode.releaseWriteLatch();
                incrementWriteLatchesReleased();
                bufferCache.unpin(parentNode);
                incrementUnpins();

                if (rightPage == -1) {
                    break;
                }

                parentId = rightPage;
                parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
                incrementPins();
                parentNode.acquireWriteLatch();
                incrementWriteLatchesAcquired();
                ctx.interiorFrame.setPage(parentNode);
            }
        }
        if (foundParent) {
            if (tupleIndex == -1) {
                tupleIndex = ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), interiorCmp);
            }
            boolean recomputeMBR = ctx.interiorFrame.recomputeMBR(ctx.splitKey.getLeftTuple(), tupleIndex, interiorCmp);

            if (recomputeMBR) {
                ctx.interiorFrame.adjustKey(ctx.splitKey.getLeftTuple(), tupleIndex, interiorCmp);
                ctx.pathList.removeLast();

                ctx.splitKey.reset();
                if (!ctx.pathList.isEmpty()) {
                    ctx.interiorFrame.computeMBR(ctx.splitKey, interiorCmp);
                    ctx.splitKey.setLeftPage(parentId);
                }
            } else {
                ctx.pathList.removeLast();
                ctx.splitKey.reset();
            }

            parentNode.releaseWriteLatch();
            incrementWriteLatchesReleased();
            bufferCache.unpin(parentNode);
            incrementUnpins();
            return;
        }

        // very rare situation when the there is a root split, do an exhaustive
        // breadth-first traversal looking for the parent tuple

        ctx.pathList.clear();
        ctx.traverseList.clear();
        findPath(ctx);
        updateParentForDelete(ctx);
    }

    public int findTupleToDelete(RTreeOpContext ctx) throws Exception {

        ctx.traverseList.add(rootPage, -1, -1);
        ctx.pathList.add(rootPage, -1, ctx.traverseList.size() - 1);

        while (!ctx.pathList.isEmpty()) {
            int pageId = ctx.pathList.getLastPageId();
            int parentLsn = ctx.pathList.getLastPageLsn();
            int pageIndex = ctx.pathList.getLastPageIndex();
            ctx.pathList.removeLast();
            ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            incrementPins();
            node.acquireReadLatch();
            incrementReadLatchesAcquired();
            ctx.interiorFrame.setPage(node);
            boolean isLeaf = ctx.interiorFrame.isLeaf();
            int pageLsn = ctx.interiorFrame.getPageLsn();
            int parentIndex = ctx.traverseList.getParentIndex(pageIndex);

            if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                // Concurrent split detected, we need to visit the right page
                int rightPage = ctx.interiorFrame.getRightPage();
                if (rightPage != -1) {
                    ctx.traverseList.add(rightPage, parentLsn, parentIndex);
                    ctx.pathList.add(rightPage, parentLsn, ctx.traverseList.size() - 1);
                }
            }

            if (!isLeaf) {
                parentLsn = pageLsn;
                for (int i = 0; i < ctx.interiorFrame.getTupleCount(); i++) {
                    int childPageId = ctx.interiorFrame.getChildPageIdIfIntersect(ctx.tuple, i, interiorCmp);
                    if (childPageId != -1) {
                        ctx.traverseList.add(childPageId, parentLsn, pageIndex);
                        ctx.pathList.add(childPageId, parentLsn, ctx.traverseList.size() - 1);
                    }
                }
            } else {
                ctx.leafFrame.setPage(node);
                int tupleIndex = ctx.leafFrame.findTupleIndex(ctx.tuple, leafCmp);
                if (tupleIndex != -1) {

                    node.releaseReadLatch();
                    incrementReadLatchesReleased();
                    bufferCache.unpin(node);
                    incrementUnpins();

                    node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                    incrementPins();
                    node.acquireWriteLatch();
                    incrementWriteLatchesAcquired();
                    ctx.leafFrame.setPage(node);

                    if (ctx.leafFrame.getPageLsn() != pageLsn) {
                        // The page was changed while we unlocked it

                        tupleIndex = ctx.leafFrame.findTupleIndex(ctx.tuple, leafCmp);
                        if (tupleIndex == -1) {
                            ctx.traverseList.add(pageId, parentLsn, parentIndex);
                            ctx.pathList.add(pageId, parentLsn, ctx.traverseList.size() - 1);
                        }
                    } else {
                        ctx.pathList.clear();
                        fillPath(ctx, pageIndex);
                        return tupleIndex;
                    }
                }
            }
            node.releaseReadLatch();
            incrementReadLatchesReleased();
            bufferCache.unpin(node);
            incrementUnpins();
        }
        return -1;
    }

    public void deleteTuple(int pageId, int tupleIndex, RTreeOpContext ctx) throws Exception {
        ctx.leafFrame.delete(tupleIndex, leafCmp);

        // if the page is empty, just leave it there for future inserts
        if (ctx.leafFrame.getTupleCount() > 0) {
            ctx.leafFrame.computeMBR(ctx.splitKey, leafCmp);
            ctx.splitKey.setLeftPage(pageId);
        }
    }

    public void search(Stack<Integer> s, ITupleReference tuple, RTreeOpContext ctx, ArrayList<Rectangle> results)
            throws Exception {
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.interiorFrame.setPageTupleFieldCount(interiorCmp.getFieldCount());
        ctx.leafFrame.setPageTupleFieldCount(leafCmp.getFieldCount());
        s.push(rootPage);
        while (!s.isEmpty()) {
            int pageId = s.pop();
            ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            incrementPins();
            node.acquireReadLatch();
            incrementReadLatchesAcquired();

            try {

                ctx.interiorFrame.setPage(node);
                boolean isLeaf = ctx.interiorFrame.isLeaf();
                int tupleCount = ctx.interiorFrame.getTupleCount();

                if (!isLeaf) {
                    for (int i = 0; i < tupleCount; i++) {
                        // check intersection, if intersect, call search
                        int childPageId = ctx.interiorFrame.getChildPageIdIfIntersect(ctx.tuple, i, interiorCmp);
                        if (childPageId != -1) {
                            s.push(childPageId);
                        }
                    }

                } else {
                    for (int i = 0; i < tupleCount; i++) {
                        ctx.leafFrame.setPage(node);

                        // check intersection, if intersect, add the tuple to
                        // the
                        // result set
                        Rectangle rec = ctx.leafFrame.intersect(ctx.tuple, i, leafCmp);
                        if (rec != null) {
                            // add the tuple to the result set
                            results.add(rec);
                        }
                    }

                }

            } finally {
                node.releaseReadLatch();
                incrementReadLatchesReleased();
                bufferCache.unpin(node);
                incrementUnpins();
            }
        }
    }

    public IRTreeFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public IRTreeFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public MultiComparator getInteriorCmp() {
        return interiorCmp;
    }

    public MultiComparator getLeafCmp() {
        return leafCmp;
    }

    public IFreePageManager getFreePageManager() {
        return freePageManager;
    }
}
