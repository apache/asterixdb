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
        pins++;
        node.acquireReadLatch();
        readLatchesAcquired++;

        try {
            if (parent != null && unpin == true) {
                parent.releaseReadLatch();
                readLatchesReleased++;
                bufferCache.unpin(parent);
                unpins++;
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
                readLatchesReleased++;
                bufferCache.unpin(node);
                unpins++;
            }
        } catch (Exception e) {
            node.releaseReadLatch();
            readLatchesReleased++;
            bufferCache.unpin(node);
            unpins++;
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
            pins++;

            rootNode.acquireWriteLatch();
            writeLatchesAcquired++;
            try {
                leafFrame.setPage(rootNode);
                leafFrame.initBuffer((byte) 0);
            } finally {
                rootNode.releaseWriteLatch();
                writeLatchesReleased++;
                bufferCache.unpin(rootNode);
                unpins++;
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
            ITreeIndexMetaDataFrame metaFrame) {
        // TODO: figure out better tree-height hint
        return new RTreeOpContext(op, interiorFrame, leafFrame, metaFrame, 8, dim);
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
        insertTuple(leafNode, pageId, ctx.getTuple(), ctx, true);

        while (true) {
            if (ctx.splitKey.getLeftPageBuffer() != null) {
                updateParentForInsert(ctx);
            } else {
                break;
            }
        }

        leafNode.releaseWriteLatch();
        writeLatchesReleased++;
        bufferCache.unpin(leafNode);
        unpins++;
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
                pins++;
                ctx.interiorFrame.setPage(node);
                isLeaf = ctx.interiorFrame.isLeaf();
                if (isLeaf) {
                    node.acquireWriteLatch();
                    writeLatchesAcquired++;
                    writeLatched = true;
                } else {
                    // Be optimistic and grab read latch first. We will swap it
                    // to write latch if we need to enlarge the best child
                    // tuple.
                    node.acquireReadLatch();
                    readLatchesAcquired++;
                }
            }
            pageLsn = ctx.interiorFrame.getPageLsn();
            ctx.pathList.add(pageId, pageLsn, -1);

            if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                // Concurrent split detected, go back to parent and re-choose
                // the best child
                if (isLeaf) {
                    node.releaseWriteLatch();
                    writeLatchesReleased++;
                    bufferCache.unpin(node);
                    unpins++;
                } else {
                    node.releaseReadLatch();
                    readLatchesReleased++;
                    bufferCache.unpin(node);
                    unpins++;
                }

                ctx.pathList.removeLast();

                pageId = ctx.pathList.getLastPageId();
                if (pageId != rootPage) {
                    parentLsn = ctx.pathList.getPageLsn(ctx.pathList.size() - 2);
                }

                writeLatched = false;
                continue;
            }

            if (!isLeaf) {
                // checkEnlargement must be called *before* getBestChildPageId
                boolean needsEnlargement = ctx.interiorFrame.findBestChild(ctx.getTuple(), ctx.tupleEntries1,
                        interiorCmp);
                int childPageId = ctx.interiorFrame.getBestChildPageId(interiorCmp);

                if (needsEnlargement) {
                    if (!writeLatched) {
                        node.releaseReadLatch();
                        readLatchesReleased++;
                        // TODO: do we need to un-pin and pin again?
                        bufferCache.unpin(node);
                        unpins++;

                        node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                        pins++;
                        node.acquireWriteLatch();
                        writeLatchesAcquired++;
                        ctx.interiorFrame.setPage(node);
                        writeLatched = true;

                        if (ctx.interiorFrame.getPageLsn() != pageLsn) {
                            // The page was changed while we unlocked it; thus,
                            // retry (re-choose best child)
                            ctx.pathList.removeLast();
                            continue;
                        }
                    }

                    // We don't need to reset the frameTuple because it is
                    // already pointing to the best child
                    ctx.interiorFrame.enlarge(ctx.getTuple(), interiorCmp);

                    node.releaseWriteLatch();
                    writeLatchesReleased++;
                    bufferCache.unpin(node);
                    unpins++;
                    writeLatched = false;
                } else {
                    node.releaseReadLatch();
                    readLatchesReleased++;
                    bufferCache.unpin(node);
                    unpins++;
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
        } else {
            spaceStatus = ctx.leafFrame.hasSpaceInsert(ctx.getTuple(), leafCmp);
        }

        switch (spaceStatus) {
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                if (!isLeaf) {
                    ctx.interiorFrame.insert(tuple, interiorCmp);
                } else {
                    ctx.leafFrame.insert(tuple, leafCmp);
                }
                ctx.splitKey.reset();
                break;
            }

            case SUFFICIENT_SPACE: {
                if (!isLeaf) {
                    ctx.interiorFrame.compact(interiorCmp);
                    ctx.interiorFrame.insert(tuple, interiorCmp);
                } else {
                    ctx.leafFrame.compact(leafCmp);
                    ctx.leafFrame.insert(tuple, leafCmp);
                }
                ctx.splitKey.reset();
                break;
            }

            case INSUFFICIENT_SPACE: {
                System.out.println("Split");
                int rightPageId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightPageId), true);
                pins++;
                rightNode.acquireWriteLatch();
                writeLatchesAcquired++;

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
                        rightFrame.setPageLsn(ctx.interiorFrame.getPageNsn());
                        incrementGlobalNsn();
                        ctx.interiorFrame.setPageLsn(getGlobalNsn());
                    } else {
                        splitsByLevel[0]++; // debug
                        rightFrame = leafFrameFactory.getFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) 0);
                        rightFrame.setPageTupleFieldCount(leafCmp.getFieldCount());
                        ret = ctx.leafFrame.split(rightFrame, tuple, leafCmp, ctx.splitKey, ctx.tupleEntries1,
                                ctx.tupleEntries2, ctx.rec);
                        rightFrame.setPageLsn(ctx.leafFrame.getPageNsn());
                        incrementGlobalNsn();
                        ctx.leafFrame.setPageLsn(getGlobalNsn());
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
                        pins++;
                        newLeftNode.acquireWriteLatch();
                        writeLatchesAcquired++;
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
                            ctx.interiorFrame.setPageLsn(getGlobalNsn());
                        } finally {
                            newLeftNode.releaseWriteLatch();
                            writeLatchesReleased++;
                            bufferCache.unpin(newLeftNode);
                            unpins++;
                        }

                        ctx.splitKey.reset();
                    }
                    rightNode.releaseWriteLatch();
                    writeLatchesReleased++;
                    bufferCache.unpin(rightNode);
                    unpins++;
                }
                break;
            }
        }
    }

    public void updateParentForInsert(RTreeOpContext ctx) throws Exception {
        int parentId = ctx.pathList.getLastPageId();
        ICachedPage parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
        pins++;
        parentNode.acquireWriteLatch();
        writeLatchesAcquired++;
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
                writeLatchesReleased++;
                bufferCache.unpin(parentNode);
                unpins++;

                if (rightPage == -1) {
                    break;
                }

                parentId = rightPage;
                parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
                pins++;
                parentNode.acquireWriteLatch();
                writeLatchesAcquired++;
                ctx.interiorFrame.setPage(parentNode);
            }
        }
        if (foundParent) {
            ctx.interiorFrame.adjustKey(ctx.splitKey.getLeftTuple(), -1, interiorCmp);
            insertTuple(parentNode, parentId, ctx.splitKey.getRightTuple(), ctx, ctx.interiorFrame.isLeaf());
            ctx.pathList.removeLast();

            parentNode.releaseWriteLatch();
            writeLatchesReleased++;
            bufferCache.unpin(parentNode);
            unpins++;
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
            pins++;
            node.acquireReadLatch();
            readLatchesAcquired++;
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
                readLatchesReleased++;
                bufferCache.unpin(node);
                unpins++;
                return;
            }
            node.releaseReadLatch();
            readLatchesReleased++;
            bufferCache.unpin(node);
            unpins++;
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
            writeLatchesReleased++;
            bufferCache.unpin(ctx.leafFrame.getPage());
            unpins++;
        }
    }

    public void updateParentForDelete(RTreeOpContext ctx) throws Exception {
        int parentId = ctx.pathList.getLastPageId();
        ICachedPage parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
        pins++;
        parentNode.acquireWriteLatch();
        writeLatchesAcquired++;
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
                writeLatchesReleased++;
                bufferCache.unpin(parentNode);
                unpins++;

                if (rightPage == -1) {
                    break;
                }

                parentId = rightPage;
                parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
                pins++;
                parentNode.acquireWriteLatch();
                writeLatchesAcquired++;
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
            writeLatchesReleased++;
            bufferCache.unpin(parentNode);
            unpins++;
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
            pins++;
            node.acquireReadLatch();
            readLatchesAcquired++;
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
                    readLatchesReleased++;
                    bufferCache.unpin(node);
                    unpins++;

                    node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                    pins++;
                    node.acquireWriteLatch();
                    writeLatchesAcquired++;
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
            readLatchesReleased++;
            bufferCache.unpin(node);
            unpins++;
        }
        return -1;
    }

    public void deleteTuple(int pageId, int tupleIndex, RTreeOpContext ctx) throws Exception {
        ctx.leafFrame.delete(tupleIndex, leafCmp);
        if (ctx.leafFrame.getTupleCount() == 0) {
            ctx.leafFrame.setSmFlag(true);
        } else {
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
            pins++;
            node.acquireReadLatch();
            readLatchesAcquired++;

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
                readLatchesReleased++;
                bufferCache.unpin(node);
                unpins++;
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
