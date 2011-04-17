package edu.uci.ics.hyracks.storage.am.rtree.impls;

import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

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

    private void createNewRoot(RTreeOpContext ctx) throws Exception {
        rootSplits++; // debug
        splitsByLevel[currentLevel]++;
        currentLevel++;

        // make sure the root is always at the same level
        ICachedPage leftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, ctx.splitKey.getLeftPage()),
                false);
        pins++;
        leftNode.acquireWriteLatch(); // TODO: think about whether latching is
                                      // really required
        writeLatchesAcquired++;
        try {
            ICachedPage rightNode = bufferCache.pin(
                    BufferedFileHandle.getDiskPageId(fileId, ctx.splitKey.getRightPage()), false);
            pins++;
            rightNode.acquireWriteLatch(); // TODO: think about whether latching
                                           // is really required
            writeLatchesAcquired++;
            try {
                int newLeftId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage newLeftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeftId), true);
                pins++;
                newLeftNode.acquireWriteLatch(); // TODO: think about whether
                                                 // latching is really required
                writeLatchesAcquired++;
                try {
                    // copy left child to new left child
                    System.arraycopy(leftNode.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0, newLeftNode
                            .getBuffer().capacity());

                    // initialize new root (leftNode becomes new root)
                    ctx.interiorFrame.setPage(leftNode);
                    ctx.interiorFrame.initBuffer((byte) (ctx.interiorFrame.getLevel() + 1));

                    ctx.splitKey.setLeftPage(newLeftId);

                    ctx.interiorFrame.insert(ctx.splitKey.getLeftTuple(), interiorCmp);
                    ctx.interiorFrame.insert(ctx.splitKey.getRightTuple(), interiorCmp);
                } finally {
                    newLeftNode.releaseWriteLatch();
                    writeLatchesReleased++;
                    bufferCache.unpin(newLeftNode);
                    unpins++;
                }
            } finally {
                rightNode.releaseWriteLatch();
                writeLatchesReleased++;
                bufferCache.unpin(rightNode);
                unpins++;
            }
        } finally {
            leftNode.releaseWriteLatch();
            writeLatchesReleased++;
            bufferCache.unpin(leftNode);
            unpins++;
        }
    }

    private final void acquireLatch(ICachedPage node, TreeIndexOp op, boolean isLeaf) {
        if (isLeaf && (op.equals(TreeIndexOp.TI_INSERT) || op.equals(TreeIndexOp.TI_DELETE))) {
            node.acquireWriteLatch();
            writeLatchesAcquired++;
        } else {
            node.acquireReadLatch();
            readLatchesAcquired++;
        }
    }

    // public void insert(ITupleReference tuple, RTreeOpContext ctx) throws
    // Exception {
    // ctx.reset();
    // ctx.setTuple(tuple);
    // ctx.splitKey.reset();
    // ctx.splitKey.getLeftTuple().setFieldCount(interiorCmp.getFieldCount());
    // ctx.splitKey.getRightTuple().setFieldCount(interiorCmp.getFieldCount());
    // ctx.interiorFrame.setPageTupleFieldCount(interiorCmp.getFieldCount());
    // for (int i = 0; i < currentLevel; i++) {
    // ctx.overflowArray.add((byte) 0);
    // }
    // insertImpl(rootPage, null, (byte) 0, ctx);
    //
    // // we split the root, here is the key for a new root
    // if (ctx.splitKey.getLeftPageBuffer() != null) {
    // createNewRoot(ctx);
    // }
    // }

    // public void insertImpl(int pageId, ICachedPage parent, byte desiredLevel,
    // RTreeOpContext ctx) throws Exception {
    // ICachedPage node =
    // bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
    // pins++;
    // ctx.interiorFrame.setPage(node);
    // boolean isLeaf = ctx.interiorFrame.isLeaf();
    // acquireLatch(node, ctx.op, isLeaf);
    //
    // // latch coupling TODO: check the correctness of this
    // if (parent != null) {
    // parent.releaseReadLatch();
    // readLatchesReleased++;
    // bufferCache.unpin(parent);
    // unpins++;
    // }
    //
    // if (ctx.interiorFrame.getLevel() > desiredLevel) {
    // int childPageId = ctx.interiorFrame.checkEnlargement(ctx.getTuple(),
    // ctx.tupleEntries1, ctx.nodesMBRs,
    // interiorCmp);
    //
    // insertImpl(childPageId, node, desiredLevel, ctx);
    // if (ctx.splitKey.getLeftPageBuffer() != null) {
    // node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId),
    // false);
    // pins++;
    // node.acquireWriteLatch();
    // writeLatchesAcquired++;
    // try {
    // ctx.interiorFrame.setPage(node);
    // ctx.interiorFrame.adjustKey(ctx.splitKey.getLeftTuple(), interiorCmp);
    // insertTuple(pageId, ctx.splitKey.getRightTuple(), ctx, isLeaf);
    // } finally {
    // node.releaseWriteLatch();
    // writeLatchesReleased++;
    // bufferCache.unpin(node);
    // unpins++;
    // }
    // }
    // } else {
    // try {
    // if (isLeaf) {
    // ctx.leafFrame.setPage(node);
    // ctx.leafFrame.setPageTupleFieldCount(leafCmp.getFieldCount());
    // }
    // insertTuple(pageId, ctx.getTuple(), ctx, isLeaf);
    // } finally {
    // node.releaseWriteLatch();
    // writeLatchesReleased++;
    // bufferCache.unpin(node);
    // unpins++;
    // }
    // }
    // }

    public void insert(ITupleReference tuple, RTreeOpContext ctx) throws Exception {
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.splitKey.reset();
        ctx.splitKey.getLeftTuple().setFieldCount(interiorCmp.getFieldCount());
        ctx.splitKey.getRightTuple().setFieldCount(interiorCmp.getFieldCount());
        ctx.interiorFrame.setPageTupleFieldCount(interiorCmp.getFieldCount());
        for (int i = 0; i < currentLevel; i++) {
            ctx.overflowArray.add((byte) 0);
        }

        ICachedPage leafNode = findLeaf(ctx);

        int pageId = ctx.path.getLast();
        ctx.path.removeLast();
        ctx.pageLsns.removeLast();
        insertTuple(leafNode, pageId, ctx.getTuple(), ctx, ctx.leafFrame.isLeaf());

        while (true) {
            if (ctx.splitKey.getLeftPageBuffer() != null) {
                updateParent(ctx);
            } else {
                break;
            }
        }

        leafNode.releaseWriteLatch();
        writeLatchesReleased++;
        bufferCache.unpin(leafNode);
        unpins++;
    }

    public void updateParent(RTreeOpContext ctx) throws Exception {
        int parentId = ctx.path.getLast();
        ICachedPage parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
        pins++;
        parentNode.acquireWriteLatch();
        writeLatchesAcquired++;
        ctx.interiorFrame.setPage(parentNode);
        boolean foundParent = true;

        if (ctx.interiorFrame.getPageLsn() != ctx.pageLsns.getLast()) {
            foundParent = false;
            while (true) {
                if (ctx.interiorFrame.findTuple(ctx.splitKey.getLeftTuple(), interiorCmp) != -1) {
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
            ctx.interiorFrame.adjustKey(ctx.splitKey.getLeftTuple(), interiorCmp);
            insertTuple(parentNode, parentId, ctx.splitKey.getRightTuple(), ctx, ctx.interiorFrame.isLeaf());
            ctx.path.removeLast();
            ctx.pageLsns.removeLast();

            parentNode.releaseWriteLatch();
            writeLatchesReleased++;
            bufferCache.unpin(parentNode);
            unpins++;
            return;
        }

        // very rare situation when the there is a root split, do an exhaustive
        // breadth-first traversal looking for the parent tuple

        ctx.path.clear();
        ctx.pageLsns.clear();
        ctx.findPathList.clear();
        findPath(ctx);
        updateParent(ctx);
    }

    public void findPath(RTreeOpContext ctx) throws Exception {
        int pageId = rootPage;
        int parentIndex = -1;
        int parentLsn = 0;
        int pageLsn, pageIndex;
        ctx.findPathList.add(pageId, parentIndex);

        while (!ctx.findPathList.isLast()) {
            pageId = ctx.findPathList.getFirstPageId();
            parentIndex = ctx.findPathList.getFirstParentIndex();
            ctx.findPathList.moveFirst();
            ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            pins++;
            node.acquireReadLatch();
            readLatchesAcquired++;
            ctx.interiorFrame.setPage(node);
            pageLsn = ctx.interiorFrame.getPageLsn();
            pageIndex = ctx.findPathList.size() - 1;
            ctx.findPathList.setPageLsn(pageIndex, pageLsn);

            if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                ctx.findPathList.add(ctx.interiorFrame.getRightPage(), parentIndex);
            }
            parentLsn = pageLsn;
            
            if (ctx.interiorFrame.findTuple(ctx.splitKey.getLeftTuple(), ctx.findPathList, pageIndex, interiorCmp) != -1) {
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
            fillPath(ctx, ctx.findPathList.getParentIndex(pageIndex));
            ctx.path.add(ctx.findPathList.getPageId(pageIndex));
            ctx.pageLsns.add(ctx.findPathList.getPageLsn(pageIndex));
        }
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
            ctx.path.add(pageId);
            pageLsn = ctx.interiorFrame.getPageLsn();
            ctx.pageLsns.add(pageLsn);

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

                ctx.path.removeLast();
                ctx.pageLsns.removeLast();
                writeLatched = false;
                continue;
            }
            parentLsn = pageLsn;

            if (!isLeaf) {
                // checkEnlargement must be called *before* getBestChildPageId
                boolean needsEnlargement = ctx.interiorFrame.checkEnlargement(ctx.getTuple(), ctx.tupleEntries1,
                        ctx.nodesMBRs, interiorCmp);
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
                            // retry
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
            } else {
                ctx.leafFrame.setPage(node);
                ctx.leafFrame.setPageTupleFieldCount(leafCmp.getFieldCount());
                return node;
            }
        }

    }

    private void insertTuple(ICachedPage leafNode, int pageId, ITupleReference tuple, RTreeOpContext ctx, boolean isLeaf)
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
                    // if the level is not the root level and this is the
                    // first overflow treatment in the given level during the
                    // insertion of one data rectangle
                    /*
                     * if (ctx.overflowArray.get((int)
                     * ctx.interiorFrame.getLevel()) == 0 && pageId != rootPage)
                     * { if (!isLeaf) { ctx.overflowArray.set((int)
                     * ctx.interiorFrame.getLevel(), (byte) 1);
                     * ctx.interiorFrame.reinsert(tuple,
                     * ctx.nodesMBRs[ctx.interiorFrame.getLevel()], ctx.entries,
                     * ctx.splitKey, interiorCmp); } else {
                     * ctx.overflowArray.set(0, (byte) 1);
                     * ctx.leafFrame.reinsert(tuple, ctx.nodesMBRs[0],
                     * ctx.entries, ctx.splitKey, leafCmp); } } else {
                     */
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
                            System.arraycopy(leafNode.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0,
                                    newLeftNode.getBuffer().capacity());

                            // initialize new root (leftNode becomes new root)
                            ctx.interiorFrame.setPage(leafNode);
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
