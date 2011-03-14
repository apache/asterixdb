package edu.uci.ics.hyracks.storage.am.rtree.impls;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
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
    private final int metaDataPage = 0; // page containing meta data, e.g.,
                                        // maxPage
    private final int rootPage = 1; // the root page never changes

    private final IFreePageManager freePageManager;
    private final IBufferCache bufferCache;
    private int fileId;

    private final IRTreeFrameFactory interiorFrameFactory;
    private final IRTreeFrameFactory leafFrameFactory;
    private final MultiComparator interiorCmp;
    private final MultiComparator leafCmp;

    public long pins = 0;
    public long unpins = 0;
    public byte currentLevel = 0;

    public RTree(IBufferCache bufferCache, IFreePageManager freePageManager, IRTreeFrameFactory interiorFrameFactory,
            IRTreeFrameFactory leafFrameFactory, MultiComparator interiorCmp, MultiComparator leafCmp) {
        this.bufferCache = bufferCache;
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.interiorCmp = interiorCmp;
        this.leafCmp = leafCmp;
    }

    public void printTree(IRTreeFrame leafFrame, IRTreeFrame interiorFrame, ISerializerDeserializer[] fields)
            throws Exception {
        printTree(rootPage, null, false, leafFrame, interiorFrame, fields);
    }

    public void printTree(int pageId, ICachedPage parent, boolean unpin, IRTreeFrame leafFrame,
            IRTreeFrame interiorFrame, ISerializerDeserializer[] fields) throws Exception {

        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        pins++;

        try {
            if (parent != null && unpin == true) {
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

                //System.out.println("num of children for page id " + pageId + " is: "+ children.size());
                for (int i = 0; i < children.size(); i++) {
                    printTree(children.get(i), node, i == children.size() - 1, leafFrame, interiorFrame, fields);
                }
                //System.out.println();
            } else {
                bufferCache.unpin(node);
                unpins++;
            }
        } catch (Exception e) {
            bufferCache.unpin(node);
            unpins++;
            e.printStackTrace();
        }
    }

    public void create(int fileId, IRTreeFrame leafFrame, ITreeIndexMetaDataFrame metaFrame) throws Exception {
        if (created)
            return;

        // initialize meta data page
        ICachedPage metaNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, metaDataPage), false);
        pins++;

        metaNode.acquireWriteLatch();
        try {
            metaFrame.setPage(metaNode);
            metaFrame.initBuffer((byte) -1);
            metaFrame.setMaxPage(rootPage);
        } finally {
            bufferCache.unpin(metaNode);
            unpins++;
        }

        // initialize root page
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
        pins++;

        try {
            leafFrame.setPage(rootNode);
            leafFrame.initBuffer((byte) 0);
        } finally {
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
        return new RTreeOpContext(op, interiorFrame, leafFrame, metaFrame, 6);
    }

    private void createNewRoot(RTreeOpContext ctx) throws Exception {
        currentLevel++;

        // make sure the root is always at the same level
        ICachedPage leftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, ctx.leftSplitKey.getPage()),
                false);
        pins++;
        try {
            ICachedPage rightNode = bufferCache.pin(
                    BufferedFileHandle.getDiskPageId(fileId, ctx.rightSplitKey.getPage()), false);
            pins++;
            try {
                int newLeftId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage newLeftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeftId), true);
                pins++;
                try {
                    // copy left child to new left child
                    System.arraycopy(leftNode.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0, newLeftNode
                            .getBuffer().capacity());

                    // initialize new root (leftNode becomes new root)
                    ctx.interiorFrame.setPage(leftNode);
                    ctx.interiorFrame.initBuffer((byte) (ctx.interiorFrame.getLevel() + 1));

                    ctx.leftSplitKey.setPage(newLeftId);
                    
                    ctx.interiorFrame.insert(ctx.leftSplitKey.getTuple(), interiorCmp);
                    ctx.interiorFrame.insert(ctx.rightSplitKey.getTuple(), interiorCmp);

                    //System.out.println("R Created page id: " + newLeftId + "level: " + ctx.interiorFrame.getLevel());
                    
                    System.out.println("Created new root");

                } finally {
                    bufferCache.unpin(newLeftNode);
                    unpins++;
                }
            } finally {
                bufferCache.unpin(rightNode);
                unpins++;
            }
        } finally {
            bufferCache.unpin(leftNode);
            unpins++;
        }
    }

    public int temp = 0;

    public void insert(ITupleReference tuple, RTreeOpContext ctx) throws Exception {
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.leftSplitKey.reset();
        ctx.rightSplitKey.reset();
        ctx.leftSplitKey.getTuple().setFieldCount(interiorCmp.getFieldCount());
        ctx.rightSplitKey.getTuple().setFieldCount(interiorCmp.getFieldCount());
        ctx.interiorFrame.setPageTupleFieldCount(interiorCmp.getFieldCount());
        temp++;
        insertImpl(rootPage, null, (byte) 0, ctx);

        // we split the root, here is the key for a new root
        if (ctx.leftSplitKey.getBuffer() != null && ctx.rightSplitKey.getBuffer() != null) {
            createNewRoot(ctx);
        }
    }

    public void insertImpl(int pageId, ICachedPage parent, byte desiredLevel, RTreeOpContext ctx) throws Exception {
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        pins++;
        ctx.interiorFrame.setPage(node);

        // latch coupling TODO: check the correctness of this
        if (parent != null) {
            bufferCache.unpin(parent);
            unpins++;
        }
        if (temp >= 36) {
            //System.out.println("HHHHHHHHHHHHHH");
        }
        boolean isLeaf = ctx.interiorFrame.isLeaf();
        //System.out.println("level: " + ctx.interiorFrame.getLevel());
        // the children pointers in the node point to leaves
        if (ctx.interiorFrame.getLevel() > desiredLevel) {
            int childPageId = ctx.interiorFrame.getChildPageId(ctx.getTuple(), interiorCmp);
            //System.out.println("PAGE ID: " + childPageId);
            insertImpl(childPageId, node, desiredLevel, ctx);

            if (ctx.leftSplitKey.getBuffer() != null && ctx.rightSplitKey.getBuffer() != null) { // TODO:
                                                                                                 // checking
                                                                                                 // one
                                                                                                 // is
                                                                                                 // enough?
                node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                pins++;
                try {
                    ctx.interiorFrame.setPage(node);
                    ctx.interiorFrame.adjustTuple(ctx.leftSplitKey.getTuple(), interiorCmp);
                    insertTuple(pageId, ctx.rightSplitKey.getTuple(), ctx, isLeaf);
                    // RTreeSplitKey splitKey =
                    // ctx.rightSplitKey.duplicate(ctx.interiorFrame.getTupleWriter().createTupleReference());
                    // insertTuple(pageId, splitKey.getTuple(), ctx, isLeaf);
                } finally {
                    bufferCache.unpin(node);
                    unpins++;
                }
            }
        } else {
            try {
                if (isLeaf) {
                    ctx.leafFrame.setPage(node);
                    ctx.leafFrame.setPageTupleFieldCount(leafCmp.getFieldCount());
                }
                insertTuple(pageId, ctx.getTuple(), ctx, isLeaf);
            } finally {
                bufferCache.unpin(node);
                unpins++;
            }
        }
    }

    private void insertTuple(int pageId, ITupleReference tuple, RTreeOpContext ctx, boolean isLeaf) throws Exception {
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
                ctx.leftSplitKey.reset();
                ctx.rightSplitKey.reset();
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
                ctx.leftSplitKey.reset();
                ctx.rightSplitKey.reset();
                break;
            }

            case INSUFFICIENT_SPACE: {
                int rightPageId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightPageId), true);
                pins++;

                try {
                    IRTreeFrame rightFrame;
                    int ret;
                    if (!isLeaf) {
                        rightFrame = interiorFrameFactory.getFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) ctx.interiorFrame.getLevel());
                        rightFrame.setPageTupleFieldCount(interiorCmp.getFieldCount());
                        ret = ctx.interiorFrame.split(rightFrame, tuple, interiorCmp, ctx.leftSplitKey,
                                ctx.rightSplitKey);
                        rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
                        ctx.interiorFrame.setPageLsn(ctx.interiorFrame.getPageLsn() + 1);

//                         System.out.println("I Created page id: " +
//                         rightPageId + "level: "
//                         + ctx.interiorFrame.getLevel());
                    } else {
                        rightFrame = leafFrameFactory.getFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) 0);
                        rightFrame.setPageTupleFieldCount(leafCmp.getFieldCount());
                        ret = ctx.leafFrame.split(rightFrame, tuple, leafCmp, ctx.leftSplitKey, ctx.rightSplitKey);
                        rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
                        ctx.leafFrame.setPageLsn(ctx.leafFrame.getPageLsn() + 1);

//                         System.out.println("L Created page id: " +
//                         rightPageId + "level: " + 0);
                    }

                    if (ret != 0) {
                        ctx.leftSplitKey.reset();
                        ctx.rightSplitKey.reset();
                    } else {
                        ctx.leftSplitKey.setPage(pageId);
                        ctx.rightSplitKey.setPage(rightPageId);
                    }

                } finally {
                    bufferCache.unpin(rightNode);
                    unpins++;
                }
                // System.out.println("Splitted");
                break;
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
