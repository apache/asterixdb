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

package edu.uci.ics.hyracks.storage.am.rtree.impls;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class RTree implements ITreeIndex {

    private boolean created = false;
    private boolean loaded = false;
    private final int rootPage = 1; // the root page never changes

    private final AtomicInteger globalNsn; // Global node sequence number
    private int numOfPages = 1;
    private final ReadWriteLock treeLatch;

    private final IFreePageManager freePageManager;
    private final IBufferCache bufferCache;
    private int fileId;

    private final SearchPredicate diskOrderScanPredicate;
    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final ITreeIndexFrameFactory leafFrameFactory;
    private final int fieldCount;
    private final MultiComparator cmp;

    public int rootSplits = 0;
    public int[] splitsByLevel = new int[500];
    public AtomicLong readLatchesAcquired = new AtomicLong();
    public AtomicLong readLatchesReleased = new AtomicLong();
    public AtomicLong writeLatchesAcquired = new AtomicLong();
    public AtomicLong writeLatchesReleased = new AtomicLong();
    public AtomicLong pins = new AtomicLong();
    public AtomicLong unpins = new AtomicLong();
    public byte currentLevel = 0;

    // TODO: is MultiComparator needed at all?
    public RTree(IBufferCache bufferCache, int fieldCount, MultiComparator cmp, IFreePageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory) {
        this.bufferCache = bufferCache;
        this.fieldCount = fieldCount;
        this.cmp = cmp;
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;        
        globalNsn = new AtomicInteger();
        this.treeLatch = new ReentrantReadWriteLock(true);
        this.diskOrderScanPredicate = new SearchPredicate(null, cmp);
    }

    public void incrementGlobalNsn() {
        globalNsn.incrementAndGet();
    }

    public int getGlobalNsn() {
        return globalNsn.get();
    }

    public void incrementReadLatchesAcquired() {
        readLatchesAcquired.incrementAndGet();
    }

    public void incrementReadLatchesReleased() {
        readLatchesReleased.incrementAndGet();
    }

    public void incrementWriteLatchesAcquired() {
        writeLatchesAcquired.incrementAndGet();
    }

    public void incrementWriteLatchesReleased() {
        writeLatchesReleased.incrementAndGet();
    }

    public void incrementPins() {
        pins.incrementAndGet();
    }

    public void incrementUnpins() {
        unpins.incrementAndGet();
    }

    public String printStats() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("\n");
        strBuilder.append("ROOTSPLITS: " + rootSplits + "\n");
        strBuilder.append("SPLITS BY LEVEL\n");
        for (int i = 0; i < currentLevel; i++) {
            strBuilder.append(String.format("%3d ", i) + String.format("%8d ", splitsByLevel[i]) + "\n");
        }
        strBuilder.append(String.format("READ LATCHES:  %10d %10d\n", readLatchesAcquired.get(),
                readLatchesReleased.get()));
        strBuilder.append(String.format("WRITE LATCHES: %10d %10d\n", writeLatchesAcquired.get(),
                writeLatchesReleased.get()));
        strBuilder.append(String.format("PINS:          %10d %10d\n", pins.get(), unpins.get()));

        strBuilder.append(String.format("Num of Pages:          %10d\n", numOfPages));

        return strBuilder.toString();
    }

    public void printTree(IRTreeFrame leafFrame, IRTreeFrame interiorFrame, ISerializerDeserializer[] fields)
            throws Exception {
        printTree(rootPage, null, false, leafFrame, interiorFrame, fields);
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
                keyString = leafFrame.printKeys(cmp, fields);
            } else {
                keyString = interiorFrame.printKeys(cmp, fields);
            }

            System.out.format(keyString);
            if (!interiorFrame.isLeaf()) {
                ArrayList<Integer> children = ((RTreeNSMFrame) (interiorFrame)).getChildren(cmp);
                for (int i = 0; i < children.size(); i++) {
                    printTree(children.get(i), node, i == children.size() - 1, leafFrame, interiorFrame, fields);
                }
            } else {
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
            throw e;
        }
    }

    @Override
    public void create(int fileId, ITreeIndexFrame leafFrame, ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
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

    @Override
    public RTreeOpContext createOpContext(IndexOp op, ITreeIndexFrame leafFrame, ITreeIndexFrame interiorFrame,
            ITreeIndexMetaDataFrame metaFrame) {
        return new RTreeOpContext(op, (IRTreeLeafFrame) leafFrame, (IRTreeInteriorFrame) interiorFrame, metaFrame, 8);
    }

    @Override
    public void insert(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException, TreeIndexException {
        RTreeOpContext ctx = (RTreeOpContext) ictx;
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.splitKey.reset();
        ctx.splitKey.getLeftTuple().setFieldCount(cmp.getKeyFieldCount());
        ctx.splitKey.getRightTuple().setFieldCount(cmp.getKeyFieldCount());

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j));
            if (c > 0) {
                throw new IllegalArgumentException("The low key point has larger coordinates than the high key point.");
            }
        }

        ICachedPage leafNode = findLeaf(ctx);

        int pageId = ctx.pathList.getLastPageId();
        ctx.pathList.moveLast();
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

    public ICachedPage findLeaf(RTreeOpContext ctx) throws HyracksDataException {
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
                    node.acquireWriteLatch();
                    incrementWriteLatchesAcquired();
                    writeLatched = true;

                    if (!ctx.interiorFrame.isLeaf()) {
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
                    node.acquireReadLatch();
                    incrementReadLatchesAcquired();
                }
            }

            if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                // Concurrent split detected, go back to parent and re-choose
                // the best child
                if (writeLatched) {
                    node.releaseWriteLatch();
                    incrementWriteLatchesReleased();
                    bufferCache.unpin(node);
                    incrementUnpins();
                    writeLatched = false;
                } else {
                    node.releaseReadLatch();
                    incrementReadLatchesReleased();
                    bufferCache.unpin(node);
                    incrementUnpins();
                }

                pageId = ctx.pathList.getLastPageId();
                if (pageId != rootPage) {
                    parentLsn = ctx.pathList.getPageLsn(ctx.pathList.size() - 2);
                }
                ctx.pathList.moveLast();
                continue;
            }

            pageLsn = ctx.interiorFrame.getPageLsn();
            ctx.pathList.add(pageId, pageLsn, -1);

            if (!isLeaf) {
                // findBestChild must be called *before* getBestChildPageId
                ctx.interiorFrame.findBestChild(ctx.getTuple(), cmp);
                int childPageId = ctx.interiorFrame.getBestChildPageId();

                if (!writeLatched) {
                    node.releaseReadLatch();
                    incrementReadLatchesReleased();
                    // TODO: do we need to un-pin and pin again?
                    bufferCache.unpin(node);
                    incrementUnpins();

                    node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                    incrementPins();
                    node.acquireWriteLatch();
                    incrementWriteLatchesAcquired();
                    ctx.interiorFrame.setPage(node);
                    writeLatched = true;

                    if (ctx.interiorFrame.getPageLsn() != pageLsn) {
                        // The page was changed while we unlocked it; thus,
                        // retry (re-choose best child)

                        ctx.pathList.moveLast();
                        continue;
                    }
                }

                // We don't need to reset the frameTuple because it is
                // already pointing to the best child
                ctx.interiorFrame.enlarge(ctx.getTuple(), cmp);

                node.releaseWriteLatch();
                incrementWriteLatchesReleased();
                bufferCache.unpin(node);
                incrementUnpins();
                writeLatched = false;

                pageId = childPageId;
                parentLsn = pageLsn;
            } else {
                ctx.leafFrame.setPage(node);
                return node;
            }
        }
    }

    private void insertTuple(ICachedPage node, int pageId, ITupleReference tuple, RTreeOpContext ctx, boolean isLeaf)
            throws HyracksDataException, TreeIndexException {
    	FrameOpSpaceStatus spaceStatus;
        if (!isLeaf) {
            spaceStatus = ctx.interiorFrame.hasSpaceInsert(tuple, cmp);
        } else {
            spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple, cmp);
        }

        switch (spaceStatus) {
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                if (!isLeaf) {
                    ctx.interiorFrame.insert(tuple, -1);
                    incrementGlobalNsn();
                    ctx.interiorFrame.setPageLsn(getGlobalNsn());
                } else {
                    ctx.leafFrame.insert(tuple, -1);
                    incrementGlobalNsn();
                    ctx.leafFrame.setPageLsn(getGlobalNsn());
                }
                ctx.splitKey.reset();
                break;
            }

            case SUFFICIENT_SPACE: {
                if (!isLeaf) {
                    ctx.interiorFrame.compact(cmp);
                    ctx.interiorFrame.insert(tuple, -1);
                    incrementGlobalNsn();
                    ctx.interiorFrame.setPageLsn(getGlobalNsn());
                } else {
                    ctx.leafFrame.compact(cmp);
                    ctx.leafFrame.insert(tuple, -1);
                    incrementGlobalNsn();
                    ctx.leafFrame.setPageLsn(getGlobalNsn());
                }
                ctx.splitKey.reset();
                break;
            }

            case INSUFFICIENT_SPACE: {
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
                        rightFrame = (IRTreeFrame) interiorFrameFactory.createFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) ctx.interiorFrame.getLevel());
                        //rightFrame.setPageTupleFieldCount(cmp.getKeyFieldCount());
                        ret = ctx.interiorFrame.split(rightFrame, tuple, cmp, ctx.splitKey);
                        ctx.interiorFrame.setRightPage(rightPageId);
                        rightFrame.setPageNsn(ctx.interiorFrame.getPageNsn());
                        incrementGlobalNsn();
                        int newNsn = getGlobalNsn();
                        rightFrame.setPageLsn(newNsn);
                        ctx.interiorFrame.setPageNsn(newNsn);
                        ctx.interiorFrame.setPageLsn(newNsn);
                    } else {
                        splitsByLevel[0]++; // debug
                        rightFrame = (IRTreeFrame) leafFrameFactory.createFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) 0);
                        ret = ctx.leafFrame.split(rightFrame, tuple, cmp, ctx.splitKey);
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

                            ctx.interiorFrame.insert(ctx.splitKey.getLeftTuple(), -1);
                            ctx.interiorFrame.insert(ctx.splitKey.getRightTuple(), -1);

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
                } finally {
                    rightNode.releaseWriteLatch();
                    incrementWriteLatchesReleased();
                    bufferCache.unpin(rightNode);
                    incrementUnpins();
                }
                break;
            }
        }
    }

    public void updateParentForInsert(RTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
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
                if (ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), cmp) != -1) {
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
            ctx.interiorFrame.adjustKey(ctx.splitKey.getLeftTuple(), -1, cmp);
            insertTuple(parentNode, parentId, ctx.splitKey.getRightTuple(), ctx, ctx.interiorFrame.isLeaf());
            ctx.pathList.moveLast();

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

    public void findPath(RTreeOpContext ctx) throws HyracksDataException {
        int pageId = rootPage;
        int parentIndex = -1;
        int parentLsn = 0;
        int pageLsn, pageIndex;
        ctx.traverseList.add(pageId, -1, parentIndex);
        while (!ctx.traverseList.isLast()) {
            pageId = ctx.traverseList.getFirstPageId();
            parentIndex = ctx.traverseList.getFirstPageIndex();

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

            if (ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), ctx.traverseList, pageIndex, cmp) != -1) {
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

    public void fillPath(RTreeOpContext ctx, int pageIndex) {
        if (pageIndex != -1) {
            fillPath(ctx, ctx.traverseList.getPageIndex(pageIndex));
            ctx.pathList.add(ctx.traverseList.getPageId(pageIndex), ctx.traverseList.getPageLsn(pageIndex), -1);
        }
    }

    @Override
    public void delete(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException, TreeIndexException {
        RTreeOpContext ctx = (RTreeOpContext) ictx;
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.splitKey.reset();
        ctx.splitKey.getLeftTuple().setFieldCount(cmp.getKeyFieldCount());

        int tupleIndex = findTupleToDelete(ctx);

        if (tupleIndex != -1) {
            int pageId = ctx.pathList.getLastPageId();
            ctx.pathList.moveLast();
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

    public void updateParentForDelete(RTreeOpContext ctx) throws HyracksDataException {
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
                tupleIndex = ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), cmp);
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
                tupleIndex = ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), cmp);
            }
            boolean recomputeMBR = ctx.interiorFrame.recomputeMBR(ctx.splitKey.getLeftTuple(), tupleIndex, cmp);

            if (recomputeMBR) {
                ctx.interiorFrame.adjustKey(ctx.splitKey.getLeftTuple(), tupleIndex, cmp);
                ctx.pathList.moveLast();

                incrementGlobalNsn();
                ctx.interiorFrame.setPageLsn(getGlobalNsn());

                ctx.splitKey.reset();
                if (!ctx.pathList.isEmpty()) {
                    ctx.interiorFrame.computeMBR(ctx.splitKey, cmp);
                    ctx.splitKey.setLeftPage(parentId);
                }
            } else {
                ctx.pathList.moveLast();
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

    public int findTupleToDelete(RTreeOpContext ctx) throws HyracksDataException {

        ctx.traverseList.add(rootPage, -1, -1);
        ctx.pathList.add(rootPage, -1, ctx.traverseList.size() - 1);

        while (!ctx.pathList.isEmpty()) {
            int pageId = ctx.pathList.getLastPageId();
            int parentLsn = ctx.pathList.getLastPageLsn();
            int pageIndex = ctx.pathList.getLastPageIndex();
            ctx.pathList.moveLast();
            ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            incrementPins();
            node.acquireReadLatch();
            incrementReadLatchesAcquired();
            ctx.interiorFrame.setPage(node);
            boolean isLeaf = ctx.interiorFrame.isLeaf();
            int pageLsn = ctx.interiorFrame.getPageLsn();
            int parentIndex = ctx.traverseList.getPageIndex(pageIndex);
            ctx.traverseList.setPageLsn(pageIndex, pageLsn);

            if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                // Concurrent split detected, we need to visit the right page
                int rightPage = ctx.interiorFrame.getRightPage();
                if (rightPage != -1) {
                    ctx.traverseList.add(rightPage, -1, parentIndex);
                    ctx.pathList.add(rightPage, parentLsn, ctx.traverseList.size() - 1);
                }
            }

            if (!isLeaf) {
                for (int i = 0; i < ctx.interiorFrame.getTupleCount(); i++) {
                    int childPageId = ctx.interiorFrame.getChildPageIdIfIntersect(ctx.tuple, i, cmp);
                    if (childPageId != -1) {
                        ctx.traverseList.add(childPageId, -1, pageIndex);
                        ctx.pathList.add(childPageId, pageLsn, ctx.traverseList.size() - 1);
                    }
                }
            } else {
                ctx.leafFrame.setPage(node);
                int tupleIndex = ctx.leafFrame.findTupleIndex(ctx.tuple, cmp);
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

                        tupleIndex = ctx.leafFrame.findTupleIndex(ctx.tuple, cmp);
                        if (tupleIndex == -1) {
                            ctx.traverseList.add(pageId, -1, parentIndex);
                            ctx.pathList.add(pageId, parentLsn, ctx.traverseList.size() - 1);

                            node.releaseWriteLatch();
                            incrementWriteLatchesReleased();
                            bufferCache.unpin(node);
                            incrementUnpins();
                            continue;
                        } else {
                            ctx.pathList.clear();
                            fillPath(ctx, pageIndex);
                            return tupleIndex;
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

    public void deleteTuple(int pageId, int tupleIndex, RTreeOpContext ctx) throws HyracksDataException {
        ctx.leafFrame.delete(tupleIndex, cmp);
        incrementGlobalNsn();
        ctx.leafFrame.setPageLsn(getGlobalNsn());

        // if the page is empty, just leave it there for future inserts
        if (pageId != rootPage && ctx.leafFrame.getTupleCount() > 0) {
            ctx.leafFrame.computeMBR(ctx.splitKey, cmp);
            ctx.splitKey.setLeftPage(pageId);
        }
    }

    public void search(ITreeIndexCursor cursor, SearchPredicate pred, RTreeOpContext ctx) throws Exception {
        ctx.reset();
        ctx.cursor = cursor;

        cursor.setBufferCache(bufferCache);
        cursor.setFileId(fileId);
        ctx.cursorInitialState.setRootPage(rootPage);
        ctx.cursor.open(ctx.cursorInitialState, pred);
    }

    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public MultiComparator getCmp() {
        return cmp;
    }

    public IFreePageManager getFreePageManager() {
        return freePageManager;
    }

    @Override
    public void update(ITupleReference tuple, IIndexOpContext ictx) {
        throw new UnsupportedOperationException("RTree Update not implemented.");
    }

    public final class BulkLoadContext implements IIndexBulkLoadContext {

        public RTreeOpContext insertOpCtx;

        public BulkLoadContext(float fillFactor, IRTreeFrame leafFrame, IRTreeFrame interiorFrame,
                ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {

            insertOpCtx = createOpContext(IndexOp.INSERT, leafFrame, interiorFrame, metaFrame);
        }
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor, ITreeIndexFrame leafFrame,
            ITreeIndexFrame interiorFrame, ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        if (loaded)
            throw new HyracksDataException("Trying to bulk-load RTree but RTree has already been loaded.");

        BulkLoadContext ctx = new BulkLoadContext(fillFactor, (IRTreeFrame) leafFrame, (IRTreeFrame) interiorFrame,
                metaFrame);
        return ctx;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        try {
            insert(tuple, ((BulkLoadContext) ictx).insertOpCtx);
        } catch (Exception e) {
            throw new HyracksDataException("BulkLoad Error");
        }
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        loaded = true;
    }

    @Override
    public void diskOrderScan(ITreeIndexCursor icursor, ITreeIndexFrame leafFrame, ITreeIndexMetaDataFrame metaFrame,
            IIndexOpContext ictx) throws HyracksDataException {
        TreeDiskOrderScanCursor cursor = (TreeDiskOrderScanCursor) icursor;
        RTreeOpContext ctx = (RTreeOpContext) ictx;
        ctx.reset();

        int currentPageId = rootPage + 1;
        int maxPageId = freePageManager.getMaxPage(metaFrame);

        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
        page.acquireReadLatch();
        cursor.setBufferCache(bufferCache);
        cursor.setFileId(fileId);
        cursor.setCurrentPageId(currentPageId);
        cursor.setMaxPageId(maxPageId);
        ctx.cursorInitialState.setPage(page);
        cursor.open(ctx.cursorInitialState, diskOrderScanPredicate);
    }

    @Override
    public int getRootPageId() {
        return rootPage;
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.RTREE;
    }
}
