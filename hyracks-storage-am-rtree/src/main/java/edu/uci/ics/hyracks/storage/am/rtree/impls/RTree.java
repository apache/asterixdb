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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.util.TreeIndexUtils;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class RTree implements ITreeIndex {

    private final int rootPage = 1;

    // Global node sequence number used for the concurrency control protocol
    private final AtomicLong globalNsn;
    private final ReadWriteLock treeLatch;

    private final IFreePageManager freePageManager;
    private final IBufferCache bufferCache;
    private int fileId;

    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final ITreeIndexFrameFactory leafFrameFactory;
    private final int fieldCount;
    private final IBinaryComparatorFactory[] cmpFactories;

    public RTree(IBufferCache bufferCache, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            IFreePageManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory) {
        this.bufferCache = bufferCache;
        this.fieldCount = fieldCount;
        this.cmpFactories = cmpFactories;
        this.freePageManager = freePageManager;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        globalNsn = new AtomicLong();
        this.treeLatch = new ReentrantReadWriteLock(true);
    }

    private long incrementGlobalNsn() {
        return globalNsn.incrementAndGet();
    }

    public byte getTreeHeight(IRTreeLeafFrame leafFrame) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            leafFrame.setPage(rootNode);
            return leafFrame.getLevel();
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    @SuppressWarnings("rawtypes")
    public String printTree(IRTreeLeafFrame leafFrame, IRTreeInteriorFrame interiorFrame,
            ISerializerDeserializer[] keySerdes) throws Exception {
        MultiComparator cmp = MultiComparator.create(cmpFactories);
        byte treeHeight = getTreeHeight(leafFrame);
        StringBuilder strBuilder = new StringBuilder();
        printTree(rootPage, null, false, leafFrame, interiorFrame, treeHeight, keySerdes, strBuilder, cmp);
        return strBuilder.toString();
    }

    @SuppressWarnings("rawtypes")
    public void printTree(int pageId, ICachedPage parent, boolean unpin, IRTreeLeafFrame leafFrame,
            IRTreeInteriorFrame interiorFrame, byte treeHeight, ISerializerDeserializer[] keySerdes,
            StringBuilder strBuilder, MultiComparator cmp) throws Exception {
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        node.acquireReadLatch();
        try {
            if (parent != null && unpin == true) {
                parent.releaseReadLatch();
                bufferCache.unpin(parent);
            }
            interiorFrame.setPage(node);
            int level = interiorFrame.getLevel();
            strBuilder.append(String.format("%1d ", level));
            strBuilder.append(String.format("%3d ", pageId) + ": ");
            for (int i = 0; i < treeHeight - level; i++) {
                strBuilder.append("    ");
            }

            String keyString;
            long LSN, NSN;
            int rightPage;
            if (interiorFrame.isLeaf()) {
                leafFrame.setPage(node);
                keyString = TreeIndexUtils.printFrameTuples(leafFrame, keySerdes);
                LSN = leafFrame.getPageLsn();
                NSN = leafFrame.getPageNsn();
                rightPage = leafFrame.getRightPage();

            } else {
                keyString = TreeIndexUtils.printFrameTuples(interiorFrame, keySerdes);
                LSN = interiorFrame.getPageLsn();
                NSN = interiorFrame.getPageNsn();
                rightPage = interiorFrame.getRightPage();
            }

            strBuilder.append(keyString + "\n" + "pageId: " + pageId + " LSN: " + LSN + " NSN: " + NSN + " rightPage: "
                    + rightPage + "\n");
            if (!interiorFrame.isLeaf()) {
                ArrayList<Integer> children = ((RTreeNSMInteriorFrame) (interiorFrame)).getChildren(cmp);
                for (int i = 0; i < children.size(); i++) {
                    printTree(children.get(i), node, i == children.size() - 1, leafFrame, interiorFrame, treeHeight,
                            keySerdes, strBuilder, cmp);
                }
            } else {
                node.releaseReadLatch();
                bufferCache.unpin(node);
            }
        } catch (Exception e) {
            node.releaseReadLatch();
            bufferCache.unpin(node);
            e.printStackTrace();
        }
    }

    @Override
    public void create(int fileId) throws HyracksDataException {
        treeLatch.writeLock().lock();
        try {
            ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
            ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
            freePageManager.open(fileId);
            freePageManager.init(metaFrame, rootPage);

            // initialize root page
            ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);

            rootNode.acquireWriteLatch();
            try {
                leafFrame.setPage(rootNode);
                leafFrame.initBuffer((byte) 0);
            } finally {
                rootNode.releaseWriteLatch();
                bufferCache.unpin(rootNode);
            }
        } finally {
            treeLatch.writeLock().unlock();
        }
    }

    @Override
    public void open(int fileId) {
        this.fileId = fileId;
        freePageManager.open(fileId);
    }

    @Override
    public void close() {
        fileId = -1;
        freePageManager.close();
    }

    @Override
    public int getFileId() {
        return fileId;
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    private RTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new RTreeOpContext((IRTreeLeafFrame) leafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) interiorFrameFactory.createFrame(), freePageManager.getMetaDataFrameFactory()
                        .createFrame(), cmpFactories, 8, modificationCallback, searchCallback);
    }

    private void insert(ITupleReference tuple, IIndexOpContext ictx) throws HyracksDataException, TreeIndexException {
        RTreeOpContext ctx = (RTreeOpContext) ictx;
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.splitKey.reset();
        ctx.splitKey.getLeftTuple().setFieldCount(cmpFactories.length);
        ctx.splitKey.getRightTuple().setFieldCount(cmpFactories.length);
        ctx.modificationCallback.commence(tuple);

        int maxFieldPos = cmpFactories.length / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = ctx.cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j));
            if (c > 0) {
                throw new IllegalArgumentException("The low key point has larger coordinates than the high key point.");
            }
        }
        try {
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
        } finally {
            for (int i = ctx.NSNUpdates.size() - 1; i >= 0; i--) {
                ICachedPage node = ctx.NSNUpdates.get(i);
                ctx.interiorFrame.setPage(node);
                ctx.interiorFrame.setPageNsn(incrementGlobalNsn());
            }

            for (int i = ctx.LSNUpdates.size() - 1; i >= 0; i--) {
                ICachedPage node = ctx.LSNUpdates.get(i);
                ctx.interiorFrame.setPage(node);
                ctx.interiorFrame.setPageLsn(incrementGlobalNsn());
                node.releaseWriteLatch();
                bufferCache.unpin(node);
            }
        }
    }

    private ICachedPage findLeaf(RTreeOpContext ctx) throws HyracksDataException {
        int pageId = rootPage;
        boolean writeLatched = false;
        boolean readLatched = false;
        boolean succeeded = false;
        ICachedPage node = null;
        boolean isLeaf = false;
        long pageLsn = 0, parentLsn = 0;

        try {

            while (true) {
                if (!writeLatched) {
                    node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                    ctx.interiorFrame.setPage(node);
                    isLeaf = ctx.interiorFrame.isLeaf();
                    if (isLeaf) {
                        node.acquireWriteLatch();
                        writeLatched = true;

                        if (!ctx.interiorFrame.isLeaf()) {
                            node.releaseWriteLatch();
                            writeLatched = false;
                            bufferCache.unpin(node);
                            continue;
                        }
                    } else {
                        // Be optimistic and grab read latch first. We will swap
                        // it to write latch if we need to enlarge the best
                        // child tuple.
                        node.acquireReadLatch();
                        readLatched = true;
                    }
                }

                if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                    // Concurrent split detected, go back to parent and
                    // re-choose the best child
                    if (writeLatched) {
                        node.releaseWriteLatch();
                        writeLatched = false;
                        bufferCache.unpin(node);
                    } else {
                        node.releaseReadLatch();
                        readLatched = false;
                        bufferCache.unpin(node);
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
                    boolean enlarementIsNeeded = ctx.interiorFrame.findBestChild(ctx.getTuple(), ctx.cmp);
                    int childPageId = ctx.interiorFrame.getBestChildPageId();

                    if (enlarementIsNeeded) {
                        if (!writeLatched) {
                            node.releaseReadLatch();
                            readLatched = false;
                            bufferCache.unpin(node);

                            node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                            node.acquireWriteLatch();
                            writeLatched = true;
                            ctx.interiorFrame.setPage(node);

                            if (ctx.interiorFrame.getPageLsn() != pageLsn) {
                                // The page was changed while we unlocked it;
                                // thus, retry (re-choose best child)

                                ctx.pathList.moveLast();
                                continue;
                            }
                        }
                        // We don't need to reset the frameTuple because it is
                        // already pointing to the best child
                        ctx.interiorFrame.enlarge(ctx.getTuple(), ctx.cmp);

                        node.releaseWriteLatch();
                        writeLatched = false;
                        bufferCache.unpin(node);
                    } else {
                        if (readLatched) {
                            node.releaseReadLatch();
                            readLatched = false;
                            bufferCache.unpin(node);
                        } else if (writeLatched) {
                            node.releaseWriteLatch();
                            writeLatched = false;
                            bufferCache.unpin(node);
                        }
                    }

                    pageId = childPageId;
                    parentLsn = pageLsn;
                } else {
                    ctx.leafFrame.setPage(node);
                    succeeded = true;
                    return node;
                }
            }
        } finally {
            if (!succeeded) {
                if (readLatched) {
                    node.releaseReadLatch();
                    readLatched = false;
                    bufferCache.unpin(node);
                } else if (writeLatched) {
                    node.releaseWriteLatch();
                    writeLatched = false;
                    bufferCache.unpin(node);
                }
            }
        }
    }

    private void insertTuple(ICachedPage node, int pageId, ITupleReference tuple, RTreeOpContext ctx, boolean isLeaf)
            throws HyracksDataException, TreeIndexException {
        boolean succeeded = false;
        FrameOpSpaceStatus spaceStatus;
        if (!isLeaf) {
            spaceStatus = ctx.interiorFrame.hasSpaceInsert(tuple);
        } else {
            spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple);
        }

        switch (spaceStatus) {
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                try {
                    if (!isLeaf) {
                        ctx.interiorFrame.insert(tuple, -1);
                    } else {
                        ctx.modificationCallback.found(null);
                        ctx.leafFrame.insert(tuple, -1);
                    }
                    succeeded = true;
                } finally {
                    if (succeeded) {
                        ctx.LSNUpdates.add(node);
                        ctx.splitKey.reset();
                    } else if (isLeaf) {
                        // In case of a crash, we un-latch the interior node
                        // inside updateParentForInsert.
                        node.releaseWriteLatch();
                        bufferCache.unpin(node);
                    }
                }
                break;
            }

            case SUFFICIENT_SPACE: {
                try {
                    if (!isLeaf) {
                        ctx.interiorFrame.compact();
                        ctx.interiorFrame.insert(tuple, -1);
                    } else {
                        ctx.leafFrame.compact();
                        ctx.modificationCallback.found(null);
                        ctx.leafFrame.insert(tuple, -1);
                    }
                    succeeded = true;
                } finally {
                    if (succeeded) {
                        ctx.LSNUpdates.add(node);
                        ctx.splitKey.reset();
                    } else if (isLeaf) {
                        // In case of a crash, we un-latch the interior node
                        // inside updateParentForInsert.
                        node.releaseWriteLatch();
                        bufferCache.unpin(node);
                    }
                }
                break;
            }

            case INSUFFICIENT_SPACE: {
                int rightPageId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightPageId), true);
                rightNode.acquireWriteLatch();

                try {
                    IRTreeFrame rightFrame;
                    if (!isLeaf) {
                        rightFrame = (IRTreeFrame) interiorFrameFactory.createFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) ctx.interiorFrame.getLevel());
                        rightFrame.setRightPage(ctx.interiorFrame.getRightPage());
                        ctx.interiorFrame.split(rightFrame, tuple, ctx.splitKey);
                        ctx.interiorFrame.setRightPage(rightPageId);
                    } else {
                        rightFrame = (IRTreeFrame) leafFrameFactory.createFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) 0);
                        rightFrame.setRightPage(ctx.interiorFrame.getRightPage());
                        ctx.modificationCallback.found(null);
                        ctx.leafFrame.split(rightFrame, tuple, ctx.splitKey);
                        ctx.leafFrame.setRightPage(rightPageId);
                    }
                    succeeded = true;
                } finally {
                    if (succeeded) {
                        ctx.NSNUpdates.add(rightNode);
                        ctx.LSNUpdates.add(rightNode);
                        ctx.NSNUpdates.add(node);
                        ctx.LSNUpdates.add(node);
                    } else if (isLeaf) {
                        // In case of a crash, we un-latch the interior node
                        // inside updateParentForInsert.
                        node.releaseWriteLatch();
                        bufferCache.unpin(node);
                        rightNode.releaseWriteLatch();
                        bufferCache.unpin(rightNode);
                    } else {
                        rightNode.releaseWriteLatch();
                        bufferCache.unpin(rightNode);
                    }

                }
                ctx.splitKey.setPages(pageId, rightPageId);
                if (pageId == rootPage) {
                    int newLeftId = freePageManager.getFreePage(ctx.metaFrame);
                    ICachedPage newLeftNode = bufferCache
                            .pin(BufferedFileHandle.getDiskPageId(fileId, newLeftId), true);
                    newLeftNode.acquireWriteLatch();
                    succeeded = false;
                    try {
                        // copy left child to new left child
                        System.arraycopy(node.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0, newLeftNode
                                .getBuffer().capacity());

                        // initialize new root (leftNode becomes new root)
                        ctx.interiorFrame.setPage(node);
                        ctx.interiorFrame.initBuffer((byte) (ctx.interiorFrame.getLevel() + 1));

                        ctx.splitKey.setLeftPage(newLeftId);
                        ctx.interiorFrame.insert(ctx.splitKey.getLeftTuple(), -1);
                        ctx.interiorFrame.insert(ctx.splitKey.getRightTuple(), -1);

                        succeeded = true;
                    } finally {
                        if (succeeded) {
                            ctx.NSNUpdates.remove(ctx.NSNUpdates.size() - 1);
                            ctx.LSNUpdates.remove(ctx.LSNUpdates.size() - 1);

                            ctx.NSNUpdates.add(newLeftNode);
                            ctx.LSNUpdates.add(newLeftNode);

                            ctx.NSNUpdates.add(node);
                            ctx.LSNUpdates.add(node);
                            ctx.splitKey.reset();
                        } else if (isLeaf) {
                            // In case of a crash, we un-latch the interior node
                            // inside updateParentForInsert.
                            node.releaseWriteLatch();
                            bufferCache.unpin(node);
                            rightNode.releaseWriteLatch();
                            bufferCache.unpin(rightNode);
                            newLeftNode.releaseWriteLatch();
                            bufferCache.unpin(newLeftNode);
                        } else {
                            rightNode.releaseWriteLatch();
                            bufferCache.unpin(rightNode);
                            newLeftNode.releaseWriteLatch();
                            bufferCache.unpin(newLeftNode);
                        }
                    }
                }
                break;
            }
        }
    }

    private void updateParentForInsert(RTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        boolean succeeded = false;
        boolean writeLatched = false;
        int parentId = ctx.pathList.getLastPageId();
        ICachedPage parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
        parentNode.acquireWriteLatch();
        writeLatched = true;
        ctx.interiorFrame.setPage(parentNode);
        boolean foundParent = true;

        try {
            if (ctx.interiorFrame.getPageLsn() != ctx.pathList.getLastPageLsn()) {
                foundParent = false;
                while (true) {
                    if (ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), ctx.cmp) != -1) {
                        // found the parent
                        foundParent = true;
                        break;
                    }
                    int rightPage = ctx.interiorFrame.getRightPage();
                    parentNode.releaseWriteLatch();
                    writeLatched = false;
                    bufferCache.unpin(parentNode);

                    if (rightPage == -1) {
                        break;
                    }

                    parentId = rightPage;
                    parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, parentId), false);
                    parentNode.acquireWriteLatch();
                    writeLatched = true;
                    ctx.interiorFrame.setPage(parentNode);
                }
            }

            if (foundParent) {
                try {
                    ctx.interiorFrame.adjustKey(ctx.splitKey.getLeftTuple(), -1, ctx.cmp);
                } catch (TreeIndexException e) {
                    if (writeLatched) {
                        parentNode.releaseWriteLatch();
                        writeLatched = false;
                        bufferCache.unpin(parentNode);
                    }
                    throw e;
                }
                insertTuple(parentNode, parentId, ctx.splitKey.getRightTuple(), ctx, ctx.interiorFrame.isLeaf());
                ctx.pathList.moveLast();
                succeeded = true;
                return;

            }
        } finally {
            if (!succeeded) {
                if (writeLatched) {
                    parentNode.releaseWriteLatch();
                    writeLatched = false;
                    bufferCache.unpin(parentNode);
                }
            }
        }

        ctx.traverseList.clear();
        findPath(ctx);
        updateParentForInsert(ctx);
    }

    private void findPath(RTreeOpContext ctx) throws TreeIndexException, HyracksDataException {
        boolean readLatched = false;
        int pageId = rootPage;
        int parentIndex = -1;
        long parentLsn = 0;
        long pageLsn;
        int pageIndex;
        ICachedPage node = null;
        ctx.traverseList.add(pageId, -1, parentIndex);
        try {
            while (!ctx.traverseList.isLast()) {
                pageId = ctx.traverseList.getFirstPageId();
                parentIndex = ctx.traverseList.getFirstPageIndex();

                node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                node.acquireReadLatch();
                readLatched = true;
                ctx.interiorFrame.setPage(node);
                pageLsn = ctx.interiorFrame.getPageLsn();
                pageIndex = ctx.traverseList.first();
                ctx.traverseList.setPageLsn(pageIndex, pageLsn);

                ctx.traverseList.moveFirst();

                if (ctx.interiorFrame.isLeaf()) {
                    throw new TreeIndexException("Error: Failed to re-find parent of a page in the tree.");
                }

                if (pageId != rootPage) {
                    parentLsn = ctx.traverseList.getPageLsn(ctx.traverseList.getPageIndex(pageIndex));
                }
                if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                    int rightPage = ctx.interiorFrame.getRightPage();
                    if (rightPage != -1) {
                        ctx.traverseList.addFirst(rightPage, -1, parentIndex);
                    }
                }

                if (ctx.interiorFrame.findTupleByPointer(ctx.splitKey.getLeftTuple(), ctx.traverseList, pageIndex,
                        ctx.cmp) != -1) {
                    ctx.pathList.clear();
                    fillPath(ctx, pageIndex);
                    return;
                }
                node.releaseReadLatch();
                readLatched = false;
                bufferCache.unpin(node);
            }
        } finally {
            if (readLatched) {
                node.releaseReadLatch();
                readLatched = false;
                bufferCache.unpin(node);
            }
        }
    }

    private void fillPath(RTreeOpContext ctx, int pageIndex) {
        if (pageIndex != -1) {
            fillPath(ctx, ctx.traverseList.getPageIndex(pageIndex));
            ctx.pathList.add(ctx.traverseList.getPageId(pageIndex), ctx.traverseList.getPageLsn(pageIndex), -1);
        }
    }

    private void delete(ITupleReference tuple, RTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.splitKey.reset();
        ctx.splitKey.getLeftTuple().setFieldCount(cmpFactories.length);

        // We delete the first matching tuple (including the payload data.
        // We don't update the MBRs of the parents after deleting the record.
        int tupleIndex = findTupleToDelete(ctx);

        if (tupleIndex != -1) {
            try {
                deleteTuple(tupleIndex, ctx);
            } finally {
                ctx.leafFrame.getPage().releaseWriteLatch();
                bufferCache.unpin(ctx.leafFrame.getPage());
            }
        }
    }

    private int findTupleToDelete(RTreeOpContext ctx) throws HyracksDataException {
        boolean writeLatched = false;
        boolean readLatched = false;
        boolean succeeded = false;
        ICachedPage node = null;
        ctx.pathList.add(rootPage, -1, -1);

        try {
            while (!ctx.pathList.isEmpty()) {
                int pageId = ctx.pathList.getLastPageId();
                long parentLsn = ctx.pathList.getLastPageLsn();
                ctx.pathList.moveLast();
                node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                node.acquireReadLatch();
                readLatched = true;
                ctx.interiorFrame.setPage(node);
                boolean isLeaf = ctx.interiorFrame.isLeaf();
                long pageLsn = ctx.interiorFrame.getPageLsn();

                if (pageId != rootPage && parentLsn < ctx.interiorFrame.getPageNsn()) {
                    // Concurrent split detected, we need to visit the right
                    // page
                    int rightPage = ctx.interiorFrame.getRightPage();
                    if (rightPage != -1) {
                        ctx.pathList.add(rightPage, parentLsn, -1);
                    }
                }

                if (!isLeaf) {
                    for (int i = 0; i < ctx.interiorFrame.getTupleCount(); i++) {
                        int childPageId = ctx.interiorFrame.getChildPageIdIfIntersect(ctx.tuple, i, ctx.cmp);
                        if (childPageId != -1) {
                            ctx.pathList.add(childPageId, pageLsn, -1);
                        }
                    }
                } else {
                    ctx.leafFrame.setPage(node);
                    int tupleIndex = ctx.leafFrame.findTupleIndex(ctx.tuple, ctx.cmp);
                    if (tupleIndex != -1) {

                        node.releaseReadLatch();
                        readLatched = false;
                        bufferCache.unpin(node);

                        node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                        node.acquireWriteLatch();
                        writeLatched = true;
                        ctx.leafFrame.setPage(node);

                        // A rare case only happen when a root is no longer a
                        // leaf page. Simply we restart the search.
                        if (!ctx.leafFrame.isLeaf()) {
                            ctx.pathList.add(pageId, -1, -1);

                            node.releaseWriteLatch();
                            writeLatched = false;
                            bufferCache.unpin(node);
                            continue;
                        }

                        if (ctx.leafFrame.getPageLsn() != pageLsn) {
                            // The page was changed while we unlocked it

                            tupleIndex = ctx.leafFrame.findTupleIndex(ctx.tuple, ctx.cmp);
                            if (tupleIndex == -1) {
                                ctx.pathList.add(pageId, parentLsn, -1);

                                node.releaseWriteLatch();
                                writeLatched = false;
                                bufferCache.unpin(node);
                                continue;
                            } else {
                                succeeded = true;
                                return tupleIndex;
                            }
                        } else {
                            succeeded = true;
                            return tupleIndex;
                        }
                    }
                }
                node.releaseReadLatch();
                readLatched = false;
                bufferCache.unpin(node);
            }
        } finally {
            if (!succeeded) {
                if (readLatched) {
                    node.releaseReadLatch();
                    readLatched = false;
                    bufferCache.unpin(node);
                } else if (writeLatched) {
                    node.releaseWriteLatch();
                    writeLatched = false;
                    bufferCache.unpin(node);
                }
            }
        }
        return -1;
    }

    private void deleteTuple(int tupleIndex, RTreeOpContext ctx) throws HyracksDataException {
        ITupleReference beforeTuple = ctx.leafFrame.getBeforeTuple(ctx.getTuple(), tupleIndex, ctx.cmp);
        ctx.modificationCallback.found(beforeTuple);
        ctx.leafFrame.delete(tupleIndex, ctx.cmp);
        ctx.leafFrame.setPageLsn(incrementGlobalNsn());
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate searchPred, RTreeOpContext ctx)
            throws HyracksDataException, TreeIndexException {
        ctx.reset();
        ctx.cursor = cursor;

        cursor.setBufferCache(bufferCache);
        cursor.setFileId(fileId);
        ctx.cursorInitialState.setRootPage(rootPage);
        ctx.cursor.open(ctx.cursorInitialState, (SearchPredicate) searchPred);
    }

    @Override
    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    @Override
    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    @Override
    public IFreePageManager getFreePageManager() {
        return freePageManager;
    }

    private void update(ITupleReference tuple, RTreeOpContext ctx) {
        throw new UnsupportedOperationException("RTree Update not implemented.");
    }

    public boolean isEmptyTree(IRTreeLeafFrame leafFrame) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), false);
        rootNode.acquireReadLatch();
        try {
            leafFrame.setPage(rootNode);
            if (leafFrame.getLevel() == 0 && leafFrame.getTupleCount() == 0) {
                return true;
            } else {
                return false;
            }
        } finally {
            rootNode.releaseReadLatch();
            bufferCache.unpin(rootNode);
        }
    }

    public final class BulkLoadContext implements IIndexBulkLoadContext {

        public ITreeIndexAccessor indexAccessor;

        public BulkLoadContext(float fillFactor, IRTreeFrame leafFrame, IRTreeFrame interiorFrame,
                ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
            indexAccessor = createAccessor(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        }
    }

    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws HyracksDataException {
        IRTreeLeafFrame leafFrame = (IRTreeLeafFrame) leafFrameFactory.createFrame();
        if (!isEmptyTree(leafFrame)) {
            throw new HyracksDataException("Trying to Bulk-load a non-empty RTree.");
        }

        BulkLoadContext ctx = new BulkLoadContext(fillFactor, (IRTreeFrame) leafFrameFactory.createFrame(),
                (IRTreeFrame) interiorFrameFactory.createFrame(), freePageManager.getMetaDataFrameFactory()
                        .createFrame());
        return ctx;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        try {
            ((BulkLoadContext) ictx).indexAccessor.insert(tuple);
        } catch (Exception e) {
            throw new HyracksDataException("BulkLoad Error", e);
        }
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
    }

    private void diskOrderScan(ITreeIndexCursor icursor, RTreeOpContext ctx) throws HyracksDataException {
        TreeDiskOrderScanCursor cursor = (TreeDiskOrderScanCursor) icursor;
        ctx.reset();

        MultiComparator cmp = MultiComparator.create(cmpFactories);
        SearchPredicate searchPred = new SearchPredicate(null, cmp);

        int currentPageId = rootPage + 1;
        int maxPageId = freePageManager.getMaxPage(ctx.metaFrame);

        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
        page.acquireReadLatch();
        try {
            cursor.setBufferCache(bufferCache);
            cursor.setFileId(fileId);
            cursor.setCurrentPageId(currentPageId);
            cursor.setMaxPageId(maxPageId);
            ctx.cursorInitialState.setSearchOperationCallback(ctx.searchCallback);
            ctx.cursorInitialState.setPage(page);
            cursor.open(ctx.cursorInitialState, searchPred);
        } catch (Exception e) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            throw new HyracksDataException(e);
        }
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

    @Override
    public ITreeIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new RTreeAccessor(this, modificationCallback, searchCallback);
    }

    public class RTreeAccessor implements ITreeIndexAccessor {
        private RTree rtree;
        private RTreeOpContext ctx;

        public RTreeAccessor(RTree rtree, IModificationOperationCallback modificationCallback,
                ISearchOperationCallback searchCallback) {
            this.rtree = rtree;
            this.ctx = rtree.createOpContext(modificationCallback, searchCallback);
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.INSERT);
            rtree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.UPDATE);
            rtree.update(tuple, ctx);
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.DELETE);
            rtree.delete(tuple, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            return new RTreeSearchCursor((IRTreeInteriorFrame) interiorFrameFactory.createFrame(),
                    (IRTreeLeafFrame) leafFrameFactory.createFrame());
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                IndexException {
            ctx.reset(IndexOp.SEARCH);
            rtree.search((ITreeIndexCursor) cursor, searchPred, ctx);
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            return new TreeDiskOrderScanCursor(leafFrameFactory.createFrame());
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            ctx.reset(IndexOp.DISKORDERSCAN);
            rtree.diskOrderScan(cursor, ctx);
        }

        public RTreeOpContext getOpContext() {
            return ctx;
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            throw new UnsupportedOperationException(
                    "The RTree does not support the notion of keys, therefore upsert does not make sense.");
        }
    }
}