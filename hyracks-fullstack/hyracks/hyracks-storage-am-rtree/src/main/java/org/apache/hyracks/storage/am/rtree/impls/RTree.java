/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.rtree.impls;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexBulkLoader;
import org.apache.hyracks.storage.am.common.api.IIndexCursor;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallback;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.frames.AbstractSlotManager;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.common.impls.NodeFrontier;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.util.TreeIndexUtils;
import org.apache.hyracks.storage.am.rtree.api.IRTreeFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMFrame;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrame;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

public class RTree extends AbstractTreeIndex {

    // Global node sequence number used for the concurrency control protocol
    private final AtomicLong globalNsn;

    private final int maxTupleSize;
    private final boolean isPointMBR; // used for reducing storage space to store point objects.

    public RTree(IBufferCache bufferCache, IFileMapProvider fileMapProvider, IMetaDataPageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            IBinaryComparatorFactory[] cmpFactories, int fieldCount, FileReference file, boolean isPointMBR) {
        super(bufferCache, fileMapProvider, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                fieldCount, file);
        globalNsn = new AtomicLong();
        ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
        ITreeIndexFrame interiorFrame = interiorFrameFactory.createFrame();
        maxTupleSize = Math.min(leafFrame.getMaxTupleSize(bufferCache.getPageSize()),
                interiorFrame.getMaxTupleSize(bufferCache.getPageSize()));
        this.isPointMBR = isPointMBR;
    }

    private long incrementGlobalNsn() {
        return globalNsn.incrementAndGet();
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

    private RTreeOpContext createOpContext(IModificationOperationCallback modificationCallback) {
        return new RTreeOpContext((IRTreeLeafFrame) leafFrameFactory.createFrame(),
                (IRTreeInteriorFrame) interiorFrameFactory.createFrame(),
                freePageManager.getMetaDataFrameFactory().createFrame(), cmpFactories, modificationCallback);
    }

    private void insert(ITupleReference tuple, IIndexOperationContext ictx)
            throws HyracksDataException, TreeIndexException {
        RTreeOpContext ctx = (RTreeOpContext) ictx;
        int tupleSize = Math.max(ctx.leafFrame.getBytesRequiredToWriteTuple(tuple),
                ctx.interiorFrame.getBytesRequiredToWriteTuple(tuple));
        if (tupleSize > maxTupleSize) {
            throw new TreeIndexException("Record size (" + tupleSize + ") larger than maximum acceptable record size ("
                    + maxTupleSize + ")");
        }
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.splitKey.reset();
        ctx.splitKey.getLeftTuple().setFieldCount(cmpFactories.length);
        ctx.splitKey.getRightTuple().setFieldCount(cmpFactories.length);
        ctx.modificationCallback.before(tuple);

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
                node.releaseWriteLatch(true);
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
                            node.releaseWriteLatch(true);
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
                        node.releaseWriteLatch(true);
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
                    // findBestChild must be called *before* checkIfEnlarementIsNeeded
                    int childPageId = ctx.interiorFrame.findBestChild(ctx.getTuple(), ctx.cmp);
                    boolean enlarementIsNeeded = ctx.interiorFrame.checkIfEnlarementIsNeeded(ctx.getTuple(), ctx.cmp);

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

                        node.releaseWriteLatch(true);
                        writeLatched = false;
                        bufferCache.unpin(node);
                    } else {
                        if (readLatched) {
                            node.releaseReadLatch();
                            readLatched = false;
                            bufferCache.unpin(node);
                        } else if (writeLatched) {
                            node.releaseWriteLatch(true);
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
                    node.releaseWriteLatch(true);
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
                        ctx.modificationCallback.found(null, tuple);
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
                        node.releaseWriteLatch(true);
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
                        ctx.modificationCallback.found(null, tuple);
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
                        node.releaseWriteLatch(true);
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
                        ctx.modificationCallback.found(null, tuple);
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
                        node.releaseWriteLatch(true);
                        bufferCache.unpin(node);
                        rightNode.releaseWriteLatch(true);
                        bufferCache.unpin(rightNode);
                    } else {
                        rightNode.releaseWriteLatch(true);
                        bufferCache.unpin(rightNode);
                    }

                }
                ctx.splitKey.setPages(pageId, rightPageId);
                if (pageId == rootPage) {
                    int newLeftId = freePageManager.getFreePage(ctx.metaFrame);
                    ICachedPage newLeftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeftId),
                            true);
                    newLeftNode.acquireWriteLatch();
                    succeeded = false;
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
                            node.releaseWriteLatch(true);
                            bufferCache.unpin(node);
                            rightNode.releaseWriteLatch(true);
                            bufferCache.unpin(rightNode);
                            newLeftNode.releaseWriteLatch(true);
                            bufferCache.unpin(newLeftNode);
                        } else {
                            rightNode.releaseWriteLatch(true);
                            bufferCache.unpin(rightNode);
                            newLeftNode.releaseWriteLatch(true);
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
                    parentNode.releaseWriteLatch(true);
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
                        parentNode.releaseWriteLatch(true);
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
                    parentNode.releaseWriteLatch(true);
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

        // We delete the first matching tuple (including the payload data).
        // We don't update the MBRs of the parents after deleting the record.
        int tupleIndex = findTupleToDelete(ctx);

        if (tupleIndex != -1) {
            try {
                deleteTuple(tupleIndex, ctx);
            } finally {
                ctx.leafFrame.getPage().releaseWriteLatch(true);
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

                            node.releaseWriteLatch(true);
                            writeLatched = false;
                            bufferCache.unpin(node);
                            continue;
                        }

                        if (ctx.leafFrame.getPageLsn() != pageLsn) {
                            // The page was changed while we unlocked it

                            tupleIndex = ctx.leafFrame.findTupleIndex(ctx.tuple, ctx.cmp);
                            if (tupleIndex == -1) {
                                ctx.pathList.add(pageId, parentLsn, -1);

                                node.releaseWriteLatch(true);
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
                    node.releaseWriteLatch(true);
                    writeLatched = false;
                    bufferCache.unpin(node);
                }
            }
        }
        return -1;
    }

    private void deleteTuple(int tupleIndex, RTreeOpContext ctx) throws HyracksDataException {
        ITupleReference beforeTuple = ctx.leafFrame.getBeforeTuple(ctx.getTuple(), tupleIndex, ctx.cmp);
        ctx.modificationCallback.found(beforeTuple, ctx.getTuple());
        ctx.leafFrame.delete(tupleIndex, ctx.cmp);
        ctx.leafFrame.setPageLsn(incrementGlobalNsn());
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate searchPred, RTreeOpContext ctx)
            throws HyracksDataException, IndexException {
        ctx.reset();
        ctx.cursor = cursor;

        cursor.setBufferCache(bufferCache);
        cursor.setFileId(fileId);
        ctx.cursorInitialState.setRootPage(rootPage);
        ctx.cursor.open(ctx.cursorInitialState, (SearchPredicate) searchPred);
    }

    private void update(ITupleReference tuple, RTreeOpContext ctx) {
        throw new UnsupportedOperationException("RTree Update not implemented.");
    }

    private void diskOrderScan(ITreeIndexCursor icursor, RTreeOpContext ctx) throws HyracksDataException {
        TreeIndexDiskOrderScanCursor cursor = (TreeIndexDiskOrderScanCursor) icursor;
        ctx.reset();

        MultiComparator cmp = MultiComparator.create(cmpFactories);
        SearchPredicate searchPred = new SearchPredicate(null, cmp);

        int currentPageId = bulkloadLeafStart;
        int maxPageId = freePageManager.getMaxPage(ctx.metaFrame);

        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
        page.acquireReadLatch();
        try {
            cursor.setBufferCache(bufferCache);
            cursor.setFileId(fileId);
            cursor.setCurrentPageId(currentPageId);
            cursor.setMaxPageId(maxPageId);
            ctx.cursorInitialState.setOriginialKeyComparator(ctx.cmp);
            ctx.cursorInitialState.setPage(page);
            cursor.open(ctx.cursorInitialState, searchPred);
        } catch (Exception e) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            throw new HyracksDataException(e);
        }
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
            this.ctx = rtree.createOpContext(modificationCallback);
        }

        public void reset(RTree rtree, IModificationOperationCallback modificationCallback) {
            this.rtree = rtree;
            ctx.setModificationCallback(modificationCallback);
            ctx.reset();
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.setOperation(IndexOperation.INSERT);
            rtree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.setOperation(IndexOperation.UPDATE);
            rtree.update(tuple, ctx);
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.setOperation(IndexOperation.DELETE);
            rtree.delete(tuple, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor(boolean exclusive) {
            return new RTreeSearchCursor((IRTreeInteriorFrame) interiorFrameFactory.createFrame(),
                    (IRTreeLeafFrame) leafFrameFactory.createFrame());
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred)
                throws HyracksDataException, IndexException {
            ctx.setOperation(IndexOperation.SEARCH);
            rtree.search((ITreeIndexCursor) cursor, searchPred, ctx);
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            return new TreeIndexDiskOrderScanCursor(leafFrameFactory.createFrame());
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DISKORDERSCAN);
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

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        // TODO: verifyInput currently does nothing.
        return createBulkLoader(fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, false);
    }

    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, boolean appendOnly) throws TreeIndexException {
        // TODO: verifyInput currently does nothing.
        try {
            return new RTreeBulkLoader(fillFactor, appendOnly);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    public class RTreeBulkLoader extends AbstractTreeIndex.AbstractTreeIndexBulkLoader {
        ITreeIndexFrame lowerFrame, prevInteriorFrame;
        RTreeTypeAwareTupleWriter interiorFrameTupleWriter = ((RTreeTypeAwareTupleWriter) interiorFrame
                .getTupleWriter());
        ITreeIndexTupleReference mbrTuple = interiorFrame.createTupleReference();
        ByteBuffer mbr;
        List<Integer> prevNodeFrontierPages = new ArrayList<Integer>();

        public RTreeBulkLoader(float fillFactor, boolean appendOnly) throws TreeIndexException, HyracksDataException {
            super(fillFactor, appendOnly);
            prevInteriorFrame = interiorFrameFactory.createFrame();
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                int leafFrameTupleSize = leafFrame.getBytesRequiredToWriteTuple(tuple);
                int interiorFrameTupleSize = interiorFrame.getBytesRequiredToWriteTuple(tuple);
                int tupleSize = Math.max(leafFrameTupleSize, interiorFrameTupleSize);
                if (tupleSize > maxTupleSize) {
                    throw new TreeIndexException("Space required for record (" + tupleSize
                            + ") larger than maximum acceptable size (" + maxTupleSize + ")");
                }

                NodeFrontier leafFrontier = nodeFrontiers.get(0);

                int spaceNeeded = leafFrameTupleSize;
                int spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();

                // try to free space by compression
                if (spaceUsed + spaceNeeded > leafMaxBytes) {
                    leafFrame.compress();
                    spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
                }

                if (spaceUsed + spaceNeeded > leafMaxBytes) {

                    if (prevNodeFrontierPages.size() == 0) {
                        prevNodeFrontierPages.add(leafFrontier.pageId);
                    } else {
                        prevNodeFrontierPages.set(0, leafFrontier.pageId);
                    }
                    propagateBulk(1, false, pagesToWrite);

                    leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
                    queue.put(leafFrontier.page);
                    for (ICachedPage c : pagesToWrite) {
                        queue.put(c);
                    }

                    pagesToWrite.clear();
                    leafFrontier.page = bufferCache
                            .confiscatePage(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId));
                    leafFrame.setPage(leafFrontier.page);
                    leafFrame.initBuffer((byte) 0);

                }

                leafFrame.setPage(leafFrontier.page);
                leafFrame.insert(tuple, AbstractSlotManager.GREATEST_KEY_INDICATOR);
            } catch (HyracksDataException e) {
                handleException();
                throw e;
            } catch (RuntimeException e) {
                handleException();
                throw e;
            }

        }

        public void end() throws HyracksDataException {
            pagesToWrite.clear();
            //if writing a trivial 1-page tree, don't try and propagate up
            if (nodeFrontiers.size() > 1) {
                propagateBulk(1, true, pagesToWrite);
            }

            for (ICachedPage c : pagesToWrite) {
                queue.put(c);
            }
            finish();
            super.end();
        }

        @Override
        public void abort() throws HyracksDataException {
            super.handleException();
        }

        protected void finish() throws HyracksDataException {
            int prevPageId = -1;
            //here we assign physical identifiers to everything we can
            for (NodeFrontier n : nodeFrontiers) {
                //not a leaf
                if (nodeFrontiers.indexOf(n) != 0) {
                    interiorFrame.setPage(n.page);
                    mbrTuple.resetByTupleOffset(mbr, 0);
                    interiorFrame.insert(mbrTuple, -1);
                    interiorFrame.getBuffer().putInt(
                            interiorFrame.getTupleOffset(interiorFrame.getTupleCount() - 1) + mbrTuple.getTupleSize(),
                            prevPageId);

                    int finalPageId = freePageManager.getFreePage(metaFrame);
                    n.pageId = finalPageId;
                    bufferCache.setPageDiskId(n.page, BufferedFileHandle.getDiskPageId(fileId, finalPageId));
                    //else we are looking at a leaf
                }
                //set next guide MBR
                //if propagateBulk didnt have to do anything this may be un-necessary
                if (nodeFrontiers.size() > 1 && nodeFrontiers.indexOf(n) < nodeFrontiers.size() - 1) {
                    lowerFrame.setPage(n.page);
                    ((RTreeNSMFrame) lowerFrame).adjustMBR();
                    interiorFrameTupleWriter.writeTupleFields(((RTreeNSMFrame) lowerFrame).getMBRTuples(), 0, mbr, 0);
                }
                queue.put(n.page);
                n.page = null;
                prevPageId = n.pageId;
            }
            if (appendOnly) {
                rootPage = nodeFrontiers.get(nodeFrontiers.size() - 1).pageId;
            }
            releasedLatches = true;
        }

        protected void propagateBulk(int level, boolean toRoot, List<ICachedPage> pagesToWrite)
                throws HyracksDataException {
            boolean propagated = false;

            if (level == 1)
                lowerFrame = leafFrame;

            if (lowerFrame.getTupleCount() == 0)
                return;

            if (level >= nodeFrontiers.size())
                addLevel();

            //adjust the tuple pointers of the lower frame to allow us to calculate our MBR
            //if this is a leaf, then there is only one tuple, so this is trivial
            ((RTreeNSMFrame) lowerFrame).adjustMBR();

            if (mbr == null) {
                int bytesRequired = interiorFrameTupleWriter
                        .bytesRequired(((RTreeNSMFrame) lowerFrame).getMBRTuples()[0], 0, cmp.getKeyFieldCount())
                        + ((RTreeNSMInteriorFrame) interiorFrame).getChildPointerSize();
                mbr = ByteBuffer.allocate(bytesRequired);
            }
            interiorFrameTupleWriter.writeTupleFields(((RTreeNSMFrame) lowerFrame).getMBRTuples(), 0, mbr, 0);
            mbrTuple.resetByTupleOffset(mbr, 0);

            NodeFrontier frontier = nodeFrontiers.get(level);
            interiorFrame.setPage(frontier.page);
            //see if we have space for two tuples. this works around a  tricky boundary condition with sequential bulk load where
            //finalization can possibly lead to a split
            //TODO: accomplish this without wasting 1 tuple
            int sizeOfTwoTuples = 2 * (mbrTuple.getTupleSize() + RTreeNSMInteriorFrame.childPtrSize);
            FrameOpSpaceStatus spaceForTwoTuples = (((RTreeNSMInteriorFrame) interiorFrame)
                    .hasSpaceInsert(sizeOfTwoTuples));
            if (spaceForTwoTuples != FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE && !toRoot) {

                int finalPageId = freePageManager.getFreePage(metaFrame);
                if (prevNodeFrontierPages.size() <= level) {
                    prevNodeFrontierPages.add(finalPageId);
                } else {
                    prevNodeFrontierPages.set(level, finalPageId);
                }
                bufferCache.setPageDiskId(frontier.page, BufferedFileHandle.getDiskPageId(fileId, finalPageId));
                pagesToWrite.add(frontier.page);

                lowerFrame = prevInteriorFrame;
                lowerFrame.setPage(frontier.page);

                frontier.page = bufferCache.confiscatePage(BufferCache.INVALID_DPID);
                interiorFrame.setPage(frontier.page);
                interiorFrame.initBuffer((byte) level);

                interiorFrame.insert(mbrTuple, AbstractSlotManager.GREATEST_KEY_INDICATOR);

                interiorFrame.getBuffer().putInt(
                        interiorFrame.getTupleOffset(interiorFrame.getTupleCount() - 1) + mbrTuple.getTupleSize(),
                        prevNodeFrontierPages.get(level - 1));

                propagateBulk(level + 1, toRoot, pagesToWrite);
            } else if (interiorFrame.hasSpaceInsert(mbrTuple) == FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE
                    && !toRoot) {

                interiorFrame.insert(mbrTuple, -1);

                interiorFrame.getBuffer().putInt(
                        interiorFrame.getTupleOffset(interiorFrame.getTupleCount() - 1) + mbrTuple.getTupleSize(),
                        prevNodeFrontierPages.get(level - 1));
            }

            if (toRoot && level < nodeFrontiers.size() - 1) {
                lowerFrame = prevInteriorFrame;
                lowerFrame.setPage(frontier.page);
                propagateBulk(level + 1, true, pagesToWrite);
            }

            leafFrame.setPage(nodeFrontiers.get(0).page);
        }
    }

    @Override
    public void validate() throws HyracksDataException {
        throw new UnsupportedOperationException("Validation not implemented for R-Trees.");
    }
}
