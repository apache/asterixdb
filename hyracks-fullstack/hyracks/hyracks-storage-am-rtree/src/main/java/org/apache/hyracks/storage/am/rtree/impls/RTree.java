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
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.frames.AbstractSlotManager;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.common.impls.NodeFrontier;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.util.TreeIndexUtils;
import org.apache.hyracks.storage.am.rtree.api.IRTreeFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMFrame;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrame;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class RTree extends AbstractTreeIndex {

    // Global node sequence number used for the concurrency control protocol
    private final AtomicLong globalNsn;

    private final int maxTupleSize;
    private final boolean isPointMBR; // used for reducing storage space to store point objects.

    public RTree(IBufferCache bufferCache, IPageManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            FileReference file, boolean isPointMBR) {
        super(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
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
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
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
                (IRTreeInteriorFrame) interiorFrameFactory.createFrame(), freePageManager, cmpFactories,
                modificationCallback);
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
                    node = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
                    ctx.getInteriorFrame().setPage(node);
                    isLeaf = ctx.getInteriorFrame().isLeaf();
                    if (isLeaf) {
                        node.acquireWriteLatch();
                        writeLatched = true;

                        if (!ctx.getInteriorFrame().isLeaf()) {
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

                if (pageId != rootPage && parentLsn < ctx.getInteriorFrame().getPageNsn()) {
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

                    pageId = ctx.getPathList().getLastPageId();
                    if (pageId != rootPage) {
                        parentLsn = ctx.getPathList().getPageLsn(ctx.getPathList().size() - 2);
                    }
                    ctx.getPathList().moveLast();
                    continue;
                }

                pageLsn = ctx.getInteriorFrame().getPageLsn();
                ctx.getPathList().add(pageId, pageLsn, -1);

                if (!isLeaf) {
                    // findBestChild must be called *before* checkIfEnlarementIsNeeded
                    int childPageId = ctx.getInteriorFrame().findBestChild(ctx.getTuple(), ctx.getCmp());
                    boolean enlarementIsNeeded =
                            ctx.getInteriorFrame().checkIfEnlarementIsNeeded(ctx.getTuple(), ctx.getCmp());

                    if (enlarementIsNeeded) {
                        if (!writeLatched) {
                            node.releaseReadLatch();
                            readLatched = false;
                            bufferCache.unpin(node);

                            node = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
                            node.acquireWriteLatch();
                            writeLatched = true;
                            ctx.getInteriorFrame().setPage(node);

                            if (ctx.getInteriorFrame().getPageLsn() != pageLsn) {
                                // The page was changed while we unlocked it;
                                // thus, retry (re-choose best child)

                                ctx.getPathList().moveLast();
                                continue;
                            }
                        }
                        // We don't need to reset the frameTuple because it is
                        // already pointing to the best child
                        ctx.getInteriorFrame().enlarge(ctx.getTuple(), ctx.getCmp());

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
                    ctx.getLeafFrame().setPage(node);
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
            throws HyracksDataException {
        boolean succeeded = false;
        FrameOpSpaceStatus spaceStatus;
        if (!isLeaf) {
            spaceStatus = ctx.getInteriorFrame().hasSpaceInsert(tuple);
        } else {
            spaceStatus = ctx.getLeafFrame().hasSpaceInsert(tuple);
        }

        switch (spaceStatus) {
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                try {
                    if (!isLeaf) {
                        ctx.getInteriorFrame().insert(tuple, -1);
                    } else {
                        ctx.getModificationCallback().found(null, tuple);
                        ctx.getLeafFrame().insert(tuple, -1);
                    }
                    succeeded = true;
                } finally {
                    if (succeeded) {
                        ctx.getLSNUpdates().add(node);
                        ctx.getSplitKey().reset();
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
                        ctx.getInteriorFrame().compact();
                        ctx.getInteriorFrame().insert(tuple, -1);
                    } else {
                        ctx.getLeafFrame().compact();
                        ctx.getModificationCallback().found(null, tuple);
                        ctx.getLeafFrame().insert(tuple, -1);
                    }
                    succeeded = true;
                } finally {
                    if (succeeded) {
                        ctx.getLSNUpdates().add(node);
                        ctx.getSplitKey().reset();
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
                int rightPageId = freePageManager.takePage(ctx.getMetaFrame());
                ICachedPage rightNode =
                        bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), rightPageId), true);
                rightNode.acquireWriteLatch();

                try {
                    IRTreeFrame rightFrame;
                    if (!isLeaf) {
                        rightFrame = (IRTreeFrame) interiorFrameFactory.createFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer(ctx.getInteriorFrame().getLevel());
                        rightFrame.setRightPage(ctx.getInteriorFrame().getRightPage());
                        ctx.getInteriorFrame().split(rightFrame, tuple, ctx.getSplitKey(), ctx, bufferCache);
                        ctx.getInteriorFrame().setRightPage(rightPageId);
                    } else {
                        rightFrame = (IRTreeFrame) leafFrameFactory.createFrame();
                        rightFrame.setPage(rightNode);
                        rightFrame.initBuffer((byte) 0);
                        rightFrame.setRightPage(ctx.getInteriorFrame().getRightPage());
                        ctx.getModificationCallback().found(null, tuple);
                        ctx.getLeafFrame().split(rightFrame, tuple, ctx.getSplitKey(), ctx, bufferCache);
                        ctx.getLeafFrame().setRightPage(rightPageId);
                    }
                    succeeded = true;
                } finally {
                    if (succeeded) {
                        ctx.getNSNUpdates().add(rightNode);
                        ctx.getLSNUpdates().add(rightNode);
                        ctx.getNSNUpdates().add(node);
                        ctx.getLSNUpdates().add(node);
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
                ctx.getSplitKey().setPages(pageId, rightPageId);
                if (pageId == rootPage) {
                    int newLeftId = freePageManager.takePage(ctx.getMetaFrame());
                    ICachedPage newLeftNode =
                            bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newLeftId), true);
                    newLeftNode.acquireWriteLatch();
                    succeeded = false;
                    try {
                        // copy left child to new left child
                        System.arraycopy(node.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0,
                                newLeftNode.getBuffer().capacity());

                        // initialize new root (leftNode becomes new root)
                        ctx.getInteriorFrame().setPage(node);
                        ctx.getInteriorFrame().initBuffer((byte) (ctx.getInteriorFrame().getLevel() + 1));

                        ctx.getSplitKey().setLeftPage(newLeftId);
                        ctx.getInteriorFrame().insert(ctx.getSplitKey().getLeftTuple(), -1);
                        ctx.getInteriorFrame().insert(ctx.getSplitKey().getRightTuple(), -1);

                        succeeded = true;
                    } finally {
                        if (succeeded) {
                            ctx.getNSNUpdates().remove(ctx.getNSNUpdates().size() - 1);
                            ctx.getLSNUpdates().remove(ctx.getLSNUpdates().size() - 1);

                            ctx.getNSNUpdates().add(newLeftNode);
                            ctx.getLSNUpdates().add(newLeftNode);

                            ctx.getNSNUpdates().add(node);
                            ctx.getLSNUpdates().add(node);
                            ctx.getSplitKey().reset();
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

            default: {
                throw new IllegalStateException("NYI: " + spaceStatus);
            }
        }
    }

    private void updateParentForInsert(RTreeOpContext ctx) throws HyracksDataException {
        boolean succeeded = false;
        boolean writeLatched = false;
        int parentId = ctx.getPathList().getLastPageId();
        ICachedPage parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), parentId), false);
        parentNode.acquireWriteLatch();
        writeLatched = true;
        ctx.getInteriorFrame().setPage(parentNode);
        boolean foundParent = true;

        try {
            if (ctx.getInteriorFrame().getPageLsn() != ctx.getPathList().getLastPageLsn()) {
                foundParent = false;
                while (true) {
                    if (ctx.getInteriorFrame().findTupleByPointer(ctx.getSplitKey().getLeftTuple(),
                            ctx.getCmp()) != -1) {
                        // found the parent
                        foundParent = true;
                        break;
                    }
                    int rightPage = ctx.getInteriorFrame().getRightPage();
                    parentNode.releaseWriteLatch(true);
                    writeLatched = false;
                    bufferCache.unpin(parentNode);

                    if (rightPage == -1) {
                        break;
                    }

                    parentId = rightPage;
                    parentNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), parentId), false);
                    parentNode.acquireWriteLatch();
                    writeLatched = true;
                    ctx.getInteriorFrame().setPage(parentNode);
                }
            }

            if (foundParent) {
                try {
                    ctx.getInteriorFrame().adjustKey(ctx.getSplitKey().getLeftTuple(), -1, ctx.getCmp());
                } catch (Exception e) {
                    if (writeLatched) {
                        parentNode.releaseWriteLatch(true);
                        writeLatched = false;
                        bufferCache.unpin(parentNode);
                    }
                    throw e;
                }
                insertTuple(parentNode, parentId, ctx.getSplitKey().getRightTuple(), ctx,
                        ctx.getInteriorFrame().isLeaf());
                ctx.getPathList().moveLast();
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

        ctx.getTraverseList().clear();
        findPath(ctx);
        updateParentForInsert(ctx);
    }

    private void findPath(RTreeOpContext ctx) throws HyracksDataException {
        boolean readLatched = false;
        int pageId = rootPage;
        int parentIndex = -1;
        long parentLsn = 0;
        long pageLsn;
        int pageIndex;
        ICachedPage node = null;
        ctx.getTraverseList().add(pageId, -1, parentIndex);
        try {
            while (!ctx.getTraverseList().isLast()) {
                pageId = ctx.getTraverseList().getFirstPageId();
                parentIndex = ctx.getTraverseList().getFirstPageIndex();

                node = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
                node.acquireReadLatch();
                readLatched = true;
                ctx.getInteriorFrame().setPage(node);
                pageLsn = ctx.getInteriorFrame().getPageLsn();
                pageIndex = ctx.getTraverseList().first();
                ctx.getTraverseList().setPageLsn(pageIndex, pageLsn);

                ctx.getTraverseList().moveFirst();

                if (ctx.getInteriorFrame().isLeaf()) {
                    throw HyracksDataException.create(ErrorCode.FAILED_TO_RE_FIND_PARENT);
                }

                if (pageId != rootPage) {
                    parentLsn = ctx.getTraverseList().getPageLsn(ctx.getTraverseList().getPageIndex(pageIndex));
                }
                if (pageId != rootPage && parentLsn < ctx.getInteriorFrame().getPageNsn()) {
                    int rightPage = ctx.getInteriorFrame().getRightPage();
                    if (rightPage != -1) {
                        ctx.getTraverseList().addFirst(rightPage, -1, parentIndex);
                    }
                }

                if (ctx.getInteriorFrame().findTupleByPointer(ctx.getSplitKey().getLeftTuple(), ctx.getTraverseList(),
                        pageIndex, ctx.getCmp()) != -1) {
                    ctx.getPathList().clear();
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
            fillPath(ctx, ctx.getTraverseList().getPageIndex(pageIndex));
            ctx.getPathList().add(ctx.getTraverseList().getPageId(pageIndex),
                    ctx.getTraverseList().getPageLsn(pageIndex), -1);
        }
    }

    private void delete(ITupleReference tuple, RTreeOpContext ctx) throws HyracksDataException {
        ctx.reset();
        ctx.setTuple(tuple);
        ctx.getSplitKey().reset();
        ctx.getSplitKey().getLeftTuple().setFieldCount(cmpFactories.length);

        // We delete the first matching tuple (including the payload data).
        // We don't update the MBRs of the parents after deleting the record.
        int tupleIndex = findTupleToDelete(ctx);

        if (tupleIndex != -1) {
            try {
                deleteTuple(tupleIndex, ctx);
            } finally {
                ctx.getLeafFrame().getPage().releaseWriteLatch(true);
                bufferCache.unpin(ctx.getLeafFrame().getPage());
            }
        }
    }

    private int findTupleToDelete(RTreeOpContext ctx) throws HyracksDataException {
        boolean writeLatched = false;
        boolean readLatched = false;
        boolean succeeded = false;
        ICachedPage node = null;
        ctx.getPathList().add(rootPage, -1, -1);

        try {
            while (!ctx.getPathList().isEmpty()) {
                int pageId = ctx.getPathList().getLastPageId();
                long parentLsn = ctx.getPathList().getLastPageLsn();
                ctx.getPathList().moveLast();
                node = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
                node.acquireReadLatch();
                readLatched = true;
                ctx.getInteriorFrame().setPage(node);
                boolean isLeaf = ctx.getInteriorFrame().isLeaf();
                long pageLsn = ctx.getInteriorFrame().getPageLsn();

                if (pageId != rootPage && parentLsn < ctx.getInteriorFrame().getPageNsn()) {
                    // Concurrent split detected, we need to visit the right
                    // page
                    int rightPage = ctx.getInteriorFrame().getRightPage();
                    if (rightPage != -1) {
                        ctx.getPathList().add(rightPage, parentLsn, -1);
                    }
                }

                if (!isLeaf) {
                    for (int i = 0; i < ctx.getInteriorFrame().getTupleCount(); i++) {
                        int childPageId =
                                ctx.getInteriorFrame().getChildPageIdIfIntersect(ctx.getTuple(), i, ctx.getCmp());
                        if (childPageId != -1) {
                            ctx.getPathList().add(childPageId, pageLsn, -1);
                        }
                    }
                } else {
                    ctx.getLeafFrame().setPage(node);
                    int tupleIndex = ctx.getLeafFrame().findTupleIndex(ctx.getTuple(), ctx.getCmp());
                    if (tupleIndex != -1) {

                        node.releaseReadLatch();
                        readLatched = false;
                        bufferCache.unpin(node);

                        node = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
                        node.acquireWriteLatch();
                        writeLatched = true;
                        ctx.getLeafFrame().setPage(node);

                        // A rare case only happen when a root is no longer a
                        // leaf page. Simply we restart the search.
                        if (!ctx.getLeafFrame().isLeaf()) {
                            ctx.getPathList().add(pageId, -1, -1);

                            node.releaseWriteLatch(true);
                            writeLatched = false;
                            bufferCache.unpin(node);
                            continue;
                        }

                        if (ctx.getLeafFrame().getPageLsn() != pageLsn) {
                            // The page was changed while we unlocked it

                            tupleIndex = ctx.getLeafFrame().findTupleIndex(ctx.getTuple(), ctx.getCmp());
                            if (tupleIndex == -1) {
                                ctx.getPathList().add(pageId, parentLsn, -1);

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
        ITupleReference beforeTuple = ctx.getLeafFrame().getBeforeTuple(ctx.getTuple(), tupleIndex, ctx.getCmp());
        ctx.getModificationCallback().found(beforeTuple, ctx.getTuple());
        ctx.getLeafFrame().delete(tupleIndex, ctx.getCmp());
        ctx.getLeafFrame().setPageLsn(incrementGlobalNsn());
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate searchPred, RTreeOpContext ctx)
            throws HyracksDataException {
        ctx.reset();
        ctx.setCursor(cursor);

        cursor.setBufferCache(bufferCache);
        cursor.setFileId(getFileId());
        ctx.getCursorInitialState().setRootPage(rootPage);
        ctx.getCursor().open(ctx.getCursorInitialState(), searchPred);
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
        int maxPageId = freePageManager.getMaxPageId(ctx.getMetaFrame());

        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), currentPageId), false);
        page.acquireReadLatch();
        try {
            cursor.setBufferCache(bufferCache);
            cursor.setFileId(getFileId());
            cursor.setCurrentPageId(currentPageId);
            cursor.setMaxPageId(maxPageId);
            ctx.getCursorInitialState().setOriginialKeyComparator(ctx.getCmp());
            ctx.getCursorInitialState().setPage(page);
            cursor.open(ctx.getCursorInitialState(), searchPred);
        } catch (Exception e) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public RTreeAccessor createAccessor(IIndexAccessParameters iap) {
        return new RTreeAccessor(this, iap);
    }

    public class RTreeAccessor implements ITreeIndexAccessor {
        private RTree rtree;
        private RTreeOpContext ctx;
        private boolean destroyed = false;
        private IIndexAccessParameters iap;

        public RTreeAccessor(RTree rtree, IIndexAccessParameters iap) {
            this.rtree = rtree;
            this.ctx = rtree.createOpContext(iap.getModificationCallback());
            this.iap = iap;
        }

        public void reset(RTree rtree, IIndexAccessParameters iap) {
            this.rtree = rtree;
            ctx.setModificationCallback(iap.getModificationCallback());
            ctx.reset();
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.INSERT);
            insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.UPDATE);
            rtree.update(tuple, ctx);
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DELETE);
            rtree.delete(tuple, ctx);
        }

        @Override
        public RTreeSearchCursor createSearchCursor(boolean exclusive) {
            return new RTreeSearchCursor((IRTreeInteriorFrame) interiorFrameFactory.createFrame(),
                    (IRTreeLeafFrame) leafFrameFactory.createFrame(), (IIndexCursorStats) iap.getParameters()
                            .getOrDefault(HyracksConstants.INDEX_CURSOR_STATS, NoOpIndexCursorStats.INSTANCE));
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
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
        public void upsert(ITupleReference tuple) throws HyracksDataException {
            throw new UnsupportedOperationException(
                    "The RTree does not support the notion of keys, therefore upsert does not make sense.");
        }

        private void insert(ITupleReference tuple, IIndexOperationContext ictx) throws HyracksDataException {
            RTreeOpContext ctx = (RTreeOpContext) ictx;
            int tupleSize = Math.max(ctx.getLeafFrame().getBytesRequiredToWriteTuple(tuple),
                    ctx.getInteriorFrame().getBytesRequiredToWriteTuple(tuple));
            if (tupleSize > maxTupleSize) {
                throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE, tupleSize, maxTupleSize);
            }
            ctx.reset();
            ctx.setTuple(tuple);
            ctx.getSplitKey().reset();
            ctx.getSplitKey().getLeftTuple().setFieldCount(cmpFactories.length);
            ctx.getSplitKey().getRightTuple().setFieldCount(cmpFactories.length);
            ctx.getModificationCallback().before(tuple);

            int maxFieldPos = cmpFactories.length / 2;
            for (int i = 0; i < maxFieldPos; i++) {
                int j = maxFieldPos + i;
                int c = ctx.getCmp().getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                        tuple.getFieldLength(i), tuple.getFieldData(j), tuple.getFieldStart(j),
                        tuple.getFieldLength(j));
                if (c > 0) {
                    throw new IllegalArgumentException(
                            "The low key point has larger coordinates than the high key point.");
                }
            }

            try {
                ICachedPage leafNode = findLeaf(ctx);

                int pageId = ctx.getPathList().getLastPageId();
                ctx.getPathList().moveLast();
                insertTuple(leafNode, pageId, ctx.getTuple(), ctx, true);

                while (true) {
                    if (ctx.getSplitKey().getLeftPageBuffer() != null) {
                        updateParentForInsert(ctx);
                    } else {
                        break;
                    }
                }
            } finally {
                for (int i = ctx.getNSNUpdates().size() - 1; i >= 0; i--) {
                    ICachedPage node = ctx.getNSNUpdates().get(i);
                    ctx.getInteriorFrame().setPage(node);
                    ctx.getInteriorFrame().setPageNsn(incrementGlobalNsn());
                }

                for (int i = ctx.getLSNUpdates().size() - 1; i >= 0; i--) {
                    ICachedPage node = ctx.getLSNUpdates().get(i);
                    ctx.getInteriorFrame().setPage(node);
                    ctx.getInteriorFrame().setPageLsn(incrementGlobalNsn());
                    node.releaseWriteLatch(true);
                    bufferCache.unpin(node);
                }
            }
        }

        @Override
        public void destroy() throws HyracksDataException {
            if (destroyed) {
                return;
            }
            destroyed = true;
            ctx.destroy();
        }
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
        // TODO: verifyInput currently does nothing.
        return new RTreeBulkLoader(fillFactor, callback);
    }

    public class RTreeBulkLoader extends AbstractTreeIndex.AbstractTreeIndexBulkLoader {
        ITreeIndexFrame lowerFrame, prevInteriorFrame;
        RTreeTypeAwareTupleWriter interiorFrameTupleWriter =
                ((RTreeTypeAwareTupleWriter) interiorFrame.getTupleWriter());
        ITreeIndexTupleReference mbrTuple = interiorFrame.createTupleReference();
        ByteBuffer mbr;
        List<Integer> prevNodeFrontierPages = new ArrayList<>();

        public RTreeBulkLoader(float fillFactor, IPageWriteCallback callback) throws HyracksDataException {
            super(fillFactor, callback);
            prevInteriorFrame = interiorFrameFactory.createFrame();
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            try {
                int leafFrameTupleSize = leafFrame.getBytesRequiredToWriteTuple(tuple);
                int interiorFrameTupleSize = interiorFrame.getBytesRequiredToWriteTuple(tuple);
                int tupleSize = Math.max(leafFrameTupleSize, interiorFrameTupleSize);
                if (tupleSize > maxTupleSize) {
                    throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE, tupleSize, maxTupleSize);
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

                    leafFrontier.pageId = freePageManager.takePage(metaFrame);

                    write(leafFrontier.page);
                    for (ICachedPage c : pagesToWrite) {
                        write(c);
                    }
                    pagesToWrite.clear();
                    leafFrontier.page = bufferCache
                            .confiscatePage(BufferedFileHandle.getDiskPageId(getFileId(), leafFrontier.pageId));
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

        @Override
        public void end() throws HyracksDataException {
            pagesToWrite.clear();
            //if writing a trivial 1-page tree, don't try and propagate up
            if (nodeFrontiers.size() > 1) {
                propagateBulk(1, true, pagesToWrite);
            }

            for (ICachedPage c : pagesToWrite) {
                write(c);
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
                    mbrTuple.resetByTupleOffset(mbr.array(), 0);
                    interiorFrame.insert(mbrTuple, -1);
                    interiorFrame.getBuffer().putInt(
                            interiorFrame.getTupleOffset(interiorFrame.getTupleCount() - 1) + mbrTuple.getTupleSize(),
                            prevPageId);

                    int finalPageId = freePageManager.takePage(metaFrame);
                    n.pageId = finalPageId;
                    n.page.setDiskPageId(BufferedFileHandle.getDiskPageId(getFileId(), finalPageId));
                    //else we are looking at a leaf
                }
                //set next guide MBR
                //if propagateBulk didnt have to do anything this may be un-necessary
                if (nodeFrontiers.size() > 1 && nodeFrontiers.indexOf(n) < nodeFrontiers.size() - 1) {
                    lowerFrame = nodeFrontiers.indexOf(n) != 0 ? prevInteriorFrame : leafFrame;
                    lowerFrame.setPage(n.page);
                    ((RTreeNSMFrame) lowerFrame).adjustMBR();
                    interiorFrameTupleWriter.writeTupleFields(((RTreeNSMFrame) lowerFrame).getMBRTuples(), 0, mbr, 0);
                }
                write(n.page);
                n.page = null;
                prevPageId = n.pageId;
            }
            rootPage = nodeFrontiers.get(nodeFrontiers.size() - 1).pageId;
            releasedLatches = true;
        }

        protected void propagateBulk(int level, boolean toRoot, List<ICachedPage> pagesToWrite)
                throws HyracksDataException {

            if (level == 1) {
                lowerFrame = leafFrame;
            }

            if (lowerFrame.getTupleCount() == 0) {
                return;
            }

            if (level >= nodeFrontiers.size()) {
                addLevel();
            }

            //adjust the tuple pointers of the lower frame to allow us to calculate our MBR
            //if this is a leaf, then there is only one tuple, so this is trivial
            ((RTreeNSMFrame) lowerFrame).adjustMBR();

            if (mbr == null) {
                int bytesRequired =
                        interiorFrameTupleWriter.bytesRequired(((RTreeNSMFrame) lowerFrame).getMBRTuples()[0], 0,
                                cmp.getKeyFieldCount()) + ((RTreeNSMInteriorFrame) interiorFrame).getChildPointerSize();
                mbr = ByteBuffer.allocate(bytesRequired);
            }
            interiorFrameTupleWriter.writeTupleFields(((RTreeNSMFrame) lowerFrame).getMBRTuples(), 0, mbr, 0);
            mbrTuple.resetByTupleOffset(mbr.array(), 0);

            NodeFrontier frontier = nodeFrontiers.get(level);
            interiorFrame.setPage(frontier.page);
            //see if we have space for two tuples. this works around a  tricky boundary condition with sequential bulk
            // load where finalization can possibly lead to a split
            //TODO: accomplish this without wasting 1 tuple
            int sizeOfTwoTuples = 2 * (mbrTuple.getTupleSize() + RTreeNSMInteriorFrame.childPtrSize);
            FrameOpSpaceStatus spaceForTwoTuples =
                    (((RTreeNSMInteriorFrame) interiorFrame).hasSpaceInsert(sizeOfTwoTuples));
            if (spaceForTwoTuples != FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE && !toRoot) {

                int finalPageId = freePageManager.takePage(metaFrame);
                if (prevNodeFrontierPages.size() <= level) {
                    prevNodeFrontierPages.add(finalPageId);
                } else {
                    prevNodeFrontierPages.set(level, finalPageId);
                }
                frontier.page.setDiskPageId(BufferedFileHandle.getDiskPageId(getFileId(), finalPageId));
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

    @Override
    public int getNumOfFilterFields() {
        return 0;
    }
}
