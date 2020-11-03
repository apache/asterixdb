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

package org.apache.hyracks.storage.am.btree.impls;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeFrame;
import org.apache.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.api.ITupleAcceptor;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrame;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import org.apache.hyracks.storage.am.common.api.IBTreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.impls.AbstractTreeIndex;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NodeFrontier;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IIndexCursorStats;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.NoOpIndexCursorStats;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class BTree extends AbstractTreeIndex {

    public static final float DEFAULT_FILL_FACTOR = 0.7f;
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long RESTART_OP = Long.MIN_VALUE;
    private static final long FULL_RESTART_OP = Long.MIN_VALUE + 1;
    private static final int MAX_RESTARTS = 10;

    private final AtomicInteger smoCounter;
    private final ReadWriteLock treeLatch;
    private final int maxTupleSize;

    public BTree(IBufferCache bufferCache, IPageManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, IBinaryComparatorFactory[] cmpFactories, int fieldCount,
            FileReference file) {
        super(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories, fieldCount, file);
        this.treeLatch = new ReentrantReadWriteLock(true);
        this.smoCounter = new AtomicInteger();
        ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
        ITreeIndexFrame interiorFrame = interiorFrameFactory.createFrame();
        maxTupleSize = Math.min(leafFrame.getMaxTupleSize(bufferCache.getPageSize()),
                interiorFrame.getMaxTupleSize(bufferCache.getPageSize()));
    }

    private void diskOrderScan(ITreeIndexCursor icursor, BTreeOpContext ctx) throws HyracksDataException {
        TreeIndexDiskOrderScanCursor cursor = (TreeIndexDiskOrderScanCursor) icursor;
        ctx.reset();
        RangePredicate diskOrderScanPred = new RangePredicate(null, null, true, true, ctx.getCmp(), ctx.getCmp());
        int maxPageId = freePageManager.getMaxPageId(ctx.getMetaFrame());
        int currentPageId = bulkloadLeafStart;
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), currentPageId), false);
        page.acquireReadLatch();
        try {
            cursor.setBufferCache(bufferCache);
            cursor.setFileId(getFileId());
            cursor.setCurrentPageId(currentPageId);
            cursor.setMaxPageId(maxPageId);
            ctx.getCursorInitialState().setPage(page);
            ctx.getCursorInitialState().setSearchOperationCallback(ctx.getSearchCallback());
            ctx.getCursorInitialState().setOriginialKeyComparator(ctx.getCmp());
            cursor.open(ctx.getCursorInitialState(), diskOrderScanPred);
        } catch (Exception e) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void validate() throws HyracksDataException {
        // Stack validation protocol:
        //      * parent pushes the validation information onto the stack before validation
        //      * child pops the validation information off of the stack after validating
        BTreeAccessor accessor = createAccessor(NoOpIndexAccessParameters.INSTANCE);
        PageValidationInfo pvi = accessor.ctx.createPageValidationInfo(null);
        accessor.ctx.getValidationInfos().addFirst(pvi);
        if (isActive) {
            validate(accessor.ctx, rootPage);
        }
    }

    private void validate(BTreeOpContext ctx, int pageId) throws HyracksDataException {
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
        ctx.getInteriorFrame().setPage(page);
        PageValidationInfo currentPvi = ctx.getValidationInfos().peekFirst();

        boolean isLeaf = ctx.getInteriorFrame().isLeaf();
        if (isLeaf) {
            ctx.getLeafFrame().setPage(page);
            ctx.getLeafFrame().validate(currentPvi);
        } else {
            PageValidationInfo nextPvi = ctx.createPageValidationInfo(currentPvi);
            List<Integer> children = ((BTreeNSMInteriorFrame) ctx.getInteriorFrame()).getChildren(ctx.getCmp());
            ctx.getInteriorFrame().validate(currentPvi);
            for (int i = 0; i < children.size(); i++) {
                ctx.getInteriorFrame().setPage(page);

                if (children.size() == 1) {
                    // There is a single child pointer with no keys, so propagate both low and high ranges
                    nextPvi.propagateLowRangeKey(currentPvi);
                    nextPvi.propagateHighRangeKey(currentPvi);
                } else if (i == 0) {
                    // There is more than one child pointer and this is the left-most child pointer, so:
                    //      1) propagate the low range key from the parent
                    //      2) adjust the high range key
                    nextPvi.propagateLowRangeKey(currentPvi);
                    ctx.getInteriorFrameTuple().resetByTupleIndex(ctx.getInteriorFrame(), i);
                    nextPvi.adjustHighRangeKey(ctx.getInteriorFrameTuple());
                } else if (i == children.size() - 1) {
                    // There is more than one child pointer and this is the right-most child pointer, so:
                    //      1) propagate the high range key from the parent
                    //      2) adjust the low range key
                    nextPvi.propagateHighRangeKey(currentPvi);
                    ctx.getInteriorFrameTuple().resetByTupleIndex(ctx.getInteriorFrame(), i - 1);
                    nextPvi.adjustLowRangeKey(ctx.getInteriorFrameTuple());
                } else {
                    // There is more than one child pointer and this pointer is not the left/right-most pointer, so:
                    //      1) adjust the low range key
                    //      2) adjust the high range key
                    ctx.getInteriorFrameTuple().resetByTupleIndex(ctx.getInteriorFrame(), i - 1);
                    nextPvi.adjustLowRangeKey(ctx.getInteriorFrameTuple());
                    ctx.getInteriorFrameTuple().resetByTupleIndex(ctx.getInteriorFrame(), i);
                    nextPvi.adjustHighRangeKey(ctx.getInteriorFrameTuple());
                }

                ctx.getValidationInfos().addFirst(nextPvi);
                validate(ctx, children.get(i));
            }
        }
        bufferCache.unpin(page);
        ctx.getValidationInfos().removeFirst();
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate searchPred, BTreeOpContext ctx)
            throws HyracksDataException {
        ctx.reset();
        ctx.setPred((RangePredicate) searchPred);
        ctx.setCursor(cursor);
        // simple index scan
        if (ctx.getPred().getLowKeyComparator() == null) {
            ctx.getPred().setLowKeyComparator(ctx.getCmp());
        }
        if (ctx.getPred().getHighKeyComparator() == null) {
            ctx.getPred().setHighKeyComparator(ctx.getCmp());
        }
        // we use this loop to deal with possibly multiple operation restarts
        // due to ongoing structure modifications during the descent
        boolean repeatOp = true;
        while (repeatOp && ctx.getOpRestarts() < MAX_RESTARTS) {
            performOp(rootPage, null, true, ctx);
            // if we reach this stage then we need to restart from the (possibly
            // new) root
            if (!ctx.getPageLsns().isEmpty() && ctx.getPageLsns().getLast() == RESTART_OP) {
                ctx.getPageLsns().removeLast(); // pop the restart op indicator
                continue;
            }
            repeatOp = false;
        }
        cursor.setBufferCache(bufferCache);
        cursor.setFileId(getFileId());
    }

    private void unsetSmPages(BTreeOpContext ctx) throws HyracksDataException {
        ICachedPage originalPage = ctx.getInteriorFrame().getPage();
        for (int i = 0; i < ctx.getSmPages().size(); i++) {
            int pageId = ctx.getSmPages().get(i);
            ICachedPage smPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
            smPage.acquireWriteLatch();
            try {
                ctx.getInteriorFrame().setPage(smPage);
                ctx.getInteriorFrame().setSmFlag(false);
            } finally {
                smPage.releaseWriteLatch(true);
                bufferCache.unpin(smPage);
            }
        }
        if (ctx.getSmPages().size() > 0) {
            if (ctx.getSmoCount() == Integer.MAX_VALUE) {
                smoCounter.set(0);
            } else {
                smoCounter.incrementAndGet();
            }
            treeLatch.writeLock().unlock();
            ctx.getSmPages().clear();
        }
        ctx.getInteriorFrame().setPage(originalPage);
    }

    private void createNewRoot(BTreeOpContext ctx) throws HyracksDataException {
        // Make sure the root is always in the same page.
        ICachedPage leftNode =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), ctx.getSplitKey().getLeftPage()), false);
        leftNode.acquireWriteLatch();
        try {
            int newLeftId = freePageManager.takePage(ctx.getMetaFrame());
            ICachedPage newLeftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), newLeftId), true);
            newLeftNode.acquireWriteLatch();
            try {
                boolean largePage = false;
                if (leftNode.getBuffer().capacity() > newLeftNode.getBuffer().capacity()) {
                    bufferCache.resizePage(newLeftNode, leftNode.getBuffer().capacity() / bufferCache.getPageSize(),
                            ctx);
                    largePage = true;
                }
                // Copy left child to new left child.
                System.arraycopy(leftNode.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0,
                        newLeftNode.getBuffer().capacity());
                ctx.getInteriorFrame().setPage(newLeftNode);
                ctx.getInteriorFrame().setSmFlag(false);
                // Remember LSN to set it in the root.
                long leftNodeLSN = ctx.getInteriorFrame().getPageLsn();
                // Initialize new root (leftNode becomes new root).
                if (largePage) {
                    bufferCache.resizePage(leftNode, 1, ctx);
                    ctx.getInteriorFrame().setPage(leftNode);
                    ctx.getInteriorFrame().setLargeFlag(false);
                } else {
                    ctx.getInteriorFrame().setPage(leftNode);
                    ctx.getInteriorFrame().setLargeFlag(false);
                }
                ctx.getInteriorFrame().initBuffer((byte) (ctx.getInteriorFrame().getLevel() + 1));
                // Copy over LSN.
                ctx.getInteriorFrame().setPageLsn(leftNodeLSN);
                // Will be cleared later in unsetSmPages.
                ctx.getInteriorFrame().setSmFlag(true);
                ctx.getSplitKey().setLeftPage(newLeftId);
                int targetTupleIndex = ctx.getInteriorFrame().findInsertTupleIndex(ctx.getSplitKey().getTuple());
                int tupleSize = ctx.getInteriorFrame().getBytesRequiredToWriteTuple(ctx.getSplitKey().getTuple());
                if (tupleSize > maxTupleSize) {
                    throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE, tupleSize, maxTupleSize);
                }
                ctx.getInteriorFrame().insert(ctx.getSplitKey().getTuple(), targetTupleIndex);
            } finally {
                newLeftNode.releaseWriteLatch(true);
                bufferCache.unpin(newLeftNode);
            }
        } finally {
            leftNode.releaseWriteLatch(true);
            bufferCache.unpin(leftNode);
        }
    }

    private boolean insertLeaf(ITupleReference tuple, int targetTupleIndex, int pageId, BTreeOpContext ctx)
            throws Exception {
        boolean restartOp = false;
        FrameOpSpaceStatus spaceStatus = ctx.getLeafFrame().hasSpaceInsert(tuple);

        switch (spaceStatus) {
            case EXPAND: {
                // TODO: avoid repeated calculation of tuple size
                ctx.getLeafFrame().ensureCapacity(bufferCache, tuple, ctx);
            }
            // fall-through
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.getModificationCallback().found(null, tuple);
                ctx.getLeafFrame().insert(tuple, targetTupleIndex);
                ctx.getSplitKey().reset();
                break;
            }
            case SUFFICIENT_SPACE: {
                int finalIndex = ctx.getLeafFrame().compact() ? ctx.getLeafFrame().findInsertTupleIndex(tuple)
                        : targetTupleIndex;
                ctx.getModificationCallback().found(null, tuple);
                ctx.getLeafFrame().insert(tuple, finalIndex);
                ctx.getSplitKey().reset();
                break;
            }
            case INSUFFICIENT_SPACE: {
                // Try compressing the page first and see if there is space available.
                if (ctx.getLeafFrame().compress()
                        && ctx.getLeafFrame().hasSpaceInsert(tuple) == FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
                    ctx.getModificationCallback().found(null, tuple);
                    ctx.getLeafFrame().insert(tuple, ctx.getLeafFrame().findInsertTupleIndex(tuple));
                    ctx.getSplitKey().reset();
                } else {
                    restartOp = performLeafSplit(pageId, tuple, ctx, -1);
                }
                break;
            }
            default: {
                throw new IllegalStateException("NYI: " + spaceStatus);
            }
        }
        return restartOp;
    }

    private boolean performLeafSplit(int pageId, ITupleReference tuple, BTreeOpContext ctx, int updateTupleIndex)
            throws Exception {
        // We must never hold a latch on a page while waiting to obtain the tree
        // latch, because it this could lead to a latch-deadlock.
        // If we can't get the tree latch, we return, release our page latches,
        // and restart the operation from one level above.
        // Lock is released in unsetSmPages(), after sm has fully completed.
        if (!treeLatch.writeLock().tryLock()) {
            return true;
        } else {
            int tempSmoCount = smoCounter.get();
            if (tempSmoCount != ctx.getSmoCount()) {
                treeLatch.writeLock().unlock();
                return true;
            }
        }
        int rightPageId = freePageManager.takePage(ctx.getMetaFrame());
        ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), rightPageId), true);
        rightNode.acquireWriteLatch();
        try {
            IBTreeLeafFrame rightFrame = ctx.createLeafFrame();
            rightFrame.setPage(rightNode);
            rightFrame.initBuffer((byte) 0);
            rightFrame.setMultiComparator(ctx.getCmp());

            // Perform an update (delete + insert) if the updateTupleIndex != -1
            if (updateTupleIndex != -1) {
                ITupleReference beforeTuple = ctx.getLeafFrame().getMatchingKeyTuple(tuple, updateTupleIndex);
                ctx.getModificationCallback().found(beforeTuple, tuple);
                ctx.getLeafFrame().delete(tuple, updateTupleIndex);
            } else {
                ctx.getModificationCallback().found(null, tuple);
            }
            ctx.getLeafFrame().split(rightFrame, tuple, ctx.getSplitKey(), ctx, bufferCache);

            ctx.getSmPages().add(pageId);
            ctx.getSmPages().add(rightPageId);
            ctx.getLeafFrame().setSmFlag(true);
            rightFrame.setSmFlag(true);

            rightFrame.setNextLeaf(ctx.getLeafFrame().getNextLeaf());
            ctx.getLeafFrame().setNextLeaf(rightPageId);

            rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
            ctx.getLeafFrame().setPageLsn(ctx.getLeafFrame().getPageLsn() + 1);

            ctx.getSplitKey().setPages(pageId, rightPageId);
        } catch (Exception e) {
            treeLatch.writeLock().unlock();
            throw e;
        } finally {
            rightNode.releaseWriteLatch(true);
            bufferCache.unpin(rightNode);
        }
        return false;
    }

    private boolean updateLeaf(ITupleReference tuple, int oldTupleIndex, int pageId, BTreeOpContext ctx)
            throws Exception {
        FrameOpSpaceStatus spaceStatus = ctx.getLeafFrame().hasSpaceUpdate(tuple, oldTupleIndex);
        ITupleReference beforeTuple = ctx.getLeafFrame().getMatchingKeyTuple(tuple, oldTupleIndex);
        IBTreeIndexTupleReference beforeBTreeTuple = (IBTreeIndexTupleReference) beforeTuple;
        ctx.getLeafFrame().getTupleWriter().setUpdated(beforeBTreeTuple.flipUpdated());
        boolean restartOp = false;
        switch (spaceStatus) {
            case SUFFICIENT_INPLACE_SPACE: {
                ctx.getModificationCallback().found(beforeTuple, tuple);
                ctx.getLeafFrame().update(tuple, oldTupleIndex, true);
                ctx.getSplitKey().reset();
                break;
            }
            case EXPAND: {
                // TODO: avoid repeated calculation of tuple size
                // TODO: in-place update on expand
                // Delete the old tuple, compact the frame, and insert the new tuple.
                ctx.getModificationCallback().found(beforeTuple, tuple);
                ctx.getLeafFrame().delete(tuple, oldTupleIndex);
                ctx.getLeafFrame().compact();
                ctx.getLeafFrame().ensureCapacity(bufferCache, tuple, ctx);
                int targetTupleIndex = ctx.getLeafFrame().findInsertTupleIndex(tuple);
                ctx.getLeafFrame().insert(tuple, targetTupleIndex);
                ctx.getSplitKey().reset();
                break;
            }
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.getModificationCallback().found(beforeTuple, tuple);
                ctx.getLeafFrame().update(tuple, oldTupleIndex, false);
                ctx.getSplitKey().reset();
                break;
            }
            case SUFFICIENT_SPACE: {
                // Delete the old tuple, compact the frame, and insert the new tuple.
                ctx.getModificationCallback().found(beforeTuple, tuple);
                ctx.getLeafFrame().delete(tuple, oldTupleIndex);
                ctx.getLeafFrame().compact();
                int targetTupleIndex = ctx.getLeafFrame().findInsertTupleIndex(tuple);
                ctx.getLeafFrame().insert(tuple, targetTupleIndex);
                ctx.getSplitKey().reset();
                break;
            }
            case INSUFFICIENT_SPACE: {
                restartOp = performLeafSplit(pageId, tuple, ctx, oldTupleIndex);
                break;
            }
            default: {
                throw new IllegalStateException("NYI: " + spaceStatus);
            }
        }
        ctx.getLeafFrame().getTupleWriter().setUpdated(false);
        return restartOp;
    }

    private boolean upsertLeaf(ITupleReference tuple, int targetTupleIndex, int pageId, BTreeOpContext ctx)
            throws Exception {
        boolean restartOp;
        ITupleReference beforeTuple = ctx.getLeafFrame().getMatchingKeyTuple(tuple, targetTupleIndex);
        if (ctx.getAcceptor().accept(beforeTuple)) {
            if (beforeTuple == null) {
                restartOp = insertLeaf(tuple, targetTupleIndex, pageId, ctx);
            } else {
                restartOp = updateLeaf(tuple, targetTupleIndex, pageId, ctx);
            }
        } else {
            restartOp = insertLeaf(tuple, ctx.getLeafFrame().findInsertTupleIndex(tuple), pageId, ctx);
        }
        return restartOp;
    }

    private void insertInterior(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx)
            throws Exception {
        ctx.getInteriorFrame().setPage(node);
        int targetTupleIndex = ctx.getInteriorFrame().findInsertTupleIndex(tuple);
        FrameOpSpaceStatus spaceStatus = ctx.getInteriorFrame().hasSpaceInsert(tuple);
        switch (spaceStatus) {
            case INSUFFICIENT_SPACE: {
                int rightPageId = freePageManager.takePage(ctx.getMetaFrame());
                ICachedPage rightNode =
                        bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), rightPageId), true);
                rightNode.acquireWriteLatch();
                try {
                    IBTreeFrame rightFrame = ctx.createInteriorFrame();
                    rightFrame.setPage(rightNode);
                    rightFrame.initBuffer(ctx.getInteriorFrame().getLevel());
                    rightFrame.setMultiComparator(ctx.getCmp());
                    // instead of creating a new split key, use the existing
                    // splitKey
                    ctx.getInteriorFrame().split(rightFrame, ctx.getSplitKey().getTuple(), ctx.getSplitKey(), ctx,
                            bufferCache);
                    ctx.getSmPages().add(pageId);
                    ctx.getSmPages().add(rightPageId);
                    ctx.getInteriorFrame().setSmFlag(true);
                    rightFrame.setSmFlag(true);
                    rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
                    ctx.getInteriorFrame().setPageLsn(ctx.getInteriorFrame().getPageLsn() + 1);

                    ctx.getSplitKey().setPages(pageId, rightPageId);
                } finally {
                    rightNode.releaseWriteLatch(true);
                    bufferCache.unpin(rightNode);
                }
                break;
            }

            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.getInteriorFrame().insert(tuple, targetTupleIndex);
                ctx.getSplitKey().reset();
                break;
            }

            case SUFFICIENT_SPACE: {
                boolean slotsChanged = ctx.getInteriorFrame().compact();
                if (slotsChanged) {
                    targetTupleIndex = ctx.getInteriorFrame().findInsertTupleIndex(tuple);
                }
                ctx.getInteriorFrame().insert(tuple, targetTupleIndex);
                ctx.getSplitKey().reset();
                break;
            }

            case TOO_LARGE: {
                int tupleSize = ctx.getInteriorFrame().getBytesRequiredToWriteTuple(tuple);
                throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE, tupleSize, maxTupleSize);
            }

            default: {
                throw new IllegalStateException("NYI: " + spaceStatus);
            }
        }
    }

    private boolean deleteLeaf(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx)
            throws Exception {
        // Simply delete the tuple, and don't do any rebalancing.
        // This means that there could be underflow, even an empty page that is
        // pointed to by an interior node.
        if (ctx.getLeafFrame().getTupleCount() == 0) {
            throw HyracksDataException.create(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY);
        }
        int tupleIndex = ctx.getLeafFrame().findDeleteTupleIndex(tuple);
        ITupleReference beforeTuple = ctx.getLeafFrame().getMatchingKeyTuple(tuple, tupleIndex);
        ctx.getModificationCallback().found(beforeTuple, tuple);
        ctx.getLeafFrame().delete(tuple, tupleIndex);
        return false;
    }

    private final boolean acquireLatch(ICachedPage node, BTreeOpContext ctx, boolean isLeaf) {
        if (!isLeaf || (ctx.getOperation() == IndexOperation.SEARCH && !ctx.getCursor().isExclusiveLatchNodes())) {
            node.acquireReadLatch();
            return true;
        } else {
            node.acquireWriteLatch();
            return false;
        }
    }

    private ICachedPage isConsistent(int pageId, BTreeOpContext ctx) throws Exception {
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
        node.acquireReadLatch();
        ctx.getInteriorFrame().setPage(node);
        boolean isConsistent = ctx.getPageLsns().getLast() == ctx.getInteriorFrame().getPageLsn();
        if (!isConsistent) {
            node.releaseReadLatch();
            bufferCache.unpin(node);
            return null;
        }
        return node;
    }

    private void performOp(int pageId, ICachedPage parent, boolean parentIsReadLatched, BTreeOpContext ctx)
            throws HyracksDataException {
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
        ctx.getInteriorFrame().setPage(node);
        // this check performs an unprotected read in the page
        // the following could happen: TODO fill out
        boolean unsafeIsLeaf = ctx.getInteriorFrame().isLeaf();
        boolean isReadLatched = acquireLatch(node, ctx, unsafeIsLeaf);
        boolean smFlag = ctx.getInteriorFrame().getSmFlag();
        // re-check leafness after latching
        boolean isLeaf = ctx.getInteriorFrame().isLeaf();

        // remember trail of pageLsns, to unwind recursion in case of an ongoing
        // structure modification
        ctx.getPageLsns().add(ctx.getInteriorFrame().getPageLsn());
        try {
            // Latch coupling: unlatch parent.
            if (parent != null) {
                if (parentIsReadLatched) {
                    parent.releaseReadLatch();
                } else {
                    parent.releaseWriteLatch(true);
                }
                bufferCache.unpin(parent);
            }
            if (!isLeaf || smFlag) {
                if (!smFlag) {
                    // We use this loop to deal with possibly multiple operation
                    // restarts due to ongoing structure modifications during
                    // the descent.
                    boolean repeatOp = true;
                    while (repeatOp && ctx.getOpRestarts() < MAX_RESTARTS) {
                        int childPageId = ctx.getInteriorFrame().getChildPageId(ctx.getPred());
                        performOp(childPageId, node, isReadLatched, ctx);
                        node = null;

                        if (!ctx.getPageLsns().isEmpty()) {
                            if (ctx.getPageLsns().getLast() == FULL_RESTART_OP) {
                                break;
                            } else if (ctx.getPageLsns().getLast() == RESTART_OP) {
                                // Pop the restart op indicator.
                                ctx.getPageLsns().removeLast();
                                node = isConsistent(pageId, ctx);
                                if (node != null) {
                                    isReadLatched = true;
                                    // Descend the tree again.
                                    continue;
                                } else {
                                    // Pop pageLsn of this page (version seen by this op during descent).
                                    ctx.getPageLsns().removeLast();
                                    // This node is not consistent set the restart indicator for upper level.
                                    ctx.getPageLsns().add(RESTART_OP);
                                    break;
                                }
                            }
                        }

                        switch (ctx.getOperation()) {
                            case INSERT:
                            case UPSERT:
                            case UPDATE: {
                                // Is there a propagated split key?
                                if (ctx.getSplitKey().getBuffer() != null) {
                                    ICachedPage interiorNode = bufferCache
                                            .pin(BufferedFileHandle.getDiskPageId(getFileId(), pageId), false);
                                    interiorNode.acquireWriteLatch();
                                    try {
                                        // Insert or update op. Both can cause split keys to propagate upwards.
                                        insertInterior(interiorNode, pageId, ctx.getSplitKey().getTuple(), ctx);
                                    } finally {
                                        interiorNode.releaseWriteLatch(true);
                                        bufferCache.unpin(interiorNode);
                                    }
                                } else {
                                    unsetSmPages(ctx);
                                }
                                break;
                            }

                            case DELETE: {
                                if (ctx.getSplitKey().getBuffer() != null) {
                                    throw new HyracksDataException(
                                            "Split key was propagated during delete. Delete allows empty leaf pages.");
                                }
                                break;
                            }

                            default: {
                                // Do nothing for Search and DiskOrderScan.
                                break;
                            }
                        }
                        // Operation completed.
                        repeatOp = false;
                    } // end while
                } else { // smFlag
                    ctx.setOpRestarts(ctx.getOpRestarts() + 1);
                    if (isReadLatched) {
                        node.releaseReadLatch();
                    } else {
                        node.releaseWriteLatch(true);
                    }
                    bufferCache.unpin(node);

                    // TODO: this should be an instant duration lock, how to do
                    // this in java?
                    // instead we just immediately release the lock. this is
                    // inefficient but still correct and will not cause
                    // latch-deadlock
                    treeLatch.readLock().lock();
                    treeLatch.readLock().unlock();

                    // unwind recursion and restart operation, find lowest page
                    // with a pageLsn as seen by this operation during descent
                    ctx.getPageLsns().removeLast(); // pop current page lsn
                    // put special value on the stack to inform caller of
                    // restart
                    ctx.getPageLsns().add(RESTART_OP);
                }
            } else { // isLeaf and !smFlag
                // We may have to restart an op to avoid latch deadlock.
                boolean restartOp = false;
                ctx.getLeafFrame().setPage(node);
                switch (ctx.getOperation()) {
                    case INSERT: {
                        int targetTupleIndex = ctx.getLeafFrame().findInsertTupleIndex(ctx.getPred().getLowKey());
                        restartOp = insertLeaf(ctx.getPred().getLowKey(), targetTupleIndex, pageId, ctx);
                        break;
                    }
                    case UPSERT: {
                        int targetTupleIndex = ctx.getLeafFrame().findUpsertTupleIndex(ctx.getPred().getLowKey());
                        restartOp = upsertLeaf(ctx.getPred().getLowKey(), targetTupleIndex, pageId, ctx);
                        break;
                    }
                    case UPDATE: {
                        int oldTupleIndex = ctx.getLeafFrame().findUpdateTupleIndex(ctx.getPred().getLowKey());
                        restartOp = updateLeaf(ctx.getPred().getLowKey(), oldTupleIndex, pageId, ctx);
                        break;
                    }
                    case DELETE: {
                        restartOp = deleteLeaf(node, pageId, ctx.getPred().getLowKey(), ctx);
                        break;
                    }
                    case SEARCH: {
                        ctx.getCursorInitialState().setSearchOperationCallback(ctx.getSearchCallback());
                        ctx.getCursorInitialState().setOriginialKeyComparator(ctx.getCmp());
                        ctx.getCursorInitialState().setPage(node);
                        ctx.getCursorInitialState().setPageId(pageId);
                        ctx.getCursor().open(ctx.getCursorInitialState(), ctx.getPred());
                        break;
                    }
                }
                if (ctx.getOperation() != IndexOperation.SEARCH) {
                    node.releaseWriteLatch(true);
                    bufferCache.unpin(node);
                }
                if (restartOp) {
                    // Wait for the SMO to persistFrontiers before restarting.
                    // We didn't release the pin on the page!!
                    treeLatch.readLock().lock();
                    treeLatch.readLock().unlock();
                    ctx.getPageLsns().removeLast();
                    ctx.getPageLsns().add(FULL_RESTART_OP);
                }
            }
        } catch (HyracksDataException e) {
            if (!ctx.isExceptionHandled()) {
                if (node != null) {
                    if (isReadLatched) {
                        node.releaseReadLatch();
                    } else {
                        node.releaseWriteLatch(true);
                    }
                    bufferCache.unpin(node);
                    ctx.setExceptionHandled(true);
                }
            }
            throw e;
        } catch (Exception e) {
            if (node != null) {
                if (isReadLatched) {
                    node.releaseReadLatch();
                } else {
                    node.releaseWriteLatch(true);
                }
                bufferCache.unpin(node);
            }
            HyracksDataException wrappedException = HyracksDataException.create(e);
            ctx.setExceptionHandled(true);
            throw wrappedException;
        }
    }

    private BTreeOpContext createOpContext(IIndexAccessor accessor, IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new BTreeOpContext(accessor, leafFrameFactory, interiorFrameFactory, freePageManager, cmpFactories,
                modificationCallback, searchCallback);
    }

    @SuppressWarnings("rawtypes")
    public String printTree(IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame,
            ISerializerDeserializer[] keySerdes) throws Exception {
        MultiComparator cmp = MultiComparator.create(cmpFactories);
        byte treeHeight = getTreeHeight(leafFrame);
        StringBuilder strBuilder = new StringBuilder();
        printTree(rootPage, null, false, leafFrame, interiorFrame, treeHeight, keySerdes, strBuilder, cmp);
        return strBuilder.toString();
    }

    @SuppressWarnings("rawtypes")
    public void printTree(int pageId, ICachedPage parent, boolean unpin, IBTreeLeafFrame leafFrame,
            IBTreeInteriorFrame interiorFrame, byte treeHeight, ISerializerDeserializer[] keySerdes,
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
            if (interiorFrame.isLeaf()) {
                leafFrame.setPage(node);
                keyString = printLeafFrameTuples(leafFrame, keySerdes);
            } else {
                keyString = printInteriorFrameTuples(interiorFrame, keySerdes);
            }

            strBuilder.append(keyString + "\n");
            if (!interiorFrame.isLeaf()) {
                ArrayList<Integer> children = ((BTreeNSMInteriorFrame) (interiorFrame)).getChildren(cmp);
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
        }
    }

    @Override
    public BTreeAccessor createAccessor(IIndexAccessParameters iap) {
        return new BTreeAccessor(this, iap);
    }

    // TODO: Class should be private. But currently we need to expose the
    // setOpContext() API to the LSM Tree for it to work correctly.

    /* TODO: Class should be re-usable to avoid massive object creation on a per tuple basis. two solutions for this:
     * 1. have an accessor pool as part of the btree class (cleaner but introduce additional synchronization)
     * 2. don't make it an inner class (no synchronization overhead)
     *
     * for now, we are reusing it while it is an inner class !!!!
     */
    public class BTreeAccessor implements ITreeIndexAccessor {
        protected BTree btree;
        protected BTreeOpContext ctx;
        private boolean destroyed = false;
        protected IIndexAccessParameters iap;

        public BTreeAccessor(BTree btree, IIndexAccessParameters iap) {
            this.btree = btree;
            this.ctx = btree.createOpContext(this, iap.getModificationCallback(), iap.getSearchOperationCallback());
            this.iap = iap;
        }

        public void reset(BTree btree, IIndexAccessParameters iap) {
            this.btree = btree;
            ctx.setCallbacks(iap.getModificationCallback(), iap.getSearchOperationCallback());
            ctx.reset();
            this.iap = iap;
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.INSERT);
            insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.UPDATE);
            update(tuple, ctx);
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DELETE);
            delete(tuple, ctx);
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException {
            upsertIfConditionElseInsert(tuple, UnconditionalTupleAcceptor.INSTANCE);
        }

        public void upsertIfConditionElseInsert(ITupleReference tuple, ITupleAcceptor acceptor)
                throws HyracksDataException {
            ctx.setOperation(IndexOperation.UPSERT);
            ctx.setAcceptor(acceptor);
            upsert(tuple, ctx);
        }

        @Override
        public BTreeRangeSearchCursor createSearchCursor(boolean exclusive) {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new BTreeRangeSearchCursor(leafFrame, exclusive, (IIndexCursorStats) iap.getParameters()
                    .getOrDefault(HyracksConstants.INDEX_CURSOR_STATS, NoOpIndexCursorStats.INSTANCE));
        }

        public BTreeRangeSearchCursor createPointCursor(boolean exclusive, boolean stateful) {
            return createSearchCursor(exclusive);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException {
            ctx.setOperation(IndexOperation.SEARCH);
            btree.search((ITreeIndexCursor) cursor, searchPred, ctx);
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new TreeIndexDiskOrderScanCursor(leafFrame);
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            ctx.setOperation(IndexOperation.DISKORDERSCAN);
            btree.diskOrderScan(cursor, ctx);
        }

        // TODO: Ideally, this method should not exist. But we need it for
        // the changing the leafFrame and leafFrameFactory of the op context for
        // the LSM-BTree to work correctly.
        public BTreeOpContext getOpContext() {
            return ctx;
        }

        public ITreeIndexCursor createCountingSearchCursor() {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new BTreeCountingSearchCursor(leafFrame, false);
        }

        private void insert(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException {
            ctx.getModificationCallback().before(tuple);
            insertUpdateOrDelete(tuple, ctx);
        }

        private void upsert(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException {
            ctx.getModificationCallback().before(tuple);
            insertUpdateOrDelete(tuple, ctx);
        }

        private void update(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException {
            // This call only allows updating of non-key fields.
            // Updating a tuple's key necessitates deleting the old entry, and inserting the new entry.
            // The user of the BTree is responsible for dealing with non-key updates (i.e., doing a delete + insert).
            if (fieldCount == ctx.getCmp().getKeyFieldCount()) {
                HyracksDataException.create(ErrorCode.INDEX_NOT_UPDATABLE);
            }
            ctx.getModificationCallback().before(tuple);
            insertUpdateOrDelete(tuple, ctx);
        }

        private void delete(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException {
            ctx.getModificationCallback().before(tuple);
            insertUpdateOrDelete(tuple, ctx);
        }

        private void insertUpdateOrDelete(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException {
            ctx.reset();
            ctx.getPred().setLowKeyComparator(ctx.getCmp());
            ctx.getPred().setHighKeyComparator(ctx.getCmp());
            ctx.getPred().setLowKey(tuple, true);
            ctx.getPred().setHighKey(tuple, true);
            ctx.getSplitKey().reset();
            ctx.getSplitKey().getTuple().setFieldCount(ctx.getCmp().getKeyFieldCount());
            // We use this loop to deal with possibly multiple operation restarts
            // due to ongoing structure modifications during the descent.
            boolean repeatOp = true;
            while (repeatOp && ctx.getOpRestarts() < MAX_RESTARTS) {
                ctx.setSmoCount(smoCounter.get());
                performOp(rootPage, null, true, ctx);
                // Do we need to restart from the (possibly new) root?
                if (!ctx.getPageLsns().isEmpty()) {
                    if (ctx.getPageLsns().getLast() == FULL_RESTART_OP) {
                        ctx.getPageLsns().clear();
                        continue;
                    } else if (ctx.getPageLsns().getLast() == RESTART_OP) {
                        ctx.getPageLsns().removeLast(); // pop the restart op indicator
                        continue;
                    }

                }
                // Split key propagated?
                if (ctx.getSplitKey().getBuffer() != null) {
                    // Insert or update op. Create a new root.
                    createNewRoot(ctx);
                }
                unsetSmPages(ctx);
                repeatOp = false;
            }

            if (ctx.getOpRestarts() >= MAX_RESTARTS) {
                throw HyracksDataException.create(ErrorCode.OPERATION_EXCEEDED_MAX_RESTARTS, MAX_RESTARTS);
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
        return new BTreeBulkLoader(fillFactor, verifyInput, callback);
    }

    public class BTreeBulkLoader extends AbstractTreeIndex.AbstractTreeIndexBulkLoader {
        protected final ISplitKey splitKey;
        protected final boolean verifyInput;

        public BTreeBulkLoader(float fillFactor, boolean verifyInput, IPageWriteCallback callback)
                throws HyracksDataException {
            super(fillFactor, callback);
            this.verifyInput = verifyInput;
            splitKey = new BTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
            splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
        }

        @Override
        public void add(ITupleReference tuple) throws HyracksDataException {
            try {
                int tupleSize = Math.max(leafFrame.getBytesRequiredToWriteTuple(tuple),
                        interiorFrame.getBytesRequiredToWriteTuple(tuple));
                NodeFrontier leafFrontier = nodeFrontiers.get(0);
                int spaceNeeded = tupleWriter.bytesRequired(tuple) + slotSize;
                int spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();

                // try to free space by compression
                if (spaceUsed + spaceNeeded > leafMaxBytes) {
                    leafFrame.compress();
                    spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
                }
                //full, allocate new page
                if (spaceUsed + spaceNeeded > leafMaxBytes) {
                    if (leafFrame.getTupleCount() == 0) {
                        bufferCache.returnPage(leafFrontier.page, false);
                    } else {
                        leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
                        if (verifyInput) {
                            verifyInputTuple(tuple, leafFrontier.lastTuple);
                        }
                        int splitKeySize = tupleWriter.bytesRequired(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount());
                        splitKey.initData(splitKeySize);
                        tupleWriter.writeTupleFields(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount(),
                                splitKey.getBuffer().array(), 0);
                        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);
                        splitKey.setLeftPage(leafFrontier.pageId);

                        propagateBulk(1, pagesToWrite);

                        leafFrontier.pageId = freePageManager.takePage(metaFrame);

                        ((IBTreeLeafFrame) leafFrame).setNextLeaf(leafFrontier.pageId);

                        write(leafFrontier.page);
                        for (ICachedPage c : pagesToWrite) {
                            write(c);
                        }
                        pagesToWrite.clear();
                        splitKey.setRightPage(leafFrontier.pageId);
                    }
                    if (tupleSize > maxTupleSize) {
                        final long dpid = BufferedFileHandle.getDiskPageId(getFileId(), leafFrontier.pageId);
                        // calculate required number of pages.
                        int headerSize = Math.max(leafFrame.getPageHeaderSize(), interiorFrame.getPageHeaderSize());
                        final int multiplier =
                                (int) Math.ceil((double) tupleSize / (bufferCache.getPageSize() - headerSize));
                        if (multiplier > 1) {
                            leafFrontier.page = bufferCache.confiscateLargePage(dpid, multiplier,
                                    freePageManager.takeBlock(metaFrame, multiplier - 1));
                        } else {
                            leafFrontier.page = bufferCache.confiscatePage(dpid);
                        }
                        leafFrame.setPage(leafFrontier.page);
                        leafFrame.initBuffer((byte) 0);
                        ((IBTreeLeafFrame) leafFrame).setLargeFlag(true);
                    } else {
                        final long dpid = BufferedFileHandle.getDiskPageId(getFileId(), leafFrontier.pageId);
                        leafFrontier.page = bufferCache.confiscatePage(dpid);
                        leafFrame.setPage(leafFrontier.page);
                        leafFrame.initBuffer((byte) 0);
                    }
                } else {
                    if (verifyInput && leafFrame.getTupleCount() > 0) {
                        leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
                        verifyInputTuple(tuple, leafFrontier.lastTuple);
                    }
                }
                ((IBTreeLeafFrame) leafFrame).insertSorted(tuple);
            } catch (HyracksDataException | RuntimeException e) {
                logState(tuple, e);
                handleException();
                throw e;
            }
        }

        protected void verifyInputTuple(ITupleReference tuple, ITupleReference prevTuple) throws HyracksDataException {
            // New tuple should be strictly greater than last tuple.
            int cmpResult = cmp.compare(tuple, prevTuple);
            if (cmpResult < 0) {
                throw HyracksDataException.create(ErrorCode.UNSORTED_LOAD_INPUT);
            }
            if (cmpResult == 0) {
                throw HyracksDataException.create(ErrorCode.DUPLICATE_LOAD_INPUT);
            }
        }

        protected void propagateBulk(int level, List<ICachedPage> pagesToWrite) throws HyracksDataException {
            if (splitKey.getBuffer() == null) {
                return;
            }

            if (level >= nodeFrontiers.size()) {
                addLevel();
            }

            NodeFrontier frontier = nodeFrontiers.get(level);
            interiorFrame.setPage(frontier.page);

            ITupleReference tuple = splitKey.getTuple();
            int tupleBytes = tupleWriter.bytesRequired(tuple, 0, cmp.getKeyFieldCount());
            int spaceNeeded = tupleBytes + slotSize + 4;
            if (tupleBytes > interiorFrame.getMaxTupleSize(BTree.this.bufferCache.getPageSize())) {
                throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE, tupleBytes,
                        interiorFrame.getMaxTupleSize(BTree.this.bufferCache.getPageSize()));
            }

            int spaceUsed = interiorFrame.getBuffer().capacity() - interiorFrame.getTotalFreeSpace();
            if (spaceUsed + spaceNeeded > interiorMaxBytes) {

                ISplitKey copyKey = splitKey.duplicate(leafFrame.getTupleWriter().createTupleReference());
                tuple = copyKey.getTuple();

                frontier.lastTuple.resetByTupleIndex(interiorFrame, interiorFrame.getTupleCount() - 1);
                int splitKeySize = tupleWriter.bytesRequired(frontier.lastTuple, 0, cmp.getKeyFieldCount());
                splitKey.initData(splitKeySize);
                tupleWriter.writeTupleFields(frontier.lastTuple, 0, cmp.getKeyFieldCount(),
                        splitKey.getBuffer().array(), 0);
                splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);

                ((IBTreeInteriorFrame) interiorFrame).deleteGreatest();
                int finalPageId = freePageManager.takePage(metaFrame);
                frontier.page.setDiskPageId(BufferedFileHandle.getDiskPageId(getFileId(), finalPageId));
                pagesToWrite.add(frontier.page);
                splitKey.setLeftPage(finalPageId);

                propagateBulk(level + 1, pagesToWrite);
                frontier.page = bufferCache.confiscatePage(BufferCache.INVALID_DPID);
                interiorFrame.setPage(frontier.page);
                interiorFrame.initBuffer((byte) level);
            }
            ((IBTreeInteriorFrame) interiorFrame).insertSorted(tuple);
        }

        private void persistFrontiers(int level, int rightPage) throws HyracksDataException {
            if (level >= nodeFrontiers.size()) {
                rootPage = nodeFrontiers.get(level - 1).pageId;
                releasedLatches = true;
                return;
            }
            if (level < 1) {
                ICachedPage lastLeaf = nodeFrontiers.get(level).page;
                int lastLeafPage = nodeFrontiers.get(level).pageId;
                lastLeaf.setDiskPageId(BufferedFileHandle.getDiskPageId(getFileId(), nodeFrontiers.get(level).pageId));
                write(lastLeaf);
                nodeFrontiers.get(level).page = null;
                persistFrontiers(level + 1, lastLeafPage);
                return;
            }
            NodeFrontier frontier = nodeFrontiers.get(level);
            interiorFrame.setPage(frontier.page);
            //just finalize = the layer right above the leaves has correct righthand pointers already
            if (rightPage < 0) {
                throw new HyracksDataException(
                        "Error in index creation. Internal node appears to have no rightmost guide");
            }
            ((IBTreeInteriorFrame) interiorFrame).setRightmostChildPageId(rightPage);
            int finalPageId = freePageManager.takePage(metaFrame);
            frontier.page.setDiskPageId(BufferedFileHandle.getDiskPageId(getFileId(), finalPageId));
            write(frontier.page);
            frontier.pageId = finalPageId;
            persistFrontiers(level + 1, finalPageId);
        }

        @Override
        public void end() throws HyracksDataException {
            try {
                persistFrontiers(0, -1);
                super.end();
            } catch (HyracksDataException | RuntimeException e) {
                handleException();
                throw e;
            }
        }

        @Override
        public void abort() throws HyracksDataException {
            super.handleException();
        }

        private void logState(ITupleReference tuple, Exception e) {
            try {
                ObjectNode state = JSONUtil.createObject();
                state.set("leafFrame", leafFrame.getState());
                state.set("interiorFrame", interiorFrame.getState());
                int tupleSize = Math.max(leafFrame.getBytesRequiredToWriteTuple(tuple),
                        interiorFrame.getBytesRequiredToWriteTuple(tuple));
                state.put("tupleSize", tupleSize);
                state.put("spaceNeeded", tupleWriter.bytesRequired(tuple) + slotSize);
                state.put("spaceUsed", leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace());
                state.put("leafMaxBytes", leafMaxBytes);
                state.put("maxTupleSize", maxTupleSize);
                LOGGER.error("failed to add tuple {}", state, e);
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static String printLeafFrameTuples(IBTreeLeafFrame leafFrame, ISerializerDeserializer[] fieldSerdes)
            throws HyracksDataException {
        StringBuilder strBuilder = new StringBuilder();
        ITreeIndexTupleReference tuple = leafFrame.createTupleReference();
        for (int i = 0; i < leafFrame.getTupleCount(); i++) {
            tuple.resetByTupleIndex(leafFrame, i);
            String tupleString = TupleUtils.printTuple(tuple, fieldSerdes);
            strBuilder.append(tupleString + " | ");
        }
        // Print right link.
        int rightPageId = leafFrame.getNextLeaf();
        strBuilder.append("(" + rightPageId + ")");
        return strBuilder.toString();
    }

    @SuppressWarnings("rawtypes")
    public static String printInteriorFrameTuples(IBTreeInteriorFrame interiorFrame,
            ISerializerDeserializer[] fieldSerdes) throws HyracksDataException {
        StringBuilder strBuilder = new StringBuilder();
        ITreeIndexTupleReference tuple = interiorFrame.createTupleReference();
        for (int i = 0; i < interiorFrame.getTupleCount(); i++) {
            tuple.resetByTupleIndex(interiorFrame, i);
            // Print child pointer.
            int numFields = tuple.getFieldCount();
            int childPageId = IntegerPointable.getInteger(tuple.getFieldData(numFields - 1),
                    tuple.getFieldStart(numFields - 1) + tuple.getFieldLength(numFields - 1));
            strBuilder.append("(" + childPageId + ") ");
            String tupleString = TupleUtils.printTuple(tuple, fieldSerdes);
            strBuilder.append(tupleString + " | ");
        }
        // Print rightmost pointer.
        int rightMostChildPageId = interiorFrame.getRightmostChildPageId();
        strBuilder.append("(" + rightMostChildPageId + ")");
        return strBuilder.toString();
    }

    @Override
    public int getNumOfFilterFields() {
        return 0;
    }
}
