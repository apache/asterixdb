/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.ITupleAcceptor;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeNotUpdateableException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.api.UnsortedInputException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.impls.AbstractTreeIndex;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NodeFrontier;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class BTree extends AbstractTreeIndex {

    public static final float DEFAULT_FILL_FACTOR = 0.7f;

    private final static long RESTART_OP = Long.MIN_VALUE;
    private final static long FULL_RESTART_OP = Long.MIN_VALUE + 1;
    private final static int MAX_RESTARTS = 10;

    private final AtomicInteger smoCounter;
    private final ReadWriteLock treeLatch;
    private final int maxTupleSize;

    public BTree(IBufferCache bufferCache, IFileMapProvider fileMapProvider, IFreePageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            IBinaryComparatorFactory[] cmpFactories, int fieldCount, FileReference file) {
        super(bufferCache, fileMapProvider, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                fieldCount, file);
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
        RangePredicate diskOrderScanPred = new RangePredicate(null, null, true, true, ctx.cmp, ctx.cmp);
        int currentPageId = rootPage;
        int maxPageId = freePageManager.getMaxPage(ctx.metaFrame);
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
        page.acquireReadLatch();
        try {
            cursor.setBufferCache(bufferCache);
            cursor.setFileId(fileId);
            cursor.setCurrentPageId(currentPageId);
            cursor.setMaxPageId(maxPageId);
            ctx.cursorInitialState.setPage(page);
            ctx.cursorInitialState.setSearchOperationCallback(ctx.searchCallback);
            ctx.cursorInitialState.setOriginialKeyComparator(ctx.cmp);
            cursor.open(ctx.cursorInitialState, diskOrderScanPred);
        } catch (Exception e) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            throw new HyracksDataException(e);
        }
    }

    public void validate() throws HyracksDataException {
        // Stack validation protocol:
        //      * parent pushes the validation information onto the stack before validation
        //      * child pops the validation information off of the stack after validating
        BTreeAccessor accessor = (BTreeAccessor) createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        PageValidationInfo pvi = accessor.ctx.createPageValidationInfo(null);
        accessor.ctx.validationInfos.addFirst(pvi);
        validate(accessor.ctx, rootPage);
    }

    private void validate(BTreeOpContext ctx, int pageId) throws HyracksDataException {
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        ctx.interiorFrame.setPage(page);
        PageValidationInfo currentPvi = ctx.validationInfos.peekFirst();

        boolean isLeaf = ctx.interiorFrame.isLeaf();
        if (isLeaf) {
            ctx.leafFrame.setPage(page);
            ctx.leafFrame.validate(currentPvi);
        } else {
            PageValidationInfo nextPvi = ctx.createPageValidationInfo(currentPvi);
            List<Integer> children = ((BTreeNSMInteriorFrame) ctx.interiorFrame).getChildren(ctx.cmp);
            ctx.interiorFrame.validate(currentPvi);
            for (int i = 0; i < children.size(); i++) {
                ctx.interiorFrame.setPage(page);

                if (children.size() == 1) {
                    // There is a single child pointer with no keys, so propagate both low and high ranges
                    nextPvi.propagateLowRangeKey(currentPvi);
                    nextPvi.propagateHighRangeKey(currentPvi);
                } else if (i == 0) {
                    // There is more than one child pointer and this is the left-most child pointer, so:
                    //      1) propagate the low range key from the parent
                    //      2) adjust the high range key
                    nextPvi.propagateLowRangeKey(currentPvi);
                    ctx.interiorFrameTuple.resetByTupleIndex(ctx.interiorFrame, i);
                    nextPvi.adjustHighRangeKey(ctx.interiorFrameTuple);
                } else if (i == children.size() - 1) {
                    // There is more than one child pointer and this is the right-most child pointer, so:
                    //      1) propagate the high range key from the parent
                    //      2) adjust the low range key
                    nextPvi.propagateHighRangeKey(currentPvi);
                    ctx.interiorFrameTuple.resetByTupleIndex(ctx.interiorFrame, i - 1);
                    nextPvi.adjustLowRangeKey(ctx.interiorFrameTuple);
                } else {
                    // There is more than one child pointer and this pointer is not the left/right-most pointer, so:
                    //      1) adjust the low range key
                    //      2) adjust the high range key
                    ctx.interiorFrameTuple.resetByTupleIndex(ctx.interiorFrame, i - 1);
                    nextPvi.adjustLowRangeKey(ctx.interiorFrameTuple);
                    ctx.interiorFrameTuple.resetByTupleIndex(ctx.interiorFrame, i);
                    nextPvi.adjustHighRangeKey(ctx.interiorFrameTuple);
                }

                ctx.validationInfos.addFirst(nextPvi);
                validate(ctx, children.get(i));
            }
        }
        bufferCache.unpin(page);
        ctx.validationInfos.removeFirst();
    }

    private void search(ITreeIndexCursor cursor, ISearchPredicate searchPred, BTreeOpContext ctx)
            throws TreeIndexException, HyracksDataException {
        ctx.reset();
        ctx.pred = (RangePredicate) searchPred;
        ctx.cursor = cursor;
        // simple index scan
        if (ctx.pred.getLowKeyComparator() == null) {
            ctx.pred.setLowKeyComparator(ctx.cmp);
        }
        if (ctx.pred.getHighKeyComparator() == null) {
            ctx.pred.setHighKeyComparator(ctx.cmp);
        }
        // we use this loop to deal with possibly multiple operation restarts
        // due to ongoing structure modifications during the descent
        boolean repeatOp = true;
        while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
            performOp(rootPage, null, true, ctx);
            // if we reach this stage then we need to restart from the (possibly
            // new) root
            if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                ctx.pageLsns.removeLast(); // pop the restart op indicator
                continue;
            }
            repeatOp = false;
        }
        cursor.setBufferCache(bufferCache);
        cursor.setFileId(fileId);
    }

    private void unsetSmPages(BTreeOpContext ctx) throws HyracksDataException {
        ICachedPage originalPage = ctx.interiorFrame.getPage();
        for (int i = 0; i < ctx.smPages.size(); i++) {
            int pageId = ctx.smPages.get(i);
            ICachedPage smPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
            smPage.acquireWriteLatch();
            try {
                ctx.interiorFrame.setPage(smPage);
                ctx.interiorFrame.setSmFlag(false);
            } finally {
                smPage.releaseWriteLatch();
                bufferCache.unpin(smPage);
            }
        }
        if (ctx.smPages.size() > 0) {
            if (ctx.smoCount == Integer.MAX_VALUE) {
                smoCounter.set(0);
            } else {
                smoCounter.incrementAndGet();
            }
            treeLatch.writeLock().unlock();
            ctx.smPages.clear();
        }
        ctx.interiorFrame.setPage(originalPage);
    }

    private void createNewRoot(BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        // Make sure the root is always in the same page.
        ICachedPage leftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, ctx.splitKey.getLeftPage()),
                false);
        leftNode.acquireWriteLatch();
        try {
            int newLeftId = freePageManager.getFreePage(ctx.metaFrame);
            ICachedPage newLeftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeftId), true);
            newLeftNode.acquireWriteLatch();
            try {
                // Copy left child to new left child.
                System.arraycopy(leftNode.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0, newLeftNode
                        .getBuffer().capacity());
                ctx.interiorFrame.setPage(newLeftNode);
                ctx.interiorFrame.setSmFlag(false);
                // Remember LSN to set it in the root.
                long leftNodeLSN = ctx.interiorFrame.getPageLsn();
                // Initialize new root (leftNode becomes new root).
                ctx.interiorFrame.setPage(leftNode);
                ctx.interiorFrame.initBuffer((byte) (ctx.interiorFrame.getLevel() + 1));
                // Copy over LSN.
                ctx.interiorFrame.setPageLsn(leftNodeLSN);
                // Will be cleared later in unsetSmPages.
                ctx.interiorFrame.setSmFlag(true);
                ctx.splitKey.setLeftPage(newLeftId);
                int targetTupleIndex = ctx.interiorFrame.findInsertTupleIndex(ctx.splitKey.getTuple());
                ctx.interiorFrame.insert(ctx.splitKey.getTuple(), targetTupleIndex);
            } finally {
                newLeftNode.releaseWriteLatch();
                bufferCache.unpin(newLeftNode);
            }
        } finally {
            leftNode.releaseWriteLatch();
            bufferCache.unpin(leftNode);
        }
    }

    private void insertUpdateOrDelete(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException,
            TreeIndexException {
        ctx.reset();
        ctx.pred.setLowKeyComparator(ctx.cmp);
        ctx.pred.setHighKeyComparator(ctx.cmp);
        ctx.pred.setLowKey(tuple, true);
        ctx.pred.setHighKey(tuple, true);
        ctx.splitKey.reset();
        ctx.splitKey.getTuple().setFieldCount(ctx.cmp.getKeyFieldCount());
        // We use this loop to deal with possibly multiple operation restarts
        // due to ongoing structure modifications during the descent.
        boolean repeatOp = true;
        while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
            ctx.smoCount = smoCounter.get();
            performOp(rootPage, null, true, ctx);
            // Do we need to restart from the (possibly new) root?
            if (!ctx.pageLsns.isEmpty()) {
                if (ctx.pageLsns.getLast() == FULL_RESTART_OP) {
                    ctx.pageLsns.clear();
                    continue;
                } else if (ctx.pageLsns.getLast() == RESTART_OP) {
                    ctx.pageLsns.removeLast(); // pop the restart op indicator
                    continue;
                }

            }
            // Split key propagated?
            if (ctx.splitKey.getBuffer() != null) {
                // Insert or update op. Create a new root.
                createNewRoot(ctx);
            }
            unsetSmPages(ctx);
            repeatOp = false;
        }

        if (ctx.opRestarts >= MAX_RESTARTS) {
            throw new BTreeException("Operation exceeded the maximum number of restarts");
        }
    }

    private void insert(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        int tupleSize = Math.max(ctx.leafFrame.getBytesRequriedToWriteTuple(tuple),
                ctx.interiorFrame.getBytesRequriedToWriteTuple(tuple));
        if (tupleSize > maxTupleSize) {
            throw new TreeIndexException("Space required for record (" + tupleSize
                    + ") larger than maximum acceptable size (" + maxTupleSize + ")");
        }
        ctx.modificationCallback.before(tuple);
        insertUpdateOrDelete(tuple, ctx);
    }

    private void upsert(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        int tupleSize = Math.max(ctx.leafFrame.getBytesRequriedToWriteTuple(tuple),
                ctx.interiorFrame.getBytesRequriedToWriteTuple(tuple));
        if (tupleSize > maxTupleSize) {
            throw new TreeIndexException("Space required for record (" + tupleSize
                    + ") larger than maximum acceptable size (" + maxTupleSize + ")");
        }
        ctx.modificationCallback.before(tuple);
        insertUpdateOrDelete(tuple, ctx);
    }

    private void update(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        // This call only allows updating of non-key fields.
        // Updating a tuple's key necessitates deleting the old entry, and inserting the new entry.
        // The user of the BTree is responsible for dealing with non-key updates (i.e., doing a delete + insert). 
        if (fieldCount == ctx.cmp.getKeyFieldCount()) {
            throw new BTreeNotUpdateableException("Cannot perform updates when the entire tuple forms the key.");
        }
        int tupleSize = Math.max(ctx.leafFrame.getBytesRequriedToWriteTuple(tuple),
                ctx.interiorFrame.getBytesRequriedToWriteTuple(tuple));
        if (tupleSize > maxTupleSize) {
            throw new TreeIndexException("Space required for record (" + tupleSize
                    + ") larger than maximum acceptable size (" + maxTupleSize + ")");
        }
        ctx.modificationCallback.before(tuple);
        insertUpdateOrDelete(tuple, ctx);
    }

    private void delete(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        ctx.modificationCallback.before(tuple);
        insertUpdateOrDelete(tuple, ctx);
    }

    private boolean insertLeaf(ITupleReference tuple, int targetTupleIndex, int pageId, BTreeOpContext ctx)
            throws Exception {
        boolean restartOp = false;
        FrameOpSpaceStatus spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple);
        switch (spaceStatus) {
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.modificationCallback.found(null, tuple);
                ctx.leafFrame.insert(tuple, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }
            case SUFFICIENT_SPACE: {
                boolean slotsChanged = ctx.leafFrame.compact();
                if (slotsChanged) {
                    targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple);
                }
                ctx.modificationCallback.found(null, tuple);
                ctx.leafFrame.insert(tuple, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }
            case INSUFFICIENT_SPACE: {
                // Try compressing the page first and see if there is space available.
                boolean reCompressed = ctx.leafFrame.compress();
                if (reCompressed) {
                    // Compression could have changed the target tuple index, find it again.
                    targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple);
                    spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple);
                }
                if (spaceStatus == FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
                    ctx.modificationCallback.found(null, tuple);
                    ctx.leafFrame.insert(tuple, targetTupleIndex);
                    ctx.splitKey.reset();
                } else {
                    restartOp = performLeafSplit(pageId, tuple, ctx, -1);
                }
                break;
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
            if (tempSmoCount != ctx.smoCount) {
                treeLatch.writeLock().unlock();
                return true;
            }
        }
        int rightPageId = freePageManager.getFreePage(ctx.metaFrame);
        ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightPageId), true);
        rightNode.acquireWriteLatch();
        try {
            IBTreeLeafFrame rightFrame = ctx.createLeafFrame();
            rightFrame.setPage(rightNode);
            rightFrame.initBuffer((byte) 0);
            rightFrame.setMultiComparator(ctx.cmp);

            // Perform an update (delete + insert) if the updateTupleIndex != -1
            if (updateTupleIndex != -1) {
                ITupleReference beforeTuple = ctx.leafFrame.getMatchingKeyTuple(tuple, updateTupleIndex);
                ctx.modificationCallback.found(beforeTuple, tuple);
                ctx.leafFrame.delete(tuple, updateTupleIndex);
            } else {
                ctx.modificationCallback.found(null, tuple);
            }
            ctx.leafFrame.split(rightFrame, tuple, ctx.splitKey);

            ctx.smPages.add(pageId);
            ctx.smPages.add(rightPageId);
            ctx.leafFrame.setSmFlag(true);
            rightFrame.setSmFlag(true);

            rightFrame.setNextLeaf(ctx.leafFrame.getNextLeaf());
            ctx.leafFrame.setNextLeaf(rightPageId);

            rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
            ctx.leafFrame.setPageLsn(ctx.leafFrame.getPageLsn() + 1);

            ctx.splitKey.setPages(pageId, rightPageId);
        } catch (Exception e) {
            treeLatch.writeLock().unlock();
            throw e;
        } finally {
            rightNode.releaseWriteLatch();
            bufferCache.unpin(rightNode);
        }
        return false;
    }

    private boolean updateLeaf(ITupleReference tuple, int oldTupleIndex, int pageId, BTreeOpContext ctx)
            throws Exception {
        FrameOpSpaceStatus spaceStatus = ctx.leafFrame.hasSpaceUpdate(tuple, oldTupleIndex);
        ITupleReference beforeTuple = ctx.leafFrame.getMatchingKeyTuple(tuple, oldTupleIndex);
        boolean restartOp = false;
        switch (spaceStatus) {
            case SUFFICIENT_INPLACE_SPACE: {
                ctx.modificationCallback.found(beforeTuple, tuple);
                ctx.leafFrame.update(tuple, oldTupleIndex, true);
                ctx.splitKey.reset();
                break;
            }
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.modificationCallback.found(beforeTuple, tuple);
                ctx.leafFrame.update(tuple, oldTupleIndex, false);
                ctx.splitKey.reset();
                break;
            }
            case SUFFICIENT_SPACE: {
                // Delete the old tuple, compact the frame, and insert the new tuple.
                ctx.modificationCallback.found(beforeTuple, tuple);
                ctx.leafFrame.delete(tuple, oldTupleIndex);
                ctx.leafFrame.compact();
                int targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple);
                ctx.leafFrame.insert(tuple, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }
            case INSUFFICIENT_SPACE: {
                restartOp = performLeafSplit(pageId, tuple, ctx, oldTupleIndex);
                break;
            }
        }
        return restartOp;
    }

    private boolean upsertLeaf(ITupleReference tuple, int targetTupleIndex, int pageId, BTreeOpContext ctx)
            throws Exception {
        boolean restartOp = false;
        ITupleReference beforeTuple = ctx.leafFrame.getMatchingKeyTuple(tuple, targetTupleIndex);
        if (ctx.acceptor.accept(beforeTuple)) {
            if (beforeTuple == null) {
                restartOp = insertLeaf(tuple, targetTupleIndex, pageId, ctx);
            } else {
                restartOp = updateLeaf(tuple, targetTupleIndex, pageId, ctx);
            }
        } else {
            targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple);
            restartOp = insertLeaf(tuple, targetTupleIndex, pageId, ctx);
        }
        return restartOp;
    }

    private void insertInterior(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx)
            throws Exception {
        ctx.interiorFrame.setPage(node);
        int targetTupleIndex = ctx.interiorFrame.findInsertTupleIndex(tuple);
        FrameOpSpaceStatus spaceStatus = ctx.interiorFrame.hasSpaceInsert(tuple);
        switch (spaceStatus) {
            case INSUFFICIENT_SPACE: {
                int rightPageId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightPageId), true);
                rightNode.acquireWriteLatch();
                try {
                    IBTreeFrame rightFrame = ctx.createInteriorFrame();
                    rightFrame.setPage(rightNode);
                    rightFrame.initBuffer((byte) ctx.interiorFrame.getLevel());
                    rightFrame.setMultiComparator(ctx.cmp);
                    // instead of creating a new split key, use the existing
                    // splitKey
                    ctx.interiorFrame.split(rightFrame, ctx.splitKey.getTuple(), ctx.splitKey);
                    ctx.smPages.add(pageId);
                    ctx.smPages.add(rightPageId);
                    ctx.interiorFrame.setSmFlag(true);
                    rightFrame.setSmFlag(true);
                    rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
                    ctx.interiorFrame.setPageLsn(ctx.interiorFrame.getPageLsn() + 1);

                    ctx.splitKey.setPages(pageId, rightPageId);
                } finally {
                    rightNode.releaseWriteLatch();
                    bufferCache.unpin(rightNode);
                }
                break;
            }

            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.interiorFrame.insert(tuple, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }

            case SUFFICIENT_SPACE: {
                boolean slotsChanged = ctx.interiorFrame.compact();
                if (slotsChanged) {
                    targetTupleIndex = ctx.interiorFrame.findInsertTupleIndex(tuple);
                }
                ctx.interiorFrame.insert(tuple, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }
        }
    }

    private boolean deleteLeaf(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx)
            throws Exception {
        // Simply delete the tuple, and don't do any rebalancing.
        // This means that there could be underflow, even an empty page that is
        // pointed to by an interior node.
        if (ctx.leafFrame.getTupleCount() == 0) {
            throw new TreeIndexNonExistentKeyException("Trying to delete a tuple with a nonexistent key in leaf node.");
        }
        int tupleIndex = ctx.leafFrame.findDeleteTupleIndex(tuple);
        ITupleReference beforeTuple = ctx.leafFrame.getMatchingKeyTuple(tuple, tupleIndex);
        ctx.modificationCallback.found(beforeTuple, tuple);
        ctx.leafFrame.delete(tuple, tupleIndex);
        return false;
    }

    private final boolean acquireLatch(ICachedPage node, BTreeOpContext ctx, boolean isLeaf) {
        if (!isLeaf || (ctx.op == IndexOperation.SEARCH && !ctx.cursor.exclusiveLatchNodes())) {
            node.acquireReadLatch();
            return true;
        } else {
            node.acquireWriteLatch();
            return false;
        }
    }

    private ICachedPage isConsistent(int pageId, BTreeOpContext ctx) throws Exception {
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        node.acquireReadLatch();
        ctx.interiorFrame.setPage(node);
        boolean isConsistent = ctx.pageLsns.getLast() == ctx.interiorFrame.getPageLsn();
        if (!isConsistent) {
            node.releaseReadLatch();
            bufferCache.unpin(node);
            return null;
        }
        return node;
    }

    private void performOp(int pageId, ICachedPage parent, boolean parentIsReadLatched, BTreeOpContext ctx)
            throws HyracksDataException, TreeIndexException {
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        ctx.interiorFrame.setPage(node);
        // this check performs an unprotected read in the page
        // the following could happen: TODO fill out
        boolean unsafeIsLeaf = ctx.interiorFrame.isLeaf();
        boolean isReadLatched = acquireLatch(node, ctx, unsafeIsLeaf);
        boolean smFlag = ctx.interiorFrame.getSmFlag();
        // re-check leafness after latching
        boolean isLeaf = ctx.interiorFrame.isLeaf();

        // remember trail of pageLsns, to unwind recursion in case of an ongoing
        // structure modification
        ctx.pageLsns.add(ctx.interiorFrame.getPageLsn());
        try {
            // Latch coupling: unlatch parent.
            if (parent != null) {
                if (parentIsReadLatched) {
                    parent.releaseReadLatch();
                } else {
                    parent.releaseWriteLatch();
                }
                bufferCache.unpin(parent);
            }
            if (!isLeaf || smFlag) {
                if (!smFlag) {
                    // We use this loop to deal with possibly multiple operation
                    // restarts due to ongoing structure modifications during
                    // the descent.
                    boolean repeatOp = true;
                    while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
                        int childPageId = ctx.interiorFrame.getChildPageId(ctx.pred);
                        performOp(childPageId, node, isReadLatched, ctx);

                        if (!ctx.pageLsns.isEmpty()) {
                            if (ctx.pageLsns.getLast() == FULL_RESTART_OP) {
                                break;
                            } else if (ctx.pageLsns.getLast() == RESTART_OP) {
                                // Pop the restart op indicator.
                                ctx.pageLsns.removeLast();
                                node = isConsistent(pageId, ctx);
                                if (node != null) {
                                    isReadLatched = true;
                                    // Descend the tree again.                                
                                    continue;
                                } else {
                                    // Pop pageLsn of this page (version seen by this op during descent).
                                    ctx.pageLsns.removeLast();
                                    // This node is not consistent set the restart indicator for upper level.
                                    ctx.pageLsns.add(RESTART_OP);
                                    break;
                                }
                            }
                        }

                        switch (ctx.op) {
                            case INSERT:
                            case UPSERT:
                            case UPDATE: {
                                // Is there a propagated split key?
                                if (ctx.splitKey.getBuffer() != null) {
                                    ICachedPage interiorNode = bufferCache.pin(
                                            BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                                    interiorNode.acquireWriteLatch();
                                    try {
                                        // Insert or update op. Both can cause split keys to propagate upwards. 
                                        insertInterior(interiorNode, pageId, ctx.splitKey.getTuple(), ctx);
                                    } finally {
                                        interiorNode.releaseWriteLatch();
                                        bufferCache.unpin(interiorNode);
                                    }
                                } else {
                                    unsetSmPages(ctx);
                                }
                                break;
                            }

                            case DELETE: {
                                if (ctx.splitKey.getBuffer() != null) {
                                    throw new BTreeException(
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
                    ctx.opRestarts++;
                    if (isReadLatched) {
                        node.releaseReadLatch();
                    } else {
                        node.releaseWriteLatch();
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
                    ctx.pageLsns.removeLast(); // pop current page lsn
                    // put special value on the stack to inform caller of
                    // restart
                    ctx.pageLsns.add(RESTART_OP);
                }
            } else { // isLeaf and !smFlag
                // We may have to restart an op to avoid latch deadlock.
                boolean restartOp = false;
                ctx.leafFrame.setPage(node);
                switch (ctx.op) {
                    case INSERT: {
                        int targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(ctx.pred.getLowKey());
                        restartOp = insertLeaf(ctx.pred.getLowKey(), targetTupleIndex, pageId, ctx);
                        break;
                    }
                    case UPSERT: {
                        int targetTupleIndex = ctx.leafFrame.findUpsertTupleIndex(ctx.pred.getLowKey());
                        restartOp = upsertLeaf(ctx.pred.getLowKey(), targetTupleIndex, pageId, ctx);
                        break;
                    }
                    case UPDATE: {
                        int oldTupleIndex = ctx.leafFrame.findUpdateTupleIndex(ctx.pred.getLowKey());
                        restartOp = updateLeaf(ctx.pred.getLowKey(), oldTupleIndex, pageId, ctx);
                        break;
                    }
                    case DELETE: {
                        restartOp = deleteLeaf(node, pageId, ctx.pred.getLowKey(), ctx);
                        break;
                    }
                    case SEARCH: {
                        ctx.cursorInitialState.setSearchOperationCallback(ctx.searchCallback);
                        ctx.cursorInitialState.setOriginialKeyComparator(ctx.cmp);
                        ctx.cursorInitialState.setPage(node);
                        ctx.cursorInitialState.setPageId(pageId);
                        ctx.cursor.open(ctx.cursorInitialState, ctx.pred);
                        break;
                    }
                }
                if (ctx.op != IndexOperation.SEARCH) {
                    node.releaseWriteLatch();
                    bufferCache.unpin(node);
                }
                if (restartOp) {
                    // Wait for the SMO to finish before restarting.
                    treeLatch.readLock().lock();
                    treeLatch.readLock().unlock();
                    ctx.pageLsns.removeLast();
                    ctx.pageLsns.add(FULL_RESTART_OP);
                }
            }
        } catch (TreeIndexException e) {
            if (!ctx.exceptionHandled) {
                if (node != null) {
                    if (isReadLatched) {
                        node.releaseReadLatch();
                    } else {
                        node.releaseWriteLatch();
                    }
                    bufferCache.unpin(node);
                    ctx.exceptionHandled = true;
                }
            }
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            if (node != null) {
                if (isReadLatched) {
                    node.releaseReadLatch();
                } else {
                    node.releaseWriteLatch();
                }
                bufferCache.unpin(node);
            }
            BTreeException wrappedException = new BTreeException(e);
            ctx.exceptionHandled = true;
            throw wrappedException;
        }
    }

    private BTreeOpContext createOpContext(IIndexAccessor accessor,
            IModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback) {
        return new BTreeOpContext(accessor, leafFrameFactory, interiorFrameFactory, freePageManager
                .getMetaDataFrameFactory().createFrame(), cmpFactories, modificationCallback, searchCallback);
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
            e.printStackTrace();
        }
    }

    @Override
    public ITreeIndexAccessor createAccessor(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new BTreeAccessor(this, modificationCallback, searchCallback);
    }

    // TODO: Class should be private. But currently we need to expose the
    // setOpContext() API to the LSM Tree for it to work correctly.
    public class BTreeAccessor implements ITreeIndexAccessor {
        private BTree btree;
        private BTreeOpContext ctx;

        public BTreeAccessor(BTree btree, IModificationOperationCallback modificationCalback,
                ISearchOperationCallback searchCallback) {
            this.btree = btree;
            this.ctx = btree.createOpContext(this, modificationCalback, searchCallback);
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.setOperation(IndexOperation.INSERT);
            btree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.setOperation(IndexOperation.UPDATE);
            btree.update(tuple, ctx);
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.setOperation(IndexOperation.DELETE);
            btree.delete(tuple, ctx);
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            upsertIfConditionElseInsert(tuple, UnconditionalTupleAcceptor.INSTANCE);
        }

        public void upsertIfConditionElseInsert(ITupleReference tuple, ITupleAcceptor acceptor)
                throws HyracksDataException, TreeIndexException {
            ctx.setOperation(IndexOperation.UPSERT);
            ctx.acceptor = acceptor;
            btree.upsert(tuple, ctx);
        }

        @Override
        public ITreeIndexCursor createSearchCursor() {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new BTreeRangeSearchCursor(leafFrame, false);
        }

        @Override
        public void search(IIndexCursor cursor, ISearchPredicate searchPred) throws HyracksDataException,
                TreeIndexException {
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
    }

    @Override
    public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws TreeIndexException {
        try {
            return new BTreeBulkLoader(fillFactor, verifyInput);
        } catch (HyracksDataException e) {
            throw new TreeIndexException(e);
        }
    }

    public class BTreeBulkLoader extends AbstractTreeIndex.AbstractTreeIndexBulkLoader {
        protected final ISplitKey splitKey;
        protected final boolean verifyInput;

        public BTreeBulkLoader(float fillFactor, boolean verifyInput) throws TreeIndexException, HyracksDataException {
            super(fillFactor);
            this.verifyInput = verifyInput;
            splitKey = new BTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
            splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
        }

        @Override
        public void add(ITupleReference tuple) throws IndexException, HyracksDataException {
            try {
                int tupleSize = Math.max(leafFrame.getBytesRequriedToWriteTuple(tuple),
                        interiorFrame.getBytesRequriedToWriteTuple(tuple));
                if (tupleSize > maxTupleSize) {
                    throw new TreeIndexException("Space required for record (" + tupleSize
                            + ") larger than maximum acceptable size (" + maxTupleSize + ")");
                }

                NodeFrontier leafFrontier = nodeFrontiers.get(0);

                int spaceNeeded = tupleWriter.bytesRequired(tuple) + slotSize;
                int spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();

                // try to free space by compression
                if (spaceUsed + spaceNeeded > leafMaxBytes) {
                    leafFrame.compress();
                    spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
                }

                if (spaceUsed + spaceNeeded > leafMaxBytes) {
                    leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
                    if (verifyInput) {
                        verifyInputTuple(tuple, leafFrontier.lastTuple);
                    }
                    int splitKeySize = tupleWriter.bytesRequired(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount());
                    splitKey.initData(splitKeySize);
                    tupleWriter.writeTupleFields(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount(), splitKey
                            .getBuffer().array(), 0);
                    splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);
                    splitKey.setLeftPage(leafFrontier.pageId);
                    leafFrontier.pageId = freePageManager.getFreePage(metaFrame);

                    ((IBTreeLeafFrame) leafFrame).setNextLeaf(leafFrontier.pageId);
                    leafFrontier.page.releaseWriteLatch();
                    bufferCache.unpin(leafFrontier.page);

                    splitKey.setRightPage(leafFrontier.pageId);
                    propagateBulk(1);

                    leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId),
                            true);
                    leafFrontier.page.acquireWriteLatch();
                    leafFrame.setPage(leafFrontier.page);
                    leafFrame.initBuffer((byte) 0);
                } else {
                    if (verifyInput && leafFrame.getTupleCount() > 0) {
                        leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
                        verifyInputTuple(tuple, leafFrontier.lastTuple);
                    }
                }

                leafFrame.setPage(leafFrontier.page);
                ((IBTreeLeafFrame) leafFrame).insertSorted(tuple);
            } catch (IndexException e) {
                handleException();
                throw e;
            } catch (HyracksDataException e) {
                handleException();
                throw e;
            } catch (RuntimeException e) {
                handleException();
                throw e;
            }
        }

        protected void verifyInputTuple(ITupleReference tuple, ITupleReference prevTuple) throws IndexException,
                HyracksDataException {
            // New tuple should be strictly greater than last tuple.
            int cmpResult = cmp.compare(tuple, prevTuple);
            if (cmpResult < 0) {
                throw new UnsortedInputException("Input stream given to BTree bulk load is not sorted.");
            }
            if (cmpResult == 0) {
                throw new TreeIndexDuplicateKeyException("Input stream given to BTree bulk load has duplicates.");
            }
        }

        protected void propagateBulk(int level) throws HyracksDataException {
            if (splitKey.getBuffer() == null)
                return;

            if (level >= nodeFrontiers.size())
                addLevel();

            NodeFrontier frontier = nodeFrontiers.get(level);
            interiorFrame.setPage(frontier.page);

            ITupleReference tuple = splitKey.getTuple();
            int spaceNeeded = tupleWriter.bytesRequired(tuple, 0, cmp.getKeyFieldCount()) + slotSize + 4;
            int spaceUsed = interiorFrame.getBuffer().capacity() - interiorFrame.getTotalFreeSpace();
            if (spaceUsed + spaceNeeded > interiorMaxBytes) {

                ISplitKey copyKey = splitKey.duplicate(leafFrame.getTupleWriter().createTupleReference());
                tuple = copyKey.getTuple();

                frontier.lastTuple.resetByTupleIndex(interiorFrame, interiorFrame.getTupleCount() - 1);
                int splitKeySize = tupleWriter.bytesRequired(frontier.lastTuple, 0, cmp.getKeyFieldCount());
                splitKey.initData(splitKeySize);
                tupleWriter.writeTupleFields(frontier.lastTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer()
                        .array(), 0);
                splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);
                splitKey.setLeftPage(frontier.pageId);

                ((IBTreeInteriorFrame) interiorFrame).deleteGreatest();

                frontier.page.releaseWriteLatch();
                bufferCache.unpin(frontier.page);
                frontier.pageId = freePageManager.getFreePage(metaFrame);

                splitKey.setRightPage(frontier.pageId);
                propagateBulk(level + 1);

                frontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, frontier.pageId), true);
                frontier.page.acquireWriteLatch();
                interiorFrame.setPage(frontier.page);
                interiorFrame.initBuffer((byte) level);
            }
            ((IBTreeInteriorFrame) interiorFrame).insertSorted(tuple);
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
            int childPageId = IntegerSerializerDeserializer.getInt(tuple.getFieldData(numFields - 1),
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
}
