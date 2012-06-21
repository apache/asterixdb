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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeNotUpdateableException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.IndexType;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.util.TreeIndexUtils;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class BTree implements ITreeIndex {

    public static final float DEFAULT_FILL_FACTOR = 0.7f;

    private final static long RESTART_OP = Long.MIN_VALUE;
    private final static int MAX_RESTARTS = 10;
    private final static int rootPage = 1;

    private final IFreePageManager freePageManager;
    private final IBufferCache bufferCache;
    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final ITreeIndexFrameFactory leafFrameFactory;
    private final int fieldCount;
    private final IBinaryComparatorFactory[] cmpFactories;
    private final ReadWriteLock treeLatch;
    private int fileId;

    public BTree(IBufferCache bufferCache, int fieldCount, IBinaryComparatorFactory[] cmpFactories,
            IFreePageManager freePageManager, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory) {
        this.bufferCache = bufferCache;
        this.fieldCount = fieldCount;
        this.cmpFactories = cmpFactories;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.freePageManager = freePageManager;
        this.treeLatch = new ReentrantReadWriteLock(true);
    }

    @Override
    public void create(int fileId) throws HyracksDataException {
        treeLatch.writeLock().lock();
        try {
            ITreeIndexFrame leafFrame = leafFrameFactory.createFrame();
            ITreeIndexMetaDataFrame metaFrame = freePageManager.getMetaDataFrameFactory().createFrame();
            this.fileId = fileId;
            freePageManager.open(fileId);
            freePageManager.init(metaFrame, rootPage);
            initRoot(leafFrame, true);
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

    private void diskOrderScan(ITreeIndexCursor icursor, BTreeOpContext ctx) throws HyracksDataException {
        TreeDiskOrderScanCursor cursor = (TreeDiskOrderScanCursor) icursor;
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
            cursor.open(ctx.cursorInitialState, diskOrderScanPred);
        } catch (Exception e) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            throw new HyracksDataException(e);
        }
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
            treeLatch.writeLock().unlock();
            ctx.smPages.clear();
        }
        ctx.interiorFrame.setPage(originalPage);
    }

    private void initRoot(ITreeIndexFrame leafFrame, boolean firstInit) throws HyracksDataException {
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), firstInit);
        rootNode.acquireWriteLatch();
        try {
            leafFrame.setPage(rootNode);
            leafFrame.initBuffer((byte) 0);
        } finally {
            rootNode.releaseWriteLatch();
            bufferCache.unpin(rootNode);
        }
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
                // Initialize new root (leftNode becomes new root).
                ctx.interiorFrame.setPage(leftNode);
                ctx.interiorFrame.initBuffer((byte) (ctx.leafFrame.getLevel() + 1));
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
            performOp(rootPage, null, true, ctx);
            // Do we need to restart from the (possibly new) root?
            if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                ctx.pageLsns.removeLast(); // pop the restart op indicator
                continue;
            }
            // Split key propagated?
            if (ctx.splitKey.getBuffer() != null) {
                // Insert or update op. Create a new root.
                createNewRoot(ctx);
            }
            unsetSmPages(ctx);
            repeatOp = false;
        }
        
        if(ctx.opRestarts >= MAX_RESTARTS) {
            throw new BTreeException("Operation exceeded the maximum number of restarts");
        }
    }

    private void insert(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        ctx.modificationCallback.commence(tuple);
        insertUpdateOrDelete(tuple, ctx);
    }

    private void upsert(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        ctx.modificationCallback.commence(tuple);
        insertUpdateOrDelete(tuple, ctx);
    }

    private void update(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        // This call only allows updating of non-key fields.
        // Updating a tuple's key necessitates deleting the old entry, and inserting the new entry.
        // The user of the BTree is responsible for dealing with non-key updates (i.e., doing a delete + insert). 
        if (fieldCount == ctx.cmp.getKeyFieldCount()) {
            throw new BTreeNotUpdateableException("Cannot perform updates when the entire tuple forms the key.");
        }
        ctx.modificationCallback.commence(tuple);
        insertUpdateOrDelete(tuple, ctx);
    }

    private void delete(ITupleReference tuple, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        ctx.modificationCallback.commence(tuple);
        insertUpdateOrDelete(tuple, ctx);
    }

    private boolean insertLeaf(ITupleReference tuple, int targetTupleIndex, int pageId, BTreeOpContext ctx)
            throws Exception {
        boolean restartOp = false;
        FrameOpSpaceStatus spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple);
        switch (spaceStatus) {
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.modificationCallback.found(null);
                ctx.leafFrame.insert(tuple, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }
            case SUFFICIENT_SPACE: {
                boolean slotsChanged = ctx.leafFrame.compact();
                if (slotsChanged) {
                    targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple);
                }
                ctx.modificationCallback.found(null);
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
                    ctx.modificationCallback.found(null);
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
                ITupleReference beforeTuple = ctx.leafFrame.getUpsertBeforeTuple(tuple, updateTupleIndex);
                ctx.modificationCallback.found(beforeTuple);
                ctx.leafFrame.delete(tuple, updateTupleIndex);
            } else {
                ctx.modificationCallback.found(null);
            }
            ctx.leafFrame.split(rightFrame, tuple, ctx.splitKey);

            ctx.smPages.add(pageId);
            ctx.smPages.add(rightPageId);
            ctx.leafFrame.setSmFlag(true);
            rightFrame.setSmFlag(true);

            rightFrame.setNextLeaf(ctx.leafFrame.getNextLeaf());
            ctx.leafFrame.setNextLeaf(rightPageId);

            // TODO: we just use increasing numbers as pageLsn,
            // we
            // should tie this together with the LogManager and
            // TransactionManager
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
        ITupleReference beforeTuple = ctx.leafFrame.getUpsertBeforeTuple(tuple, oldTupleIndex);
        boolean restartOp = false;
        switch (spaceStatus) {
            case SUFFICIENT_INPLACE_SPACE: {
                ctx.modificationCallback.found(beforeTuple);
                ctx.leafFrame.update(tuple, oldTupleIndex, true);
                ctx.splitKey.reset();
                break;
            }
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.modificationCallback.found(beforeTuple);
                ctx.leafFrame.update(tuple, oldTupleIndex, false);
                ctx.splitKey.reset();
                break;
            }
            case SUFFICIENT_SPACE: {
                // Delete the old tuple, compact the frame, and insert the new tuple.
                ctx.modificationCallback.found(beforeTuple);
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
        ITupleReference beforeTuple = ctx.leafFrame.getUpsertBeforeTuple(tuple, targetTupleIndex);
        if (beforeTuple == null) {
            restartOp = insertLeaf(tuple, targetTupleIndex, pageId, ctx);
        } else {
            restartOp = updateLeaf(tuple, targetTupleIndex, pageId, ctx);
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
                    // TODO: we just use increasing numbers as pageLsn, we
                    // should tie this together with the LogManager and
                    // TransactionManager
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
            throw new BTreeNonExistentKeyException("Trying to delete a tuple with a nonexistent key in leaf node.");
        }
        int tupleIndex = ctx.leafFrame.findDeleteTupleIndex(tuple);
        ITupleReference beforeTuple = ctx.leafFrame.getUpsertBeforeTuple(tuple, tupleIndex);
        ctx.modificationCallback.found(beforeTuple);
        ctx.leafFrame.delete(tuple, tupleIndex);
        return false;
    }

    private final boolean acquireLatch(ICachedPage node, BTreeOpContext ctx, boolean isLeaf) {
        if (!isLeaf || (ctx.op == IndexOp.SEARCH && !ctx.cursor.exclusiveLatchNodes())) {
            node.acquireReadLatch();
            return true;
        } else {
            node.acquireWriteLatch();
            return false;
        }
    }

    private boolean isConsistent(int pageId, BTreeOpContext ctx) throws Exception {
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        node.acquireReadLatch();
        ctx.interiorFrame.setPage(node);
        boolean isConsistent = false;
        try {
            isConsistent = ctx.pageLsns.getLast() == ctx.interiorFrame.getPageLsn();
        } finally {
            node.releaseReadLatch();
            bufferCache.unpin(node);
        }
        return isConsistent;
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

                        if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                            // Pop the restart op indicator.
                            ctx.pageLsns.removeLast();
                            if (isConsistent(pageId, ctx)) {
                                // Pin and latch page again, since it was unpinned and unlatched in call to performOp (passed as parent).
                                node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                                node.acquireReadLatch();
                                ctx.interiorFrame.setPage(node);
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
                    treeLatch.writeLock().lock();
                    treeLatch.writeLock().unlock();

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
                        ctx.cursorInitialState.setPage(node);
                        ctx.cursor.open(ctx.cursorInitialState, ctx.pred);
                        break;
                    }
                }
                if (ctx.op != IndexOp.SEARCH) {
                    node.releaseWriteLatch();
                    bufferCache.unpin(node);
                }
                if (restartOp) {
                    ctx.pageLsns.removeLast();
                    ctx.pageLsns.add(RESTART_OP);
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

    public class BulkLoadContext implements IIndexBulkLoadContext {
        public final MultiComparator cmp;
        public final int slotSize;
        public final int leafMaxBytes;
        public final int interiorMaxBytes;
        public final BTreeSplitKey splitKey;
        // we maintain a frontier of nodes for each level
        private final ArrayList<NodeFrontier> nodeFrontiers = new ArrayList<NodeFrontier>();
        private final IBTreeLeafFrame leafFrame;
        private final IBTreeInteriorFrame interiorFrame;
        private final ITreeIndexMetaDataFrame metaFrame;
        private final ITreeIndexTupleWriter tupleWriter;

        public BulkLoadContext(float fillFactor, IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame,
                ITreeIndexMetaDataFrame metaFrame, IBinaryComparatorFactory[] cmpFactories) throws HyracksDataException {
            this.cmp = MultiComparator.create(cmpFactories);

            leafFrame.setMultiComparator(cmp);
            interiorFrame.setMultiComparator(cmp);

            splitKey = new BTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId), true);
            leafFrontier.page.acquireWriteLatch();

            interiorFrame.setPage(leafFrontier.page);
            interiorFrame.initBuffer((byte) 0);
            interiorMaxBytes = (int) ((float) interiorFrame.getBuffer().capacity() * fillFactor);

            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafMaxBytes = (int) ((float) leafFrame.getBuffer().capacity() * fillFactor);

            slotSize = leafFrame.getSlotSize();

            this.leafFrame = leafFrame;
            this.interiorFrame = interiorFrame;
            this.metaFrame = metaFrame;

            nodeFrontiers.add(leafFrontier);
        }

        private void addLevel() throws HyracksDataException {
            NodeFrontier frontier = new NodeFrontier(tupleWriter.createTupleReference());
            frontier.pageId = freePageManager.getFreePage(metaFrame);
            frontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, frontier.pageId), true);
            frontier.page.acquireWriteLatch();
            frontier.lastTuple.setFieldCount(cmp.getKeyFieldCount());
            interiorFrame.setPage(frontier.page);
            interiorFrame.initBuffer((byte) nodeFrontiers.size());
            nodeFrontiers.add(frontier);
        }
    }

    private void propagateBulk(BulkLoadContext ctx, int level) throws HyracksDataException {

        if (ctx.splitKey.getBuffer() == null)
            return;

        if (level >= ctx.nodeFrontiers.size())
            ctx.addLevel();

        NodeFrontier frontier = ctx.nodeFrontiers.get(level);
        ctx.interiorFrame.setPage(frontier.page);

        ITupleReference tuple = ctx.splitKey.getTuple();
        int spaceNeeded = ctx.tupleWriter.bytesRequired(tuple, 0, ctx.cmp.getKeyFieldCount()) + ctx.slotSize + 4;
        int spaceUsed = ctx.interiorFrame.getBuffer().capacity() - ctx.interiorFrame.getTotalFreeSpace();
        if (spaceUsed + spaceNeeded > ctx.interiorMaxBytes) {

            BTreeSplitKey copyKey = ctx.splitKey.duplicate(ctx.leafFrame.getTupleWriter().createTupleReference());
            tuple = copyKey.getTuple();

            frontier.lastTuple.resetByTupleIndex(ctx.interiorFrame, ctx.interiorFrame.getTupleCount() - 1);
            int splitKeySize = ctx.tupleWriter.bytesRequired(frontier.lastTuple, 0, ctx.cmp.getKeyFieldCount());
            ctx.splitKey.initData(splitKeySize);
            ctx.tupleWriter.writeTupleFields(frontier.lastTuple, 0, ctx.cmp.getKeyFieldCount(), ctx.splitKey
                    .getBuffer().array(), 0);
            ctx.splitKey.getTuple().resetByTupleOffset(ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.setLeftPage(frontier.pageId);

            ctx.interiorFrame.deleteGreatest();

            frontier.page.releaseWriteLatch();
            bufferCache.unpin(frontier.page);
            frontier.pageId = freePageManager.getFreePage(ctx.metaFrame);

            ctx.splitKey.setRightPage(frontier.pageId);
            propagateBulk(ctx, level + 1);

            frontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, frontier.pageId), true);
            frontier.page.acquireWriteLatch();
            ctx.interiorFrame.setPage(frontier.page);
            ctx.interiorFrame.initBuffer((byte) level);
        }
        ctx.interiorFrame.insertSorted(tuple);
    }

    // assumes btree has been created and opened
    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor) throws TreeIndexException, HyracksDataException {
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        if (!isEmptyTree(leafFrame)) {
            throw new BTreeException("Trying to Bulk-load a non-empty BTree.");
        }

        BulkLoadContext ctx = new BulkLoadContext(fillFactor, leafFrame,
                (IBTreeInteriorFrame) interiorFrameFactory.createFrame(), freePageManager.getMetaDataFrameFactory()
                        .createFrame(), cmpFactories);
        ctx.splitKey.getTuple().setFieldCount(ctx.cmp.getKeyFieldCount());
        return ctx;
    }

    @Override
    public void bulkLoadAddTuple(ITupleReference tuple, IIndexBulkLoadContext ictx) throws HyracksDataException {
        BulkLoadContext ctx = (BulkLoadContext) ictx;
        NodeFrontier leafFrontier = ctx.nodeFrontiers.get(0);
        IBTreeLeafFrame leafFrame = ctx.leafFrame;

        int spaceNeeded = ctx.tupleWriter.bytesRequired(tuple) + ctx.slotSize;
        int spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();

        // try to free space by compression
        if (spaceUsed + spaceNeeded > ctx.leafMaxBytes) {
            leafFrame.compress();
            spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
        }

        if (spaceUsed + spaceNeeded > ctx.leafMaxBytes) {
            leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
            int splitKeySize = ctx.tupleWriter.bytesRequired(leafFrontier.lastTuple, 0, ctx.cmp.getKeyFieldCount());
            ctx.splitKey.initData(splitKeySize);
            ctx.tupleWriter.writeTupleFields(leafFrontier.lastTuple, 0, ctx.cmp.getKeyFieldCount(), ctx.splitKey
                    .getBuffer().array(), 0);
            ctx.splitKey.getTuple().resetByTupleOffset(ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.setLeftPage(leafFrontier.pageId);
            leafFrontier.pageId = freePageManager.getFreePage(ctx.metaFrame);

            leafFrame.setNextLeaf(leafFrontier.pageId);
            leafFrontier.page.releaseWriteLatch();
            bufferCache.unpin(leafFrontier.page);

            ctx.splitKey.setRightPage(leafFrontier.pageId);
            propagateBulk(ctx, 1);

            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId), true);
            leafFrontier.page.acquireWriteLatch();
            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
        }

        leafFrame.setPage(leafFrontier.page);
        leafFrame.insertSorted(tuple);
    }

    @Override
    public void endBulkLoad(IIndexBulkLoadContext ictx) throws HyracksDataException {
        // copy root
        BulkLoadContext ctx = (BulkLoadContext) ictx;
        ICachedPage rootNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rootPage), true);
        rootNode.acquireWriteLatch();
        NodeFrontier lastNodeFrontier = ctx.nodeFrontiers.get(ctx.nodeFrontiers.size() - 1);
        IBTreeInteriorFrame interiorFrame = ctx.interiorFrame;
        try {
            ICachedPage toBeRoot = lastNodeFrontier.page;
            System.arraycopy(toBeRoot.getBuffer().array(), 0, rootNode.getBuffer().array(), 0, toBeRoot.getBuffer()
                    .capacity());
        } finally {
            rootNode.releaseWriteLatch();
            bufferCache.unpin(rootNode);

            // register old root as free page
            freePageManager.addFreePage(ctx.metaFrame, lastNodeFrontier.pageId);

            // make old root a free page
            interiorFrame.setPage(lastNodeFrontier.page);
            interiorFrame.initBuffer(freePageManager.getFreePageLevelIndicator());

            // cleanup
            for (int i = 0; i < ctx.nodeFrontiers.size(); i++) {
                ctx.nodeFrontiers.get(i).page.releaseWriteLatch();
                bufferCache.unpin(ctx.nodeFrontiers.get(i).page);
            }
        }
    }

    private BTreeOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) {
        return new BTreeOpContext(leafFrameFactory, interiorFrameFactory, freePageManager.getMetaDataFrameFactory()
                .createFrame(), cmpFactories, modificationCallback, searchCallback);
    }

    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public IBinaryComparatorFactory[] getComparatorFactories() {
        return cmpFactories;
    }

    public IFreePageManager getFreePageManager() {
        return freePageManager;
    }

    public int getRootPageId() {
        return rootPage;
    }

    @Override
    public int getFieldCount() {
        return fieldCount;
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.BTREE;
    }

    @Override
    public int getFileId() {
        return fileId;
    }

    @Override
    public IBufferCache getBufferCache() {
        return bufferCache;
    }

    public byte getTreeHeight(IBTreeLeafFrame leafFrame) throws HyracksDataException {
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

    public boolean isEmptyTree(IBTreeLeafFrame leafFrame) throws HyracksDataException {
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
                keyString = TreeIndexUtils.printFrameTuples(leafFrame, keySerdes);
            } else {
                keyString = TreeIndexUtils.printFrameTuples(interiorFrame, keySerdes);
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
            this.ctx = btree.createOpContext(modificationCalback, searchCallback);
        }

        @Override
        public void insert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.INSERT);
            btree.insert(tuple, ctx);
        }

        @Override
        public void update(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.UPDATE);
            btree.update(tuple, ctx);
        }

        @Override
        public void delete(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.DELETE);
            btree.delete(tuple, ctx);
        }

        @Override
        public void upsert(ITupleReference tuple) throws HyracksDataException, TreeIndexException {
            ctx.reset(IndexOp.UPSERT);
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
            ctx.reset(IndexOp.SEARCH);
            btree.search((ITreeIndexCursor) cursor, searchPred, ctx);
        }

        @Override
        public ITreeIndexCursor createDiskOrderScanCursor() {
            IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
            return new TreeDiskOrderScanCursor(leafFrame);
        }

        @Override
        public void diskOrderScan(ITreeIndexCursor cursor) throws HyracksDataException {
            ctx.reset(IndexOp.DISKORDERSCAN);
            btree.diskOrderScan(cursor, ctx);
        }

        // TODO: Ideally, this method should not exist. But we need it for
        // the changing the leafFrame and leafFrameFactory of the op context for
        // the LSM-BTree to work correctly.
        public BTreeOpContext getOpContext() {
            return ctx;
        }
    }
}
