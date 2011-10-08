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

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeNotUpdateableException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
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
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;
import edu.uci.ics.hyracks.storage.common.file.BufferedFileHandle;

public class BTree implements ITreeIndex {

    public static final float DEFAULT_FILL_FACTOR = 0.7f;

    private final static int RESTART_OP = Integer.MIN_VALUE;
    private final static int MAX_RESTARTS = 10;
    // Fixed root page.
    private final static int rootPage = 1;    
        
    private boolean created = false;
    private boolean loaded = false;

    private final IFreePageManager freePageManager;
    private final IBufferCache bufferCache;    
    private final ITreeIndexFrameFactory interiorFrameFactory;
    private final ITreeIndexFrameFactory leafFrameFactory;
    private final MultiComparator cmp;
    private final ReadWriteLock treeLatch;
    private final RangePredicate diskOrderScanPredicate;
    private int fileId;

    public BTree(IBufferCache bufferCache, IFreePageManager freePageManager,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory, MultiComparator cmp) {
        this.bufferCache = bufferCache;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.cmp = cmp;
        this.freePageManager = freePageManager;
        this.treeLatch = new ReentrantReadWriteLock(true);
        this.diskOrderScanPredicate = new RangePredicate(true, null, null, true, true, cmp, cmp);
    }

    @Override
    public void create(int fileId, ITreeIndexFrame leafFrame, ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        treeLatch.writeLock().lock();
        try {
            if (created) {
                return;
            }
            freePageManager.init(metaFrame, rootPage);
            initRoot(leafFrame, true);
            created = true;
        } finally {
            treeLatch.writeLock().unlock();
        }
    }

    @Override
    public void open(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public void close() {
        fileId = -1;
    }

    private void addFreePages(BTreeOpContext ctx) throws HyracksDataException {
        for (int i = 0; i < ctx.freePages.size(); i++) {
            // root page is special, don't add it to free pages
            if (ctx.freePages.get(i) != rootPage) {
                freePageManager.addFreePage(ctx.metaFrame, ctx.freePages.get(i));
            }
        }
        ctx.freePages.clear();
    }
    
    @Override
    public void diskOrderScan(ITreeIndexCursor icursor, ITreeIndexFrame leafFrame, ITreeIndexMetaDataFrame metaFrame,
            IndexOpContext ictx) throws HyracksDataException {
        TreeDiskOrderScanCursor cursor = (TreeDiskOrderScanCursor) icursor;
        BTreeOpContext ctx = (BTreeOpContext) ictx;
        ctx.reset();

        int currentPageId = rootPage + 1;
        int maxPageId = freePageManager.getMaxPage(metaFrame);

        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId), false);
        page.acquireReadLatch();
        try {
            cursor.setBufferCache(bufferCache);
            cursor.setFileId(fileId);
            cursor.setCurrentPageId(currentPageId);
            cursor.setMaxPageId(maxPageId);
            ctx.cursorInitialState.setPage(page);
            cursor.open(ctx.cursorInitialState, diskOrderScanPredicate);
        } catch (Exception e) {
            page.releaseReadLatch();
            bufferCache.unpin(page);
            throw new HyracksDataException(e);
        }
    }

    public void search(ITreeIndexCursor cursor, RangePredicate pred, BTreeOpContext ctx) throws Exception {
        ctx.reset();
        ctx.pred = pred;
        ctx.cursor = cursor;
        // simple index scan
        if (ctx.pred.getLowKeyComparator() == null)
            ctx.pred.setLowKeyComparator(cmp);
        if (ctx.pred.getHighKeyComparator() == null)
            ctx.pred.setHighKeyComparator(cmp);
        // we use this loop to deal with possibly multiple operation restarts
        // due to ongoing structure modifications during the descent
        boolean repeatOp = true;
        while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
            performOp(rootPage, null, ctx);
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
        // make sure the root is always at the same level
        ICachedPage leftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, ctx.splitKey.getLeftPage()),
                false);
        leftNode.acquireWriteLatch();
        try {
            ICachedPage rightNode = bufferCache.pin(
                    BufferedFileHandle.getDiskPageId(fileId, ctx.splitKey.getRightPage()), false);
            rightNode.acquireWriteLatch();
            try {
                int newLeftId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage newLeftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newLeftId), true);
                newLeftNode.acquireWriteLatch();
                try {
                    // copy left child to new left child
                    System.arraycopy(leftNode.getBuffer().array(), 0, newLeftNode.getBuffer().array(), 0, newLeftNode
                            .getBuffer().capacity());
                    ctx.interiorFrame.setPage(newLeftNode);
                    ctx.interiorFrame.setSmFlag(false);

                    // change sibling pointer if children are leaves
                    ctx.leafFrame.setPage(rightNode);
                    if (ctx.leafFrame.isLeaf()) {
                        ctx.leafFrame.setPrevLeaf(newLeftId);
                    }

                    // initialize new root (leftNode becomes new root)
                    ctx.interiorFrame.setPage(leftNode);
                    ctx.interiorFrame.initBuffer((byte) (ctx.leafFrame.getLevel() + 1));
                    ctx.interiorFrame.setSmFlag(true); // will be cleared later
                    // in unsetSmPages
                    ctx.splitKey.setLeftPage(newLeftId);
                    int targetTupleIndex = ctx.interiorFrame.findInsertTupleIndex(ctx.splitKey.getTuple(), cmp);
                    ctx.interiorFrame.insert(ctx.splitKey.getTuple(), cmp, targetTupleIndex);
                } finally {
                    newLeftNode.releaseWriteLatch();
                    bufferCache.unpin(newLeftNode);
                }
            } finally {
                rightNode.releaseWriteLatch();
                bufferCache.unpin(rightNode);
            }
        } finally {
            leftNode.releaseWriteLatch();
            bufferCache.unpin(leftNode);
        }
    }
    
    private void insertUpdateOrDelete(ITupleReference tuple, IndexOpContext ictx) throws HyracksDataException, TreeIndexException {
        BTreeOpContext ctx = (BTreeOpContext) ictx;
        ctx.reset();
        ctx.pred.setLowKeyComparator(cmp);
        ctx.pred.setHighKeyComparator(cmp);
        ctx.pred.setLowKey(tuple, true);
        ctx.pred.setHighKey(tuple, true);
        ctx.splitKey.reset();
        ctx.splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());        
        // We use this loop to deal with possibly multiple operation restarts
        // due to ongoing structure modifications during the descent.
        boolean repeatOp = true;
        while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
            performOp(rootPage, null, ctx);
            // Do we need to restart from the (possibly new) root?
            if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                ctx.pageLsns.removeLast(); // pop the restart op indicator
                continue;
            }
            // Split key propagated?
            if (ctx.splitKey.getBuffer() != null) {
                if (ctx.op == IndexOp.DELETE) {
                    // Reset level of root to zero.
                    initRoot(ctx.leafFrame, false);
                } else {
                    // Insert or update op. Create a new root.
                    createNewRoot(ctx);
                }
            }
            unsetSmPages(ctx);
            if (ctx.op == IndexOp.DELETE) {
                addFreePages(ctx);
            }
            repeatOp = false;
        }
    }
    
    @Override
    public void insert(ITupleReference tuple, IndexOpContext ictx) throws HyracksDataException, TreeIndexException {
        insertUpdateOrDelete(tuple, ictx);
    }

    @Override
    public void update(ITupleReference tuple, IndexOpContext ictx) throws HyracksDataException, TreeIndexException {
        // Updating a tuple's key necessitates deleting the old entry, and inserting the new entry.
        // This call only allows updating of non-key fields.
        // The user of the BTree is responsible for dealing with non-key updates (i.e., doing a delete + insert). 
        if (cmp.getFieldCount() == cmp.getKeyFieldCount()) {
            throw new BTreeNotUpdateableException("Cannot perform updates when the entire tuple forms the key.");
        }
        insertUpdateOrDelete(tuple, ictx);
    }
    
    @Override
    public void delete(ITupleReference tuple, IndexOpContext ictx) throws HyracksDataException, TreeIndexException {
        insertUpdateOrDelete(tuple, ictx);
    }
    
    private void insertLeaf(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx) throws Exception {
        ctx.leafFrame.setPage(node);
        ctx.leafFrame.setPageTupleFieldCount(cmp.getFieldCount());
        int targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple, cmp);
        FrameOpSpaceStatus spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple, cmp);
        switch (spaceStatus) {
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.leafFrame.insert(tuple, cmp, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }
            case SUFFICIENT_SPACE: {
                boolean slotsChanged = ctx.leafFrame.compact(cmp);
                if (slotsChanged) {
                    targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple, cmp);
                }
                ctx.leafFrame.insert(tuple, cmp, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }
            case INSUFFICIENT_SPACE: {
                // Try compressing the page first and see if there is space available.
                boolean reCompressed = ctx.leafFrame.compress(cmp);
                if (reCompressed) {
                    // Compression could have changed the target tuple index, find it again.
                    targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple, cmp);
                    spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple, cmp);
                }
                if (spaceStatus == FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
                    ctx.leafFrame.insert(tuple, cmp, targetTupleIndex);
                    ctx.splitKey.reset();
                } else {
                    performLeafSplit(pageId, tuple, ctx);
                }
                break;
            }
        }
        node.releaseWriteLatch();
        bufferCache.unpin(node);
    }
    
    private void performLeafSplit(int pageId, ITupleReference tuple, BTreeOpContext ctx) throws Exception {
        int rightSiblingPageId = ctx.leafFrame.getNextLeaf();
        ICachedPage rightSibling = null;
        if (rightSiblingPageId > 0) {
            rightSibling = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightSiblingPageId),
                    false);
        }
        // Lock is released in unsetSmPages(), after sm has fully completed.
        treeLatch.writeLock().lock(); 
        try {
            int rightPageId = freePageManager.getFreePage(ctx.metaFrame);
            ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightPageId),
                    true);
            rightNode.acquireWriteLatch();
            try {
                IBTreeLeafFrame rightFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
                rightFrame.setPage(rightNode);
                rightFrame.initBuffer((byte) 0);
                rightFrame.setPageTupleFieldCount(cmp.getFieldCount());

                int ret = ctx.leafFrame.split(rightFrame, tuple, cmp, ctx.splitKey);

                ctx.smPages.add(pageId);
                ctx.smPages.add(rightPageId);
                ctx.leafFrame.setSmFlag(true);
                rightFrame.setSmFlag(true);

                rightFrame.setNextLeaf(ctx.leafFrame.getNextLeaf());
                rightFrame.setPrevLeaf(pageId);
                ctx.leafFrame.setNextLeaf(rightPageId);

                // TODO: we just use increasing numbers as pageLsn,
                // we
                // should tie this together with the LogManager and
                // TransactionManager
                rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
                ctx.leafFrame.setPageLsn(ctx.leafFrame.getPageLsn() + 1);

                if (ret != 0) {
                    ctx.splitKey.reset();
                } else {
                    ctx.splitKey.setPages(pageId, rightPageId);
                }
                if (rightSibling != null) {
                    rightSibling.acquireWriteLatch();
                    try {
                        // reuse
                        // rightFrame
                        // for
                        // modification
                        rightFrame.setPage(rightSibling);
                        rightFrame.setPrevLeaf(rightPageId);
                    } finally {
                        rightSibling.releaseWriteLatch();
                    }
                }
            } finally {
                rightNode.releaseWriteLatch();
                bufferCache.unpin(rightNode);
            }
        } catch (Exception e) {
            treeLatch.writeLock().unlock();
            throw e;
        } finally {
            if (rightSibling != null) {
                bufferCache.unpin(rightSibling);
            }
        }
    }
    
    private void updateLeaf(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx) throws Exception {
        ctx.leafFrame.setPage(node);
        ctx.leafFrame.setPageTupleFieldCount(cmp.getFieldCount());
        int oldTupleIndex = ctx.leafFrame.findUpdateTupleIndex(tuple, cmp);
        FrameOpSpaceStatus spaceStatus = ctx.leafFrame.hasSpaceUpdate(tuple, oldTupleIndex, cmp);
        switch (spaceStatus) {
            case SUFFICIENT_INPLACE_SPACE: {
                ctx.leafFrame.update(tuple, oldTupleIndex, true);
                ctx.splitKey.reset();
                break;
            }
            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.leafFrame.update(tuple, oldTupleIndex, false);
                ctx.splitKey.reset();
                break;
            }                
            case SUFFICIENT_SPACE: {
                // Delete the old tuple, compact the frame, and insert the new tuple.
                ctx.leafFrame.delete(tuple, cmp, oldTupleIndex);
                ctx.leafFrame.compact(cmp);
                int targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple, cmp);
                ctx.leafFrame.insert(tuple, cmp, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }                
            case INSUFFICIENT_SPACE: {
                // Delete the old tuple, and try compressing the page to make space available.
                ctx.leafFrame.delete(tuple, cmp, oldTupleIndex);
                ctx.leafFrame.compress(cmp);
                // We need to insert the new tuple, so check if there is space.
                spaceStatus = ctx.leafFrame.hasSpaceInsert(tuple, cmp);                
                if (spaceStatus == FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
                    int targetTupleIndex = ctx.leafFrame.findInsertTupleIndex(tuple, cmp);
                    ctx.leafFrame.insert(tuple, cmp, targetTupleIndex);
                    ctx.splitKey.reset();
                } else {
                    performLeafSplit(pageId, tuple, ctx);
                }
                break;
            }
        }
        node.releaseWriteLatch();
        bufferCache.unpin(node);
    }

    private void insertInterior(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx)
            throws Exception {
        ctx.interiorFrame.setPage(node);
        ctx.interiorFrame.setPageTupleFieldCount(cmp.getKeyFieldCount());
        int targetTupleIndex = ctx.interiorFrame.findInsertTupleIndex(tuple, cmp);
        FrameOpSpaceStatus spaceStatus = ctx.interiorFrame.hasSpaceInsert(tuple, cmp);
        switch (spaceStatus) {
            case INSUFFICIENT_SPACE: {
                int rightPageId = freePageManager.getFreePage(ctx.metaFrame);
                ICachedPage rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, rightPageId), true);
                rightNode.acquireWriteLatch();
                try {
                    ITreeIndexFrame rightFrame = interiorFrameFactory.createFrame();
                    rightFrame.setPage(rightNode);
                    rightFrame.initBuffer((byte) ctx.interiorFrame.getLevel());
                    rightFrame.setPageTupleFieldCount(cmp.getKeyFieldCount());
                    // instead of creating a new split key, use the existing
                    // splitKey
                    int ret = ctx.interiorFrame.split(rightFrame, ctx.splitKey.getTuple(), cmp, ctx.splitKey);

                    ctx.smPages.add(pageId);
                    ctx.smPages.add(rightPageId);
                    ctx.interiorFrame.setSmFlag(true);
                    rightFrame.setSmFlag(true);

                    // TODO: we just use increasing numbers as pageLsn, we
                    // should tie this together with the LogManager and
                    // TransactionManager
                    rightFrame.setPageLsn(rightFrame.getPageLsn() + 1);
                    ctx.interiorFrame.setPageLsn(ctx.interiorFrame.getPageLsn() + 1);

                    if (ret != 0) {
                        ctx.splitKey.reset();
                    } else {
                        ctx.splitKey.setPages(pageId, rightPageId);
                    }
                } finally {
                    rightNode.releaseWriteLatch();
                    bufferCache.unpin(rightNode);
                }
                break;
            }                

            case SUFFICIENT_CONTIGUOUS_SPACE: {
                ctx.interiorFrame.insert(tuple, cmp, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }

            case SUFFICIENT_SPACE: {
                boolean slotsChanged = ctx.interiorFrame.compact(cmp);
                if (slotsChanged) {
                    targetTupleIndex = ctx.interiorFrame.findInsertTupleIndex(tuple, cmp);
                }
                ctx.interiorFrame.insert(tuple, cmp, targetTupleIndex);
                ctx.splitKey.reset();
                break;
            }
        }
    }

    // TODO: to avoid latch deadlock, must modify cursor to detect empty leaves
    private void deleteLeaf(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx) throws Exception {
        ctx.leafFrame.setPage(node);
        int tupleIndex = ctx.leafFrame.findDeleteTupleIndex(tuple, cmp);
        // Will this leaf become empty?
        if (ctx.leafFrame.getTupleCount() == 1) {
            IBTreeLeafFrame siblingFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
            ICachedPage leftNode = null;
            ICachedPage rightNode = null;
            int nextLeaf = ctx.leafFrame.getNextLeaf();
            int prevLeaf = ctx.leafFrame.getPrevLeaf();
            if (prevLeaf > 0) {
                leftNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, prevLeaf), false);
            }
            try {
                if (nextLeaf > 0) {
                    rightNode = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, nextLeaf), false);
                }
                try {
                    treeLatch.writeLock().lock();
                    try {
                        ctx.leafFrame.delete(tuple, cmp, tupleIndex);
                        // to propagate the deletion we only need to make the
                        // splitKey != null
                        // we can reuse data to identify which key to delete in
                        // the parent
                        ctx.splitKey.initData(1);
                    } catch (Exception e) {
                        // don't propagate deletion upwards if deletion at this
                        // level fails
                        ctx.splitKey.reset();
                        throw e;
                    }

                    // TODO: Tie together with logging.
                    ctx.leafFrame.setPageLsn(ctx.leafFrame.getPageLsn() + 1);
                    ctx.leafFrame.setLevel(freePageManager.getFreePageLevelIndicator());

                    ctx.smPages.add(pageId);
                    ctx.leafFrame.setSmFlag(true);

                    node.releaseWriteLatch();
                    bufferCache.unpin(node);

                    if (leftNode != null) {
                        leftNode.acquireWriteLatch();
                        try {
                            siblingFrame.setPage(leftNode);
                            siblingFrame.setNextLeaf(nextLeaf);
                            // TODO: Tie together with logging.
                            siblingFrame.setPageLsn(siblingFrame.getPageLsn() + 1);
                        } finally {
                            leftNode.releaseWriteLatch();
                        }
                    }

                    if (rightNode != null) {
                        rightNode.acquireWriteLatch();
                        try {
                            siblingFrame.setPage(rightNode);
                            siblingFrame.setPrevLeaf(prevLeaf);
                            // TODO: Tie together with logging.
                            siblingFrame.setPageLsn(siblingFrame.getPageLsn() + 1);
                        } finally {
                            rightNode.releaseWriteLatch();
                        }
                    }
                    // Register pageId as a free.
                    ctx.freePages.add(pageId);
                } catch (Exception e) {
                    treeLatch.writeLock().unlock();
                    throw e;
                } finally {
                    if (rightNode != null) {
                        bufferCache.unpin(rightNode);
                    }
                }
            } finally {
                if (leftNode != null) {
                    bufferCache.unpin(leftNode);
                }
            }
        } else {
            // Leaf will not become empty
            ctx.leafFrame.delete(tuple, cmp, tupleIndex);
            node.releaseWriteLatch();
            bufferCache.unpin(node);
        }
    }

    private void deleteInterior(ICachedPage node, int pageId, ITupleReference tuple, BTreeOpContext ctx)
            throws Exception {
        ctx.interiorFrame.setPage(node);

        int tupleIndex = ctx.interiorFrame.findDeleteTupleIndex(tuple, cmp);
        
        // this means there is only a child pointer but no key, this case
        // propagates the split
        if (ctx.interiorFrame.getTupleCount() == 0) {
            ctx.interiorFrame.setPageLsn(ctx.interiorFrame.getPageLsn() + 1); // TODO:
            // tie
            // together
            // with
            // logging
            ctx.leafFrame.setLevel(freePageManager.getFreePageLevelIndicator());
            ctx.smPages.add(pageId);
            ctx.interiorFrame.setSmFlag(true);
            ctx.interiorFrame.setRightmostChildPageId(-1); // this node is
            // completely empty
            // register this pageId as a free page
            ctx.freePages.add(pageId);

        } else {
            ctx.interiorFrame.delete(tuple, cmp, tupleIndex);
            ctx.interiorFrame.setPageLsn(ctx.interiorFrame.getPageLsn() + 1); // TODO:
            // tie
            // together
            // with
            // logging
            ctx.splitKey.reset(); // don't propagate deletion
        }
    }

    private final void acquireLatch(ICachedPage node, IndexOp op, boolean isLeaf) {
        if (isLeaf && (op == IndexOp.INSERT || op == IndexOp.DELETE || op == IndexOp.UPDATE)) {
            node.acquireWriteLatch();
        } else {
            node.acquireReadLatch();
        }
    }

    private final void releaseLatch(ICachedPage node, IndexOp op, boolean isLeaf) {
        if (isLeaf && (op == IndexOp.INSERT || op == IndexOp.DELETE || op == IndexOp.UPDATE)) {
            node.releaseWriteLatch();
        } else {
            node.releaseReadLatch();
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

    private void performOp(int pageId, ICachedPage parent, BTreeOpContext ctx) throws HyracksDataException, TreeIndexException {
        ICachedPage node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
        ctx.interiorFrame.setPage(node);
        // this check performs an unprotected read in the page
        // the following could happen: TODO fill out
        boolean unsafeIsLeaf = ctx.interiorFrame.isLeaf();
        acquireLatch(node, ctx.op, unsafeIsLeaf);
        boolean smFlag = ctx.interiorFrame.getSmFlag();
        // re-check leafness after latching
        boolean isLeaf = ctx.interiorFrame.isLeaf();

        // remember trail of pageLsns, to unwind recursion in case of an ongoing
        // structure modification
        ctx.pageLsns.add(ctx.interiorFrame.getPageLsn());
        try {

            // latch coupling, note: parent should never be write latched,
            // otherwise something is wrong.
            if (parent != null) {
                parent.releaseReadLatch();
                bufferCache.unpin(parent);
            }
            if (!isLeaf || smFlag) {
                if (!smFlag) {
                    // We use this loop to deal with possibly multiple operation
                    // restarts due to ongoing structure modifications during
                    // the descent.
                    boolean repeatOp = true;
                    while (repeatOp && ctx.opRestarts < MAX_RESTARTS) {
                        int childPageId = ctx.interiorFrame.getChildPageId(ctx.pred, cmp);
                        performOp(childPageId, node, ctx);

                        if (!ctx.pageLsns.isEmpty() && ctx.pageLsns.getLast() == RESTART_OP) {
                            // Pop the restart op indicator.
                            ctx.pageLsns.removeLast();
                            if (isConsistent(pageId, ctx)) {
                                // Don't unpin and unlatch node again in recursive call.
                                node = null; 
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
                            case UPDATE:
                            case DELETE: {
                                // Is there a propagated split key?
                                if (ctx.splitKey.getBuffer() != null) {
                                    node = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), false);
                                    node.acquireWriteLatch();
                                    try {
                                        if (ctx.op == IndexOp.DELETE) {
                                            deleteInterior(node, pageId, ctx.pred.getLowKey(), ctx);                                          
                                        } else {
                                            // Insert or update op. Both can cause split keys to propagate upwards.                                            
                                            insertInterior(node, pageId, ctx.splitKey.getTuple(), ctx);
                                        }
                                    } finally {
                                        node.releaseWriteLatch();
                                        bufferCache.unpin(node);
                                    }
                                } else {
                                    unsetSmPages(ctx);
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
                    System.out.println("ONGOING SM ON PAGE " + pageId + " AT LEVEL " + ctx.interiorFrame.getLevel()
                            + ", RESTARTS: " + ctx.opRestarts);
                    releaseLatch(node, ctx.op, unsafeIsLeaf);
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
                switch (ctx.op) {
                    case INSERT: {
                        insertLeaf(node, pageId, ctx.pred.getLowKey(), ctx);
                        break;
                    }
                    case UPDATE: {
                        updateLeaf(node, pageId, ctx.pred.getLowKey(), ctx);
                        break;
                    }
                    case DELETE: {
                        deleteLeaf(node, pageId, ctx.pred.getLowKey(), ctx);
                        break;
                    }
                    case SEARCH: {
                        ctx.cursorInitialState.setPage(node);
                        ctx.cursor.open(ctx.cursorInitialState, ctx.pred);
                        break;
                    }
                }
            }
        } catch (TreeIndexException e) {
            //e.printStackTrace();
            if (!e.getHandled()) {
                releaseLatch(node, ctx.op, unsafeIsLeaf);
                bufferCache.unpin(node);
                e.setHandled(true);
            }
            throw e;
        } catch (Exception e) {
            //e.printStackTrace();
            // This could be caused, e.g. by a failure to pin a new node during a split.
            releaseLatch(node, ctx.op, unsafeIsLeaf);
            bufferCache.unpin(node);
            BTreeException propException = new BTreeException(e);
            propException.setHandled(true);
            // propagate a BTreeException,
            // indicating that the parent node
            // must not be unlatched and
            // unpinned
            throw propException;
        }
    }

    public final class BulkLoadContext implements IIndexBulkLoadContext {
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
                ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {

            splitKey = new BTreeSplitKey(leafFrame.getTupleWriter().createTupleReference());
            tupleWriter = leafFrame.getTupleWriter();

            NodeFrontier leafFrontier = new NodeFrontier(leafFrame.createTupleReference());
            leafFrontier.pageId = freePageManager.getFreePage(metaFrame);
            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId),
                    true);
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
        int spaceNeeded = ctx.tupleWriter.bytesRequired(tuple, 0, cmp.getKeyFieldCount()) + ctx.slotSize + 4;
        int spaceUsed = ctx.interiorFrame.getBuffer().capacity() - ctx.interiorFrame.getTotalFreeSpace();
        if (spaceUsed + spaceNeeded > ctx.interiorMaxBytes) {

            BTreeSplitKey copyKey = ctx.splitKey.duplicate(ctx.leafFrame.getTupleWriter().createTupleReference());
            tuple = copyKey.getTuple();

            frontier.lastTuple.resetByTupleOffset(frontier.page.getBuffer(),
                    ctx.interiorFrame.getTupleOffset(ctx.interiorFrame.getTupleCount() - 1));
            int splitKeySize = ctx.tupleWriter.bytesRequired(frontier.lastTuple, 0, cmp.getKeyFieldCount());
            ctx.splitKey.initData(splitKeySize);
            ctx.tupleWriter
                    .writeTupleFields(frontier.lastTuple, 0, cmp.getKeyFieldCount(), ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.getTuple().resetByTupleOffset(ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.setLeftPage(frontier.pageId);

            ctx.interiorFrame.deleteGreatest(cmp);

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
        ctx.interiorFrame.insertSorted(tuple, cmp);
    }

    // assumes btree has been created and opened
    @Override
    public IIndexBulkLoadContext beginBulkLoad(float fillFactor, ITreeIndexFrame leafFrame,
            ITreeIndexFrame interiorFrame, ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        if (loaded) {
            throw new HyracksDataException("Trying to bulk-load BTree but BTree has already been loaded.");
        }
        BulkLoadContext ctx = new BulkLoadContext(fillFactor, (IBTreeLeafFrame) leafFrame,
                (IBTreeInteriorFrame) interiorFrame, metaFrame);
        ctx.nodeFrontiers.get(0).lastTuple.setFieldCount(cmp.getFieldCount());
        ctx.splitKey.getTuple().setFieldCount(cmp.getKeyFieldCount());
        return ctx;
    }

    @Override
    public void bulkLoadAddTuple(IIndexBulkLoadContext ictx, ITupleReference tuple) throws HyracksDataException {
        BulkLoadContext ctx = (BulkLoadContext) ictx;
        NodeFrontier leafFrontier = ctx.nodeFrontiers.get(0);
        IBTreeLeafFrame leafFrame = ctx.leafFrame;

        int spaceNeeded = ctx.tupleWriter.bytesRequired(tuple) + ctx.slotSize;
        int spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();

        // try to free space by compression
        if (spaceUsed + spaceNeeded > ctx.leafMaxBytes) {
            leafFrame.compress(cmp);
            spaceUsed = leafFrame.getBuffer().capacity() - leafFrame.getTotalFreeSpace();
        }

        if (spaceUsed + spaceNeeded > ctx.leafMaxBytes) {
            leafFrontier.lastTuple.resetByTupleIndex(leafFrame, leafFrame.getTupleCount() - 1);
            int splitKeySize = ctx.tupleWriter.bytesRequired(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount());
            ctx.splitKey.initData(splitKeySize);
            ctx.tupleWriter.writeTupleFields(leafFrontier.lastTuple, 0, cmp.getKeyFieldCount(),
                    ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.getTuple().resetByTupleOffset(ctx.splitKey.getBuffer(), 0);
            ctx.splitKey.setLeftPage(leafFrontier.pageId);
            int prevPageId = leafFrontier.pageId;
            leafFrontier.pageId = freePageManager.getFreePage(ctx.metaFrame);

            leafFrame.setNextLeaf(leafFrontier.pageId);
            leafFrontier.page.releaseWriteLatch();
            bufferCache.unpin(leafFrontier.page);

            ctx.splitKey.setRightPage(leafFrontier.pageId);
            propagateBulk(ctx, 1);

            leafFrontier.page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafFrontier.pageId),
                    true);
            leafFrontier.page.acquireWriteLatch();
            leafFrame.setPage(leafFrontier.page);
            leafFrame.initBuffer((byte) 0);
            leafFrame.setPrevLeaf(prevPageId);
        }

        leafFrame.setPage(leafFrontier.page);
        leafFrame.insertSorted(tuple, cmp);
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
        loaded = true;
    }

    @Override
    public BTreeOpContext createOpContext(IndexOp op, ITreeIndexFrame leafFrame, ITreeIndexFrame interiorFrame,
            ITreeIndexMetaDataFrame metaFrame) {
        return new BTreeOpContext(op, (IBTreeLeafFrame) leafFrame, (IBTreeInteriorFrame) interiorFrame, metaFrame, 6);
    }

    public ITreeIndexFrameFactory getInteriorFrameFactory() {
        return interiorFrameFactory;
    }

    public ITreeIndexFrameFactory getLeafFrameFactory() {
        return leafFrameFactory;
    }

    public MultiComparator getMultiComparator() {
        return cmp;
    }

    public IFreePageManager getFreePageManager() {
        return freePageManager;
    }

    public int getRootPageId() {
        return rootPage;
    }    

    @Override
    public int getFieldCount() {
        return cmp.getFieldCount();
    }

    @Override
    public IndexType getIndexType() {
        return IndexType.BTREE;
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
    
    @SuppressWarnings("rawtypes") 
    public String printTree(IBTreeLeafFrame leafFrame, IBTreeInteriorFrame interiorFrame, ISerializerDeserializer[] fieldSerdes)
            throws Exception {
        byte treeHeight = getTreeHeight(leafFrame);
        StringBuilder strBuilder = new StringBuilder();
        printTree(rootPage, null, false, leafFrame, interiorFrame, treeHeight, fieldSerdes, strBuilder);
        return strBuilder.toString();
    }

    @SuppressWarnings("rawtypes") 
    public void printTree(int pageId, ICachedPage parent, boolean unpin, IBTreeLeafFrame leafFrame,
            IBTreeInteriorFrame interiorFrame, byte treeHeight, ISerializerDeserializer[] fieldSerdes, StringBuilder strBuilder) throws Exception {
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
            strBuilder.append(String.format("%3d ", pageId));
            for (int i = 0; i < treeHeight - level; i++) {
                strBuilder.append("    ");
            }

            String keyString;
            if (interiorFrame.isLeaf()) {
                leafFrame.setPage(node);
                keyString = leafFrame.printKeys(cmp, fieldSerdes);
            } else {
                keyString = interiorFrame.printKeys(cmp, fieldSerdes);
            }

            strBuilder.append(keyString);
            if (!interiorFrame.isLeaf()) {
                ArrayList<Integer> children = ((BTreeNSMInteriorFrame) (interiorFrame)).getChildren(cmp);
                for (int i = 0; i < children.size(); i++) {
                    printTree(children.get(i), node, i == children.size() - 1, leafFrame, interiorFrame, treeHeight, fieldSerdes, strBuilder);
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
}
