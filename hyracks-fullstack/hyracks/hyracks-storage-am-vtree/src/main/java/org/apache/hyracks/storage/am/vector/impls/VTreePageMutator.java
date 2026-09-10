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

package org.apache.hyracks.storage.am.vector.impls;

import static org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider.NEW;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.frames.VTreeDataFrame;
import org.apache.hyracks.storage.am.vector.frames.VTreeMetadataFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeDataTupleAccessor;
import org.apache.hyracks.storage.am.vector.utils.VTreeMetadataTupleAccessor;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Page-level mutation of a {@link VTree}'s directory and data pages: the half of the write path below
 * cluster selection. Extracted from {@code VTree} so that page mutation can be exercised — and reasoned
 * about — without the tree navigation, static-structure attachment and accessor machinery it used to sit
 * beside.
 * <p>
 * <b>What it does and does not know.</b> Everything here operates on a directory-page chain that a caller
 * has already chosen: the walk over that chain, the distance-ordered placement within a data page, page
 * splits, and the directory bookkeeping each split implies. It knows nothing about centroids, distance
 * metrics, cross-pollination, the static structure, or how a cluster was picked. The seam is exactly two
 * entry points — {@link #insertIntoDataPages} and {@link #tryPhysicalDelete} — both taking the directory
 * page id the caller resolved, which is why the split is a move rather than a redesign.
 * <p>
 * <b>Latching contract.</b> Both entry points pin and latch the pages they touch and release them before
 * returning; nothing is held across a call. The metadata-mutation helpers are the exception and are
 * deliberately so: they operate on the directory frame their caller already holds write-latched, which
 * {@link #requireLatchedMetadataFrame} asserts rather than documents. The frames themselves live on the
 * caller's {@link VTreeOpContext} and are shared, so an instance of this class holds no per-operation
 * state and is safe to share across threads exactly as far as the op-contexts are kept separate.
 */
// Not a record: a stateless collaborator, not a data carrier. Record accessors would publish
// bufferCache/freePageManager as package API, and record visibility cannot be narrowed.
@SuppressWarnings("ClassCanBeRecord")
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Extracted verbatim from VTree; behaviour unchanged")
class VTreePageMutator {

    private final IBufferCache bufferCache;
    private final IPageManager freePageManager;
    private final ITreeIndexFrameFactory metadataFrameFactory;

    VTreePageMutator(IBufferCache bufferCache, IPageManager freePageManager,
            ITreeIndexFrameFactory metadataFrameFactory) {
        this.bufferCache = bufferCache;
        this.freePageManager = freePageManager;
        this.metadataFrameFactory = metadataFrameFactory;
    }

    /**
     * Insert vector data into data pages via metadata pages. This method traverses through all linked metadata pages to
     * find the appropriate data page.
     */
    void insertIntoDataPages(long metadataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx, int fileId) throws HyracksDataException {

        // A forward walk that write-latches one directory page at a time. A concurrent split of a page
        // moves only entries at or below its maximum to a new page linked after it, so a page the walk
        // has passed can never come to hold this key. Visited ids catch a chain corrupted into a cycle.
        ITupleReference key = ctx.getDataTupleBuilder().buildKeyTuple(distance, originalTuple);

        long currentMetadataPageId = metadataPageId;
        Set<Long> visitedMetadataPageIds = new HashSet<>();

        while (currentMetadataPageId != -1) {
            if (!visitedMetadataPageIds.add(currentMetadataPageId)) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "Cycle detected in directory page chain starting at page " + metadataPageId
                                + " (revisited page " + currentMetadataPageId + ")");
            }
            ICachedPage metadataPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) currentMetadataPageId));
            ctx.setMetadataPageId(currentMetadataPageId);
            boolean latched = false;
            try {
                metadataPage.acquireWriteLatch();
                latched = true;
                ctx.getMetadataFrame().setPage(metadataPage);

                int nextMetadataPageId = ctx.getMetadataFrame().getNextPage();
                boolean isLastInChain = (nextMetadataPageId == VTreeDataTupleAccessor.NO_NEXT_PAGE);

                long targetDataPageId = selectDataPageForKey(ctx.getMetadataFrame(), key, isLastInChain);

                if (targetDataPageId != -1) {
                    insertIntoDataPage(targetDataPageId, vector, distance, centroidId, originalTuple, ctx, fileId);
                    return;
                }

                if (isLastInChain) {
                    handleDataPageOverflow(currentMetadataPageId, vector, distance, centroidId, originalTuple, ctx,
                            fileId);
                    return;
                }

                currentMetadataPageId = nextMetadataPageId;

            } finally {
                if (latched) {
                    metadataPage.releaseWriteLatch(true);
                }
                bufferCache.unpin(metadataPage);
            }
        }
    }

    /**
     * The data page of this directory page that {@code key} belongs in, or {@code -1} to carry on to
     * the next directory page. A separator carries the whole key of its page's last record, and keys
     * are unique within a component, so the first entry whose separator is {@code >=} the key is the
     * only page that can hold it. No tie to resolve and no page to probe.
     */
    private long selectDataPageForKey(IVTreeMetadataFrame metadataFrame, ITupleReference key, boolean isLastInChain)
            throws HyracksDataException {
        int tupleCount = metadataFrame.getTupleCount();
        int lo = metadataFrame.findInsertPosition(key);
        if (lo < tupleCount) {
            return metadataFrame.getDataPagePointer(lo);
        }

        if (isLastInChain && tupleCount > 0) {
            return metadataFrame.getDataPagePointer(tupleCount - 1);
        }

        return -1; // No match on this page (or empty)
    }

    /**
     * Insert into a specific data page. The page always absorbs the tuple: with contiguous free space it is
     * inserted directly, with fragmented free space the page is compacted first, and with no free space the
     * page is split (the tuple then lands in whichever half covers its distance). Any other space status is a
     * frame-level invariant violation and is reported as {@code ILLEGAL_STATE}.
     */
    private void insertIntoDataPage(long dataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx, int fileId) throws HyracksDataException {

        ICachedPage dataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) dataPageId));

        boolean latched = false;
        try {
            dataPage.acquireWriteLatch();
            latched = true;
            ctx.getDataFrame().setPage(dataPage);

            ITupleReference dataTuple =
                    ctx.getDataTupleBuilder().buildDataTuple(vector, distance, centroidId, originalTuple);
            requireFits(ctx.getDataFrame(), dataTuple);

            // A component holds at most one entry per key, so the entry being replaced goes before the
            // page decides whether the new one fits; a split must never see the key twice.
            VTreeDataFrame dataFrame = (VTreeDataFrame) ctx.getDataFrame();
            int existing = dataFrame.findTupleByKey(dataFrame.keyOf(dataTuple));
            if (existing >= 0) {
                if (!dataFrame.isReplaceable(existing)) {
                    throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                            "A tuple with this ordering key is already present in the data page and is "
                                    + "not overwritable; refusing to append a duplicate key");
                }
                dataFrame.delete(dataTuple, existing);
            }

            FrameOpSpaceStatus spaceStatus = ctx.getDataFrame().hasSpaceInsert(dataTuple);

            switch (spaceStatus) {
                case SUFFICIENT_CONTIGUOUS_SPACE:
                    insertSortedIntoDataPage(dataTuple, distance, dataPageId, originalTuple, ctx);
                    return;
                case SUFFICIENT_SPACE:
                    // Free bytes exist but are fragmented: a delete returns space to the total without
                    // moving FREE_SPACE_OFFSET back. Compact first, as BTreeNSMLeafFrame does.
                    ctx.getDataFrame().compact();
                    insertSortedIntoDataPage(dataTuple, distance, dataPageId, originalTuple, ctx);
                    return;
                case INSUFFICIENT_SPACE:
                    // The split recomputes the insertion index in whichever half the tuple lands.
                    splitDataPageMaintainOrder(ctx.getMetadataPageId(), dataPageId, dataTuple, ctx, fileId);
                    return;

                default:
                    throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Unexpected FrameOpSpaceStatus "
                            + spaceStatus + " from VTreeDataFrame.hasSpaceInsert on data page " + dataPageId);
            }

        } finally {
            if (latched) {
                dataPage.releaseWriteLatch(true);
            }
            bufferCache.unpin(dataPage);
        }
    }

    /**
     * Refuse a tuple wider than half a page. A full page splits in halves before the tuple is placed,
     * so this is the widest tuple the write path can ever fit, as {@code BTree.maxTupleSize} is.
     */
    private void requireFits(ITreeIndexFrame frame, ITupleReference dataTuple) throws HyracksDataException {
        int bytes = frame.getBytesRequiredToWriteTuple(dataTuple);
        int max = frame.getMaxTupleSize(bufferCache.getPageSize());
        if (bytes > max) {
            throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE, bytes, max);
        }
    }

    /**
     * Insert a data tuple into the currently-latched data frame at its key-sorted position, fire the
     * modification callback and bump the page LSN. Shared by the contiguous-space and post-compaction
     * insert paths in {@link #insertIntoDataPage}, which has already removed any entry the tuple replaces.
     */
    private void insertSortedIntoDataPage(ITupleReference dataTuple, double distance, long dataPageId,
            ITupleReference originalTuple, VTreeOpContext ctx) throws HyracksDataException {
        VTreeDataFrame dataFrame = (VTreeDataFrame) ctx.getDataFrame();
        dataFrame.insert(dataTuple, dataFrame.findInsertPosition(dataFrame.keyOf(dataTuple)));
        ctx.getModificationCallback().found(null, originalTuple);
        ctx.getDataFrame().setPageLsn(ctx.getDataFrame().getPageLsn() + 1);
        // No separator to maintain: a key past the page's maximum was routed here by the catch-all, so a
        // lagging separator costs nothing, as a BTree reaches its rightmost child without one. A split
        // restores exact separators.
    }

    /**
     * Split data page while maintaining distance-based ordering.
     */
    private void splitDataPageMaintainOrder(long metadataPageId, long dataPageId, ITupleReference newTuple,
            VTreeOpContext ctx, int fileId) throws HyracksDataException {

        int newDataPageId = freePageManager.takePage(ctx.getMetaFrame());
        ICachedPage newDataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newDataPageId), NEW);

        boolean latched = false;
        try {
            newDataPage.acquireWriteLatch();
            latched = true;
            VTreeDataFrame newFrame = (VTreeDataFrame) ctx.getDataFrameFactory().createFrame();
            newFrame.setPage(newDataPage);
            newFrame.initBuffer((byte) 0);

            ctx.getDataFrame().split(newFrame, newTuple);

            int originalNextPage = ctx.getDataFrame().getNextPage();
            ctx.getDataFrame().setNextPage(newDataPageId);
            newFrame.setNextPage(originalNextPage);

            // Page LSNs are not consulted for recovery here; keep them monotonic as the rest of this class does.
            long currentLsn = ctx.getDataFrame().getPageLsn() + 1;
            ctx.getDataFrame().setPageLsn(currentLsn);
            newFrame.setPageLsn(currentLsn);

            updateMetadataAfterDataSplit(metadataPageId, dataPageId, newDataPageId, ctx, fileId);

        } finally {
            if (latched) {
                newDataPage.releaseWriteLatch(true);
            }
            bufferCache.unpin(newDataPage);
        }
    }

    /**
     * Record a data page split in the directory: lower the original page's separator to its new maximum
     * and add the new page. Both separators go through one insert path, so a directory split forced by
     * either of them routes the other to the half whose key range covers it.
     */
    private void updateMetadataAfterDataSplit(long targetMetadataPageId, long originalDataPageId, int newDataPageId,
            VTreeOpContext ctx, int fileId) throws HyracksDataException {
        if (targetMetadataPageId == -1) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "updateMetadataAfterDataSplit called without a metadata page id (originalDataPageId="
                            + originalDataPageId + ")");
        }

        ITupleReference originalPageMaxKey = readMaxKeyInDataPage(originalDataPageId, ctx, fileId);
        ITupleReference newPageMaxKey = readMaxKeyInDataPage(newDataPageId, ctx, fileId);
        if (newPageMaxKey == null) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Data page " + newDataPageId
                    + " is empty immediately after a split, so it has no key to record in the directory");
        }
        ITupleReference newPageEntry = VTreeMetadataTupleAccessor.createMetadataTuple(newPageMaxKey, newDataPageId);

        // A one-tuple page splits into an empty original with no maximum: leave its separator alone, and
        // the next key routed to it restores an exact bound.
        if (originalPageMaxKey == null) {
            insertSeparators(targetMetadataPageId, ctx, fileId, newPageEntry);
            return;
        }

        // The original page's separator only lowers and stays above its predecessor, so its slot holds
        // unless the replacement is wider than the page can absorb.
        VTreeMetadataFrame metadataFrame = requireLatchedMetadataFrame(targetMetadataPageId, ctx);
        int slot = findSeparatorSlot(metadataFrame, originalDataPageId);
        if (slot < 0 || metadataFrame.replaceSeparator(slot, originalPageMaxKey, (int) originalDataPageId)) {
            insertSeparators(targetMetadataPageId, ctx, fileId, newPageEntry);
            return;
        }
        metadataFrame.deleteSeparator(slot);
        insertSeparators(targetMetadataPageId, ctx, fileId,
                VTreeMetadataTupleAccessor.createMetadataTuple(originalPageMaxKey, (int) originalDataPageId),
                newPageEntry);
    }

    /**
     * Read the maximum ordering key stored in a data page. Tuples are kept key-ascending, so the last
     * one carries the page's maximum; an empty page has no key and reports {@code null}. The result is
     * copied, since the page is unlatched before the caller uses it. Pins and read-latches for the
     * duration.
     */
    private ITupleReference readMaxKeyInDataPage(long dataPageId, VTreeOpContext ctx, int fileId)
            throws HyracksDataException {
        ICachedPage dataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) dataPageId));
        boolean latched = false;
        try {
            dataPage.acquireReadLatch();
            latched = true;
            IVTreeDataFrame dataFrame = (IVTreeDataFrame) ctx.getDataFrameFactory().createFrame();
            dataFrame.setPage(dataPage);
            int tupleCount = dataFrame.getTupleCount();
            return tupleCount > 0 ? TupleUtils.copyTuple(dataFrame.keyAt(tupleCount - 1)) : null;
        } finally {
            if (latched) {
                dataPage.releaseReadLatch();
            }
            bufferCache.unpin(dataPage);
        }
    }

    /**
     * Try to physically delete a tuple from data pages. Searches through metadata
     * pages to find the tuple and delete it. Returns true if found and deleted,
     * false if not found (caller should insert a delete-marker tuple).
     * <p>
     * The key is matched with the index's own comparators, over every key field of
     * {@code originalTuple}, so no field type is assumed and no field is skipped.
     */
    boolean tryPhysicalDelete(long metadataPageId, double distance, ITupleReference originalTuple, VTreeOpContext ctx,
            int fileId) throws HyracksDataException {

        ITupleReference key = ctx.getDataTupleBuilder().buildKeyTuple(distance, originalTuple);

        long currentMetadataPageId = metadataPageId;

        while (currentMetadataPageId != -1) {
            ICachedPage metadataPage =
                    bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) currentMetadataPageId));

            boolean metadataLatched = false;
            try {
                metadataPage.acquireReadLatch();
                metadataLatched = true;
                ctx.getMetadataFrame().setPage(metadataPage);

                int nextMetadataPageId = ctx.getMetadataFrame().getNextPage();
                boolean isLastInChain = (nextMetadataPageId == VTreeDataTupleAccessor.NO_NEXT_PAGE);

                long targetDataPageId = selectDataPageForKey(ctx.getMetadataFrame(), key, isLastInChain);

                if (targetDataPageId != -1) {
                    ICachedPage dataPage =
                            bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, (int) targetDataPageId));

                    boolean dataLatched = false;
                    try {
                        dataPage.acquireWriteLatch();
                        dataLatched = true;
                        ctx.getDataFrame().setPage(dataPage);

                        VTreeDataFrame dataFrame = (VTreeDataFrame) ctx.getDataFrame();
                        int tupleIndex = dataFrame.findTupleByKey(key);

                        if (tupleIndex >= 0) {
                            // A delete marker means the record is already deleted here, and removing
                            // it would unsuppress an older component's matter.
                            if (dataFrame.isReplaceable(tupleIndex)) {
                                return true;
                            }
                            ctx.getDataFrame().delete(originalTuple, tupleIndex);
                            return true;
                        }

                    } finally {
                        if (dataLatched) {
                            dataPage.releaseWriteLatch(true);
                        }
                        bufferCache.unpin(dataPage);
                    }
                }

                if (isLastInChain) {
                    break; // End of chain
                }
                currentMetadataPageId = nextMetadataPageId;

            } finally {
                if (metadataLatched) {
                    metadataPage.releaseReadLatch();
                }
                bufferCache.unpin(metadataPage);
            }
        }

        return false; // Not found in any data page
    }

    private void handleDataPageOverflow(long metadataPageId, double[] vector, double distance, int centroidId,
            ITupleReference originalTuple, VTreeOpContext ctx, int fileId) throws HyracksDataException {
        // Creates a directory page's first data page, so there is no predecessor to chain from and the
        // data-page chain is left alone. On a populated directory the page would be reachable through
        // the directory but not the chain, so refuse that.
        VTreeMetadataFrame directoryFrame = requireLatchedMetadataFrame(metadataPageId, ctx);
        if (directoryFrame.getTupleCount() != 0) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "handleDataPageOverflow on a non-empty directory page " + metadataPageId + " ("
                            + directoryFrame.getTupleCount()
                            + " entries): the new data page would not be linked into the data-page chain");
        }

        IVTreeDataFrame dataFrame = (IVTreeDataFrame) ctx.getDataFrameFactory().createFrame();
        IPageManager pageManager = ctx.getFreePageManager();

        int newDataPageId = pageManager.takePage(ctx.getMetaFrame());
        ICachedPage newPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newDataPageId), NEW);

        boolean latched = false;
        try {
            newPage.acquireWriteLatch();
            latched = true;
            dataFrame.setPage(newPage);
            dataFrame.initBuffer((byte) 0);

            ITupleReference dataTuple =
                    ctx.getDataTupleBuilder().buildDataTuple(vector, distance, centroidId, originalTuple);
            requireFits(dataFrame, dataTuple);

            dataFrame.insert(dataTuple, 0);

            // The new page holds exactly this one tuple, so its maximum key is that tuple's key.
            insertSeparators(metadataPageId, ctx, fileId,
                    VTreeMetadataTupleAccessor.createMetadataTuple(dataFrame.keyOf(dataTuple), newDataPageId));

        } finally {
            if (latched) {
                newPage.releaseWriteLatch(true);
            }
            bufferCache.unpin(newPage);
        }
    }

    /** The slot of {@code dataPageId}'s separator on the latched directory page, or {@code -1}. */
    private static int findSeparatorSlot(VTreeMetadataFrame metadataFrame, long dataPageId)
            throws HyracksDataException {
        int tupleCount = metadataFrame.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            if (metadataFrame.getDataPagePointer(i) == dataPageId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Insert separators into the latched directory page at their sorted positions, splitting the page
     * at most once. The entries that follow a split are routed by key to whichever half covers them
     * while both halves are still latched.
     */
    private void insertSeparators(long metadataPageId, VTreeOpContext ctx, int fileId, ITupleReference... entries)
            throws HyracksDataException {

        VTreeMetadataFrame metadataFrame = requireLatchedMetadataFrame(metadataPageId, ctx);

        for (int i = 0; i < entries.length; i++) {
            // Ask the frame rather than getTotalFreeSpace(): a delete returns bytes to that total without
            // moving FREE_SPACE_OFFSET back, so the total overstates the contiguous room.
            FrameOpSpaceStatus spaceStatus = metadataFrame.hasSpaceInsert(entries[i]);
            if (spaceStatus == FrameOpSpaceStatus.SUFFICIENT_SPACE) {
                metadataFrame.compact();
                spaceStatus = metadataFrame.hasSpaceInsert(entries[i]);
            }

            if (spaceStatus != FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
                handleMetadataPageOverflow(ctx, fileId, Arrays.copyOfRange(entries, i, entries.length));
                return;
            }
            // Insert at the sorted position: after a non-last data-page split the new page's key falls
            // between existing entries, and selectDataPageForKey relies on the directory staying
            // key-ascending.
            metadataFrame.insert(entries[i], metadataFrame.findInsertPosition(entries[i]));
        }
    }

    /**
     * Return the shared metadata frame that the caller already holds pinned and write-latched for
     * {@code metadataPageId}.
     * <p>
     * All metadata-mutation helpers ({@link #updateMetadataAfterDataSplit},
     * {@link #insertSeparators}) run only from
     * inside {@link #insertIntoDataPages}, which pins the current directory page, write-latches it, sets
     * {@code ctx.getMetadataFrame()} to it, and releases the latch (marking the page dirty) in its own
     * {@code finally}. Operating on that already-latched frame here — instead of re-pinning and
     * re-latching the same page — removes redundant buffer-cache I/O and the former reliance on latch
     * reentrancy ({@code ReentrantReadWriteLock}) that a non-reentrant latch would have turned into a
     * self-deadlock. {@link #handleMetadataPageOverflow} already assumes this same shared-frame contract.
     */
    private VTreeMetadataFrame requireLatchedMetadataFrame(long metadataPageId, VTreeOpContext ctx) {
        assert metadataPageId == ctx.getMetadataPageId() : "metadata mutation on page " + metadataPageId
                + " but the write-latched page is " + ctx.getMetadataPageId();
        return (VTreeMetadataFrame) ctx.getMetadataFrame();
    }

    /**
     * Split the latched directory page and insert {@code entries} into whichever halves cover their keys.
     * Two halves of a full page each have room for a few separators, so every entry fits.
     */
    private void handleMetadataPageOverflow(VTreeOpContext ctx, int fileId, ITupleReference... entries)
            throws HyracksDataException {

        int newMetadataPageId = freePageManager.takePage(ctx.getMetaFrame());
        ICachedPage newMetadataPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, newMetadataPageId), NEW);

        boolean latched = false;
        try {
            newMetadataPage.acquireWriteLatch();
            latched = true;

            IVTreeMetadataFrame rightFrame = (IVTreeMetadataFrame) metadataFrameFactory.createFrame();
            rightFrame.setPage(newMetadataPage);
            rightFrame.initBuffer((byte) 0);

            // split() resets both halves' next pointers and the page being split need not be last in its
            // chain: capture its successor first and splice left -> new -> successor.
            int originalNextPage = ctx.getMetadataFrame().getNextPage();

            VTreeMetadataFrame leftFrame = (VTreeMetadataFrame) ctx.getMetadataFrame();
            leftFrame.split(rightFrame, entries[0]);
            for (int i = 1; i < entries.length; i++) {
                leftFrame.insertIntoHalf(rightFrame, entries[i]);
            }

            leftFrame.setNextPage(newMetadataPageId);

            rightFrame.setNextPage(originalNextPage);

        } finally {
            if (latched) {
                newMetadataPage.releaseWriteLatch(true);
            }
            bufferCache.unpin(newMetadataPage);
        }
    }
}
