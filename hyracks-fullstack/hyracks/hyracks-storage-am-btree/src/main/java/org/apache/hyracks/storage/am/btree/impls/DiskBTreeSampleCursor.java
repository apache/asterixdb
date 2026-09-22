/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.api.ITupleAcceptor;
import org.apache.hyracks.storage.am.common.api.ILSMIndexBatchPointCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.context.IBufferCacheReadContext;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * Disk B-tree sample cursor for row storage. Single-phase: pick a random leaf page and slot, check antimatter
 * and newer-component liveness, yield immediately. Single-phase because a row tuple is fully materialized by
 * pinning its page — there is nothing to defer, unlike the column cursor, which splits selection from column
 * loading. At the low sampling densities row components see, samples scatter over many pages, so pins dominate
 * and yielding on the spot keeps that count minimal.
 */
public final class DiskBTreeSampleCursor extends EnforcedIndexCursor implements ITreeIndexCursor {
    private static final Logger LOGGER = LogManager.getLogger();

    private final DiskBTree bTree;
    private final BTreeOpContext bTreeOpCtx;
    private final IBufferCacheReadContext bufferCacheOpCtx;
    private final IBTreeLeafFrame leafFrame;
    private final ITreeIndexTupleReference frameTuple;
    // Rejects antimatter (delete) tuples from the sample. Supplied by the LSM layer (antimatter is an LSM concept
    // that this btree-layer cursor cannot reference directly), so it is injected via the btree-visible ITupleAcceptor.
    private final ITupleAcceptor antimatterAcceptor;
    // u64: (pageId << 32) | tupleIndex
    private final LongSet seenTupleIndexes;
    // Well above 3 because the reservoir was observed to stall at ~threshold/3 when the threshold is tight.
    private static final long SAMPLE_ATTEMPT_MULTIPLIER = 32L;
    private final int maxLeafFindingAttempts;
    // Consecutive-miss give-up threshold, scaled to the target in doOpen(): as the reservoir fills, draws
    // increasingly hit already-seen tuples, so the streak needed for the last few grows with the target and a
    // fixed floor truncates near-exhaustive samples.
    private int effectiveMaxLeafFindingAttempts;
    private final long componentSampleCardinality;
    private final Random randomNumGen;
    private ICachedPage page = null;
    private int pageId = -1;
    // Uniform random access to any leaf without an int[]: succinct bitmap+select (ASTERIXDB-3702), ~15x less
    // memory on dense row components.
    private BitmapLeafIds leafPageIds = null;
    // Draws as parallel primitive arrays: no object headers or pointer chasing on the hot per-draw read.
    private final double[] drawAcceptanceSamples;
    // Packed (pageId << 32) | drawIndex. Sorting the long orders by pageId -- grouping same-page draws for pin
    // reuse -- while the low 32 bits keep the side arrays addressable without permuting them.
    private final long[] drawSortKeys;
    private final LeafDraw leafDrawView;
    private int pendingLeafDrawIndex;
    // Upper bound on one batch; the batch actually drawn is sized to the outstanding shortfall
    // (see refillLeafDrawBatch) and is never larger than this.
    private final int drawBatchCapacity;
    // Number of valid draws in the current batch. Every one of them is consumed before the next refill.
    private int currentBatchSize;
    private IBufferCache bufferCache;
    private int fileId = -1;
    // search predicate
    private final ILSMIndexBatchPointCursor searchCursor;
    private final BatchPredicateWithKeys batchPredicate;
    private final List<ITupleReference> searchKeys;
    private final BitSet foundIndexes;
    // Number of LIVE tuples sampled from the component so far.
    private int sampledCount;
    private int hasNextAttemptCount = 0;
    private int totalAccessCount;

    // Olken & Rotem rejection bound, correcting the bias from partially-filled pages.
    private final int leafTupleCapacity;

    // Gates the per-draw System.nanoTime() calls and the per-batch unique-page loop off the hot path. TRACE,
    // not DEBUG, which is on by default -- this instrumentation must never run in normal operation.
    private final boolean traceTimingEnabled;

    private long totalTimeTakenToFindRandomLeaf = 0;
    private long totalTimeTakenToFindRandomTuples = 0;
    private long totalLeafDrawBatches = 0;
    private long totalLeafDraws = 0;
    private long totalLeafDrawUniquePages = 0;
    private long totalLeafPins = 0;
    private long totalReusedPinnedPageHits = 0;
    private boolean endedPreemptively = false;

    public DiskBTreeSampleCursor(DiskBTree diskBTree, IBTreeLeafFrame leafFrame, long componentSampleCardinality,
            long sampleSeed, BTreeOpContext ctx, IBufferCacheReadContext bufferCacheOpCtx,
            ILSMIndexBatchPointCursor searchCursor, int maxLeafFindingAttempts, int leafDrawBatchSize,
            int maxLeafTupleCount, ITupleAcceptor antimatterAcceptor) {
        this.bTree = diskBTree;
        this.antimatterAcceptor = antimatterAcceptor;
        this.leafFrame = leafFrame;
        this.randomNumGen = new Random(sampleSeed);
        this.bTreeOpCtx = ctx;
        this.bufferCacheOpCtx = bufferCacheOpCtx;
        this.componentSampleCardinality = componentSampleCardinality;
        this.frameTuple = leafFrame.createTupleReference();
        this.searchCursor = searchCursor;
        this.seenTupleIndexes = new LongOpenHashSet();
        this.foundIndexes = new BitSet();
        this.searchKeys = new ArrayList<>();
        this.batchPredicate = new BatchPredicateWithKeys();
        this.totalAccessCount = 0;
        this.maxLeafFindingAttempts = maxLeafFindingAttempts;
        this.leafTupleCapacity = maxLeafTupleCount;
        this.traceTimingEnabled = LOGGER.isTraceEnabled();
        // A draw yields at most one sample, so a batch beyond the outstanding shortfall is waste, and it must
        // be consumed whole to stay unbiased (refillLeafDrawBatch). The configured size stays an upper bound --
        // an I/O-locality knob.
        long capacity = leafDrawBatchSize > 0 ? Math.min(leafDrawBatchSize, componentSampleCardinality)
                : componentSampleCardinality;
        this.drawBatchCapacity = (int) Math.max(1, Math.min(Integer.MAX_VALUE, capacity));

        // Allocated once: refills reuse these rather than churning.
        this.drawAcceptanceSamples = new double[this.drawBatchCapacity];
        this.drawSortKeys = new long[this.drawBatchCapacity];
        this.leafDrawView = new LeafDraw(-1, 0.0);

        this.pendingLeafDrawIndex = 0;
        this.currentBatchSize = 0;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        if (page != null) {
            releasePage();
        }
        int rootPageId = ((BTreeCursorInitialState) initialState).getPageId();
        // Interior pages only: level-1 nodes record their children's ids, so this skips all leaf-level I/O and
        // costs ~0.4 % of total pages at typical fan-out -- the price of uniform random leaf access.
        leafPageIds = bTree.enumerateLeafPageIdsCompact(rootPageId, bTreeOpCtx, bufferCacheOpCtx);
        // MULTIPLIER * max(leafPages, target). Scaled to the TARGET, not the tuple population: the caller
        // already caps the target at the component's live-tuple estimate, so it is reachable and the loop must
        // not quit early. Scaling by population instead (leafPages x maxLeafTupleCount ~= total tuples) makes an
        // unreachable target grind ~32x total tuples however small it is -- tolerable here, where an attempt is
        // a slot lookup, but minutes-to-hours on the column cursor, which pays a PK-decode per attempt. The
        // leafPages floor lets a tiny target still reach every page; the clamp stops the product overflowing
        // negative, which would itself truncate.
        long attemptCeiling = SAMPLE_ATTEMPT_MULTIPLIER * Math.max(leafPageIds.size(), componentSampleCardinality);
        effectiveMaxLeafFindingAttempts =
                (int) Math.min(Integer.MAX_VALUE, Math.max(maxLeafFindingAttempts, attemptCeiling));
        pendingLeafDrawIndex = 0;
        currentBatchSize = 0;
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("DiskBTreeSampleCursor: {} leaf pages enumerated, target {} samples, attempts cap {}",
                    leafPageIds.size(), componentSampleCardinality, effectiveMaxLeafFindingAttempts);
        }
    }

    /**
     * Advances to the next accepted sample.
     * <p>
     * <b>The emitted sample is exactly uniform over the component's live tuples.</b> A draw picks a page
     * uniformly ({@code 1/P}), accepts it with the Olken &amp; Rotem fill correction ({@code n/C}, see
     * {@link #pinAndAcceptLeafPage}), then picks one of the page's {@code n} slots. A tuple is thus reached with
     * probability {@code (1/P)(n/C)(1/n) = 1/(P·C)}, free of the page's fill — cancelling {@code n} is what the
     * correction is for. Antimatter and shadowing are deterministic predicates and repeats are rejected, so this
     * is uniform sampling without replacement.
     * <p>
     * Uniformity then needs every stopping rule to be a function of <b>counts only</b>, since such rules commute
     * with relabelling the live tuples. Truncating a pageId-sorted batch is the one rule that is not — it keeps
     * precisely the lowest pageIds — hence full-batch consumption and stop conditions tested only at batch
     * boundaries ({@link #refillLeafDrawBatch()}). No {@code samplesPerPage} here, unlike the column cursor, so a
     * batch cannot overshoot and nothing needs downsampling.
     * <p>
     * <b>Caller contract: drain the cursor.</b> Emission is lazy, one sample per {@code hasNext()}, so
     * "consumed in full" holds only if the consumer calls until {@code hasNext()} is {@code false}. Stopping
     * early keeps the low-pageId prefix of the batch in flight — the same spatial filter, relocated to the
     * caller. {@code LSMIndexSampleCursor} is the only production driver and drains fully; a consumer wanting a
     * partial sample must ask for a smaller {@code componentSampleCardinality}, not abandon a larger one.
     * <p>
     * The attempts cap is also tested only at batch boundaries, so it can overshoot by one batch — count-based
     * either way, so bounded extra work and no effect on uniformity.
     */
    @Override
    protected boolean doHasNext() throws HyracksDataException {
        while (true) {
            if (pendingLeafDrawIndex >= currentBatchSize) {
                // The ONLY legal stopping point: mid-batch would keep just the sorted batch's low-pageId
                // prefix, a spatial filter rather than a sample.
                if (hasNextAttemptCount >= effectiveMaxLeafFindingAttempts) {
                    break;
                }
                refillLeafDrawBatch();
                if (currentBatchSize == 0) {
                    // Target reached, or the component has no leaf pages.
                    break;
                }
            }
            LeafDraw leafDraw = nextLeafDraw();
            if (!pinAndAcceptLeafPage(leafDraw)) {
                hasNextAttemptCount++;
                continue;
            }
            int foundTupleIndex = findRandomTuple();
            long pageTupleKey = getPageTupleKey(pageId, foundTupleIndex);
            if (foundTupleIndex == -1 || seenTupleIndexes.contains(pageTupleKey)) {
                hasNextAttemptCount++;
                continue;
            }
            searchKeys.clear();
            foundIndexes.clear();
            searchKeys.add(frameTuple);
            batchPredicate.reset(searchKeys);
            searchCursor.setPredicate(batchPredicate);
            searchCursor.hasNextWithPredicate(foundIndexes);
            if (foundIndexes.isEmpty()) {
                // Live: keep the page pinned, the caller reads the tuple straight off it.
                hasNextAttemptCount = 0;
                seenTupleIndexes.add(pageTupleKey);
                sampledCount++;
                return true;
            } else {
                // Shadowed by a newer component.
                hasNextAttemptCount++;
            }
        }
        endedPreemptively = (sampledCount < componentSampleCardinality);
        return false;
    }

    /** Decodes the next draw of the current batch. The caller guarantees the batch is not exhausted. */
    private LeafDraw nextLeafDraw() {
        // High 32 bits of the sort key ARE the pageId; the low 32 index back into the unsorted side arrays.
        long sortKey = drawSortKeys[pendingLeafDrawIndex++];
        int drawIndex = (int) sortKey;
        leafDrawView.pageId = (int) (sortKey >>> 32);
        leafDrawView.acceptanceSample = drawAcceptanceSamples[drawIndex];
        return leafDrawView;
    }

    /**
     * Draws the next batch of leaf-page visits, pageId-sorted so the pins go in ascending order.
     * <p>
     * <b>The batch is sized to the outstanding shortfall so it can be consumed whole.</b> Sorting is only a
     * permutation, and a permutation changes nothing — <em>provided every draw is consumed</em>. Truncating a
     * pageId-sorted batch keeps precisely the lowest pageIds, turning the sort into a spatial filter; a fixed
     * 32768-draw batch guaranteed that, since the target was met within a short prefix. At {@code remaining}
     * draws the target can only be reached on the last one, because a draw yields at most one sample — so the
     * caller can consume all of it. Total draws are unchanged; only the sort windows shrink.
     * <p>
     * The configured {@code leafDrawBatchSize} therefore no longer binds unless set below the target. It is kept
     * as the ceiling on the draw-buffer allocation, and a smaller batch is still consumed whole, so it can only
     * change pin locality, never which tuples are sampled.
     * <p>
     * Sets {@link #currentBatchSize} to 0 when nothing more is needed (target met) or possible (no leaf pages).
     */
    private void refillLeafDrawBatch() {
        pendingLeafDrawIndex = 0;
        currentBatchSize = 0;
        if (leafPageIds == null || leafPageIds.size() == 0) {
            return;
        }
        long remaining = componentSampleCardinality - sampledCount;
        if (remaining <= 0) {
            return;
        }
        int batchSize = (int) Math.min(drawBatchCapacity, remaining);
        for (int i = 0; i < batchSize; i++) {
            int randomLeafIndex = randomNumGen.nextInt(leafPageIds.size());
            int targetPageId = leafPageIds.get(randomLeafIndex);
            drawAcceptanceSamples[i] = randomNumGen.nextDouble();
            // pageIds are non-negative, so the high 32 bits sort correctly as a signed long.
            drawSortKeys[i] = (((long) targetPageId) << 32) | (i & 0xffffffffL);
        }

        // Comparator-free and cache-friendly, unlike a boxed-object sort.
        Arrays.sort(drawSortKeys, 0, batchSize);
        currentBatchSize = batchSize;

        if (traceTimingEnabled) {
            int uniquePages = 0;
            long prevPageId = Long.MIN_VALUE;
            for (int i = 0; i < batchSize; i++) {
                long currentPageId = drawSortKeys[i] >>> 32;
                if (currentPageId != prevPageId) {
                    uniquePages++;
                    prevPageId = currentPageId;
                }
            }
            totalLeafDrawBatches++;
            totalLeafDraws += batchSize;
            totalLeafDrawUniquePages += uniquePages;
        }
    }

    private long getPageTupleKey(int pageId, int tupleIndex) {
        return (((long) pageId) << 32) | (tupleIndex & 0xffffffffL);
    }

    /**
     * Pins the draw's leaf page and applies the <b>page-fill correction</b> (Olken &amp; Rotem, VLDB 1989):
     * accept with probability {@code tupleCount / leafTupleCapacity}, without which tuples on partly-filled
     * pages would have a higher per-tuple selection probability.
     */
    private boolean pinAndAcceptLeafPage(LeafDraw leafDraw) throws HyracksDataException {
        // Skip the nanoTime() pair entirely unless trace logging will consume it.
        long nanos = traceTimingEnabled ? System.nanoTime() : 0L;
        try {
            totalAccessCount++;
            if (pageId != leafDraw.pageId || page == null) {
                releasePage();
                int targetPageId = leafDraw.pageId;
                page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, targetPageId), bufferCacheOpCtx);
                leafFrame.setPage(page);
                pageId = targetPageId;
                totalLeafPins++;
            } else {
                totalReusedPinnedPageHits++;
            }
            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                // Load bearing: findRandomTuple's nextInt(bound) needs a positive bound.
                return false;
            }
            // Unknown capacity (pre-upgrade component, no metadata) => accept every page.
            if (leafTupleCapacity > 0) {
                double acceptProb = (double) tupleCount / leafTupleCapacity;
                if (leafDraw.acceptanceSample >= acceptProb) {
                    return false;
                }
            }
            return true;
        } finally {
            // In a finally so a rejected page still counts its elapsed time.
            if (traceTimingEnabled) {
                totalTimeTakenToFindRandomLeaf += (System.nanoTime() - nanos);
            }
        }
    }

    /**
     * Picks a random slot on the current leaf page, or {@code -1} to reject the draw.
     * <p>
     * <b>Antimatter rejects the whole draw</b> rather than re-drawing on the same page: an O(1) decision that
     * keeps the per-draw law uniform over the component, so the sample reflects the data's physical density.
     * <p>
     * The slot comes from {@link Random#nextInt(int)}, exactly uniform on {@code [0, tupleCount)} because it
     * rejects the generator's non-representable tail. Folding a pre-drawn seed with {@code % tupleCount} instead
     * over-weights the low slots unless {@code tupleCount} is a power of two. The bound must be positive, which
     * {@link #pinAndAcceptLeafPage}'s {@code tupleCount == 0} rejection guarantees.
     */
    private int findRandomTuple() {
        long nanos = traceTimingEnabled ? System.nanoTime() : 0L;
        int numberOfTuples = leafFrame.getTupleCount();

        int targetTupleIndex = randomNumGen.nextInt(numberOfTuples);

        frameTuple.resetByTupleIndex(leafFrame, targetTupleIndex);
        int result = antimatterAcceptor.accept(frameTuple) ? -1 : targetTupleIndex;

        if (traceTimingEnabled) {
            totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
        }
        return result;
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }

    @Override
    protected void doNext() throws HyracksDataException {
        // NoOp
    }

    @Override
    protected void doDestroy() throws HyracksDataException {
        // No Op all resources are released in the close call
    }

    @Override
    protected void doClose() throws HyracksDataException {
        if (LOGGER.isTraceEnabled()) {
            double avgBatchDraws = totalLeafDrawBatches == 0 ? 0.0 : (double) totalLeafDraws / totalLeafDrawBatches;
            double avgBatchUniquePages =
                    totalLeafDrawBatches == 0 ? 0.0 : (double) totalLeafDrawUniquePages / totalLeafDrawBatches;
            double uniquePerDraw = totalLeafDraws == 0 ? 0.0 : (double) totalLeafDrawUniquePages / totalLeafDraws;
            double repinAvoidRate = (totalLeafPins + totalReusedPinnedPageHits) == 0 ? 0.0
                    : (double) totalReusedPinnedPageHits / (totalLeafPins + totalReusedPinnedPageHits);
            LOGGER.trace(
                    "{} stats - sampledCount: {}, totalAccessCount: {}, "
                            + "totalTimeTakenToFindRandomLeaf: {} ns, totalTimeTakenToFindRandomTuples: {} ns, "
                            + "leafDrawBatches: {}, totalLeafDraws: {}, totalLeafDrawUniquePages: {}, "
                            + "avgBatchDraws: {}, avgBatchUniquePages: {}, uniquePerDraw: {}, totalLeafPins: {}, "
                            + "reusedPinnedPageHits: {}, repinAvoidRate: {}, endedPreemptively: {}",
                    this.getClass().getName(), sampledCount, totalAccessCount, totalTimeTakenToFindRandomLeaf,
                    totalTimeTakenToFindRandomTuples, totalLeafDrawBatches, totalLeafDraws, totalLeafDrawUniquePages,
                    avgBatchDraws, avgBatchUniquePages, uniquePerDraw, totalLeafPins, totalReusedPinnedPageHits,
                    repinAvoidRate, endedPreemptively);
        }
        if (page != null) {
            releasePage();
        }
        sampledCount = 0;
        page = null;
        seenTupleIndexes.clear();
        pageId = -1;
        leafPageIds = null;

        pendingLeafDrawIndex = 0;
        currentBatchSize = 0;

        totalAccessCount = 0;
        totalTimeTakenToFindRandomLeaf = 0;
        totalTimeTakenToFindRandomTuples = 0;
        totalLeafDrawBatches = 0;
        totalLeafDraws = 0;
        totalLeafDrawUniquePages = 0;
        totalLeafPins = 0;
        totalReusedPinnedPageHits = 0;
        endedPreemptively = false;
    }

    @Override
    protected ITupleReference doGetTuple() {
        return frameTuple;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    private void releasePage() {
        if (page != null) {
            bufferCache.unpin(page);
        }
        page = null;
        pageId = -1;
    }

    // One mutable view over the draw arrays, repointed per draw in nextLeafDraw(). Never escapes the cursor,
    // so the reuse is safe.
    private static final class LeafDraw {
        private int pageId;
        private double acceptanceSample;

        private LeafDraw(int pageId, double acceptanceSample) {
            this.pageId = pageId;
            this.acceptanceSample = acceptanceSample;
        }
    }
}
