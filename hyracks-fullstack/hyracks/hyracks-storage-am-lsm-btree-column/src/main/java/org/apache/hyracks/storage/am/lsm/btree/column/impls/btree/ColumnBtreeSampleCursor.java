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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTreeCursorInitialState;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.btree.impls.BatchPredicateWithKeys;
import org.apache.hyracks.storage.am.common.api.ILSMIndexBatchPointCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.AbstractColumnTupleReference;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * Two-phase column sample cursor. Splitting selection from collection is what keeps the cost proportional to the
 * sample rather than to the component: phase 1 rejects most candidates, and only survivors are worth a mega-page
 * load.
 * <p>
 * <b>Phase 1 — selection, page0 only.</b> Draws random leaf pages, pins page0, and decides antimatter and
 * newer-component liveness from PK data alone ({@link AbstractColumnTupleReference#startSamplingPage} +
 * {@link AbstractColumnTupleReference#seekForwardPKOnly}), so no column mega-pages are touched. Survivors are
 * recorded as packed {@code (pageId << 32) | tupleIndex}.
 * <p>
 * <b>Phase 2 — collection, sorted, full column load.</b> Survivors sorted by pageId, so each page's mega-pages
 * are loaded once and further tuples on it reached by the forward-only {@code setAt()}.
 * <p>
 * Draws are sorted by pageId for sequential page0 I/O, which also makes same-page draws contiguous — they are
 * consumed as one <b>page group</b> sharing a single rewind and forward PK pass, instead of one scan per draw.
 * <p>
 * <b>Every sort here orders a set that is then consumed in full.</b> That is the load-bearing property: a
 * pageId-sorted set consumed only partly keeps the lowest pageIds, which is a spatial filter, not a sample. See
 * {@link #runPhase1Selection()}.
 */
public class ColumnBtreeSampleCursor extends EnforcedIndexCursor implements ITreeIndexCursor, IColumnReadMultiPageOp {

    private static final Logger LOGGER = LogManager.getLogger();

    private final ColumnBTree bTree;
    private final BTreeOpContext opCtx;
    private final ColumnBTreeReadLeafFrame leafFrame;
    private final IColumnReadContext context;
    private final IColumnTupleIterator frameTuple;
    private final AbstractColumnTupleReference columnTupleRef;

    // u64: (pageId << 32) | tupleIndex
    private final LongSet seenTupleIndexes;

    // Give-up headroom must scale with the target, not be fixed: the coupon-collector tail near the end of
    // collection grows with it, so a fixed threshold truncates near-exhaustive samples. Mirrors
    // DiskBTreeSampleCursor. Scaled in doOpen().
    private static final long SAMPLE_ATTEMPT_MULTIPLIER = 32L;
    private final int maxLeafFindingAttempts;
    private int effectiveMaxLeafFindingAttempts;
    private final long componentSampleCardinality;
    private final Random randomNumGen;

    // Draws as parallel primitive arrays, sorted via a packed long key ((pageId << 32) | drawIndex) so the
    // page0 pins go in ascending pageId order without a boxed comparator.
    private final double[] drawAcceptanceSamples;
    private final long[] drawSortKeys;
    private int pendingLeafDrawIndex;

    // A page group's candidate slots, packed (tupleIndex << 32) | drawOrdinal: one primitive sort then orders
    // them ascending by slot for the forward-only PK pass, while still saying which draw each slot came from
    // (the per-draw give-up accounting needs that).
    private long[] groupCandidates;
    // Did this draw of the current group collect anything? Drives the give-up counter.
    private final boolean[] drawCollectedAny;
    private final int drawBatchCapacity;
    // Every draw of a batch is consumed before the next refill; see refillLeafDrawBatch.
    private int currentBatchSize;

    // search predicate
    private final ILSMIndexBatchPointCursor searchCursor;
    private final BatchPredicateWithKeys batchPredicate;
    private final List<ITupleReference> searchKeys;
    private final BitSet foundIndexes;

    private int sampledCount;
    private int hasNextAttemptCount = 0;
    private int totalAccessCount;

    // Static upper bound for rejection sampling
    private final int leafTupleCapacity;

    // Cached so the per-draw System.nanoTime() calls leave the hot path when trace logging is off; the only
    // consumer is doClose()'s trace line.
    private final boolean traceTimingEnabled;

    private long totalTimeTakenToFindRandomLeaf = 0;
    private long totalTimeTakenToFindRandomTuples = 0;
    private boolean endedPreemptively = false;

    // Null in production scans (zero overhead); attached by the perf harness.
    private SampleCursorStats stats;

    public void setStats(SampleCursorStats stats) {
        this.stats = stats;
    }

    public SampleCursorStats getStats() {
        return stats;
    }

    private ICachedPage page0 = null;
    private int rootPageId;
    private int[] leafPageIds = null;

    // Phase 1 collection: packed (pageId << 32) | tupleIndex
    private long[] collectedSamples = null;
    private int collectedCount = 0;

    // Phase 2 yield state
    private boolean selectionDone = false;
    private int yieldPos = 0;
    private int prevYieldPageId = -1;

    private IBufferCache bufferCache;
    private int fileId = -1;

    public ColumnBtreeSampleCursor(ColumnBTree columnBTree, ColumnBTreeReadLeafFrame leafFrame,
            BTreeOpContext opContext, IColumnReadContext context, long componentSampleCardinality, long sampleSeed,
            int index, ILSMIndexBatchPointCursor searchCursor, int maxLeafFindingAttempts, int leafDrawBatchSize,
            int maxLeafTupleCount) {
        this.bTree = columnBTree;
        this.opCtx = opContext;
        this.leafFrame = leafFrame;
        this.context = context;
        this.componentSampleCardinality = componentSampleCardinality;
        this.randomNumGen = new Random(sampleSeed);
        this.batchPredicate = new BatchPredicateWithKeys();
        this.searchCursor = searchCursor;
        this.frameTuple = leafFrame.createTupleReference(index, this);
        this.columnTupleRef = (AbstractColumnTupleReference) frameTuple;
        this.searchKeys = new ArrayList<>();
        this.foundIndexes = new BitSet();
        this.seenTupleIndexes = new LongOpenHashSet();
        this.totalAccessCount = 0;
        this.maxLeafFindingAttempts = maxLeafFindingAttempts;
        this.leafTupleCapacity = maxLeafTupleCount;
        this.traceTimingEnabled = LOGGER.isTraceEnabled();

        // A draw yields at most one sample, so a batch beyond the outstanding shortfall is waste, and it must
        // be consumed whole (refillLeafDrawBatch). The configured size stays an upper bound -- an I/O-locality
        // knob.
        long capacity = leafDrawBatchSize > 0 ? Math.min(leafDrawBatchSize, componentSampleCardinality)
                : componentSampleCardinality;
        this.drawBatchCapacity = (int) Math.max(1, Math.min(Integer.MAX_VALUE, capacity));
        this.drawAcceptanceSamples = new double[this.drawBatchCapacity];
        this.drawSortKeys = new long[this.drawBatchCapacity];
        this.drawCollectedAny = new boolean[this.drawBatchCapacity];
        this.groupCandidates = new long[16];

        // Force a batch refill on the very first draw.
        this.pendingLeafDrawIndex = 0;
        this.currentBatchSize = 0;
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        this.bufferCache = bufferCache;
    }

    @Override
    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        if (page0 != null) {
            releasePages();
        }

        rootPageId = ((BTreeCursorInitialState) initialState).getPageId();
        leafPageIds = bTree.enumerateLeafPageIds(rootPageId, opCtx, context);
        // Scale to the TARGET, not the tuple population: a bound of MULTIPLIER * leafPages * maxLeafTupleCount
        // is ~32x total tuples, so an unreachable target ground through that many attempts however small the
        // target was. Harmless on row, fatal on column -- a column attempt re-inits the PK decoders and walks
        // the PK column, so the same count is milliseconds there and hours here. Target-scaling makes it
        // O(target). The leafPages floor lets a tiny target still reach every page; the clamp stops the product
        // overflowing negative.
        long attemptCeiling = SAMPLE_ATTEMPT_MULTIPLIER * Math.max(leafPageIds.length, componentSampleCardinality);
        effectiveMaxLeafFindingAttempts =
                (int) Math.min(Integer.MAX_VALUE, Math.max(maxLeafFindingAttempts, attemptCeiling));

        collectedSamples = new long[(int) Math.min(Integer.MAX_VALUE, componentSampleCardinality)];
        collectedCount = 0;
        selectionDone = false;
        yieldPos = 0;
        prevYieldPageId = -1;
        pendingLeafDrawIndex = 0;
        currentBatchSize = 0;
    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        if (!selectionDone) {
            runPhase1Selection();
            selectionDone = true;
        }
        return yieldNextFromPhase2();
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Phase 1: PK-only selection (Batched & Sorted for sequential I/O)
    // ──────────────────────────────────────────────────────────────────────

    /**
     * Collects up to {@code componentSampleCardinality} distinct live tuples.
     * <p>
     * <b>The result is an exactly uniform sample without replacement.</b> A draw picks a page uniformly
     * ({@code 1/P}), accepts it with the Olken &amp; Rotem fill correction ({@code n/C}), then probes one of the
     * page's {@code n} slots ({@code 1/n}). The product {@code 1/(P·C)} is free of {@code n}: cancelling the
     * page's fill is the whole point of the Olken step, and dropping it would over-represent tuples on
     * partly-filled pages.
     * <p>
     * Uniformity then rests on every stopping rule being a function of <b>counts only</b>, since such rules
     * commute with relabelling the live tuples. Truncating a pageId-sorted batch is the one rule that is not —
     * it keeps precisely the lowest pageIds — hence full-batch consumption ({@link #refillLeafDrawBatch()}).
     * Page grouping is likewise safe because it only reorders probes: the RNG is consumed identically, and the
     * result is order-insensitive (set-based dedup, sorted before phase 2, count-based stopping).
     * <p>
     * <b>Taking several slots per page visit would break this</b>, and was removed: the tuples of one visit
     * share a page, so the sample becomes a cluster sample over contiguous stretches of the key space —
     * measured chi-square well past its critical value — while the per-page amortization that once justified it
     * is now unconditional, since a batch's same-page draws share one primary-key pass regardless.
     * <p>
     * One caveat: a component without {@code maxLeafTupleCount} metadata ({@code leafTupleCapacity == 0}) cannot
     * be fill-corrected, which re-weights tuples by {@code 1/n}.
     * <p>
     * The attempts cap is tested only at batch boundaries, so it can overshoot by one batch — count-based
     * either way, so bounded extra work and no effect on uniformity.
     */
    private void runPhase1Selection() throws HyracksDataException {
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
            visitNextPageGroup();
        }

        endedPreemptively = (collectedCount < componentSampleCardinality);
        unpinCurrentPage0();

        // pageId (high 32) then tupleIndex (low 32), so phase 2 loads each page's columns once.
        Arrays.sort(collectedSamples, 0, collectedCount);

        if (LOGGER.isDebugEnabled()) {
            // endedPreemptively with attempts == cap means the target was unreachable (near-exhaustive or
            // over-estimated); attempts well below cap means it was met.
            LOGGER.debug(
                    "ColumnBtreeSampleCursor Phase 1: collected {}/{} samples from {} leaf pages, "
                            + "attempts {}/{}, endedPreemptively {}",
                    collectedCount, componentSampleCardinality, leafPageIds.length, hasNextAttemptCount,
                    effectiveMaxLeafFindingAttempts, endedPreemptively);
        }
    }

    /**
     * Consumes one <b>page group</b> — the maximal run of same-pageId draws, contiguous because the batch is
     * pageId-sorted — with a single page0 pin, rewind and forward PK pass, instead of one per draw.
     * <p>
     * Two invariants keep this a pure reordering rather than a different sampling design
     * ({@link #runPhase1Selection()}): a group never crosses a batch boundary and is always consumed whole, so
     * the legal stopping points are unchanged; and the RNG is consumed in the same order and quantity, since
     * draws are visited in ascending draw index and only accepted ones pull slots.
     * <p>
     * Per-draw give-up accounting is replayed after the pass rather than interleaved. Equivalent, because
     * nothing between a group's first and last draw reads {@link #hasNextAttemptCount} — it is tested only at a
     * batch boundary.
     */
    private void visitNextPageGroup() throws HyracksDataException {
        int groupStart = pendingLeafDrawIndex;
        // High 32 bits of the sort key ARE the pageId; the low 32 index back into the unsorted side arrays.
        int pageId = (int) (drawSortKeys[groupStart] >>> 32);
        int groupEnd = groupStart + 1;
        while (groupEnd < currentBatchSize && (int) (drawSortKeys[groupEnd] >>> 32) == pageId) {
            groupEnd++;
        }
        pendingLeafDrawIndex = groupEnd;
        int groupSize = groupEnd - groupStart;
        totalAccessCount += groupSize;
        if (stats != null) {
            stats.pageGroups++;
        }

        Arrays.fill(drawCollectedAny, 0, groupSize, false);
        int tupleCount = pinLeafPage0(pageId);
        if (tupleCount == 0) {
            // An empty page rejects every draw that landed on it.
            if (stats != null) {
                stats.attempts += groupSize;
                stats.pagesRejected += groupSize;
            }
        } else {
            int candidateCount = drawGroupCandidates(groupStart, groupEnd, tupleCount);
            if (candidateCount > 0) {
                // Ascending by slot (then by draw ordinal), so the forward-only decoders never rewind.
                Arrays.sort(groupCandidates, 0, candidateCount);
                probeGroupCandidates(pageId, candidateCount);
            } else {
                // No draw survived fill-rejection: nothing to probe, so do not hold the page.
                unpinCurrentPage0();
            }
        }

        for (int drawOrdinal = 0; drawOrdinal < groupSize; drawOrdinal++) {
            if (drawCollectedAny[drawOrdinal]) {
                hasNextAttemptCount = 0;
            } else {
                hasNextAttemptCount++;
            }
        }
    }

    /**
     * Draws the next batch of leaf-page visits, pageId-sorted so the page0 pins go in ascending order.
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
        if (leafPageIds == null || leafPageIds.length == 0) {
            return;
        }
        long remaining = componentSampleCardinality - collectedCount;
        if (remaining <= 0) {
            return;
        }
        int batchSize = (int) Math.min(drawBatchCapacity, remaining);
        for (int i = 0; i < batchSize; i++) {
            int randomLeafIndex = randomNumGen.nextInt(leafPageIds.length);
            int targetPageId = leafPageIds[randomLeafIndex];
            drawAcceptanceSamples[i] = randomNumGen.nextDouble();
            drawSortKeys[i] = (((long) targetPageId) << 32) | (i & 0xffffffffL);
        }
        // Cuts random page0 seeks; primitive sort avoids boxed comparator dispatch.
        Arrays.sort(drawSortKeys, 0, batchSize);
        currentBatchSize = batchSize;
    }

    /**
     * Pins the group's page0 (reusing the pin if it is already the one we hold) and returns its tuple count, or
     * {@code 0} — having released the page — when the page holds no tuples.
     * <p>
     * The Olken &amp; Rotem fill test is not here: it is per draw, not per page, so it lives in
     * {@link #drawGroupCandidates(int, int, int)}. Splitting it out is what lets one pin serve a whole group.
     */
    private int pinLeafPage0(int pageId) throws HyracksDataException {
        long nanos = traceTimingEnabled ? System.nanoTime() : 0L;
        try {
            if (page0 != null && prevYieldPageId != pageId) {
                unpinCurrentPage0();
            }

            if (page0 == null) {
                ICachedPage randomLeafPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), context);
                leafFrame.setPage(randomLeafPage);
                page0 = leafFrame.getPage();
                prevYieldPageId = pageId;
            }

            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                unpinCurrentPage0();
            }
            return tupleCount;
        } finally {
            if (traceTimingEnabled) {
                totalTimeTakenToFindRandomLeaf += (System.nanoTime() - nanos);
            }
        }
    }

    /**
     * Applies the fill-proportional (Olken &amp; Rotem) acceptance test to each draw of the group and gives every
     * survivor one candidate slot, packed into {@link #groupCandidates} as {@code (tupleIndex << 32) | drawOrdinal}.
     * <p>
     * One slot per draw is what makes a tuple's per-draw probe probability {@code 1 / tupleCount}, cancelling the
     * page's fill against the Olken acceptance probability ({@link #runPhase1Selection()}). Distinct draws are
     * independent trials and may pick the same slot.
     * <p>
     * <b>Every candidate is probed</b> — the intra-page form of the full-batch rule. Candidates are
     * slot-ascending, so stopping at the first acceptable one would select the <em>minimum</em> of several
     * uniform draws and pull every page towards its low slots.
     *
     * @return the number of candidate slots written to {@link #groupCandidates}
     */
    private int drawGroupCandidates(int groupStart, int groupEnd, int tupleCount) {
        ensureGroupCandidateCapacity(groupEnd - groupStart);
        // Unknown capacity (pre-upgrade component, no metadata) => accept every page. Mirrors
        // DiskBTreeSampleCursor#pinAndAcceptLeafPage.
        boolean applyFillRejection = leafTupleCapacity > 0;
        double acceptProb = applyFillRejection ? (double) tupleCount / leafTupleCapacity : 1.0;

        int candidateCount = 0;
        // Ascending draw index: the order the sorted batch would have visited these anyway.
        for (int i = groupStart; i < groupEnd; i++) {
            int drawIndex = (int) drawSortKeys[i];
            if (applyFillRejection && drawAcceptanceSamples[drawIndex] >= acceptProb) {
                if (stats != null) {
                    stats.attempts++;
                    stats.pagesRejected++;
                }
                continue;
            }
            if (stats != null) {
                stats.attempts++;
                stats.pagesAccepted++;
            }
            int drawOrdinal = i - groupStart;
            groupCandidates[candidateCount++] =
                    (((long) randomNumGen.nextInt(tupleCount)) << 32) | (drawOrdinal & 0xffffffffL);
        }
        return candidateCount;
    }

    /**
     * Probes the group's slot-ascending candidates in one forward-only pass, collecting the live ones.
     * <p>
     * The PK definition-level decoder is sequential and cannot seek backwards — hence the ascending order and
     * the single {@link AbstractColumnTupleReference#startSamplingPage()} rewind. One
     * {@code O(maxCandidateIndex)} scan serves the whole group. Rejections just advance the scan.
     * <p>
     * <b>Repeated slots must not be re-seeked.</b> Two draws may pick the same slot, and
     * {@link AbstractColumnTupleReference#seekForwardPKOnly(int)} computes {@code startIndex - tupleIndex - 1},
     * so a repeat would pass a skip of {@code -1} and desync the decoder. The reader is already there, so the
     * repeat just re-runs the liveness decision.
     */
    private void probeGroupCandidates(int pageId, int candidateCount) throws HyracksDataException {
        long nanos = (traceTimingEnabled || stats != null) ? System.nanoTime() : 0L;
        // Subtracted from the method-wide window before it lands in stats.pkSeekNanos: the liveness check runs
        // inside this timed span, so without this pkSeekNanos would contain it and dilute livenessSharePct().
        long livenessNanosThisCall = 0L;
        try {
            columnTupleRef.startSamplingPage();
            int lastProbedIndex = -1;

            // Every candidate is probed: stopping early biases towards low slots (drawGroupCandidates).
            for (int c = 0; c < candidateCount; c++) {
                long packed = groupCandidates[c];
                int tupleIndex = (int) (packed >>> 32);
                int drawOrdinal = (int) packed;

                if (tupleIndex != lastProbedIndex) {
                    columnTupleRef.seekForwardPKOnly(tupleIndex);
                    lastProbedIndex = tupleIndex;
                }
                if (stats != null) {
                    stats.pkSeekCalls++;
                }
                if (frameTuple.isAntimatter()) {
                    continue;
                }

                long pageTupleKey = getPageTupleKey(pageId, tupleIndex);
                if (seenTupleIndexes.contains(pageTupleKey)) {
                    continue;
                }

                // Newer-component liveness, from page0's PK fields alone.
                searchKeys.clear();
                foundIndexes.clear();
                searchKeys.add(frameTuple);
                batchPredicate.reset(searchKeys);
                searchCursor.setPredicate(batchPredicate);
                long livenessStart = stats != null ? System.nanoTime() : 0L;
                searchCursor.hasNextWithPredicate(foundIndexes);
                if (stats != null) {
                    long livenessElapsed = System.nanoTime() - livenessStart;
                    stats.livenessNanos += livenessElapsed;
                    livenessNanosThisCall += livenessElapsed;
                    stats.livenessCalls++;
                    stats.livenessKeys++;
                }
                if (!foundIndexes.isEmpty()) {
                    continue;
                }

                seenTupleIndexes.add(pageTupleKey);
                collectedSamples[collectedCount++] = pageTupleKey;
                drawCollectedAny[drawOrdinal] = true;
            }
        } finally {
            if (traceTimingEnabled) {
                totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
            }
            if (stats != null) {
                stats.pkSeekNanos += System.nanoTime() - nanos - livenessNanosThisCall;
            }
        }
    }

    /**
     * Grows {@link #groupCandidates}. One slot per draw and a group cannot exceed the batch capacity, so this
     * settles after the first few groups rather than growing without limit.
     */
    private void ensureGroupCandidateCapacity(long needed) {
        if (groupCandidates.length < needed) {
            long grown = Math.max(needed, groupCandidates.length * 2L);
            groupCandidates = new long[(int) Math.min(Integer.MAX_VALUE, grown)];
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Phase 2: Sorted column collection — one mega-page load per page
    // ──────────────────────────────────────────────────────────────────────

    private boolean yieldNextFromPhase2() throws HyracksDataException {
        if (yieldPos >= collectedCount) {
            // Pages are released on page-id change, so the last mega-leaf is still pinned here. Release it
            // rather than hold a full mega-leaf of frames until doClose(). releasePages() is idempotent.
            releasePages();
            return false;
        }

        long nanos = (traceTimingEnabled || stats != null) ? System.nanoTime() : 0L;
        try {
            long packed = collectedSamples[yieldPos];
            int pageId = (int) (packed >>> 32);
            int tupleIdx = (int) packed;

            if (pageId != prevYieldPageId) {
                context.release(bufferCache);
                unpinCurrentPage0();

                ICachedPage newPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), context);
                if (stats != null) {
                    stats.phase2PagePins++;
                }
                leafFrame.setPage(newPage);
                page0 = leafFrame.getPage();

                // Full column load -- the point of deferring to phase 2.
                context.preparePageZeroSegments(leafFrame, bufferCache, fileId);
                frameTuple.newPage();
                context.prepareColumns(leafFrame, bufferCache, fileId);
                frameTuple.reset(tupleIdx, leafFrame.getTupleCount() - 1);
                prevYieldPageId = pageId;
            } else {
                frameTuple.setAt(tupleIdx);
            }

            sampledCount++;
            yieldPos++;
            return true;
        } finally {
            if (traceTimingEnabled) {
                totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
            }
            if (stats != null) {
                stats.phase2Nanos += System.nanoTime() - nanos;
            }
        }
    }

    private long getPageTupleKey(int pageId, int tupleIndex) {
        return (((long) pageId) << 32) | (tupleIndex & 0xffffffffL);
    }

    private void unpinCurrentPage0() {
        if (page0 != null) {
            bufferCache.unpin(page0, context);
            page0 = null;
            prevYieldPageId = -1;
        }
    }

    private void releasePages() throws HyracksDataException {
        context.release(bufferCache);
        frameTuple.unpinColumnsPages();
        unpinCurrentPage0();
    }

    @Override
    protected void doNext() throws HyracksDataException {
        //NoOp
    }

    @Override
    protected void doDestroy() throws HyracksDataException {
        // No Op all resources are released in the close call
    }

    @Override
    protected void doClose() throws HyracksDataException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(
                    "{} stats - sampledCount: {}, totalAccessCount: {}, "
                            + "totalTimeTakenToFindRandomLeaf: {} ns, totalTimeTakenToFindRandomTuples: {} ns, "
                            + "endedPreemptively: {}",
                    this.getClass().getName(), sampledCount, totalAccessCount, totalTimeTakenToFindRandomLeaf,
                    totalTimeTakenToFindRandomTuples, endedPreemptively);
        }
        releasePages();
        frameTuple.close();
        context.close(bufferCache);
        seenTupleIndexes.clear();
        page0 = null;
        leafPageIds = null;
        collectedSamples = null;
        collectedCount = 0;
        selectionDone = false;
        yieldPos = 0;
        prevYieldPageId = -1;

        // Force a batch refill on the next reuse
        pendingLeafDrawIndex = 0;
        currentBatchSize = 0;

        sampledCount = 0;
        hasNextAttemptCount = 0;
        totalAccessCount = 0;
        totalTimeTakenToFindRandomLeaf = 0;
        totalTimeTakenToFindRandomTuples = 0;
        endedPreemptively = false;
    }

    @Override
    protected ITupleReference doGetTuple() {
        return frameTuple;
    }

    @Override
    public ICachedPage pin(int pageId) throws HyracksDataException {
        return bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), context);
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        // Same context as pin(int): pin goes through context.onPin -> beforeRead (read latch), so unpin must go
        // through onUnpin -> afterRead to release it. Without the context the latch leaks and cloud storage
        // fails BufferCache.isClean.
        bufferCache.unpin(page, context);
    }

    @Override
    public int getPageSize() {
        return bufferCache.getPageSize();
    }

}