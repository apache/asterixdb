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
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * Disk B-tree sample cursor for row storage.
 * <p>
 * Performs single-phase random sampling: picks random leaf pages and tuples,
 * checks antimatter and newer-component existence, and immediately yields
 * accepted samples. For row storage with typical low sampling density
 * (scattered samples across many pages), this approach minimizes page pins.
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
    // Sampling related fields
    // Multiplier applied to the component tuple-population ceiling to derive the
    // effective consecutive-miss give-up threshold (see doOpen). Empirically the
    // reservoir stalls at roughly threshold/3 when the threshold is too tight, so a
    // multiple well above 3 is needed to reliably reach near-exhaustive targets.
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Population-scaled give-up threshold to stop near-exhaustive samples truncating at ~500 attempts")
    private static final long POPULATION_ATTEMPT_MULTIPLIER = 32L;
    private final int maxLeafFindingAttempts;
    // Effective consecutive-miss give-up threshold, scaled to the component's tuple
    // population in doOpen(). maxLeafFindingAttempts alone is a fixed floor that
    // silently truncates near-exhaustive samples (target close to the component's
    // live-tuple count): as the reservoir fills, random draws increasingly hit
    // already-seen tuples, so the consecutive-miss streak needed to find the last
    // few tuples grows with the population. Scaling the cap to the population keeps
    // collection from stopping short while still bounding the genuinely-exhausted
    // case (target >= population) to O(population) draws.
    private int effectiveMaxLeafFindingAttempts;
    private final long componentSampleCardinality;
    private final Random randomNumGen;
    private ICachedPage page = null;
    private int pageId = -1;
    private int rootPageId = -1;
    // Pre-enumerated leaf page IDs for perfectly uniform random access.
    // Enumeration touches only interior pages (level >= 1), skipping all
    // leaf-level I/O.
    private int[] leafPageIds = null;
    // Batched random draws stored in flat parallel primitive arrays (SoA) to
    // keep the hot per-draw read cache-friendly and free of object headers /
    // pointer chasing. Sorting is done on a packed long[] key
    // ((pageId << 32) | drawIndex) via Arrays.sort, which avoids the boxed
    // object comparator dispatch of the previous List<LeafDraw> approach while
    // still grouping draws by pageId for pin reuse. A single reusable LeafDraw
    // view is repointed per draw to preserve the existing call shape.
    private final int[] drawPageIds;
    private final double[] drawAcceptanceSamples;
    private final int[] drawTupleStartSeeds;
    // Packed sort key: (pageId << 32) | originalDrawIndex. Sorting by the full
    // long orders by pageId first, keeping the side arrays addressable by the
    // low 32 bits without physically permuting them.
    private final long[] drawSortKeys;
    private final LeafDraw leafDrawView;
    private int pendingLeafDrawIndex;
    private final int leafDrawBatchSize;
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

    // Rejection sampling static upper bound (Olken & Rotem)
    // to correct for bias from partially-filled pages
    private final int leafTupleCapacity;

    // Cached once: avoids querying the logging framework and, more importantly,
    // gates the per-draw System.nanoTime() timing calls (a syscall-class
    // operation) so they are skipped entirely on the hot path when debug is off.
    private final boolean debugTimingEnabled;

    // Debug and traceability
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
        this.debugTimingEnabled = LOGGER.isDebugEnabled();
        this.leafDrawBatchSize = (int) Math.max(leafDrawBatchSize, componentSampleCardinality);

        // Pre-allocate flat SoA draw buffers once to avoid churn during refills.
        this.drawPageIds = new int[this.leafDrawBatchSize];
        this.drawAcceptanceSamples = new double[this.leafDrawBatchSize];
        this.drawTupleStartSeeds = new int[this.leafDrawBatchSize];
        this.drawSortKeys = new long[this.leafDrawBatchSize];
        this.leafDrawView = new LeafDraw(-1, 0.0, 0);

        this.pendingLeafDrawIndex = this.leafDrawBatchSize;
    }

    @Override
    protected void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        if (page != null) {
            releasePage();
        }
        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();
        // Enumerate all leaf page IDs by traversing only interior pages.
        // The optimised DFS skips leaf-level I/O (level-1 nodes record their
        // children's page IDs directly), so the cost is proportional to the
        // number of interior pages (~0.4 % of total pages for typical fan-out).
        // The resulting array gives perfectly uniform random access to any leaf.
        leafPageIds = bTree.enumerateLeafPageIds(rootPageId, bTreeOpCtx, bufferCacheOpCtx);
        // Scale the give-up threshold to a multiple of the component's tuple-population
        // ceiling (leaf pages x max tuples/leaf). The per-component sample target never
        // exceeds the population (the caller caps it at the component's live-tuple
        // estimate), so a target is always reachable; the loop must not give up before
        // reaching it. Random draws with a growing already-seen set miss increasingly
        // often as the reservoir fills, so the consecutive-miss streak needed to find
        // the last few tuples grows super-linearly in the population - a give-up
        // threshold equal to the population alone stops far short (observed collected
        // ~= threshold/3). POPULATION_ATTEMPT_MULTIPLIER buys enough headroom to reach
        // the target, while still bounding the genuinely-exhausted case (target >
        // available, e.g. a "high" sample of a tiny dataset) to O(multiplier x
        // population) draws. Computed as long, then clamped to int: for huge components
        // the product overflows int, but there the target is far below the population so
        // the loop exits on reaching it; clamping to Integer.MAX_VALUE just avoids a
        // negative (overflowed) threshold that would itself cause truncation.
        long populationCeiling = (long) leafPageIds.length * leafTupleCapacity;
        effectiveMaxLeafFindingAttempts = (int) Math.min(Integer.MAX_VALUE,
                Math.max(maxLeafFindingAttempts, POPULATION_ATTEMPT_MULTIPLIER * populationCeiling));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("DiskBTreeSampleCursor: {} leaf pages enumerated, target {} samples", leafPageIds.length,
                    componentSampleCardinality);
        }
    }

    @Override
    protected boolean doHasNext() throws HyracksDataException {
        while (sampledCount < componentSampleCardinality && hasNextAttemptCount < effectiveMaxLeafFindingAttempts) {
            LeafDraw leafDraw = nextLeafDraw();
            if (leafDraw == null) {
                break;
            }
            if (!pinAndAcceptLeafPage(leafDraw)) {
                hasNextAttemptCount++;
                continue;
            }
            int foundTupleIndex = findRandomTuple(leafDraw.tupleStartSeed);
            long pageTupleKey = getPageTupleKey(pageId, foundTupleIndex);
            // Skip if no valid tuple found or already seen
            if (foundTupleIndex == -1 || seenTupleIndexes.contains(pageTupleKey)) {
                hasNextAttemptCount++;
                continue;
            }
            // Check if tuple exists in newer LSM components
            searchKeys.clear();
            foundIndexes.clear();
            searchKeys.add(frameTuple);
            batchPredicate.reset(searchKeys);
            searchCursor.setPredicate(batchPredicate);
            searchCursor.doHasNextWithPredicate(foundIndexes);
            if (foundIndexes.isEmpty()) {
                // Tuple is unique and not found in newer components - valid sample
                // Keep page pinned and return immediately
                hasNextAttemptCount = 0;
                seenTupleIndexes.add(pageTupleKey);
                sampledCount++;
                return true;
            } else {
                // Tuple exists in newer component, try again
                hasNextAttemptCount++;
            }
        }
        endedPreemptively = (sampledCount < componentSampleCardinality);
        return false;
    }

    private LeafDraw nextLeafDraw() {
        if (pendingLeafDrawIndex >= leafDrawBatchSize) {
            refillLeafDrawBatch();
            if (leafPageIds == null || leafPageIds.length == 0) {
                return null;
            }
        }
        // Resolve the next draw in sorted (pageId-ascending) order. The low 32
        // bits of the sort key index back into the unsorted SoA side arrays.
        int drawIndex = (int) drawSortKeys[pendingLeafDrawIndex++];
        leafDrawView.pageId = drawPageIds[drawIndex];
        leafDrawView.acceptanceSample = drawAcceptanceSamples[drawIndex];
        leafDrawView.tupleStartSeed = drawTupleStartSeeds[drawIndex];
        return leafDrawView;
    }

    private void refillLeafDrawBatch() {
        pendingLeafDrawIndex = 0;
        if (leafPageIds == null || leafPageIds.length == 0 || leafDrawBatchSize <= 0) {
            return;
        }
        for (int i = 0; i < leafDrawBatchSize; i++) {
            int randomLeafIndex = randomNumGen.nextInt(leafPageIds.length);
            int targetPageId = leafPageIds[randomLeafIndex];
            drawPageIds[i] = targetPageId;
            drawAcceptanceSamples[i] = randomNumGen.nextDouble();
            drawTupleStartSeeds[i] = randomNumGen.nextInt();
            // Pack (pageId, drawIndex): pageIds are non-negative leaf page IDs,
            // so the high 32 bits sort correctly as a signed long.
            drawSortKeys[i] = (((long) targetPageId) << 32) | (i & 0xffffffffL);
        }

        // Primitive long[] sort: branch-predictable, comparator-free, and
        // cache-friendly versus the previous boxed-object comparator sort.
        Arrays.sort(drawSortKeys);

        if (debugTimingEnabled) {
            int uniquePages = 0;
            long prevPageId = Long.MIN_VALUE;
            for (int i = 0; i < leafDrawBatchSize; i++) {
                long currentPageId = drawSortKeys[i] >>> 32;
                if (currentPageId != prevPageId) {
                    uniquePages++;
                    prevPageId = currentPageId;
                }
            }
            totalLeafDrawBatches++;
            totalLeafDraws += leafDrawBatchSize;
            totalLeafDrawUniquePages += uniquePages;
        }
    }

    private long getPageTupleKey(int pageId, int tupleIndex) {
        return (((long) pageId) << 32) | (tupleIndex & 0xffffffffL);
    }

    /**
     * Selects a random leaf page from the pre-enumerated leaf page ID array.
     * <p>
     * Each leaf has exactly {@code 1 / N_leaves} probability of being chosen.
     * The enumeration cost is amortized: it touches only interior pages once
     * during {@code doOpen()}, then every sample here is a single direct pin.
     * <p>
     * <b>Page-fill correction (Olken &amp; Rotem, VLDB 1989):</b> Each selected leaf
     * is accepted with probability {@code tupleCount / leafTupleCapacity} to
     * correct for the bias where tuples on partially-filled pages have higher
     * per-tuple selection probability.
     */
    private boolean pinAndAcceptLeafPage(LeafDraw leafDraw) throws HyracksDataException {
        // Skip the nanoTime() pair entirely unless debug logging will consume it.
        long nanos = debugTimingEnabled ? System.nanoTime() : 0L;
        try {
            totalAccessCount++;
            if (pageId != leafDraw.pageId || page == null) {
                releasePage();
                int targetPageId = leafDraw.pageId;
                ICachedPage randomLeafPage =
                        bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, targetPageId), bufferCacheOpCtx);
                page = randomLeafPage;
                leafFrame.setPage(page);
                pageId = targetPageId;
                totalLeafPins++;
            } else {
                totalReusedPinnedPageHits++;
            }
            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                return false;
            }
            // Rejection sampling: accept this page with probability proportional to
            // its tuple count against a static capacity bound.
            // Skip correction if capacity unknown (pre-upgrade component lacking metadata).
            if (leafTupleCapacity > 0) {
                double acceptProb = (double) tupleCount / leafTupleCapacity;
                if (leafDraw.acceptanceSample >= acceptProb) {
                    return false;
                }
            }
            return true;
        } finally {
            // Ensures elapsed time is correctly tracked even if the page is rejected
            if (debugTimingEnabled) {
                totalTimeTakenToFindRandomLeaf += (System.nanoTime() - nanos);
            }
        }
    }

    /**
     * Picks a random tuple from the current leaf page.
     * <p>
     * <b>Antimatter Rejection:</b> If the selected tuple is antimatter, it immediately
     * returns -1 (rejecting the draw completely). This is O(1) and preserves the unbiased
     * uniform distribution of live tuples, accurately reflecting the physical density of the data.
     * </p>
     */
    private int findRandomTuple(int tupleStartSeed) {
        long nanos = debugTimingEnabled ? System.nanoTime() : 0L;
        int numberOfTuples = leafFrame.getTupleCount();

        // Strips sign bit for fast, uniform modulo arithmetic
        int targetTupleIndex = (tupleStartSeed & 0x7FFFFFFF) % numberOfTuples;

        frameTuple.resetByTupleIndex(leafFrame, targetTupleIndex);
        int result = antimatterAcceptor.accept(frameTuple) ? -1 : targetTupleIndex;

        if (debugTimingEnabled) {
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
        if (LOGGER.isDebugEnabled()) {
            double avgBatchDraws = totalLeafDrawBatches == 0 ? 0.0 : (double) totalLeafDraws / totalLeafDrawBatches;
            double avgBatchUniquePages =
                    totalLeafDrawBatches == 0 ? 0.0 : (double) totalLeafDrawUniquePages / totalLeafDrawBatches;
            double uniquePerDraw = totalLeafDraws == 0 ? 0.0 : (double) totalLeafDrawUniquePages / totalLeafDraws;
            double repinAvoidRate = (totalLeafPins + totalReusedPinnedPageHits) == 0 ? 0.0
                    : (double) totalReusedPinnedPageHits / (totalLeafPins + totalReusedPinnedPageHits);
            LOGGER.debug(
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

        pendingLeafDrawIndex = leafDrawBatchSize;

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
            // first page of the cursor.
            bufferCache.unpin(page);
        }
        page = null;
        pageId = -1;
    }

    // Mutable single-instance view over the SoA draw arrays. Repointed per draw
    // in nextLeafDraw(); never escapes the cursor, so reuse is safe.
    private static final class LeafDraw {
        private int pageId;
        private double acceptanceSample;
        private int tupleStartSeed;

        private LeafDraw(int pageId, double acceptanceSample, int tupleStartSeed) {
            this.pageId = pageId;
            this.acceptanceSample = acceptanceSample;
            this.tupleStartSeed = tupleStartSeed;
        }
    }
}
