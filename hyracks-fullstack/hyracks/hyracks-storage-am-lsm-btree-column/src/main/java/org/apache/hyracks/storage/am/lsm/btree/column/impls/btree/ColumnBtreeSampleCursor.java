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
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * Two-phase column sample cursor.
 * <p>
 * <b>Phase 1 — Selection (page0-only):</b> picks random leaf pages via batched
 * sorted draws to minimize random I/O, pins only page0, uses
 * {@link AbstractColumnTupleReference#startSamplingPage} +
 * {@link AbstractColumnTupleReference#seekForwardPKOnly} to position PKs without
 * loading column mega-pages. Per-page candidate indices are sorted ascending so the
 * sequential PK level decoders are scanned forward-only (no rewind). Checks antimatter
 * and newer-component existence from PK data alone. Accepted tuples are recorded as
 * packed {@code (pageId << 32) | tupleIndex} longs.
 * <p>
 * <b>Phase 2 — Collection (sorted, full column load):</b> the collected
 * samples are sorted by pageId (then tupleIndex within a page). For each
 * distinct page we do one full column load via {@code reset()}, then
 * advance to additional tuples on the same page via the cheap forward-only
 * {@code setAt()}. This amortises mega-page I/O and gives cache-friendly
 * sequential access.
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

    // Multiplier applied to the component tuple-population ceiling to derive the
    // effective consecutive-miss give-up threshold (see doOpen). Mirrors
    // DiskBTreeSampleCursor: the reservoir stalls well short of the target when the
    // threshold only equals the population, so a multiple well above that is needed.
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Population-scaled give-up threshold to stop near-exhaustive samples truncating at ~500 attempts")
    private static final long POPULATION_ATTEMPT_MULTIPLIER = 32L;
    private final int maxLeafFindingAttempts;
    // Effective consecutive-miss give-up threshold, scaled to the component's tuple
    // population in doOpen(). A fixed maxLeafFindingAttempts silently truncates
    // near-exhaustive samples (target close to the component's live-tuple count) as
    // random draws increasingly hit already-collected pages/tuples; scaling to the
    // population keeps collection from stopping short while bounding the
    // genuinely-exhausted case to O(population) draws. Mirrors DiskBTreeSampleCursor.
    private int effectiveMaxLeafFindingAttempts;
    private final int samplesPerPage;
    // Max number of random probes per page (oversampled vs samplesPerPage to absorb
    // antimatter / collision / newer-component rejections).
    private final int maxPageSlotRetries;
    // Reused scratch buffer of per-page candidate tuple indices, sorted ascending so the
    // PK level decoders can be scanned forward-only (no rewind) within a page.
    private final int[] pageTupleCandidates;
    private final long componentSampleCardinality;
    private final Random randomNumGen;

    // Phase 1 batched I/O draws stored as flat parallel primitive arrays (SoA).
    // Sorted via a packed long[] key ((pageId << 32) | drawIndex) so page0 pins
    // proceed in ascending pageId order without boxed-object comparator overhead.
    private final int[] drawPageIds;
    private final double[] drawAcceptanceSamples;
    private final int[] drawTupleStartSeeds;
    private final long[] drawSortKeys;
    private final LeafDraw leafDrawView;
    private int pendingLeafDrawIndex;
    private final int leafDrawBatchSize;

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

    // Cached once: the per-draw / per-yield System.nanoTime() timing is consumed
    // only by the trace log in doClose(). Gating on this flag removes those
    // syscall-class calls from the hot path when trace logging is disabled.
    private final boolean traceTimingEnabled;

    // Debug and traceability
    private long totalTimeTakenToFindRandomLeaf = 0;
    private long totalTimeTakenToFindRandomTuples = 0;
    private boolean endedPreemptively = false;

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
            int maxLeafTupleCount, int samplesPerPage) {
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
        this.samplesPerPage = Math.max(1, samplesPerPage);
        // Collision probability ≈ samplesPerPage/tupleCount ≪ 1 for typical configs, so 5× is conservative.
        this.maxPageSlotRetries = this.samplesPerPage * 5;
        this.pageTupleCandidates = new int[this.maxPageSlotRetries];
        this.leafTupleCapacity = maxLeafTupleCount;
        this.traceTimingEnabled = LOGGER.isTraceEnabled();

        this.leafDrawBatchSize = (int) Math.max(leafDrawBatchSize, componentSampleCardinality);
        this.drawPageIds = new int[this.leafDrawBatchSize];
        this.drawAcceptanceSamples = new double[this.leafDrawBatchSize];
        this.drawTupleStartSeeds = new int[this.leafDrawBatchSize];
        this.drawSortKeys = new long[this.leafDrawBatchSize];
        this.leafDrawView = new LeafDraw(-1, 0.0, 0);

        // FIX: Force a batch refill on the very first nextLeafDraw() call
        this.pendingLeafDrawIndex = this.leafDrawBatchSize;
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

        rootPageId = ((BTreeCursorInitialState) initialState).getRootPageId();
        leafPageIds = bTree.enumerateLeafPageIds(rootPageId, opCtx, context);
        // Scale the give-up threshold to the component's tuple-population ceiling
        // (leaf pages x max tuples/leaf), clamped to int to avoid an overflowed
        // negative cap on huge components (where the target is far below the
        // population and the loop exits on reaching the target regardless).
        long populationCeiling = (long) leafPageIds.length * leafTupleCapacity;
        effectiveMaxLeafFindingAttempts = (int) Math.min(Integer.MAX_VALUE,
                Math.max(maxLeafFindingAttempts, POPULATION_ATTEMPT_MULTIPLIER * populationCeiling));

        collectedSamples = new long[(int) componentSampleCardinality];
        collectedCount = 0;
        selectionDone = false;
        yieldPos = 0;
        prevYieldPageId = -1;
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

    private void runPhase1Selection() throws HyracksDataException {
        while (collectedCount < componentSampleCardinality && hasNextAttemptCount < effectiveMaxLeafFindingAttempts) {
            LeafDraw leafDraw = nextLeafDraw();
            if (leafDraw == null) {
                break;
            }

            totalAccessCount++;

            int selectedPageId = pinAndAcceptLeafPage0(leafDraw);
            if (selectedPageId == -1) {
                hasNextAttemptCount++;
                continue;
            }

            int collectedFromPage = collectFromPage(selectedPageId);

            if (collectedFromPage > 0) {
                hasNextAttemptCount = 0;
            } else {
                hasNextAttemptCount++;
            }
        }

        endedPreemptively = (collectedCount < componentSampleCardinality);
        unpinCurrentPage0();

        // Sort by pageId (high 32 bits) then tupleIndex (low 32 bits) for Phase 2
        Arrays.sort(collectedSamples, 0, collectedCount);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("ColumnBtreeSampleCursor Phase 1: collected {} samples from {} leaf pages", collectedCount,
                    leafPageIds.length);
        }
    }

    private LeafDraw nextLeafDraw() {
        if (pendingLeafDrawIndex >= leafDrawBatchSize) {
            refillLeafDrawBatch();
            if (leafPageIds == null || leafPageIds.length == 0) {
                return null;
            }
        }
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
            drawSortKeys[i] = (((long) targetPageId) << 32) | (i & 0xffffffffL);
        }
        // Sorting Phase 1 draws dramatically reduces random disk seeks for page0.
        // Primitive long[] sort avoids the boxed-object comparator dispatch.
        Arrays.sort(drawSortKeys);
    }

    private int pinAndAcceptLeafPage0(LeafDraw leafDraw) throws HyracksDataException {
        long nanos = traceTimingEnabled ? System.nanoTime() : 0L;
        try {
            // Unpin previous page0 if we are moving to a new page
            if (page0 != null && prevYieldPageId != leafDraw.pageId) {
                unpinCurrentPage0();
            }

            if (page0 == null) {
                ICachedPage randomLeafPage =
                        bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, leafDraw.pageId), context);
                leafFrame.setPage(randomLeafPage);
                page0 = leafFrame.getPage();
                prevYieldPageId = leafDraw.pageId;
            }

            int tupleCount = leafFrame.getTupleCount();
            if (tupleCount == 0) {
                unpinCurrentPage0();
                return -1;
            }

            // Rejection sampling against static capacity
            double acceptProb = (double) tupleCount / leafTupleCapacity;
            if (leafDraw.acceptanceSample >= acceptProb) {
                unpinCurrentPage0();
                return -1;
            }

            return leafDraw.pageId;
        } finally {
            if (traceTimingEnabled) {
                totalTimeTakenToFindRandomLeaf += (System.nanoTime() - nanos);
            }
        }
    }

    /**
     * Collects up to {@code samplesPerPage} live tuples from one accepted page using a single
     * forward-only pass over the PK level decoders.
     * <p>
     * The PK definition-level decoder is sequential and cannot seek backwards, so we pre-draw the
     * candidate tuple indices, sort them ascending, and probe them in order via
     * {@link AbstractColumnTupleReference#seekForwardPKOnly(int)}. This scans the page once
     * ({@code O(maxIndex)} level reads) instead of rewinding the decoder per probe. Antimatter,
     * collision and newer-component rejections simply advance the forward scan.
     *
     * @return the number of live tuples collected from this page
     */
    private int collectFromPage(int selectedPageId) throws HyracksDataException {
        long nanos = traceTimingEnabled ? System.nanoTime() : 0L;
        try {
            int tupleCount = leafFrame.getTupleCount();
            int drawCount = Math.min(maxPageSlotRetries, tupleCount);

            // Pre-draw candidate tuple indices and sort ascending for a monotonic forward scan.
            for (int i = 0; i < drawCount; i++) {
                pageTupleCandidates[i] = randomNumGen.nextInt(tupleCount);
            }
            Arrays.sort(pageTupleCandidates, 0, drawCount);

            // Rewind the PK level decoders once for this page; subsequent seeks move forward only.
            columnTupleRef.startSamplingPage();

            int collectedFromPage = 0;
            int prevTupleIndex = -1;
            for (int c = 0; c < drawCount && collectedFromPage < samplesPerPage
                    && collectedCount < componentSampleCardinality; c++) {
                int tupleIndex = pageTupleCandidates[c];
                if (tupleIndex == prevTupleIndex) {
                    // Duplicate draw within the sorted batch; the decoder cannot revisit it anyway.
                    continue;
                }
                prevTupleIndex = tupleIndex;

                columnTupleRef.seekForwardPKOnly(tupleIndex);
                if (frameTuple.isAntimatter()) {
                    continue;
                }

                long pageTupleKey = getPageTupleKey(selectedPageId, tupleIndex);
                if (seenTupleIndexes.contains(pageTupleKey)) {
                    continue;
                }

                // Newer-component check (uses PK fields from page0)
                searchKeys.clear();
                foundIndexes.clear();
                searchKeys.add(frameTuple);
                batchPredicate.reset(searchKeys);
                searchCursor.setPredicate(batchPredicate);
                searchCursor.doHasNextWithPredicate(foundIndexes);
                if (!foundIndexes.isEmpty()) {
                    continue;
                }

                seenTupleIndexes.add(pageTupleKey);
                collectedSamples[collectedCount++] = pageTupleKey;
                collectedFromPage++;
            }
            return collectedFromPage;
        } finally {
            if (traceTimingEnabled) {
                totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────
    //  Phase 2: Sorted column collection — one mega-page load per page
    // ──────────────────────────────────────────────────────────────────────

    private boolean yieldNextFromPhase2() throws HyracksDataException {
        if (yieldPos >= collectedCount) {
            return false;
        }

        long nanos = traceTimingEnabled ? System.nanoTime() : 0L;
        try {
            long packed = collectedSamples[yieldPos];
            int pageId = (int) (packed >>> 32);
            int tupleIdx = (int) packed;

            if (pageId != prevYieldPageId) {
                // Release previous column pages & page0
                context.release(bufferCache);
                unpinCurrentPage0();

                // Pin the new page0
                ICachedPage newPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId), context);
                leafFrame.setPage(newPage);
                page0 = leafFrame.getPage();

                // Full column load
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
            // Track Phase 2 materialization time here
            if (traceTimingEnabled) {
                totalTimeTakenToFindRandomTuples += (System.nanoTime() - nanos);
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

        // FIX: Force a batch refill on the next reuse
        pendingLeafDrawIndex = leafDrawBatchSize;

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
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Pass read context on unpin so onUnpin/afterRead releases the read latch acquired by pin (fixes cloud isClean leak)")
    public void unpin(ICachedPage page) throws HyracksDataException {
        // Must pass the same read context used in pin(int): pin() goes through
        // context.onPin -> CloudCachedPage.beforeRead (read latch), so unpin must go
        // through context.onUnpin -> afterRead to release that latch. Unpinning without
        // the context skips onUnpin and leaks the read latch (fails BufferCache.isClean
        // under cloud storage).
        bufferCache.unpin(page, context);
    }

    @Override
    public int getPageSize() {
        return bufferCache.getPageSize();
    }

    // Mutable single-instance view over the SoA draw arrays, repointed per draw.
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