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

package org.apache.hyracks.storage.am.lsm.btree;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.impls.AntimatterAwareTupleAcceptor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeOpContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Uniformity guard for {@code DiskBTreeSampleCursor}, the <b>row</b>-format sample cursor; {@code
 * ColumnSampleUniformityTest} is the column analogue. It exists because the row cursor took the same uniformity
 * fixes (full-batch consumption, {@code Random.nextInt(bound)} over a {@code %} fold) while its correctness
 * rested on an analytical argument and functional tests that only checked "no deleted row leaked".
 * <p>
 * The sample must be uniform over the component's <em>live</em> tuples, because the CBO derives NDV and
 * selectivity from it; clustered in primary-key order it is silently wrong for every column correlated with the
 * key. The pre-fix failure was not subtle — the cursor drained the lowest page ids of a pageId-sorted batch and
 * stopped at the target, making the "sample" a contiguous run of the key space.
 * <p>
 * Chi-square goodness-of-fit on {@link #NUM_SEEDS} pooled runs over {@link #NUM_BINS} bins, at the unscaled
 * {@link #CHI_SQUARE_CRITICAL} (19 df, alpha = 0.001). Two deliberate details:
 * <ul>
 * <li><b>Per-bin expectations are proportional to each bin's live keys</b>, not a flat {@code total / NUM_BINS}.
 * They agree exactly at this fixture; the proportional form guards against a future fixture whose uneven bins
 * would otherwise inflate the statistic for reasons unrelated to the sampler.</li>
 * <li><b>The sampling fraction is appreciable</b> and the cursor samples without replacement, so cell variance
 * sits below multinomial by {@code 1 - f}. That makes the test conservative under the null and costs nothing
 * against the alternative it catches, which is off by orders of magnitude, not a variance factor.</li>
 * </ul>
 * <p>
 * <b>Sizing.</b> {@link #NUM_KEYS} keys flushed in rounds then merged — so the shared harness's
 * 50-page mutable components do not bound the result — give 118 leaf pages at 256-byte disk pages, roughly six
 * per bin. A handful of pages could not distinguish clustering from noise, since one page would span several
 * bins.
 * <p>
 * <b>Shown non-vacuous:</b> reinstating the pre-fix mid-batch stop moves the pooled statistic from 13.98 to
 * <b>42300.00</b>, with all 7200 samples in the lowest 3 bins. Recorded rather than automated — reproducing it
 * means editing production code.
 */
@SuppressWarnings("rawtypes")
public class DiskBTreeSampleUniformityTest {

    private static final int FIELD_COUNT = 2;
    private static final int KEY_FIELD_COUNT = 1;
    private static final ITypeTraits[] TYPE_TRAITS = { IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS };
    private static final IBinaryComparatorFactory[] CMP_FACTORIES = { IntegerBinaryComparatorFactory.INSTANCE };
    private static final int[] BLOOM_FILTER_KEY_FIELDS = { 0 };

    /** Keys in the sampled (oldest) component. Sized for leaf pages, see the class javadoc. */
    private static final int NUM_KEYS = 2000;
    /**
     * Each round must fit one mutable component (50 pages of 256 B in the shared harness). The rounds are merged
     * afterwards, so this bounds memory, not the component's size.
     */
    private static final int KEYS_PER_ROUND = 500;
    /** Percentage of the key space shadowed by newer components; mirrors the column fixture. */
    private static final int SHADOW_PCT = 55;
    private static final int NUM_SHADOW_COMPONENTS = 3;
    private static final int NUM_BINS = 20;
    private static final int NUM_SEEDS = 60;
    private static final int TARGET = 120;
    /** Production default of {@code STORAGE_SAMPLE_LEAF_DRAW_BATCH_SIZE} — far above the target, as in production. */
    private static final int LEAF_DRAW_BATCH_SIZE = 32768;
    /** Production default of {@code STORAGE_MAX_SAMPLE_LEAF_ATTEMPTS}; the cursor scales it up itself. */
    private static final int MAX_LEAF_ATTEMPTS = 500;
    /** 19 df, alpha = 0.001, unscaled. Deliberately loose: guards gross bias, not small-sample noise. */
    private static final double CHI_SQUARE_CRITICAL = 43.82;
    /**
     * Too few leaf pages and clustering is indistinguishable from noise, one page spanning several bins.
     * Measured at 118; set below that so page-packing changes do not fail spuriously, but far above the ~{@link
     * #NUM_BINS} pages at which the test goes blind.
     */
    private static final int EXPECTED_MIN_LEAF_PAGES = 80;

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private LSMBTree lsmBtree;

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
        lsmBtree = LSMBTreeExamplesTest.createTreeIndex(harness, TYPE_TRAITS, CMP_FACTORIES, BLOOM_FILTER_KEY_FIELDS,
                null, null, null, null);
        lsmBtree.create();
        lsmBtree.activate();
        buildComponents();
    }

    @After
    public void tearDown() throws HyracksDataException {
        try {
            lsmBtree.deactivate();
            lsmBtree.destroy();
        } finally {
            harness.tearDown();
        }
    }

    /**
     * Builds the sampled component (all {@link #NUM_KEYS} keys, flushed in rounds then merged into one disk
     * component) followed by {@link #NUM_SHADOW_COMPONENTS} newer components that delete-and-reinsert the first
     * {@code NUM_KEYS * SHADOW_PCT / 100} keys. Those keys are therefore present in a newer component and must not
     * be emitted when the oldest component is sampled — exactly the shape the column fixture uses.
     */
    private void buildComponents() throws HyracksDataException {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        for (int from = 0; from < NUM_KEYS; from += KEYS_PER_ROUND) {
            int to = Math.min(NUM_KEYS, from + KEYS_PER_ROUND);
            for (int key = from; key < to; key++) {
                insert(accessor, key);
            }
            accessor.scheduleFlush();
        }
        // Merge the rounds into ONE disk component: that component is what gets sampled, and its leaf-page count is
        // what gives the test the resolution to see clustering. The merge also bulk-loads it, which is what records
        // MAX_LEAF_TUPLE_COUNT_KEY, so the Olken fill-rejection denominator below is a real value.
        List<ILSMDiskComponent> rounds = new ArrayList<>(lsmBtree.getDiskComponents());
        Assert.assertTrue("expected several flushed rounds to merge", rounds.size() > 1);
        accessor.scheduleMerge(rounds);
        Assert.assertEquals("the sampled component must be a single merged component", 1,
                lsmBtree.getDiskComponents().size());

        int shadowBoundary = getShadowBoundary();
        int perComponent = (shadowBoundary + NUM_SHADOW_COMPONENTS - 1) / NUM_SHADOW_COMPONENTS;
        for (int component = 0; component < NUM_SHADOW_COMPONENTS; component++) {
            int from = component * perComponent;
            int to = Math.min(shadowBoundary, from + perComponent);
            for (int key = from; key < to; key++) {
                delete(accessor, key);
                insert(accessor, key);
            }
            accessor.scheduleFlush();
        }
        Assert.assertEquals("expected one sampled component plus the shadow components", 1 + NUM_SHADOW_COMPONENTS,
                lsmBtree.getDiskComponents().size());
    }

    /** Keys in {@code [0, shadowBoundary)} are shadowed by a newer component; keys at or above it stay live. */
    private static int getShadowBoundary() {
        return NUM_KEYS * SHADOW_PCT / 100;
    }

    @Test
    public void sampleIsUniformOverLiveKeys() throws Exception {
        int shadowBoundary = getShadowBoundary();
        int liveCount = NUM_KEYS - shadowBoundary;
        int leafPages = getSampledComponentLeafPageCount();
        Assert.assertTrue("a component with " + leafPages + " leaf pages is too coarse to detect spatial clustering",
                leafPages >= EXPECTED_MIN_LEAF_PAGES);

        long[] binCounts = new long[NUM_BINS];
        long total = 0;
        int minEmitted = Integer.MAX_VALUE;
        int maxEmitted = 0;
        for (int seed = 0; seed < NUM_SEEDS; seed++) {
            int[] keys = runSample(TARGET, seed);
            minEmitted = Math.min(minEmitted, keys.length);
            maxEmitted = Math.max(maxEmitted, keys.length);
            for (int key : keys) {
                Assert.assertTrue("shadowed key leaked into the sample: " + key, key >= shadowBoundary);
                Assert.assertTrue("out-of-range key in the sample: " + key, key < NUM_KEYS);
                binCounts[binOf(key, shadowBoundary, liveCount)]++;
                total++;
            }
        }
        Assert.assertTrue("no samples collected; cannot assess uniformity", total > 0);

        double chiSquare = 0.0;
        for (int bin = 0; bin < NUM_BINS; bin++) {
            // Expectation proportional to the live keys the bin actually spans (see the class javadoc): the live
            // population is not divisible by NUM_BINS, so equal expectations would themselves inflate the statistic.
            double expected = (double) total * liveKeysInBin(bin, liveCount) / liveCount;
            double delta = binCounts[bin] - expected;
            chiSquare += delta * delta / expected;
        }

        System.out.println("=".repeat(96));
        System.out.printf("row sample uniformity: keys=%d liveKeys=%d leafPages=%d target=%d seeds=%d%n", NUM_KEYS,
                liveCount, leafPages, TARGET, NUM_SEEDS);
        System.out.printf("  emitted per run: min=%d max=%d (requested %d)%n", minEmitted, maxEmitted, TARGET);
        System.out.printf("  pooled=%d chiSquare=%.2f (critical %.2f, %d df)%n", total, chiSquare, CHI_SQUARE_CRITICAL,
                NUM_BINS - 1);
        for (int bin = 0; bin < NUM_BINS; bin++) {
            int from = shadowBoundary + (int) ((long) bin * liveCount / NUM_BINS);
            int to = shadowBoundary + (int) ((long) (bin + 1) * liveCount / NUM_BINS);
            double expected = (double) total * liveKeysInBin(bin, liveCount) / liveCount;
            int barLength = (int) Math.round(40 * binCounts[bin] / expected);
            System.out.printf("  bin %2d [%5d,%5d) %7d %s%n", bin, from, to, binCounts[bin],
                    "#".repeat(Math.min(barLength, 200)));
        }
        System.out.println("=".repeat(96));

        Assert.assertEquals("every run should reach the requested sample size", TARGET, minEmitted);
        Assert.assertTrue("sample distribution is not uniform over live keys: chiSquare=" + chiSquare
                + " exceeds critical " + CHI_SQUARE_CRITICAL, chiSquare < CHI_SQUARE_CRITICAL);
    }

    /** Bin index of a live key, using the same equal-width partition of the live range as {@link #liveKeysInBin}. */
    private static int binOf(int key, int shadowBoundary, int liveCount) {
        int bin = (int) ((long) (key - shadowBoundary) * NUM_BINS / liveCount);
        return Math.min(bin, NUM_BINS - 1);
    }

    /** Number of live keys the given bin spans. Bins differ by one key when {@code liveCount % NUM_BINS != 0}. */
    private static int liveKeysInBin(int bin, int liveCount) {
        return (int) ((long) (bin + 1) * liveCount / NUM_BINS) - (int) ((long) bin * liveCount / NUM_BINS);
    }

    /**
     * Drives the real row sample cursor over the <b>oldest</b> disk component with the liveness search pointed at all
     * newer components — the wiring {@code LSMIndexSampleCursor} uses for the last component.
     *
     * @return the primary keys emitted, in emission order
     */
    private int[] runSample(int targetCardinality, long seed) throws HyracksDataException {
        LSMBTreeOpContext opCtx = lsmBtree.createOpContext(NoOpIndexAccessParameters.INSTANCE);
        opCtx.setOperation(IndexOperation.SEARCH);
        lsmBtree.getOperationalComponents(opCtx);
        List<ILSMComponent> components = opCtx.getComponentHolder(); // newest first
        int oldestIndex = components.size() - 1;
        ILSMComponent oldest = components.get(oldestIndex);
        List<ILSMComponent> newer = new ArrayList<>(components.subList(0, oldestIndex));

        RangePredicate searchPred = new RangePredicate(null, null, true, true, null, null);
        opCtx.getSearchInitialState().reset(searchPred, newer);
        LSMBTreeBatchPointSearchCursor searchCursor = new LSMBTreeBatchPointSearchCursor(opCtx);
        searchCursor.open(opCtx.getSearchInitialState(), searchPred);

        DiskBTree oldestBtree = (DiskBTree) oldest.getIndex();
        DiskBTree.DiskBTreeAccessor sampleAccessor =
                (DiskBTree.DiskBTreeAccessor) oldestBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ITreeIndexCursor sampleCursor =
                sampleAccessor.createSampleCursor(targetCardinality, seed, searchCursor, MAX_LEAF_ATTEMPTS,
                        LEAF_DRAW_BATCH_SIZE, maxLeafTupleCountOf(oldest), AntimatterAwareTupleAcceptor.INSTANCE);

        int[] keys = new int[Math.max(1, targetCardinality)];
        int emitted = 0;
        try {
            sampleAccessor.diskSampleScan(sampleCursor);
            while (sampleCursor.hasNext()) {
                sampleCursor.next();
                keys[emitted++] = IntegerPointable.getInteger(sampleCursor.getTuple().getFieldData(0),
                        sampleCursor.getTuple().getFieldStart(0));
            }
        } finally {
            sampleCursor.close();
            sampleCursor.destroy();
            searchCursor.close();
            searchCursor.destroy();
        }
        int[] result = new int[emitted];
        System.arraycopy(keys, 0, result, 0, emitted);
        return result;
    }

    /**
     * Leaf-page count of the oldest (sampled) component — the population the cursor draws uniformly over, and hence
     * the resolution at which this test can see clustering. Read via the same enumeration the cursor itself uses.
     */
    private int getSampledComponentLeafPageCount() throws HyracksDataException {
        LSMBTreeOpContext opCtx = lsmBtree.createOpContext(NoOpIndexAccessParameters.INSTANCE);
        opCtx.setOperation(IndexOperation.SEARCH);
        lsmBtree.getOperationalComponents(opCtx);
        List<ILSMComponent> components = opCtx.getComponentHolder();
        DiskBTree oldestBtree = (DiskBTree) components.get(components.size() - 1).getIndex();
        DiskBTree.DiskBTreeAccessor accessor =
                (DiskBTree.DiskBTreeAccessor) oldestBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        try {
            BTreeOpContext btreeOpCtx = accessor.getOpContext();
            btreeOpCtx.reset();
            return oldestBtree.enumerateLeafPageIdsCompact(oldestBtree.getRootPageId(), btreeOpCtx,
                    DefaultBufferCacheReadContextProvider.DEFAULT).size();
        } finally {
            accessor.destroy();
        }
    }

    /**
     * The component's recorded max leaf tuple count — the denominator of the cursor's Olken &amp; Rotem
     * fill-rejection probability, read exactly as {@code LSMIndexSampleCursor} reads it. Passing the real value
     * matters here: with fill-rejection disabled the partially-filled last leaf page would have its tuples
     * over-weighted, which this test would (correctly) report as non-uniformity of the fixture rather than of the
     * cursor.
     */
    private static int maxLeafTupleCountOf(ILSMComponent component) throws HyracksDataException {
        DiskComponentMetadata metadata = (DiskComponentMetadata) component.getMetadata();
        ArrayBackedValueStorage ref = new ArrayBackedValueStorage();
        if (metadata.get(DiskComponentMetadata.MAX_LEAF_TUPLE_COUNT_KEY, ref) && ref.getLength() >= Integer.BYTES) {
            return IntegerPointable.getInteger(ref.getByteArray(), ref.getStartOffset());
        }
        return 0;
    }

    private void insert(ILSMIndexAccessor accessor, int key) throws HyracksDataException {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(FIELD_COUNT);
        ArrayTupleReference tuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(tb, tuple, key, key);
        accessor.insert(tuple);
    }

    private void delete(ILSMIndexAccessor accessor, int key) throws HyracksDataException {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(KEY_FIELD_COUNT);
        ArrayTupleReference tuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(tb, tuple, key);
        accessor.delete(tuple);
    }
}
