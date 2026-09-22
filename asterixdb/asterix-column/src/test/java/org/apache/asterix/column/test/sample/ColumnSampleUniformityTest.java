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
package org.apache.asterix.column.test.sample;

import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.SampleCursorStats;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Uniformity guard for {@code ColumnBtreeSampleCursor}: the sample must be uniform over the component's
 * <em>live</em> tuples, because the CBO derives NDV and selectivity from it. A sample clustered in primary-key
 * order is silently wrong for every column correlated with the key — sequential ids, timestamps, the common case.
 * <p>
 * Chi-square goodness-of-fit on pooled counts over {@link #NUM_BINS} equal-width bins of the live key range, at
 * {@code NUM_BINS - 1} degrees of freedom; many seeds pooled so the statistic is stable rather than one noisy
 * draw.
 * <p>
 * <b>Sizing matters.</b> {@value #NUM_KEYS} keys give ~50 mega-leaf pages, so each bin covers roughly one page
 * and a sampler draining only the lowest page ids piles every key into the first bins. A handful of pages could
 * not distinguish clustering from noise at all, since one page would span several bins.
 * <p>
 * Bins are in <em>key</em> space, not page space, deliberately: a fix that spreads page visits without spreading
 * tuple selection must not be able to satisfy this.
 */
public class ColumnSampleUniformityTest {

    /** ~50 mega-leaf pages in the sampled component (measured ~3265 keys per leaf page). */
    private static final int NUM_KEYS = 165000;
    private static final int SHADOW_PCT = 55;
    private static final int NUM_SHADOW_COMPONENTS = 3;
    private static final int NUM_BINS = 20;
    private static final int NUM_SEEDS = 40;
    private static final int TARGET = 1063;
    /**
     * Distinct leaf pages per run. Only ~24 of the 51 hold a live key at 55 % shadowing and a correct sampler
     * reaches nearly all of them; 20 leaves slack for page-boundary effects while staying far above the 2-3 the
     * bug produced.
     */
    private static final int MIN_DISTINCT_PAGES = 20;
    /**
     * Total leaf pages in the component — a different concern from {@link #MIN_DISTINCT_PAGES}: too few and
     * clustering is indistinguishable from noise however many pages a run visits. Equal to {@link #NUM_BINS} by
     * coincidence; do not conflate them.
     */
    private static final int MIN_LEAF_PAGES_FOR_CLUSTERING_DETECTION = 20;
    // 19 df, alpha = 0.001. Deliberately loose: guards gross bias, not small-sample noise.
    private static final double CHI_SQUARE_CRITICAL = 43.82;

    private final ColumnSampleBenchHarness harness =
            new ColumnSampleBenchHarness(NUM_KEYS, SHADOW_PCT, NUM_SHADOW_COMPONENTS);

    @Before
    public void setUp() throws Exception {
        harness.setUp();
    }

    @After
    public void tearDown() throws Exception {
        harness.tearDown();
    }

    @Test
    public void sampleIsUniformOverLiveKeys() throws Exception {
        double chiSquare = runAndScore("uniform sample", CHI_SQUARE_CRITICAL);
        Assert.assertTrue("sample distribution is not uniform over live keys: chiSquare=" + chiSquare
                + " exceeds critical " + CHI_SQUARE_CRITICAL, chiSquare < CHI_SQUARE_CRITICAL);
    }

    /**
     * Pools {@link #NUM_SEEDS} runs, prints the bin histogram and the distinct-page counts, asserts that no
     * shadowed key leaked and that every run emitted exactly {@link #TARGET} tuples, and returns the chi-square
     * statistic against the uniform expectation.
     */
    private double runAndScore(String label, double criticalValue) throws Exception {
        int leafPages = harness.getSampledComponentLeafPageCount();
        Assert.assertTrue("a component with " + leafPages + " leaf pages is too coarse to detect spatial clustering",
                leafPages >= MIN_LEAF_PAGES_FOR_CLUSTERING_DETECTION);

        int shadowBoundary = harness.getShadowBoundary();
        int liveCount = NUM_KEYS - shadowBoundary;
        long[] binCounts = new long[NUM_BINS];
        long total = 0;
        long totalPagePins = 0;
        long minPagePins = Long.MAX_VALUE;
        long maxPagePins = 0;

        for (int seed = 0; seed < NUM_SEEDS; seed++) {
            SampleCursorStats stats = new SampleCursorStats();
            ColumnSampleBenchHarness.SampleRun run = harness.runSample(TARGET, seed, stats);
            // Exactness matters here, not just "close to the target": under-shooting would mean the batch loop
            // gave up early, and over-shooting would mean the cursor emitted more than it was asked for.
            Assert.assertEquals("seed " + seed + " did not emit exactly the requested sample size", TARGET,
                    run.emitted);
            for (int key : run.sampledKeys) {
                Assert.assertTrue("shadowed key leaked: " + key, key >= shadowBoundary);
                int bin = (int) ((long) (key - shadowBoundary) * NUM_BINS / liveCount);
                binCounts[Math.min(bin, NUM_BINS - 1)]++;
                total++;
            }
            totalPagePins += stats.phase2PagePins;
            minPagePins = Math.min(minPagePins, stats.phase2PagePins);
            maxPagePins = Math.max(maxPagePins, stats.phase2PagePins);
        }

        Assert.assertTrue("no samples collected; cannot assess uniformity", total > 0);
        double expected = (double) total / NUM_BINS;
        double chiSquare = 0.0;
        for (long observed : binCounts) {
            double delta = observed - expected;
            chiSquare += delta * delta / expected;
        }

        System.out.println("=".repeat(96));
        System.out.printf("column sample uniformity [%s]: keys=%d liveKeys=%d leafPages=%d target=%d seeds=%d%n", label,
                NUM_KEYS, liveCount, leafPages, TARGET, NUM_SEEDS);
        System.out.printf("  distinct pages materialized per run: min=%d max=%d avg=%.1f (of %d leaf pages)%n",
                minPagePins, maxPagePins, (double) totalPagePins / NUM_SEEDS, leafPages);
        System.out.printf("  emitted=%d expectedPerBin=%.1f chiSquare=%.2f (critical %.2f, %d df)%n", total, expected,
                chiSquare, criticalValue, NUM_BINS - 1);
        for (int bin = 0; bin < NUM_BINS; bin++) {
            int from = shadowBoundary + (int) ((long) bin * liveCount / NUM_BINS);
            int to = shadowBoundary + (int) ((long) (bin + 1) * liveCount / NUM_BINS);
            // expected > 0 here: total > 0 was asserted above.
            int barLength = (int) Math.round(40 * binCounts[bin] / expected);
            System.out.printf("  bin %2d [%6d,%6d) %7d %s%n", bin, from, to, binCounts[bin],
                    "#".repeat(Math.min(barLength, 200)));
        }
        System.out.println("=".repeat(96));

        // The most legible symptom of the sorted-batch bug was page coverage, not the statistic: the sampler
        // materialized 3 of 51 leaf pages. Only ~24 pages hold a live key at 55 %
        // shadowing, so at the target/page ratio here a correct sampler visits essentially all of them. Asserted
        // directly so the headline symptom is guarded independently of the chi-square machinery.
        Assert.assertTrue("only " + minPagePins + " of " + leafPages + " leaf pages were materialized in some run; "
                + "the sampler is concentrating on a few pages", minPagePins >= MIN_DISTINCT_PAGES);
        return chiSquare;
    }
}
