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
import org.junit.Assert;
import org.junit.Test;

/**
 * Prints the sampling cost breakdown across mutation levels, to answer whether the newer-component liveness
 * search dominates.
 * <p>
 * <b>A measurement, not a gate.</b> {@code livenessSharePct} is printed, never asserted: batching the liveness
 * search would <em>lower</em> it, so a "share must stay high" assertion would fail on the improvement it exists
 * to motivate. The assertions that do run — disjoint phase timings, the leaf-page floor, the page-group
 * amortization — are invariants unrelated to that share.
 * <p>
 * {@code NUM_KEYS} is 165 000, not the 20 000 the plan asked for: column materialization pays per page, so a
 * ~7-page index under-exercises phase 2 and reproduces the very spatial distortion the bias fix removed.
 */
public class ColumnSampleCostBaselineTest {

    private static final int NUM_KEYS = 165000;
    private static final int NUM_SHADOW_COMPONENTS = 3;
    private static final int TARGET = 1063;
    private static final long SEED = 12345L;
    private static final int[] SHADOW_LEVELS = { 0, 15, 55 };
    /**
     * Keeps phase-2 column materialization genuinely exercised. Deliberately independent of
     * {@code ColumnSampleUniformityTest#MIN_DISTINCT_PAGES}, which guards spatial clustering instead.
     */
    private static final int MIN_LEAF_PAGES_FOR_PHASE2_COVERAGE = 50;
    /** Historical reference point recorded when this baseline was first measured; see the class javadoc. */
    private static final double HISTORICAL_LIVENESS_SHARE_PCT = 50.0;
    /** Guards that same-page draws share one PK pass rather than each paying its own rewind. */
    private static final int MIN_DRAWS_PER_PAGE_GROUP = 3;

    @Test
    public void printCostBreakdownAcrossMutationLevels() throws Exception {
        System.out.println("=".repeat(120));
        System.out.println("Column sample cost baseline (numKeys=" + NUM_KEYS + ", numShadowComponents="
                + NUM_SHADOW_COMPONENTS + ", target=" + TARGET + ", seed=" + SEED + ")");
        double worstLivenessShare = 0.0;
        for (int shadowPct : SHADOW_LEVELS) {
            ColumnSampleBenchHarness harness = new ColumnSampleBenchHarness(NUM_KEYS, shadowPct, NUM_SHADOW_COMPONENTS);
            harness.setUp();
            try {
                int leafPages = harness.getSampledComponentLeafPageCount();
                // Asserted, not just printed: a change to the harness's sizing constants that silently shrinks
                // the index would otherwise drift this baseline back into the ~7-page distortion unnoticed.
                Assert.assertTrue(
                        "expected at least " + MIN_LEAF_PAGES_FOR_PHASE2_COVERAGE + " sampled mega-leaf pages to "
                                + "keep Phase 2 column materialization genuinely exercised, got " + leafPages
                                + " at shadow=" + shadowPct + "%; check the harness's sizing constants",
                        leafPages >= MIN_LEAF_PAGES_FOR_PHASE2_COVERAGE);
                // Warm up once so class loading and page-cache effects do not skew the measured run.
                harness.runSample(TARGET, SEED, new SampleCursorStats());
                SampleCursorStats stats = new SampleCursorStats();
                ColumnSampleBenchHarness.SampleRun run = harness.runSample(TARGET, SEED, stats);
                System.out.printf("shadow=%2d%%  leafPages=%3d  wall=%7.1fms  emitted=%4d  %s%n", shadowPct, leafPages,
                        run.wallNanos / 1e6, run.emitted, stats.format());
                // Disjoint slices, or livenessSharePct and everything derived from it is silently distorted.
                // Against a live cursor, which a pure-arithmetic unit test cannot do.
                long phaseSum = stats.livenessNanos + stats.pkSeekNanos + stats.phase2Nanos;
                Assert.assertTrue("phase timings exceed the measured wall time (double-counting): sum=" + phaseSum
                        + " wall=" + run.wallNanos, phaseSum <= run.wallNanos);
                // Structural, not a timing gate: revert to one PK pass per draw and pageGroups equals attempts,
                // failing this. The floor sits far below what is measured (~7x at shadow=55%) so it catches the
                // regression without pinning a performance number.
                Assert.assertTrue(
                        "expected same-page draws to share one PK pass, but pageGroups=" + stats.pageGroups
                                + " against attempts=" + stats.attempts + " at shadow=" + shadowPct + "%",
                        stats.pageGroups * MIN_DRAWS_PER_PAGE_GROUP <= stats.attempts);
                worstLivenessShare = Math.max(worstLivenessShare, stats.livenessSharePct());
            } finally {
                harness.tearDown();
            }
        }
        // Printed, never asserted -- see the class javadoc.
        System.out.printf(
                "liveness share (max across shadow levels): %.1f%% (cleared historical %.0f%% threshold: " + "%s)%n",
                worstLivenessShare, HISTORICAL_LIVENESS_SHARE_PCT, worstLivenessShare > HISTORICAL_LIVENESS_SHARE_PCT);
        System.out.println("=".repeat(120));
    }
}
