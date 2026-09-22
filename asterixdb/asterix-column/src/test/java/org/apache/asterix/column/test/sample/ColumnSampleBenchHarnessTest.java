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

import java.util.Arrays;
import java.util.Collection;

import org.apache.asterix.column.test.sample.ColumnSampleBenchHarness.Projection;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.SampleCursorStats;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Smoke test for {@link ColumnSampleBenchHarness}, run once per projection mode: {@code ALL_COLUMNS} is the
 * production-faithful baseline (phase 2 materializes every column), {@code PK_ONLY} isolates phase 1.
 * <p>
 * The buffer-cache residency/budget checks used to live here too, parameterized over {@code Projection} like
 * everything else in this class. They moved out to {@link ColumnSampleBenchHarnessResidencyTest} because their
 * assertions are about the on-disk buffer cache (component file size vs. disk-page budget), which the
 * {@code Projection} does not affect - column materialization during phase 2 goes through the separate column
 * buffer pool, not the disk buffer cache these checks observe. Parameterizing them bought nothing but building
 * a 475 000-insert index twice for identical checks.
 */
@RunWith(Parameterized.class)
public class ColumnSampleBenchHarnessTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> projections() {
        return Arrays.asList(new Object[] { Projection.ALL_COLUMNS }, new Object[] { Projection.PK_ONLY });
    }

    private final Projection projection;
    private final ColumnSampleBenchHarness harness;

    public ColumnSampleBenchHarnessTest(Projection projection) {
        this.projection = projection;
        this.harness = new ColumnSampleBenchHarness(20000, 55, 3, projection);
    }

    @Before
    public void setUp() throws Exception {
        harness.setUp();
    }

    @After
    public void tearDown() throws Exception {
        harness.tearDown();
    }

    @Test
    public void harnessBuildsShadowedComponentsAndSamplesLiveKeysOnly() throws Exception {
        // The shadow structure only exists if every flush produced its own disk component (NoMergePolicy).
        Assert.assertEquals("expected one base component plus one per shadow round",
                1 + harness.getNumShadowComponents(), harness.getIndex().getDiskComponents().size());
        // A single-mega-leaf component would make the leaf-draw phase degenerate.
        int leafPages = harness.getSampledComponentLeafPageCount();
        Assert.assertTrue("expected the sampled component to span several mega-leaf pages, got " + leafPages,
                leafPages > 1);
        // Fill-proportional page rejection is only exercised when the real per-component value is in play.
        int maxLeafTupleCount = harness.getEffectiveMaxLeafTupleCount();
        Assert.assertTrue("expected the component's recorded max leaf tuple count to be read, got " + maxLeafTupleCount,
                maxLeafTupleCount > 0);

        SampleCursorStats stats = new SampleCursorStats();
        ColumnSampleBenchHarness.SampleRun run = harness.runSample(1063, 12345L, stats);

        Assert.assertTrue("expected the sampler to emit tuples", run.emitted > 0);
        Assert.assertTrue("expected measurable time", run.wallNanos > 0);
        // Shadowed keys live in [0, shadowBoundary) and must never be emitted.
        for (int key : run.sampledKeys) {
            Assert.assertTrue("shadowed key " + key + " leaked into the sample", key >= harness.getShadowBoundary());
        }
        // The instrumentation is inert unless a stats object is attached; prove it populated.
        Assert.assertTrue("expected phase-1 attempts to be counted", stats.attempts > 0);
        Assert.assertTrue("expected liveness time to be counted", stats.livenessNanos > 0);
        Assert.assertTrue("expected phase-2 page pins to be counted", stats.phase2PagePins > 0);
        System.out.println(
                "harness smoke [" + projection + "]: leafPages=" + leafPages + " maxLeafTupleCount=" + maxLeafTupleCount
                        + " emitted=" + run.emitted + " wallMs=" + run.wallNanos / 1_000_000 + " " + stats.format());
    }

    /**
     * Tasks 3-7 drive several runs off one harness instance, so a run must be repeatable and must not disturb the
     * index state for the next one.
     */
    @Test
    public void repeatedRunsWithTheSameSeedAgree() throws Exception {
        ColumnSampleBenchHarness.SampleRun first = harness.runSample(500, 99L, new SampleCursorStats());
        ColumnSampleBenchHarness.SampleRun second = harness.runSample(500, 99L, new SampleCursorStats());
        Assert.assertEquals(first.emitted, second.emitted);
        Assert.assertArrayEquals(first.sampledKeys, second.sampledKeys);
    }
}
