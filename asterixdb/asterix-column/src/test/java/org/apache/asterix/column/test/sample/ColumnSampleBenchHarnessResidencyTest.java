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

import org.apache.asterix.column.test.sample.ColumnSampleBenchHarness.Projection;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.SampleCursorStats;
import org.junit.Assert;
import org.junit.Test;

/**
 * Enforces {@link ColumnSampleBenchHarness}'s no-real-disk-I/O contract: a full sample scan must evict no
 * buffer-cache page, so every measurement through the harness is CPU and buffer-cache cost, never device
 * latency.
 * <p>
 * Not parameterized over {@link Projection}, unlike {@link ColumnSampleBenchHarnessTest}: phase-2 column
 * materialization is served by the column buffer pool, not the disk buffer cache these checks read, so the
 * outcome cannot depend on the projection — and a second 475 000-insert build would buy an identical result.
 */
public class ColumnSampleBenchHarnessResidencyTest {

    /**
     * At ~50 leaf pages, where the disk budget is far tighter than at the 20 000-key default.
     * <p>
     * The signal is real rather than a restatement of the sizing formula:
     * {@code getAllocatedBufferCachePages()} is {@code ClockPageReplacementStrategy}'s lazily-grown allocation
     * count, and it only evicts once that count reaches the budget. Strictly below budget after a full scan
     * therefore proves nothing was re-read from disk — which re-deriving {@code numKeys * bytesPerTuple} could
     * not.
     */
    @Test
    public void wholeIndexStaysBufferCacheResidentAtTaskScale() throws Exception {
        ColumnSampleBenchHarness bigHarness = new ColumnSampleBenchHarness(165000, 55, 3, Projection.ALL_COLUMNS);
        bigHarness.setUp();
        try {
            int budget = bigHarness.getBufferCachePageBudget();
            // Measured footprint of the real component files, independent of the sizing estimate.
            int indexPages = bigHarness.getIndexPageCount();
            Assert.assertTrue("index footprint " + indexPages + " pages must fit the " + budget
                    + "-page buffer cache with room to spare", indexPages * 2 < budget);

            SampleCursorStats stats = new SampleCursorStats();
            bigHarness.runSample(1063, 12345L, stats);

            int allocated = bigHarness.getAllocatedBufferCachePages();
            Assert.assertTrue(
                    "buffer cache allocated " + allocated + " of " + budget + " pages: at budget the clock strategy "
                            + "starts evicting, so measurements would include real disk I/O",
                    allocated < budget);
            System.out.println("harness residency: leafPages=" + bigHarness.getSampledComponentLeafPageCount()
                    + " indexPages=" + indexPages + " allocated=" + allocated + " budget=" + budget + " headroom="
                    + String.format("%.2fx", (double) budget / allocated));
        } finally {
            bigHarness.tearDown();
        }
    }

    /**
     * Same contract where the <b>sizing formula</b> supplies the budget rather than the
     * {@code MIN_DISK_NUM_PAGES} floor, so the formula and its stored-tuple basis are both load-bearing.
     * <p>
     * {@code (250000, 90, 6)} clears the floor by ~36 % at 475 000 stored tuples — the cheapest configuration
     * that does (700 000 inserts at {@code shadowPercent=100}) while still leaving live keys in the sampled
     * component; at {@code shadowPercent=100} the oldest component has none and phase 2 goes unexercised.
     */
    @Test
    public void diskBudgetIsSizedFromStoredTuplesAndKeepsIndexResident() throws Exception {
        ColumnSampleBenchHarness bigHarness = new ColumnSampleBenchHarness(250000, 90, 6, Projection.ALL_COLUMNS);
        bigHarness.setUp();
        try {
            int budget = bigHarness.getBufferCachePageBudget();
            // The formula, not the floor, must be what sizes this cache - otherwise the rest proves nothing.
            Assert.assertTrue(
                    "budget " + budget + " must exceed the " + ColumnSampleBenchHarness.MIN_DISK_NUM_PAGES
                            + "-page floor, else the floor is what is being tested",
                    budget > ColumnSampleBenchHarness.MIN_DISK_NUM_PAGES);
            // Every shadowed key is stored twice, so the basis must be numKeys + shadowBoundary. Sizing from
            // numKeys alone lands on the floor at this configuration, so this fails if that regresses.
            Assert.assertTrue(
                    "budget " + budget + " must exceed the "
                            + ColumnSampleBenchHarness.diskPagesFor(bigHarness.getNumKeys())
                            + " pages a numKeys-only basis would give",
                    budget > ColumnSampleBenchHarness.diskPagesFor(bigHarness.getNumKeys()));

            int indexPages = bigHarness.getIndexPageCount();
            Assert.assertTrue("index footprint " + indexPages + " pages must fit the " + budget
                    + "-page buffer cache with room to spare", indexPages * 2 < budget);

            bigHarness.runSample(1063, 12345L, new SampleCursorStats());

            int allocated = bigHarness.getAllocatedBufferCachePages();
            Assert.assertTrue("buffer cache allocated " + allocated + " of " + budget
                    + " pages: at budget the clock strategy starts evicting, so measurements would include real "
                    + "disk I/O", allocated < budget);
            System.out.println("harness residency/formula: storedTuples="
                    + (bigHarness.getNumKeys() + bigHarness.getShadowBoundary()) + " leafPages="
                    + bigHarness.getSampledComponentLeafPageCount() + " indexPages=" + indexPages + " allocated="
                    + allocated + " budget=" + budget + " numKeysOnlyBudget="
                    + ColumnSampleBenchHarness.diskPagesFor(bigHarness.getNumKeys()) + " headroom="
                    + String.format("%.2fx", (double) budget / allocated));
        } finally {
            bigHarness.tearDown();
        }
    }
}
