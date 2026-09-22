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
package org.apache.asterix.metadata.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.junit.Test;

/**
 * The sizing rules, checked against the worked examples in the design they implement. Pure arithmetic, so it
 * is verified here rather than through a cluster; the runtime test only has to show the values are persisted.
 */
public class VTreeParamsInferenceTest {

    /** A ceiling nothing in these cases can reach, so each one exercises the rule it is named for. */
    private static final int NO_CEILING = Integer.MAX_VALUE;

    /** D = 900: 9 clusters. The design's small-data example. */
    @Test
    public void smallDataUsesHundredRowClusters() {
        assertTrue(VTreeParamsInference.isSmallData(900));
        assertEquals(9, VTreeParamsInference.defaultNumClusters(900));
    }

    /**
     * Small data trains on the whole partition. 50*9 = 450 is the floor the design states, and reading every
     * row clears it: the scan has already produced them and nothing truncates it.
     */
    @Test
    public void smallDataTrainsOnEverythingAndFullScans() {
        long size = VTreeParamsInference.trainListSize(9, 900);
        assertEquals(900, size);
        assertTrue(size >= 50L * 9);
        assertTrue(VTreeParamsInference.useFullScan(size, 900));
        assertEquals(1.0, VTreeParamsInference.effectiveFraction(size, 900), 0.0);
    }

    /** The three non-small worked examples: 100/50%, 200/25%, 300/16.7%. */
    @Test
    public void reproducesTheWorkedExamples() {
        assertSizing(10_000, 100, 5_000, 0.5);
        assertSizing(40_000, 200, 10_000, 0.25);
        assertSizing(90_000, 300, 15_000, 1.0 / 6);
    }

    /** The two branches agree at the boundary, so the rule has no step in it. */
    @Test
    public void branchesMeetAtTheThreshold() {
        assertFalse(VTreeParamsInference.isSmallData(10_000));
        assertEquals(100, VTreeParamsInference.defaultNumClusters(10_000));
        assertEquals(100, (int) Math.ceil(10_000 / 100.0));
    }

    /** The training list grows as sqrt(D), so a bigger collection samples a smaller share, not a larger one. */
    @Test
    public void trainingListGrowsSublinearly() {
        assertSizing(1_000_000, 1_000, 50_000, 0.05);
        double small = VTreeParamsInference.effectiveFraction(VTreeParamsInference.trainListSize(100, 10_000), 10_000);
        double large =
                VTreeParamsInference.effectiveFraction(VTreeParamsInference.trainListSize(1_000, 1_000_000), 1_000_000);
        assertTrue(large < small);
    }

    /** An explicit fraction is honoured as written; nothing caps it back. */
    @Test
    public void explicitFractionIsHonoured() {
        assertEquals(900, VTreeParamsInference.trainListSizeForFraction(1.0, 900));
        assertEquals(90, VTreeParamsInference.trainListSizeForFraction(0.1, 900));
        assertEquals(500_000, VTreeParamsInference.trainListSizeForFraction(0.5, 1_000_000));
    }

    /** Fewer rows than partitions: D floors at 1 rather than 0, which k-means cannot run on. */
    @Test
    public void fewerRowsThanPartitions() {
        assertEquals(1, VTreeParamsInference.perPartitionCardinality(3, 8));
        assertEquals(1, VTreeParamsInference.defaultNumClusters(1));
        assertTrue(VTreeParamsInference.useFullScan(VTreeParamsInference.trainListSize(1, 1), 1));
    }

    /** The fraction is scale-invariant: derived per partition, it is the same number globally. */
    @Test
    public void fractionIsScaleInvariant() {
        int partitions = 8;
        long global = 8_000_000L;
        long d = VTreeParamsInference.perPartitionCardinality(global, partitions);
        int k = VTreeParamsInference.defaultNumClusters(d);
        long perPartition = VTreeParamsInference.trainListSize(k, d);
        assertEquals(VTreeParamsInference.effectiveFraction(perPartition, d),
                (double) (perPartition * partitions) / global, 1e-12);
    }

    private static void assertSizing(long d, int expectedClusters, long expectedSize, double expectedFraction) {
        int k = VTreeParamsInference.defaultNumClusters(d);
        assertEquals("clusters for D=" + d, expectedClusters, k);
        long size = VTreeParamsInference.trainListSize(k, d);
        assertEquals("training list for D=" + d, expectedSize, size);
        assertEquals("fraction for D=" + d, expectedFraction, VTreeParamsInference.effectiveFraction(size, d), 1e-9);
        assertFalse("D=" + d + " should sample, not full scan", VTreeParamsInference.useFullScan(size, d));
    }

    /**
     * The ceiling is enforced ahead of every data-relative rule, so a value above it fails the same way on
     * any collection.
     */
    @Test
    public void clustersAboveTheCeilingIsAnError() throws Exception {
        CollectingWarnings warnings = new CollectingWarnings();
        try {
            VTreeParamsInference.validateNumClusters(2000, 1000, 1_000_000, 1024, warnings, null);
            fail("expected a compilation error");
        } catch (CompilationException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("exceeds the configured maximum of 1024"));
        }
        assertTrue("an error must not also warn", warnings.messages.isEmpty());
    }

    /** Case 2: more clusters than rows cannot be built, so it is an error rather than a warning. */
    @Test
    public void moreClustersThanRowsIsAnError() throws Exception {
        CollectingWarnings warnings = new CollectingWarnings();
        try {
            VTreeParamsInference.validateNumClusters(5000, VTreeParamsInference.defaultNumClusters(30), 30, NO_CEILING,
                    warnings, null);
            fail("expected a compilation error");
        } catch (CompilationException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("records per storage partition available to train on"));
        }
        assertTrue("an error must not also warn", warnings.messages.isEmpty());
    }

    /** Case 2: a value far from the recommendation is legal, so it warns and the build proceeds. */
    @Test
    public void clustersFarFromRecommendedWarns() throws Exception {
        CollectingWarnings warnings = new CollectingWarnings();
        VTreeParamsInference.validateNumClusters(3, VTreeParamsInference.defaultNumClusters(30), 30, NO_CEILING,
                warnings, null);
        assertEquals(1, warnings.messages.size());
    }

    /** Case 2: inside the +/-10% window nothing is said. */
    @Test
    public void clustersNearRecommendedAreSilent() throws Exception {
        CollectingWarnings warnings = new CollectingWarnings();
        int recommended = VTreeParamsInference.defaultNumClusters(1_000_000);
        VTreeParamsInference.validateNumClusters(recommended, recommended, 1_000_000, NO_CEILING, warnings, null);
        VTreeParamsInference.validateNumClusters((int) (recommended * 1.05), recommended, 1_000_000, NO_CEILING,
                warnings, null);
        assertTrue(warnings.messages.toString(), warnings.messages.isEmpty());
    }

    /** Case 3: fewer training rows than clusters cannot place a centroid per cluster, so it is an error. */
    @Test
    public void trainListSmallerThanClusterCountIsAnError() throws Exception {
        CollectingWarnings warnings = new CollectingWarnings();
        try {
            VTreeParamsInference.validateTrainList(0.01, 1, 8, 900, warnings, null);
            fail("expected a compilation error");
        } catch (CompilationException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("fewer than the 8 clusters to be built"));
        }
    }

    /** Case 3: above the cluster count but below the rows-per-cluster floor is a warning, not an error. */
    @Test
    public void trainListBelowTheFloorWarns() throws Exception {
        CollectingWarnings warnings = new CollectingWarnings();
        VTreeParamsInference.validateTrainList(0.1, 90, 9, 900, warnings, null);
        assertEquals(1, warnings.messages.size());
    }

    /** Case 3: at or above the floor nothing is said. */
    @Test
    public void trainListAtTheFloorIsSilent() throws Exception {
        CollectingWarnings warnings = new CollectingWarnings();
        VTreeParamsInference.validateTrainList(0.5, 450, 9, 900, warnings, null);
        assertTrue(warnings.messages.toString(), warnings.messages.isEmpty());
    }

    private static final class CollectingWarnings implements IWarningCollector {
        private final List<Warning> messages = new ArrayList<>();

        @Override
        public void warn(Warning warning) {
            messages.add(warning);
        }

        @Override
        public boolean shouldWarn() {
            return true;
        }

        @Override
        public long getTotalWarningsCount() {
            return messages.size();
        }
    }
}
