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
package org.apache.hyracks.storage.am.lsm.common.theta;

import java.util.List;
import java.util.Set;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * Estimates the cardinality (number of distinct values) across LSM disk components using the
 * Theta Sketch algorithm, which is based on the K-Minimum Values (KMV) probabilistic data structure.
 */
public class ThetaEstimator {

    public static class ComponentStats {
        public final long[] insertSamples;
        public final long[] deleteSamples;
        public final int K;

        public ComponentStats(long[] insertSamples, long[] deleteSamples, int k) {
            this.insertSamples = insertSamples;
            this.deleteSamples = deleteSamples;
            K = k;
        }

        public double getTheta() {
            if (insertSamples.length < K) {
                return 1.0;
            }
            return (double) insertSamples[insertSamples.length - 1] / (double) Long.MAX_VALUE;
        }

        public long estimateLiveCardinality() {
            double theta = getTheta();
            if (theta == 0.0) {
                return 0;
            }

            long threshold = (long) (theta * Long.MAX_VALUE);
            LongOpenHashSet deleteSet = new LongOpenHashSet(deleteSamples.length);
            for (long hash : deleteSamples) {
                if (hash <= threshold) {
                    deleteSet.add(hash);
                }
            }

            int liveCount = 0;
            for (long hash : insertSamples) {
                if (hash <= threshold && !deleteSet.contains(hash)) {
                    liveCount++;
                }
            }

            return (long) Math.min(Long.MAX_VALUE, liveCount / theta);
        }
    }

    public static long estimateCardinality(List<ComponentStats> components) {
        return estimatePerComponentCardinality(components).totalCardinality;
    }

    public static class CardinalityEstimate {
        public final long totalCardinality;
        public final long[] perComponentCardinality;

        public CardinalityEstimate(long totalCardinality, long[] perComponentCardinality) {
            this.totalCardinality = totalCardinality;
            this.perComponentCardinality = perComponentCardinality;
        }
    }

    public static CardinalityEstimate estimatePerComponentCardinality(List<ComponentStats> components) {
        int n = components.size();
        long[] perComponent = new long[n];

        if (n == 0) {
            return new CardinalityEstimate(0, perComponent);
        }

        double globalTheta = 1.0;
        int maxPossibleKeys = 0;
        for (ComponentStats stats : components) {
            globalTheta = Math.min(globalTheta, stats.getTheta());
            maxPossibleKeys += stats.K * 2; // Each component contributes at most K inserts and K deletes
        }

        long threshold = (long) (globalTheta * Long.MAX_VALUE);

        if (globalTheta == 0.0) {
            return new CardinalityEstimate(0, perComponent);
        }

        // Pre-allocate to prevent costly rehashing
        Set<Long> seenKeys = new LongOpenHashSet(maxPossibleKeys);
        int totalAliveInSample = 0;

        for (int i = 0; i < n; i++) {
            ComponentStats stats = components.get(i);
            int componentAliveCount = 0;

            for (long hash : stats.insertSamples) {
                if (hash > threshold) {
                    continue;
                }
                if (!seenKeys.contains(hash)) {
                    componentAliveCount++;
                }
                seenKeys.add(hash);
            }

            if (stats.deleteSamples != null) {
                for (long hash : stats.deleteSamples) {
                    if (hash <= threshold) {
                        seenKeys.add(hash);
                    }
                }
            }

            perComponent[i] = (long) Math.min(Long.MAX_VALUE, componentAliveCount / globalTheta);
            totalAliveInSample += componentAliveCount;
        }

        long totalCardinality = (long) Math.min(Long.MAX_VALUE, totalAliveInSample / globalTheta);
        return new CardinalityEstimate(totalCardinality, perComponent);
    }
}