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
package org.apache.hyracks.storage.am.vector.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;

/**
 * SPTAG/SPANN-style RNG (relative neighborhood graph) acceptance filter for
 * vector-index cross-pollination at bulk-load time.
 * <p>
 * Given a candidate list of leaf centroids already sorted closest-first w.r.t. the record
 * being inserted (typically the eps-filtered output of
 * {@code VTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort}), this helper walks
 * the list and decides which candidates to accept as replica targets. A candidate
 * {@code c_i} is rejected iff some already-accepted replica {@code r} satisfies
 * {@code rngFactor * dist(c_i, r) < dist(x, c_i)} (the canonical SPTAG rule at
 * {@code rngFactor = 1}). Acceptance stops at {@code cap}.
 * <p>
 * Distances between centroids are computed in raw (full-precision) space using the
 * supplied {@link IVTreeDistanceFunction}, even for quantized indexes — diversity
 * should reflect geometric truth, not quantization artifacts.
 * <p>
 * Edge handling: the filter is defensive — a missing centroid vector or a NaN /
 * throwing pairwise distance causes the affected pair to be skipped (no veto from
 * data we can't evaluate), never a silent candidate drop.
 */
public final class RngAcceptanceFilter {

    /** Per-call accept/reject counters (for stats logging). Fields are reset by the caller. */
    public static final class Stats {
        public int accepted;
        public int rejected;
    }

    private RngAcceptanceFilter() {
    }

    /**
     * Greedy SPTAG-style acceptance pass over a closest-first candidate list.
     *
     * @param candidates ε-filtered, distance-sorted (ascending) candidate centroids;
     *                   never null, may be empty.
     * @param dist       raw vector-to-vector distance function with the index's metric.
     * @param rngFactor  RNG multiplier. Strict SPTAG = 1.0; >1 looser; non-finite
     *                   (e.g. {@link Double#POSITIVE_INFINITY}) disables the rule
     *                   (degrades to a pure top-{@code cap} slice).
     * @param cap        upper bound on accepted count (>=1). Caller-side {@code M}.
     * @param out        optional stats sink; ignored if null.
     * @return list of accepted candidates, length &lt;= {@code cap} (possibly empty).
     */
    public static List<ClusterSearchResult> accept(List<ClusterSearchResult> candidates, IVTreeDistanceFunction dist,
            double rngFactor, int cap, Stats out) throws HyracksDataException {
        if (candidates == null || candidates.isEmpty() || cap <= 0) {
            return Collections.emptyList();
        }

        final int n = candidates.size();
        final boolean rngEnabled = Double.isFinite(rngFactor) && dist != null;
        final List<ClusterSearchResult> accepted = new ArrayList<>(Math.min(cap, n));

        int acceptedCount = 0;
        int rejectedCount = 0;

        for (int i = 0; i < n && accepted.size() < cap; i++) {
            final ClusterSearchResult cand = candidates.get(i);
            if (cand == null) {
                continue;
            }

            boolean reject = false;
            // Diversity test only runs once we have at least one accepted replica and RNG is enabled.
            // If the candidate's centroid is missing we cannot evaluate the test — accept rather than
            // silently drop a legitimate candidate.
            if (rngEnabled && !accepted.isEmpty() && cand.centroid != null) {
                final double dxc = cand.distance; // dist(x, c_i) — already computed by navigation
                if (!Double.isNaN(dxc)) {
                    for (int j = 0; j < accepted.size(); j++) {
                        final ClusterSearchResult r = accepted.get(j);
                        if (r.centroid == null) {
                            continue;
                        }
                        double dcr;
                        try {
                            dcr = dist.apply(cand.centroid, r.centroid);
                        } catch (HyracksDataException dex) {
                            continue; // bad pair — don't let it veto
                        }
                        if (Double.isNaN(dcr)) {
                            continue;
                        }
                        if (rngFactor * dcr < dxc) {
                            reject = true;
                            break;
                        }
                    }
                }
            }

            if (reject) {
                rejectedCount++;
            } else {
                accepted.add(cand);
                acceptedCount++;
            }
        }

        if (out != null) {
            out.accepted = acceptedCount;
            out.rejected = rejectedCount;
        }
        return accepted;
    }
}
