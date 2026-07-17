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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaEstimator;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaEstimator.ComponentStats;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaSampler;
import org.junit.Test;

/**
 * NDV (number-of-distinct-values) accuracy benchmark for the production Theta/KMV estimator
 * ({@link ThetaSampler} + {@link ThetaEstimator}), exercised over value distributions that model
 * famous-dataset column profiles. These are REAL measurements of the shipping estimator on
 * synthetic-but-controlled data with known ground truth — not the literal famous datasets (those
 * run at scale on the external rig). Results are written to a markdown file for the paper.
 *
 * Profiles:
 *   UNIFORM       — TPC-H-like key columns (e.g. l_orderkey): near-uniform, high cardinality.
 *   ZIPF_0_99     — YCSB-like access skew (θ=0.99).
 *   ZIPF_1_2      — MovieLens movieId-like power law (few blockbusters dominate).
 *   FEW_HOT       — NYC-Taxi PULocationID-like: tiny domain (~265) with hotspots.
 */
public class LSMSamplingNdvBenchmark {

    private static final int[] KEY_FIELDS = { 0 };
    private static final long SEED = 7L;
    private static final String OUT = "/tmp/sampling_paper_ndv_results.md";

    private enum Profile {
        UNIFORM, // TPC-H-like
        ZIPF_0_99, // YCSB-like
        ZIPF_1_2, // MovieLens-like
        FEW_HOT // NYC-Taxi location-like
    }

    // ---- minimal tuple shims (value-as-key, 1 field) ----
    private static class V implements ITupleReference {
        final byte[] b;

        V(int v) {
            b = new byte[] { (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v };
        }

        public int getFieldCount() {
            return 1;
        }

        public byte[] getFieldData(int i) {
            return b;
        }

        public int getFieldStart(int i) {
            return 0;
        }

        public int getFieldLength(int i) {
            return 4;
        }
    }

    private static class Anti extends V implements ILSMTreeTupleReference {
        Anti(int v) {
            super(v);
        }

        public boolean isAntimatter() {
            return true;
        }

        public int getTupleSize() {
            return 4;
        }

        public void setFieldCount(int c) {
        }

        public void setFieldCount(int s, int c) {
        }

        public void resetByTupleOffset(byte[] buf, int off) {
        }

        public void resetByTupleIndex(org.apache.hyracks.storage.am.common.api.ITreeIndexFrame f, int i) {
        }
    }

    /** Draw a value index in [0, domain) under the given profile. */
    private static int draw(Profile p, int domain, Random rnd, double[] zipfCdf) {
        switch (p) {
            case UNIFORM:
                return rnd.nextInt(domain);
            case FEW_HOT:
            case ZIPF_0_99:
            case ZIPF_1_2:
            default:
                double u = rnd.nextDouble();
                // binary search the precomputed Zipf CDF
                int lo = 0, hi = zipfCdf.length - 1;
                while (lo < hi) {
                    int mid = (lo + hi) >>> 1;
                    if (zipfCdf[mid] < u) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                return lo;
        }
    }

    private static double[] zipfCdf(int domain, double theta) {
        double[] c = new double[domain];
        double sum = 0;
        for (int i = 0; i < domain; i++) {
            sum += 1.0 / Math.pow(i + 1, theta);
            c[i] = sum;
        }
        for (int i = 0; i < domain; i++) {
            c[i] /= sum;
        }
        return c;
    }

    /**
     * Build {@code dataComponents} insert-only component sketches over N rows from the profile. When
     * deletePct>0, model a realistic LSM delete: a strictly-newer, antimatter-ONLY component carries
     * tombstones for deletePct% of distinct values (insert and its tombstone never share a component,
     * matching flush/merge semantics). Returns {estimatedNDV, trueLiveNDV}.
     */
    private long[] runOne(Profile p, int domain, int rows, int dataComponents, int deletePct, double theta)
            throws IOException {
        Random rnd = new Random(SEED + p.ordinal() * 131 + dataComponents * 17 + deletePct);
        double[] cdf = (p == Profile.UNIFORM) ? null : zipfCdf(domain, theta);

        // Insert phase: rows round-robin across the data components; track true distinct values.
        List<List<Integer>> perComp = new ArrayList<>();
        for (int i = 0; i < dataComponents; i++) {
            perComp.add(new ArrayList<>());
        }
        Set<Integer> distinct = new HashSet<>();
        for (int r = 0; r < rows; r++) {
            int v = draw(p, domain, rnd, cdf);
            distinct.add(v);
            perComp.get(r % dataComponents).add(v);
        }

        // Delete phase: tombstone deletePct% of distinct values in a strictly-newer, delete-only component.
        Set<Integer> deleted = new HashSet<>();
        if (deletePct > 0) {
            List<Integer> dv = new ArrayList<>(distinct);
            int toDelete = (int) (dv.size() * (deletePct / 100.0));
            for (int i = 0; i < toDelete; i++) {
                deleted.add(dv.get(i));
            }
        }
        Set<Integer> liveDistinct = new HashSet<>(distinct);
        liveDistinct.removeAll(deleted);

        // Components ordered newest-first for the estimator: [deleteComp?, dataComp0..dataCompN-1].
        List<ComponentStats> stats = new ArrayList<>();
        if (!deleted.isEmpty()) {
            ThetaSampler del = new ThetaSampler(KEY_FIELDS);
            for (int dvv : deleted) {
                del.addTuple(new Anti(dvv));
            }
            ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            abvs.append(del.serialize());
            stats.add(ThetaSampler.deserialize(abvs));
        }
        for (int ci = 0; ci < dataComponents; ci++) {
            ThetaSampler s = new ThetaSampler(KEY_FIELDS);
            for (int v : perComp.get(ci)) {
                s.addTuple(new V(v));
            }
            ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            abvs.append(s.serialize());
            stats.add(ThetaSampler.deserialize(abvs));
        }
        long est = ThetaEstimator.estimateCardinality(stats);
        return new long[] { est, liveDistinct.size() };
    }

    @Test
    public void ndvAccuracyAcrossProfilesAndFragmentation() throws IOException {
        int rows = 1_000_000;
        int[] componentCounts = { 1, 5, 10, 20 };
        int[] deletePcts = { 0, 25, 50, 85 };
        Map<Profile, double[]> cfg = new HashMap<>();
        cfg.put(Profile.UNIFORM, new double[] { 200_000, 0.0 }); // domain, theta(unused)
        cfg.put(Profile.ZIPF_0_99, new double[] { 200_000, 0.99 });
        cfg.put(Profile.ZIPF_1_2, new double[] { 200_000, 1.2 });
        cfg.put(Profile.FEW_HOT, new double[] { 265, 1.1 }); // NYC PULocationID-like

        StringBuilder md = new StringBuilder();
        md.append("# NDV accuracy — production Theta/KMV estimator (real run)\n\n");
        md.append("Rows per run: ").append(rows).append(", K=").append(ThetaSampler.DEFAULT_K)
                .append(", seed-fixed. Profiles model famous-dataset column distributions ")
                .append("(synthetic, known ground truth — NOT the literal datasets).\n\n");
        md.append("Relative error = |est − trueLiveNDV| / trueLiveNDV.\n\n");
        md.append("| Profile | Components | Delete% | True live NDV | Estimated | Rel.err |\n");
        md.append("|---|---|---|---|---|---|\n");

        for (Profile p : Profile.values()) {
            double[] c = cfg.get(p);
            int domain = (int) c[0];
            double theta = c[1];
            for (int comps : componentCounts) {
                for (int del : deletePcts) {
                    long[] res = runOne(p, domain, rows, comps, del, theta);
                    long est = res[0], truth = res[1];
                    double relErr = truth == 0 ? 0.0 : Math.abs(est - truth) / (double) truth;
                    md.append(String.format("| %s | %d | %d | %,d | %,d | %.2f%% |%n", p, comps, del, truth, est,
                            relErr * 100));
                }
            }
        }
        Files.write(Paths.get(OUT), md.toString().getBytes(StandardCharsets.UTF_8));
        System.out.println("NDV results written to " + OUT);
    }
}
