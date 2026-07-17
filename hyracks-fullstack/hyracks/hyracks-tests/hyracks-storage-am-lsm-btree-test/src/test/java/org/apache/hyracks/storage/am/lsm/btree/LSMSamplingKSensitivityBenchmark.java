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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaEstimator;
import org.apache.hyracks.storage.am.lsm.common.theta.ThetaSampler;
import org.junit.Test;

/**
 * K-sensitivity sweep for the production Theta/KMV estimator under LSM fragmentation.
 *
 * The NDV limitation (overestimation at high component-count × delete%) is structural to
 * single-threshold KMV: global theta is set by insert components and tombstone hashes fall
 * outside the suppressible range. This benchmark quantifies how K (sketch size) affects that
 * ceiling, providing the paper's K-sensitivity table (§3.4 / gap-list item).
 *
 * Sweep: K ∈ {256, 512, 1024, 4096} × {1,5,10,20} comps × {0,25,50,85}% deletes.
 * Profiles: UNIFORM (TPC-H-like) and ZIPF_1_2 (MovieLens-like, worst-case skew).
 * Results → /tmp/sampling_paper_k_sensitivity.md
 */
public class LSMSamplingKSensitivityBenchmark {

    private static final int[] KEY_FIELDS = { 0 };
    private static final long SEED = 13L;
    private static final String OUT = "/tmp/sampling_paper_k_sensitivity.md";

    // Minimal tuple shims (value-as-key, 1 field) — identical layout to LSMSamplingNdvBenchmark
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

    private enum Profile {
        UNIFORM,
        ZIPF_1_2
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

    private static int draw(Profile p, int domain, Random rnd, double[] zipfCdf) {
        if (p == Profile.UNIFORM) {
            return rnd.nextInt(domain);
        }
        double u = rnd.nextDouble();
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

    /**
     * Run one NDV experiment with the given K (sketch size).
     * Returns {estimatedNDV, trueLiveNDV}.
     */
    private long[] runOne(Profile p, int domain, double zipfTheta, int rows, int dataComponents, int deletePct, int k)
            throws IOException {
        Random rnd = new Random(SEED + p.ordinal() * 131 + dataComponents * 17 + deletePct + k);
        double[] cdf = (p == Profile.UNIFORM) ? null : zipfCdf(domain, zipfTheta);

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

        List<ThetaEstimator.ComponentStats> stats = new ArrayList<>();
        if (!deleted.isEmpty()) {
            ThetaSampler del = new ThetaSampler(KEY_FIELDS, k);
            for (int dvv : deleted) {
                del.addTuple(new Anti(dvv));
            }
            ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
            abvs.append(del.serialize());
            stats.add(ThetaSampler.deserialize(abvs));
        }
        for (int ci = 0; ci < dataComponents; ci++) {
            ThetaSampler s = new ThetaSampler(KEY_FIELDS, k);
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
    public void kSensitivitySweep() throws IOException {
        int rows = 1_000_000;
        int domain = 200_000;
        int[] kValues = { 256, 512, 1024, 4096 };
        int[] componentCounts = { 1, 5, 10, 20 };
        int[] deletePcts = { 0, 25, 50, 85 };

        StringBuilder md = new StringBuilder();
        md.append("# K-Sensitivity sweep — production Theta/KMV NDV estimator\n\n");
        md.append("Rows: ").append(rows).append(", domain: ").append(domain)
                .append(", seed-fixed. UNIFORM=TPC-H-like, ZIPF_1_2=MovieLens-like.\n");
        md.append("Realistic delete model: tombstones in strictly-newer antimatter-only component.\n\n");
        md.append("**Structural ceiling:** globalTheta ≈ K / rows_per_component; tombstone coverage ∝ K·comps/rows.\n");
        md.append(
                "Larger K raises ceiling but does NOT eliminate fragmentation limitation — compaction is the fix.\n\n");
        md.append("| Profile | K | Components | Delete% | True live NDV | Estimated | Rel.err |\n");
        md.append("|---|---|---|---|---|---|---|\n");

        for (Profile p : Profile.values()) {
            double zipfTheta = (p == Profile.ZIPF_1_2) ? 1.2 : 0.0;
            for (int k : kValues) {
                for (int comps : componentCounts) {
                    for (int del : deletePcts) {
                        long[] res = runOne(p, domain, zipfTheta, rows, comps, del, k);
                        long est = res[0], truth = res[1];
                        double relErr = truth == 0 ? 0.0 : Math.abs(est - truth) / (double) truth;
                        md.append(String.format("| %s | %d | %d | %d | %,d | %,d | %.2f%% |%n", p, k, comps, del, truth,
                                est, relErr * 100));
                    }
                }
            }
        }

        // Analytical ceiling table for paper
        md.append("\n## Analytical tombstone-coverage ceiling  (threshold ≈ K·nComps/totalRows)\n\n");
        md.append("| K | 1 comp | 5 comps | 10 comps | 20 comps |\n");
        md.append("|---|---|---|---|---|\n");
        for (int k : kValues) {
            md.append(String.format("| %d", k));
            for (int comps : componentCounts) {
                double coverage = (double) k * comps / rows * 100.0;
                md.append(String.format(" | %.3f%%", coverage));
            }
            md.append(" |\n");
        }
        md.append("\nFraction of tombstone hashes expected to fall within suppressible range.\n");
        md.append("At 85% delete rate, most tombstones escape → NDV overestimated regardless of K.\n");

        Files.write(Paths.get(OUT), md.toString().getBytes(StandardCharsets.UTF_8));
        System.out.println("K-sensitivity results written to " + OUT);
    }
}
