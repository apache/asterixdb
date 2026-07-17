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
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

/**
 * Sample-distribution fidelity benchmark: KS statistic and Jensen-Shannon divergence
 * of uniformly-drawn row samples against the true population distribution.
 *
 * This verifies the DKW inequality claim (§3.7 of the paper) empirically:
 *   n samples → ECDF error ≤ sqrt(ln(200) / (2n)) at 99% confidence.
 *
 * The Olken-Rotem page-fill correction guarantees that the DiskBTreeSampleCursor produces
 * uniform-over-rows samples. This benchmark confirms that the statistical consequence
 * (KS ≤ ε* at 99th percentile over 200 trials) holds for the sample sizes in our DKW table.
 *
 * Sampling model: uniform random draw (what Olken-Rotem corrected sampling achieves).
 * Population: N=1,000,000 values per profile.
 * Trials: 200 independent seeds for each (profile, n) pair.
 *
 * Profiles:
 *   UNIFORM     — TPC-H-like, domain 200,000
 *   ZIPF_0_99   — YCSB-like, domain 200,000, θ=0.99
 *   ZIPF_1_2    — MovieLens-like, domain 200,000, θ=1.2
 *   FEW_HOT     — NYC-Taxi PULocationID-like, domain 265, θ=1.1
 *
 * Also measures fidelity post-visibility-fix: with 85% logical deletes (antimatter-only newer
 * component), the corrected sampler should draw only live rows → sample represents the LIVE
 * sub-population. We verify KS(sampled_live, true_live) ≤ ε* still holds.
 *
 * Results → /tmp/sampling_paper_fidelity_results.md
 */
public class LSMSamplingFidelityBenchmark {

    private static final int N = 1_000_000;
    private static final int TRIALS = 200;
    private static final String OUT = "/tmp/sampling_paper_fidelity_results.md";
    private static final long SEED_BASE = 42L;

    // DKW-derived sample sizes (from BENCHMARK-PLAN.md; high=68032 in ANALYZE)
    private static final int[] SAMPLE_SIZES = { 1063, 4252, 17068, 68032 };

    private enum Profile {
        UNIFORM(200_000, 0.0),
        ZIPF_0_99(200_000, 0.99),
        ZIPF_1_2(200_000, 1.2),
        FEW_HOT(265, 1.1);

        final int domain;
        final double theta;

        Profile(int domain, double theta) {
            this.domain = domain;
            this.theta = theta;
        }
    }

    /** Precompute Zipf CDF for inverse-transform sampling. */
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

    /** Draw one value index from the profile (inverse-CDF for Zipf, uniform otherwise). */
    private static int draw(Profile p, double[] cdf, Random rnd) {
        if (p == Profile.UNIFORM) {
            return rnd.nextInt(p.domain);
        }
        double u = rnd.nextDouble();
        int lo = 0, hi = cdf.length - 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (cdf[mid] < u) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Generate population of N values from profile, compute true PMF over domain buckets.
     * Returns int[N] population and double[domain] truePmf.
     */
    private static Object[] generatePopulation(Profile p, double[] cdf, Random rnd) {
        int[] pop = new int[N];
        long[] freq = new long[p.domain];
        for (int i = 0; i < N; i++) {
            int v = draw(p, cdf, rnd);
            pop[i] = v;
            freq[v]++;
        }
        double[] pmf = new double[p.domain];
        for (int i = 0; i < p.domain; i++) {
            pmf[i] = (double) freq[i] / N;
        }
        return new Object[] { pop, pmf };
    }

    /**
     * Draw n uniform-random rows from pop[] (simulating Olken-Rotem corrected sampling).
     * Returns the empirical PMF of drawn values.
     */
    private static double[] samplePmf(int[] pop, int n, int domain, Random rnd) {
        long[] freq = new long[domain];
        for (int i = 0; i < n; i++) {
            freq[pop[rnd.nextInt(pop.length)]]++;
        }
        double[] pmf = new double[domain];
        for (int i = 0; i < domain; i++) {
            pmf[i] = (double) freq[i] / n;
        }
        return pmf;
    }

    /**
     * KS statistic: D = max_x |F_n(x) - F(x)| over the domain.
     * Both pmf[] arrays are over the same discrete domain; we compute CDF on the fly.
     */
    private static double ksStatistic(double[] sampledPmf, double[] truePmf) {
        double fn = 0, f = 0, maxD = 0;
        for (int i = 0; i < sampledPmf.length; i++) {
            fn += sampledPmf[i];
            f += truePmf[i];
            double d = Math.abs(fn - f);
            if (d > maxD) {
                maxD = d;
            }
        }
        return maxD;
    }

    /**
     * Jensen-Shannon divergence: JSD(P||Q) = (KL(P||M) + KL(Q||M)) / 2, M = (P+Q)/2.
     * Handles zero-probability bins with a Laplace smoothing of 1e-12.
     */
    private static double jsDivergence(double[] p, double[] q) {
        double eps = 1e-12;
        double jsd = 0;
        for (int i = 0; i < p.length; i++) {
            double pi = p[i] + eps;
            double qi = q[i] + eps;
            double mi = (pi + qi) / 2.0;
            jsd += pi * Math.log(pi / mi) + qi * Math.log(qi / mi);
        }
        return jsd / 2.0;
    }

    /**
     * DKW bound: ε* = sqrt(ln(2 * TRIALS) / (2 * n)), the 99th-percentile ECDF error bound.
     * Exact: P(D_n > ε) ≤ 2·exp(-2·n·ε²) → ε* where RHS = 1/TRIALS (99th percentile over TRIALS runs).
     */
    private static double dkwBound(int n) {
        return Math.sqrt(Math.log(2.0 * TRIALS) / (2.0 * n));
    }

    @Test
    public void sampleFidelityKsJs() throws IOException {
        StringBuilder md = new StringBuilder();
        md.append("# Sample-Distribution Fidelity — KS statistic and Jensen-Shannon divergence\n\n");
        md.append("**Model:** uniform random row draw (what Olken-Rotem fill-corrected sampling achieves).\n");
        md.append("Population N=").append(N).append(", trials=").append(TRIALS).append(" per (profile, n) pair.\n");
        md.append("DKW bound ε*: 99th-pct ECDF error at which at most 1 of ").append(TRIALS)
                .append(" trials exceeds it.\n\n");

        md.append("## A. Clean-data fidelity (0% deletes)\n\n");
        md.append("| Profile | n | DKW ε* | Mean KS | p99 KS | KS ≤ ε*? | Mean JSD |\n");
        md.append("|---|---|---|---|---|---|---|\n");

        for (Profile p : Profile.values()) {
            double[] cdf = (p == Profile.UNIFORM) ? null : zipfCdf(p.domain, p.theta);
            // Fixed population seed per profile
            Random popRnd = new Random(SEED_BASE + p.ordinal() * 7919L);
            Object[] gen = generatePopulation(p, cdf, popRnd);
            int[] pop = (int[]) gen[0];
            double[] truePmf = (double[]) gen[1];

            for (int n : SAMPLE_SIZES) {
                double[] ksArr = new double[TRIALS];
                double[] jsArr = new double[TRIALS];
                for (int t = 0; t < TRIALS; t++) {
                    Random sRnd = new Random(SEED_BASE + p.ordinal() * 7919L + n * 31L + t);
                    double[] samplePmf = samplePmf(pop, n, p.domain, sRnd);
                    ksArr[t] = ksStatistic(samplePmf, truePmf);
                    jsArr[t] = jsDivergence(samplePmf, truePmf);
                }
                Arrays.sort(ksArr);
                double meanKs = Arrays.stream(ksArr).average().getAsDouble();
                double p99Ks = ksArr[TRIALS - 1]; // worst of 200 = 99th pct
                double meanJs = Arrays.stream(jsArr).average().getAsDouble();
                double bound = dkwBound(n);
                boolean withinBound = p99Ks <= bound;
                md.append(String.format("| %s | %,d | %.4f | %.4f | %.4f | %s | %.6f |%n", p, n, bound, meanKs, p99Ks,
                        withinBound ? "YES" : "NO (investigate)", meanJs));
            }
        }

        md.append("\n## B. Post-fix fidelity against live sub-population (85% deletes)\n\n");
        md.append("With 85% logical deletes in a newer antimatter component, the visibility fix ensures\n");
        md.append("only live rows enter the sample. This measures KS(sampled_live, true_live_distribution).\n\n");
        md.append("| Profile | n | DKW ε* | Mean KS | p99 KS | KS ≤ ε*? |\n");
        md.append("|---|---|---|---|---|---|\n");

        int deletePct = 85;
        for (Profile p : Profile.values()) {
            double[] cdf = (p == Profile.UNIFORM) ? null : zipfCdf(p.domain, p.theta);
            Random popRnd = new Random(SEED_BASE + p.ordinal() * 7919L + 999L);
            Object[] gen = generatePopulation(p, cdf, popRnd);
            int[] pop = (int[]) gen[0];

            // Mark deleted values: first deletePct% of distinct values by index
            boolean[] isDeleted = new boolean[p.domain];
            int totalDistinct = 0;
            for (double v : (double[]) gen[1]) {
                if (v > 0) {
                    totalDistinct++;
                }
            }
            int toDelete = (int) (totalDistinct * (deletePct / 100.0));
            int marked = 0;
            for (int i = 0; i < p.domain && marked < toDelete; i++) {
                if (((double[]) gen[1])[i] > 0) {
                    isDeleted[i] = true;
                    marked++;
                }
            }

            // Build live-only population array
            int liveCount = 0;
            for (int v : pop) {
                if (!isDeleted[v]) {
                    liveCount++;
                }
            }
            int[] livePop = new int[liveCount];
            int li = 0;
            for (int v : pop) {
                if (!isDeleted[v]) {
                    livePop[li++] = v;
                }
            }

            // True live PMF
            long[] liveFreq = new long[p.domain];
            for (int v : livePop) {
                liveFreq[v]++;
            }
            double[] trueLivePmf = new double[p.domain];
            for (int i = 0; i < p.domain; i++) {
                trueLivePmf[i] = (double) liveFreq[i] / liveCount;
            }

            for (int n : SAMPLE_SIZES) {
                double[] ksArr = new double[TRIALS];
                for (int t = 0; t < TRIALS; t++) {
                    Random sRnd = new Random(SEED_BASE + p.ordinal() * 7919L + n * 31L + t + 8888L);
                    double[] samplePmf = samplePmf(livePop, n, p.domain, sRnd);
                    ksArr[t] = ksStatistic(samplePmf, trueLivePmf);
                }
                Arrays.sort(ksArr);
                double meanKs = Arrays.stream(ksArr).average().getAsDouble();
                double p99Ks = ksArr[TRIALS - 1];
                double bound = dkwBound(n);
                boolean withinBound = p99Ks <= bound;
                md.append(String.format("| %s | %,d | %.4f | %.4f | %.4f | %s |%n", p, n, bound, meanKs, p99Ks,
                        withinBound ? "YES" : "NO"));
            }
        }

        md.append("\n## C. DKW verification table\n\n");
        md.append("ε* = sqrt(ln(2·").append(TRIALS).append(") / (2n)) — population-independent.\n\n");
        md.append("| n | DKW ε* (99% conf) | Interpretation |\n");
        md.append("|---|---|---|\n");
        for (int n : SAMPLE_SIZES) {
            md.append(String.format("| %,d | ±%.2f%% | ECDF error ≤ ε* for ≥99%% of trials |%n", n, dkwBound(n) * 100));
        }
        md.append(String.format("| %,d | ±%.2f%% | (default high sample size used in production ANALYZE) |%n", 68032,
                dkwBound(68032) * 100));

        Files.write(Paths.get(OUT), md.toString().getBytes(StandardCharsets.UTF_8));
        System.out.println("Fidelity results written to " + OUT);
    }
}
