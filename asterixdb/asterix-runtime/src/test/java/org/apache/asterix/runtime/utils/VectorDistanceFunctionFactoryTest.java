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
package org.apache.asterix.runtime.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Random;

import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * The fused {@code decodeAndApply} overrides must return exactly what decoding and then calling
 * {@code apply} would, for every metric. "Exactly" means bit-identical, not approximately equal: the
 * distances order the candidate list and the index is expected to rebuild reproducibly from a fixed seed, so
 * a one-ulp difference is a real behavior change.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Equivalence of fused decode+measure against decode-then-measure")
public class VectorDistanceFunctionFactoryTest {

    private static byte[] encode(double[] v) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DoubleArraySerializerDeserializer.write(v, new DataOutputStream(bos));
        return bos.toByteArray();
    }

    /** Bit-identical, treating any NaN as equal to any other NaN (all NaNs are equally "no answer"). */
    private static void assertSame(String what, double expected, double actual) {
        if (Double.isNaN(expected) && Double.isNaN(actual)) {
            return;
        }
        Assert.assertEquals(what + ": expected " + expected + " got " + actual, Double.doubleToRawLongBits(expected),
                Double.doubleToRawLongBits(actual));
    }

    private static double value(Random r) {
        switch (r.nextInt(8)) {
            case 0:
                return 0.0;
            case 1:
                return -0.0;
            case 2:
                return Double.NaN;
            case 3:
                return Double.POSITIVE_INFINITY;
            case 4:
                return Double.NEGATIVE_INFINITY;
            case 5:
                return Double.MIN_VALUE;
            case 6:
                return Double.MAX_VALUE;
            default:
                return r.nextGaussian() * Math.pow(10, r.nextInt(20) - 10);
        }
    }

    @Test
    public void fusedMatchesUnfusedForEveryMetric() throws Exception {
        Random r = new Random(20260806L);
        int comparisons = 0;
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            IVTreeDistanceFunction fn = new VectorDistanceFunctionFactory(metric).createDistanceFunction();
            for (int trial = 0; trial < 400; trial++) {
                int dim = 1 + r.nextInt(64);
                double[] query = new double[dim];
                double[] centroid = new double[dim];
                boolean plainNumbers = trial % 2 == 0; // half the trials avoid NaN/Inf so real values dominate
                for (int i = 0; i < dim; i++) {
                    query[i] = plainNumbers ? r.nextGaussian() : value(r);
                    centroid[i] = plainNumbers ? r.nextGaussian() : value(r);
                }
                if (trial % 17 == 0) {
                    Arrays.fill(centroid, 0.0); // zero vector: cosine must yield NaN through both paths
                }

                byte[] enc = encode(centroid);
                int off = r.nextInt(8);
                byte[] buf = new byte[off + enc.length + r.nextInt(8)];
                System.arraycopy(enc, 0, buf, off, enc.length);

                double expected = fn.apply(query, centroid);
                double[] dst = new double[dim];
                double actual = fn.decodeAndApply(query, buf, off, enc.length, dst);

                assertSame(metric + " dim=" + dim + " trial=" + trial, expected, actual);
                // the distance-only overload must agree with both of the above
                assertSame(metric + " distance-only dim=" + dim + " trial=" + trial, expected,
                        fn.decodeAndApply(query, buf, off, enc.length));
                // decodeAndApply must also hand back the decoded centroid, bit for bit
                for (int i = 0; i < dim; i++) {
                    Assert.assertEquals(metric + " decoded element " + i, Double.doubleToRawLongBits(centroid[i]),
                            Double.doubleToRawLongBits(dst[i]));
                }
                comparisons++;
            }
        }
        Assert.assertEquals(VectorSimilarityMetric.values().length * 400, comparisons);
    }

    /** A wrong-sized destination is a programming error and must be rejected, not silently truncated. */
    @Test
    public void wrongSizedDestinationIsRejected() throws Exception {
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            IVTreeDistanceFunction fn = new VectorDistanceFunctionFactory(metric).createDistanceFunction();
            byte[] enc = encode(new double[] { 1.0, 2.0, 3.0 });
            try {
                fn.decodeAndApply(new double[3], enc, 0, enc.length, new double[2]);
                Assert.fail(metric + ": undersized destination should have been rejected");
            } catch (Exception expected) {
                // the contract is "fails loudly"; the exact type is not part of it
            }
        }
    }

    /** The default implementation on the interface must agree with the overrides it exists to replace. */
    @Test
    public void defaultImplementationAgreesWithOverrides() throws Exception {
        Random r = new Random(4242L);
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            IVTreeDistanceFunction fused = new VectorDistanceFunctionFactory(metric).createDistanceFunction();
            // A lambda gets only the interface's default decodeAndApply, so this exercises the fallback path.
            IVTreeDistanceFunction viaDefault = fused::apply;
            for (int trial = 0; trial < 200; trial++) {
                int dim = 1 + r.nextInt(32);
                double[] query = new double[dim];
                double[] centroid = new double[dim];
                for (int i = 0; i < dim; i++) {
                    query[i] = r.nextGaussian();
                    centroid[i] = r.nextGaussian();
                }
                byte[] enc = encode(centroid);
                assertSame(metric + " default vs override",
                        viaDefault.decodeAndApply(query, enc, 0, enc.length, new double[dim]),
                        fused.decodeAndApply(query, enc, 0, enc.length, new double[dim]));
                assertSame(metric + " default vs override, distance-only",
                        viaDefault.decodeAndApply(query, enc, 0, enc.length),
                        fused.decodeAndApply(query, enc, 0, enc.length));
            }
        }
    }
}
