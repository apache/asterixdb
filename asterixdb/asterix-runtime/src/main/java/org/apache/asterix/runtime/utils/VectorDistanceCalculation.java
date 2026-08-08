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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.util.annotations.AiProvenance;

public class VectorDistanceCalculation {

    // ---- IVTreeDistanceFunction views of the calculations above, for the VTree index.
    //
    // Stateless singletons rather than method references, because each also overrides the fused
    // decodeAndApply entry points: tree navigation decodes a centroid straight from the page bytes and
    // measures it in the same pass, instead of decoding into an array and then walking that array. Each fused
    // loop accumulates in ascending index order and applies the same guards as its pair-of-arrays
    // counterpart, so results are bit-identical -- VectorDistanceFunctionFactoryTest checks that.
    //
    // Named nested classes rather than anonymous ones: these are handed to a Serializable factory, and
    // anonymous classes compile to positional names (Outer$1, Outer$2, ...) that shift if the declarations
    // are reordered. Nothing serializes them today, but the names are not something to leave load-bearing.

    /** Euclidean distance as an {@link IVTreeDistanceFunction}. */
    public static final IVTreeDistanceFunction EUCLIDEAN_FN = new EuclideanFunction();

    /** Squared Euclidean distance as an {@link IVTreeDistanceFunction}. */
    public static final IVTreeDistanceFunction EUCLIDEAN_SQUARED_FN = new EuclideanSquaredFunction();

    /** Cosine distance (1 - cosine similarity) as an {@link IVTreeDistanceFunction}. */
    public static final IVTreeDistanceFunction COSINE_DISTANCE_FN = new CosineDistanceFunction();

    /** Negated dot product as an {@link IVTreeDistanceFunction}, so that smaller still means nearer. */
    public static final IVTreeDistanceFunction DOT_DISTANCE_FN = new DotDistanceFunction();

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Fused decode+measure")
    private static final class EuclideanSquaredFunction implements IVTreeDistanceFunction {
        @Override
        public double apply(double[] a, double[] b) {
            return euclideanSquared(a, b);
        }

        @Override
        public double decodeAndApply(double[] query, byte[] bytes, int offset, int length, double[] dst)
                throws HyracksDataException {
            return fusedEuclideanSquared(query, bytes, offset, length, dst);
        }

        @Override
        public double decodeAndApply(double[] query, byte[] bytes, int offset, int length) throws HyracksDataException {
            return fusedEuclideanSquared(query, bytes, offset, length, null);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Fused decode+measure")
    private static final class EuclideanFunction implements IVTreeDistanceFunction {
        @Override
        public double apply(double[] a, double[] b) {
            return euclidean(a, b);
        }

        // euclidean() is sqrt(euclideanSquared()), so the fused form is too.
        @Override
        public double decodeAndApply(double[] query, byte[] bytes, int offset, int length, double[] dst)
                throws HyracksDataException {
            return Math.sqrt(fusedEuclideanSquared(query, bytes, offset, length, dst));
        }

        @Override
        public double decodeAndApply(double[] query, byte[] bytes, int offset, int length) throws HyracksDataException {
            return Math.sqrt(fusedEuclideanSquared(query, bytes, offset, length, null));
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Fused decode+measure")
    private static final class CosineDistanceFunction implements IVTreeDistanceFunction {
        @Override
        public double apply(double[] a, double[] b) {
            return cosineDistance(a, b);
        }

        @Override
        public double decodeAndApply(double[] query, byte[] bytes, int offset, int length, double[] dst)
                throws HyracksDataException {
            return fusedCosineDistance(query, bytes, offset, length, dst);
        }

        @Override
        public double decodeAndApply(double[] query, byte[] bytes, int offset, int length) throws HyracksDataException {
            return fusedCosineDistance(query, bytes, offset, length, null);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Fused decode+measure")
    private static final class DotDistanceFunction implements IVTreeDistanceFunction {
        @Override
        public double apply(double[] a, double[] b) {
            return dotDistance(a, b);
        }

        @Override
        public double decodeAndApply(double[] query, byte[] bytes, int offset, int length, double[] dst)
                throws HyracksDataException {
            return fusedDotDistance(query, bytes, offset, length, dst);
        }

        @Override
        public double decodeAndApply(double[] query, byte[] bytes, int offset, int length) throws HyracksDataException {
            return fusedDotDistance(query, bytes, offset, length, null);
        }
    }

    public static double euclidean(double[] a, double[] b) {
        double sum = euclideanSquared(a, b);
        return Math.sqrt(sum);
    }

    public static double euclideanSquared(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    public static double cosineSimilarity(double[] a, double[] b) {
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0 || Double.isNaN(normA) || Double.isNaN(normB) || Double.isNaN(dot)) {
            return Double.NaN;
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    // USED BY VECTOR INDEX WILL BE USED FOR COSINE DISTANCE
    public static double cosineDistance(double[] a, double[] b) {
        double similarity = cosineSimilarity(a, b);
        return Double.isNaN(similarity) ? Double.NaN : 1.0 - similarity;
    }

    public static double dotProduct(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        if (Double.isNaN(sum)) {
            return Double.NaN; // Handle NaN case
        }
        return sum;
    }

    // USED BY VECTOR INDEX WILL BE USED FOR DOT DISTANCE
    public static double dotDistance(double[] a, double[] b) {
        double dot = dotProduct(a, b);
        return Double.isNaN(dot) ? Double.NaN : -dot;
    }

    /**
     * Sum of squared differences between {@code query} and the vector encoded at {@code bytes[offset..]},
     * decoding as it goes. {@code dst} receives the decoded vector, or may be {@code null} when the caller
     * only wants the distance — the check is loop-invariant, so the JIT hoists it out and the store
     * disappears entirely in the null case.
     */
    private static double fusedEuclideanSquared(double[] query, byte[] bytes, int offset, int length, double[] dst)
            throws HyracksDataException {
        int len = checkedLength(bytes, offset, length, dst);
        double sum = 0.0;
        int pos = offset + Integer.BYTES;
        for (int i = 0; i < len; i++, pos += Double.BYTES) {
            double x = DoublePointable.getDouble(bytes, pos);
            if (dst != null) {
                dst[i] = x;
            }
            double diff = query[i] - x;
            sum += diff * diff;
        }
        return sum;
    }

    private static double fusedDotDistance(double[] query, byte[] bytes, int offset, int length, double[] dst)
            throws HyracksDataException {
        int len = checkedLength(bytes, offset, length, dst);
        double sum = 0.0;
        int pos = offset + Integer.BYTES;
        for (int i = 0; i < len; i++, pos += Double.BYTES) {
            double x = DoublePointable.getDouble(bytes, pos);
            if (dst != null) {
                dst[i] = x;
            }
            sum += query[i] * x;
        }
        return Double.isNaN(sum) ? Double.NaN : -sum;
    }

    private static double fusedCosineDistance(double[] query, byte[] bytes, int offset, int length, double[] dst)
            throws HyracksDataException {
        int len = checkedLength(bytes, offset, length, dst);
        // Same three accumulators, same order, as cosineSimilarity(). normQuery is recomputed per centroid
        // exactly as the unfused form does, so the arithmetic matches to the bit.
        double dot = 0.0;
        double normQuery = 0.0;
        double normCentroid = 0.0;
        int pos = offset + Integer.BYTES;
        for (int i = 0; i < len; i++, pos += Double.BYTES) {
            double x = DoublePointable.getDouble(bytes, pos);
            if (dst != null) {
                dst[i] = x;
            }
            dot += query[i] * x;
            normQuery += query[i] * query[i];
            normCentroid += x * x;
        }
        if (normQuery == 0.0 || normCentroid == 0.0 || Double.isNaN(normQuery) || Double.isNaN(normCentroid)
                || Double.isNaN(dot)) {
            return Double.NaN;
        }
        return 1.0 - dot / (Math.sqrt(normQuery) * Math.sqrt(normCentroid));
    }

    /** Encoded element count, validated against {@code dst} when the caller supplied one. */
    private static int checkedLength(byte[] bytes, int offset, int length, double[] dst) throws HyracksDataException {
        int len = DoubleArraySerializerDeserializer.readLength(bytes, offset, length);
        if (dst != null && dst.length != len) {
            throw HyracksDataException.create(new IllegalArgumentException(
                    "destination has " + dst.length + " elements but the encoded array has " + len));
        }
        return len;
    }
}
