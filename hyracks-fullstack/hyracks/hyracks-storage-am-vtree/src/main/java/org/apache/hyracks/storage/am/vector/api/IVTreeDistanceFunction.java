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
package org.apache.hyracks.storage.am.vector.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Distance function between two decoded vectors, injected into the VTree index by the Hyracks
 * application that creates it (e.g. Euclidean, cosine, negated dot product).
 * <p>
 * Unlike {@link org.apache.hyracks.api.dataflow.value.IBinaryComparator}, which returns a
 * three-valued ordering (-1/0/+1) over raw field bytes, this returns a real-valued magnitude over
 * two already-decoded {@code double[]} vectors: smaller means nearer, and the values are compared
 * numerically to rank candidates.
 */
@FunctionalInterface
public interface IVTreeDistanceFunction {

    /**
     * @return the distance between {@code vector1} and {@code vector2}.
     * @throws HyracksDataException if the calculation fails
     */
    double apply(double[] vector1, double[] vector2) throws HyracksDataException;

    /**
     * Decode the vector encoded at {@code bytes[offset, offset+length)} into {@code dst} and return its
     * distance from {@code query} — ideally in a single pass over the bytes.
     * <p>
     * This exists because tree navigation decodes a centroid and then immediately measures it, once per
     * centroid per probed record. Doing both in one loop halves the passes over the data and lets the decoded
     * value stay in a register between the two uses.
     * <p>
     * {@code dst} must be sized to the encoded vector — callers check with
     * {@code DoubleArraySerializerDeserializer.readLength} — and receives the decoded vector, so this is the
     * overload for a caller that needs to keep the centroid. A caller that only wants the distance should use
     * {@link #decodeAndApply(double[], byte[], int, int)} instead, which skips the destination entirely.
     * <p>
     * The default implementation decodes and then delegates to {@link #apply}, which is correct for any
     * implementation; overriding it is purely an optimization and must return exactly what the default would.
     * Accumulate in ascending index order to stay bit-identical to {@link #apply}.
     *
     * @param query  the vector to measure against, already decoded
     * @param bytes  buffer holding the encoded vector
     * @param offset position of its leading length field
     * @param length number of bytes belonging to the encoded field
     * @param dst    destination for the decoded vector; must match its element count
     * @return the distance between {@code query} and the decoded vector
     * @throws HyracksDataException if the encoding is malformed or the calculation fails
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Fused decode+measure entry point for the navigation hot path")
    default double decodeAndApply(double[] query, byte[] bytes, int offset, int length, double[] dst)
            throws HyracksDataException {
        DoubleArraySerializerDeserializer.readInto(bytes, offset, length, dst);
        return apply(query, dst);
    }

    /**
     * As {@link #decodeAndApply(double[], byte[], int, int, double[])} but without keeping the decoded vector,
     * for callers that only need the distance — interior-node navigation, which measures a centroid to decide
     * which child to descend into and then discards it.
     * <p>
     * Dropping the destination lets an implementation keep each decoded element in a register instead of
     * storing it, and spares the caller a scratch array entirely.
     * <p>
     * Must return exactly what {@link #decodeAndApply(double[], byte[], int, int, double[])} would for the
     * same input; the default implementation guarantees that by delegating.
     *
     * @param query  the vector to measure against, already decoded
     * @param bytes  buffer holding the encoded vector
     * @param offset position of its leading length field
     * @param length number of bytes belonging to the encoded field
     * @return the distance between {@code query} and the encoded vector
     * @throws HyracksDataException if the encoding is malformed or the calculation fails
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Distance-only fused entry point for callers that discard the centroid")
    default double decodeAndApply(double[] query, byte[] bytes, int offset, int length) throws HyracksDataException {
        return apply(query, DoubleArraySerializerDeserializer.read(bytes, offset, length));
    }
}
