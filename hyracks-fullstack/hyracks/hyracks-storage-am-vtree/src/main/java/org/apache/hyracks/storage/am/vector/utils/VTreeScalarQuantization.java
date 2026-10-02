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

/**
 * The per-dimension scalar quantization formula shared by the bulk-load codec
 * ({@code OptimizedScalarQuantizationCodec}) and the DML insert path ({@code VTreeDataTupleBuilder}), so both
 * encode identical input to identical codes. The parameters are global to the index:
 * {@code levels = 2^bits} and {@code alpha = (levels - 1) / (maxQ - minQ)}.
 */
public final class VTreeScalarQuantization {

    private VTreeScalarQuantization() {
    }

    /**
     * Encode one dimension to its integer code, clamped into {@code [0, levels - 1]}.
     *
     * @param value  the full-precision component
     * @param minQ   lower sample quantile over all dimensions
     * @param maxQ   upper sample quantile over all dimensions
     * @param alpha  {@code (levels - 1) / (maxQ - minQ)}
     * @param levels {@code 1 << bits}
     * @return the code, in {@code [0, levels - 1]}; narrow to the caller's storage width
     */
    public static long encodeDimension(double value, float minQ, float maxQ, float alpha, int levels) {
        // long, since for bits near 32 the rounded value can exceed Integer.MAX_VALUE until it is clamped.
        double clamped = Math.max(minQ, Math.min(maxQ, value));
        long code = Math.round((clamped - minQ) * alpha);
        return Math.max(0, Math.min(levels - 1, code));
    }

    /**
     * Decodes one unsigned code back to an approximate component, the inverse of
     * {@link #encodeDimension(double, float, float, float, int)} up to the quantization step. A signed
     * {@code byte} or {@code short} must be widened with {@code & 0xFF} or {@code & 0xFFFF} first.
     *
     * @param code  the unsigned code
     * @param alpha {@code (levels - 1) / (maxQ - minQ)}
     * @param minQ  lower sample quantile over all dimensions
     */
    public static double decodeDimension(long code, float alpha, float minQ) {
        return (double) code / alpha + minQ;
    }
}
