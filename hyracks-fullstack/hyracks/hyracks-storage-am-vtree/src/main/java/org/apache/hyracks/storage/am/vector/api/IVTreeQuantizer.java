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

import java.util.Arrays;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Vector quantization/dequantization at the Hyracks layer. Both methods return double[]
 * so distance functions can consume their output directly.
 * <p>
 * Not thread-safe: a quantizer is built per query and confined to a single operation context.
 */
public interface IVTreeQuantizer {

    /**
     * Quantize a full-precision vector and immediately dequantize it back to double[].
     *
     * @param vector the full-precision vector
     * @return a lossy double[] suitable for approximate distance computation
     * @throws HyracksDataException if quantization fails
     */
    double[] quantize(double[] vector) throws HyracksDataException;

    /**
     * Dequantize a stored quantized byte representation back to double[].
     *
     * @param quantizedBytes the quantized byte representation
     * @return the dequantized double[]
     * @throws HyracksDataException if dequantization fails
     */
    double[] dequantize(byte[] quantizedBytes) throws HyracksDataException;

    /**
     * Dequantize the code stored at {@code data[offset, offset + length)} into {@code dst}, so a caller
     * scanning many candidates can read codes in place and reuse one destination array.
     * <p>
     * The default copies the range and delegates to {@link #dequantize(byte[])}; implementations on a hot
     * path should override it to decode without allocating.
     *
     * @param data   the buffer holding the code, with any length prefix already skipped
     * @param offset start of the code in {@code data}
     * @param length length of the code in bytes
     * @param dst    receives the dequantized vector; must hold at least as many elements as it has dimensions
     * @throws HyracksDataException if dequantization fails or {@code dst} is too small
     */
    default void dequantize(byte[] data, int offset, int length, double[] dst) throws HyracksDataException {
        double[] decoded = dequantize(Arrays.copyOfRange(data, offset, offset + length));
        if (decoded.length > dst.length) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "The dequantized vector has " + decoded.length
                    + " dimensions but the destination holds only " + dst.length);
        }
        System.arraycopy(decoded, 0, dst, 0, decoded.length);
    }
}
