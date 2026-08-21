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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;

/**
 * Identity {@link IVTreeQuantizer} for unit tests: {@code quantize} returns the input vector
 * unchanged and {@code dequantize} reads raw big-endian doubles. Callers are responsible for
 * stripping any {@code ByteArrayPointable} length prefix before invoking {@code dequantize}.
 */
public class NoOpVectorQuantizer implements IVTreeQuantizer {

    public static final NoOpVectorQuantizer INSTANCE = new NoOpVectorQuantizer();

    @Override
    public double[] quantize(double[] vector) throws HyracksDataException {
        return vector;
    }

    @Override
    public double[] dequantize(byte[] quantizedBytes) throws HyracksDataException {
        // Identity dequantize reads whole big-endian doubles; a non-multiple-of-8 length means the
        // caller failed to strip a length prefix (or passed a truncated buffer). Fail loudly here
        // (test-only helper) rather than silently dropping trailing bytes.
        if (quantizedBytes.length % Double.BYTES != 0) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "The quantized vector is "
                    + quantizedBytes.length + " bytes, which is not a multiple of " + Double.BYTES);
        }
        int count = quantizedBytes.length / Double.BYTES;

        ByteBuffer buf = ByteBuffer.wrap(quantizedBytes);
        double[] vector = new double[count];
        for (int i = 0; i < count; i++) {
            vector[i] = buf.getDouble();
        }
        return vector;
    }
}
