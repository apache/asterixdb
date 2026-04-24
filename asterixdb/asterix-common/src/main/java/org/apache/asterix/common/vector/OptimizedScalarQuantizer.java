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
package org.apache.asterix.common.vector;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;

/**
 * AsterixDB implementation of {@link IVTreeQuantizer} that wraps
 * {@link OptimizedScalarQuantizationCodec} for scalar quantization.
 *
 * <p>
 * Does not L2-normalize vectors; callers must satisfy the index similarity contract
 * (e.g. unit-length vectors for cosine indexes).
 *
 * <p>
 * Both {@link #quantize(double[])} and {@link #dequantize(byte[])} return double[]
 * so that existing distance functions (which operate on double[]) work unchanged.
 */
public class OptimizedScalarQuantizer implements IVTreeQuantizer {

    private final OptimizedScalarQuantizationCodec.Params params;
    private final OptimizedScalarQuantizationCodec.SimilarityFunction similarityFunction;

    public OptimizedScalarQuantizer(OptimizedScalarQuantizationCodec.Params params,
            OptimizedScalarQuantizationCodec.SimilarityFunction similarityFunction) {
        this.params = params;
        this.similarityFunction = similarityFunction;
    }

    /** Encodes with OSQ, then decodes to double[] so distance functions use a lossy approximation unchanged. */
    @Override
    public double[] quantize(double[] vector) throws HyracksDataException {
        OptimizedScalarQuantizationCodec.QuantizedVector qv =
                OptimizedScalarQuantizationCodec.quantizeVector(vector, params, similarityFunction);
        return OptimizedScalarQuantizationCodec.dequantizeToDoubleArray(qv.quantizedBytes, params);
    }

    @Override
    public double[] dequantize(byte[] quantizedBytes) throws HyracksDataException {
        // Leaf storage uses byte[] when params.bits <= 8 (SQ4/SQ8); params come from index metadata.
        return OptimizedScalarQuantizationCodec.dequantizeToDoubleArray(quantizedBytes, params);
    }
}
