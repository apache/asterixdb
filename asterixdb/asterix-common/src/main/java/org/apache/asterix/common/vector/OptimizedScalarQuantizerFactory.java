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
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizerFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;

/**
 * AsterixDB-side factory that constructs a {@link OptimizedScalarQuantizer} given the
 * {@link VTreeQuantizationParams} stored on the VTree index.
 * <p>
 * Replaces the {@code Class.forName} reflection block previously living in
 * {@code VTree#search}, which had to look up
 * {@link OptimizedScalarQuantizationCodec.Params},
 * {@link OptimizedScalarQuantizationCodec.SimilarityFunction}, and
 * {@link OptimizedScalarQuantizer} reflectively because the {@code hyracks-storage-am-vtree}
 * module cannot import AsterixDB types directly.
 */
public class OptimizedScalarQuantizerFactory implements IVTreeQuantizerFactory {

    private static final long serialVersionUID = 1L;

    // The distance metric is fixed at index creation and baked into this factory (it selects the
    // metric-specific similarity function: EUCLIDEAN, COSINE, or DOT_PRODUCT); the storage layer
    // builds the quantizer without it.
    private final String distanceMetric;

    public OptimizedScalarQuantizerFactory(String distanceMetric) {
        this.distanceMetric = distanceMetric;
    }

    @Override
    public IVTreeQuantizer createQuantizer(int vectorDimensions, VTreeQuantizationParams params)
            throws HyracksDataException {
        if (params == null) {
            throw HyracksDataException.create(new IllegalArgumentException("Quantization params must not be null"));
        }

        OptimizedScalarQuantizationCodec.Params p =
                new OptimizedScalarQuantizationCodec.Params(params.bits(), vectorDimensions, params.sampleCount(),
                        params.confidenceInterval(), params.minQuantile(), params.maxQuantile(), params.alpha());

        OptimizedScalarQuantizationCodec.SimilarityFunction sim =
                OptimizedScalarQuantizationCodec.fromDistanceMetric(distanceMetric);

        return new OptimizedScalarQuantizer(p, sim);
    }
}
