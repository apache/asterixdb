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

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Factory for {@link IVTreeQuantizer} instances, supplied to the storage layer through the
 * index-access-parameters map (key {@link #IAP_KEY}).
 * <p>
 * Replaces the previous {@code Class.forName} / constructor-reflection block in
 * {@code VTree#search} that constructed an AsterixDB {@code ScalarVectorQuantizer} from a
 * {@code float[]} quantization-params payload.
 */
public interface IVTreeQuantizerFactory extends Serializable {

    /** Index-access-parameters key under which a factory instance is passed to the storage layer. */
    String IAP_KEY = "VECTOR_QUANTIZER_FACTORY";

    /**
     * Build a quantizer for the given dimensionality. The distance metric is fixed at index creation and
     * baked into the concrete factory (it selects symmetric vs asymmetric distance), so Hyracks does not
     * pass a metric. The {@code params} are those returned by {@code VTree#getQuantizationParams()}.
     *
     * @param vectorDimensions vector dimensionality the index was built with
     * @param params           the scalar-quantization params; never {@code null} on this path,
     *                         which is only reached for a quantized index
     */
    IVTreeQuantizer createQuantizer(int vectorDimensions, VTreeQuantizationParams params) throws HyracksDataException;
}
