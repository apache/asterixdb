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
import org.apache.hyracks.api.io.IJsonSerializable;

/**
 * Factory for {@link IVTreeQuantizer} instances, supplied to the storage layer at construction time
 * and persisted on the local resource so it survives NC restart.
 * <p>
 * Lets the {@code asterix-common} layer plug in a quantizer implementation without the
 * {@code hyracks-storage-am-vtree} module depending on AsterixDB types. Extends
 * {@link IJsonSerializable} so the concrete factory can be persisted on the local resource.
 */
public interface IVTreeQuantizerFactory extends Serializable, IJsonSerializable {

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
