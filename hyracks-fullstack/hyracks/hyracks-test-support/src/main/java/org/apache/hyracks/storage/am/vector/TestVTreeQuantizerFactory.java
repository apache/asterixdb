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

package org.apache.hyracks.storage.am.vector;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizerFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.utils.NoOpVectorQuantizer;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Identity quantizer factory for storage-layer unit tests, whose fixtures store full-precision vectors
 * in the quantized-embedding field. Ignores {@code params}, which a test tree does not have: it is the
 * factory's presence on the tree, not the quantization params, that turns quantized distances on.
 */
public class TestVTreeQuantizerFactory implements IVTreeQuantizerFactory {

    private static final long serialVersionUID = 1L;

    public static final TestVTreeQuantizerFactory INSTANCE = new TestVTreeQuantizerFactory();

    @Override
    public IVTreeQuantizer createQuantizer(int vectorDimensions, VTreeQuantizationParams params) {
        return NoOpVectorQuantizer.INSTANCE;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("unused")
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return INSTANCE;
    }
}
