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
package org.apache.asterix.dataflow.data.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Factory for creating AOrderedListVectorBinaryAccessor instances.
 * This factory is serializable and can be passed from AsterixDB layer to Hyracks layer.
 */
public class AOrderedListVectorBinaryAccessorFactory implements IVTreeBinaryAccessorFactory {

    private static final long serialVersionUID = 1L;

    public AOrderedListVectorBinaryAccessorFactory() {
        // No configuration needed for now
    }

    @Override
    public IVTreeBinaryAccessor createAccessor() {
        return new AOrderedListVectorBinaryAccessor();
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        return new AOrderedListVectorBinaryAccessorFactory();
    }
}
