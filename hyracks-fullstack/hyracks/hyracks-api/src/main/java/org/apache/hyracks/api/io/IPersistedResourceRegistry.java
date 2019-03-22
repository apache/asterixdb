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
package org.apache.hyracks.api.io;

import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A IPersistedResourceRegistry is responsible for maintaining a mapping between classes and type ids
 * that are used for serialization.
 */
public interface IPersistedResourceRegistry {

    String TYPE_FIELD_ID = "@type";
    String VERSION_FIELD_ID = "@version";
    String CLASS_FIELD_ID = "@class";

    /**
     * @param clazz
     * @param version
     * @return A JsonObject with the registered type id in IPersistedResourceRegistry.
     */
    ObjectNode getClassIdentifier(Class<? extends IJsonSerializable> clazz, long version);

    /**
     * @param json
     * @return A class object of the type id in {@code json}
     * @throws HyracksDataException
     */
    IJsonSerializable deserialize(JsonNode json) throws HyracksDataException;

    /**
     * This method must be used for optional fields or newly added fields to ensure back-compatibility
     *
     * @param json
     * @param clazz
     * @return A class object of the type id in {@code json} if exists
     *         or a class object of type <code>clazz</code> otherwise.
     * @throws HyracksDataException
     */
    IJsonSerializable deserializeOrDefault(JsonNode json, Class<? extends IJsonSerializable> clazz)
            throws HyracksDataException;
}
