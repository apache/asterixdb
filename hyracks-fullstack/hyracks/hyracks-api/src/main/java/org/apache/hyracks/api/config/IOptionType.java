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
package org.apache.hyracks.api.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public interface IOptionType<T> {
    /**
     * @throws IllegalArgumentException when the supplied string cannot be interpreted
     */
    T parse(String s);

    /**
     * @throws IllegalArgumentException when the supplied JSON node cannot be interpreted
     */
    T parse(JsonNode node);

    Class<T> targetType();

    /**
     * @return the value in a format suitable for serialized JSON
     */
    default Object serializeToJSON(Object value) {
        return value;
    }

    /**
     * Serializes the value as a field in the provided object node
     */
    void serializeJSONField(String fieldName, Object value, ObjectNode node);

    /**
     * @return the value in a format suitable for serialized ini file
     */
    default String serializeToIni(Object value) {
        return String.valueOf(value);
    }

    /**
     * @return the value in human-readable form (e.g. for usage)
     */
    default String serializeToHumanReadable(Object value) {
        return serializeToIni(value);
    }
}
