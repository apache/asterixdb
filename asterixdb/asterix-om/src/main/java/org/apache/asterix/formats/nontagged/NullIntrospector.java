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
package org.apache.asterix.formats.nontagged;

import static org.apache.asterix.om.types.ATypeTag.SERIALIZED_MISSING_TYPE_TAG;
import static org.apache.asterix.om.types.ATypeTag.SERIALIZED_NULL_TYPE_TAG;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;

import com.fasterxml.jackson.databind.JsonNode;

public class NullIntrospector implements INullIntrospector {

    private static final long serialVersionUID = 1L;

    public static final INullIntrospector INSTANCE = new NullIntrospector();

    private NullIntrospector() {
    }

    @Override
    public boolean isNull(byte[] bytes, int offset, int length) {
        return (bytes[offset] == SERIALIZED_MISSING_TYPE_TAG || bytes[offset] == SERIALIZED_NULL_TYPE_TAG)
                && length == 1;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return registry.getClassIdentifier(getClass(), serialVersionUID);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return INSTANCE;
    }
}
