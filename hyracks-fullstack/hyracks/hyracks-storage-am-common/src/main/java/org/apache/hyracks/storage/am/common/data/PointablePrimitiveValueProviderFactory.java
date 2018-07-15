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
package org.apache.hyracks.storage.am.common.data;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.api.INumeric;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class PointablePrimitiveValueProviderFactory implements IPrimitiveValueProviderFactory {
    private static final long serialVersionUID = 1L;

    private final IPointableFactory pf;

    public PointablePrimitiveValueProviderFactory(IPointableFactory pf) {
        this.pf = pf;
    }

    @Override
    public IPrimitiveValueProvider createPrimitiveValueProvider() {
        final IPointable p = pf.createPointable();
        ITypeTraits traits = pf.getTypeTraits();
        assert traits.isFixedLength();
        final int length = traits.getFixedLength();
        return new IPrimitiveValueProvider() {
            @Override
            public double getValue(byte[] bytes, int offset) {
                p.set(bytes, offset, length);
                return ((INumeric) p).doubleValue();
            }
        };
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        final ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.set("pf", pf.toJson(registry));
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final IPointableFactory pf = (IPointableFactory) registry.deserialize(json.get("pf"));
        return new PointablePrimitiveValueProviderFactory(pf);
    }
}
