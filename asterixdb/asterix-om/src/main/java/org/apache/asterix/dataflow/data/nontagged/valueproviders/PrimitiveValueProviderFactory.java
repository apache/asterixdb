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
package org.apache.asterix.dataflow.data.nontagged.valueproviders;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.DoublePrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.FloatPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.rtree.impls.IntegerPrimitiveValueProviderFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class PrimitiveValueProviderFactory implements IPrimitiveValueProviderFactory {

    private static final long serialVersionUID = 1L;

    public static final PrimitiveValueProviderFactory INSTANCE = new PrimitiveValueProviderFactory();

    private PrimitiveValueProviderFactory() {
    }

    @Override
    public IPrimitiveValueProvider createPrimitiveValueProvider() {
        return new IPrimitiveValueProvider() {
            final IPrimitiveValueProvider intProvider =
                    IntegerPrimitiveValueProviderFactory.INSTANCE.createPrimitiveValueProvider();
            final IPrimitiveValueProvider floatProvider =
                    FloatPrimitiveValueProviderFactory.INSTANCE.createPrimitiveValueProvider();
            final IPrimitiveValueProvider doubleProvider =
                    DoublePrimitiveValueProviderFactory.INSTANCE.createPrimitiveValueProvider();

            @Override
            public double getValue(byte[] bytes, int offset) {
                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
                switch (tag) {
                    case INTEGER:
                        return intProvider.getValue(bytes, offset + 1);
                    case FLOAT:
                        return floatProvider.getValue(bytes, offset + 1);
                    case DOUBLE:
                        return doubleProvider.getValue(bytes, offset + 1);
                    default:
                        throw new NotImplementedException("Value provider for type " + tag + " is not implemented");
                }
            }
        };
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
