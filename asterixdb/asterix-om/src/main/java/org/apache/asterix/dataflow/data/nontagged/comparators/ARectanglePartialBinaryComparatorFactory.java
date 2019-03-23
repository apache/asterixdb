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
package org.apache.asterix.dataflow.data.nontagged.comparators;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;

public class ARectanglePartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    public static final ARectanglePartialBinaryComparatorFactory INSTANCE =
            new ARectanglePartialBinaryComparatorFactory();

    private ARectanglePartialBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return ARectanglePartialBinaryComparatorFactory::compare;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int c1 = Double.compare(ADoubleSerializerDeserializer.getDouble(b1, s1),
                ADoubleSerializerDeserializer.getDouble(b2, s2));
        if (c1 == 0) {
            int c2 = Double.compare(ADoubleSerializerDeserializer.getDouble(b1, s1 + 8),
                    ADoubleSerializerDeserializer.getDouble(b2, s2 + 8));
            if (c2 == 0) {
                int c3 = Double.compare(ADoubleSerializerDeserializer.getDouble(b1, s1 + 16),
                        ADoubleSerializerDeserializer.getDouble(b2, s2 + 16));
                if (c3 == 0) {
                    int c4 = Double.compare(ADoubleSerializerDeserializer.getDouble(b1, s1 + 24),
                            ADoubleSerializerDeserializer.getDouble(b2, s2 + 24));
                    return c4;
                } else {
                    return c3;
                }
            } else {
                return c2;
            }
        } else {
            return c1;
        }
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
