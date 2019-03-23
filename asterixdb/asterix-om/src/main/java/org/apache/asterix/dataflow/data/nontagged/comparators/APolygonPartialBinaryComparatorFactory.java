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

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.primitive.DoublePointable;

import com.fasterxml.jackson.databind.JsonNode;

public class APolygonPartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    public static final APolygonPartialBinaryComparatorFactory INSTANCE = new APolygonPartialBinaryComparatorFactory();

    private APolygonPartialBinaryComparatorFactory() {
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return APolygonPartialBinaryComparatorFactory::compare;
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {
        short pointCount1 = AInt16SerializerDeserializer.getShort(b1,
                s1 + APolygonSerializerDeserializer.getNumberOfPointsOffset() - 1);
        int c = Short.compare(pointCount1, AInt16SerializerDeserializer.getShort(b2,
                s2 + APolygonSerializerDeserializer.getNumberOfPointsOffset() - 1));
        if (c == 0) {
            int ci;
            for (int i = 0; i < pointCount1; i++) {
                ci = Double.compare(
                        DoublePointable.getDouble(b1,
                                s1 + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X) - 1),
                        DoublePointable.getDouble(b2,
                                s1 + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.X) - 1));
                if (ci == 0) {
                    ci = Double
                            .compare(
                                    DoublePointable.getDouble(
                                            b1, s1 + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y)
                                                    - 1),
                                    DoublePointable.getDouble(b2, s1
                                            + APolygonSerializerDeserializer.getCoordinateOffset(i, Coordinate.Y) - 1));
                    if (ci == 0) {
                        continue;
                    }
                }
                return ci;
            }
        }
        return c;
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
