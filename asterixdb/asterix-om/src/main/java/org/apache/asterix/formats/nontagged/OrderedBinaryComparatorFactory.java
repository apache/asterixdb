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

import org.apache.asterix.dataflow.data.nontagged.comparators.ABinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OrderedBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    private final boolean ascending;
    private final IBinaryComparatorFactory factory;

    public OrderedBinaryComparatorFactory(IBinaryComparatorFactory factory, boolean ascending) {
        this.factory = factory;
        this.ascending = ascending;
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        final IBinaryComparator bc = factory.createBinaryComparator();
        if (ascending) {
            return new ABinaryComparator() {

                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {
                    return bc.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                }
            };
        } else {
            return new ABinaryComparator() {
                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {
                    return -bc.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                }
            };
        }
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.set("factory", factory.toJson(registry));
        json.put("ascending", ascending);
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final IBinaryComparatorFactory factory = (IBinaryComparatorFactory) registry.deserialize(json.get("factory"));
        return new OrderedBinaryComparatorFactory(factory, json.get("ascending").asBoolean());
    }
}
