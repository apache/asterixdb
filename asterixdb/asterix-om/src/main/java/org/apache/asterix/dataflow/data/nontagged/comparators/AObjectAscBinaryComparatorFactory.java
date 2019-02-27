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

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AObjectAscBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    // these fields can be null
    private final IAType leftType;
    private final IAType rightType;
    private final boolean ascending;

    public AObjectAscBinaryComparatorFactory(IAType leftType, IAType rightType) {
        this(leftType, rightType, true);
    }

    protected AObjectAscBinaryComparatorFactory(IAType leftType, IAType rightType, boolean ascending) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.ascending = ascending;
    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return ascending ? new AGenericAscBinaryComparator(leftType, rightType)
                : new AGenericDescBinaryComparator(leftType, rightType);
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        return convertToJson(registry, getClass(), serialVersionUID);
    }

    JsonNode convertToJson(IPersistedResourceRegistry registry, Class<? extends IJsonSerializable> clazz, long version)
            throws HyracksDataException {
        ObjectNode jsonNode = registry.getClassIdentifier(clazz, version);
        if (leftType != null) {
            jsonNode.set("leftType", leftType.toJson(registry));
        }
        if (rightType != null) {
            jsonNode.set("rightType", rightType.toJson(registry));
        }
        return jsonNode;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        return convertToObject(registry, json, true);
    }

    static IJsonSerializable convertToObject(IPersistedResourceRegistry registry, JsonNode json, boolean asc)
            throws HyracksDataException {
        JsonNode leftNode = json.get("leftType");
        JsonNode rightNode = json.get("rightType");
        IAType leftType = leftNode == null || leftNode.isNull() ? null : (IAType) registry.deserialize(leftNode);
        IAType rightType = rightNode == null || rightNode.isNull() ? null : (IAType) registry.deserialize(rightNode);
        return asc ? new AObjectAscBinaryComparatorFactory(leftType, rightType)
                : new AObjectDescBinaryComparatorFactory(leftType, rightType);
    }
}
