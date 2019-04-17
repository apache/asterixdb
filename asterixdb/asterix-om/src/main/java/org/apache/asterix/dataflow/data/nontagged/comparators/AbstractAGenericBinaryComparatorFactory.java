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

import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

abstract class AbstractAGenericBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;
    final IAType leftType;
    final IAType rightType;

    AbstractAGenericBinaryComparatorFactory(IAType leftType, IAType rightType) {
        this.leftType = TypeComputeUtils.getActualType(leftType);
        this.rightType = TypeComputeUtils.getActualType(rightType);
    }

    JsonNode convertToJson(IPersistedResourceRegistry registry, Class<? extends IJsonSerializable> clazz, long version)
            throws HyracksDataException {
        ObjectNode jsonNode = registry.getClassIdentifier(clazz, version);
        jsonNode.set("leftType", leftType.toJson(registry));
        jsonNode.set("rightType", rightType.toJson(registry));
        return jsonNode;
    }

    static IJsonSerializable convertToObject(IPersistedResourceRegistry registry, JsonNode json, boolean asc)
            throws HyracksDataException {
        JsonNode left = json.get("leftType");
        JsonNode right = json.get("rightType");
        // default to ANY for comparators that didn't originally have the new type fields
        IAType leftType = left == null || left.isNull() ? BuiltinType.ANY : (IAType) registry.deserialize(left);
        IAType rightType = right == null || right.isNull() ? BuiltinType.ANY : (IAType) registry.deserialize(right);
        return asc ? new AGenericAscBinaryComparatorFactory(leftType, rightType)
                : new AGenericDescBinaryComparatorFactory(leftType, rightType);
    }
}
