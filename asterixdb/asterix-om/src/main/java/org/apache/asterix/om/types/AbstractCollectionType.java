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
package org.apache.asterix.om.types;

import java.util.Objects;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class AbstractCollectionType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;
    private static final String ITEM_TYPE_FIELD = "itemType";
    private IAType itemType;

    AbstractCollectionType(IAType itemType, String typeName) {
        super(typeName);
        this.itemType = Objects.requireNonNull(itemType);
    }

    public boolean isTyped() {
        // TODO(ali): this should be removed. itemType is already enforced to be NonNull
        return itemType != null;
    }

    public IAType getItemType() {
        return itemType;
    }

    public void setItemType(IAType itemType) {
        this.itemType = Objects.requireNonNull(itemType);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ALL_TYPE;
    }

    @Override
    public void generateNestedDerivedTypeNames() {
        if (itemType.getTypeTag().isDerivedType() && itemType.getTypeName() == null) {
            AbstractComplexType nestedType = (AbstractComplexType) itemType;
            nestedType.setTypeName(getTypeName() + "_Item");
            nestedType.generateNestedDerivedTypeNames();
        }
    }

    @Override
    public boolean containsType(IAType type) {
        return isTyped() && itemType.getTypeName().equals(type.getTypeName());
    }

    JsonNode convertToJson(IPersistedResourceRegistry registry, Class<? extends IJsonSerializable> clazz, long version)
            throws HyracksDataException {
        final ObjectNode jsonObject = registry.getClassIdentifier(clazz, version);
        addToJson(jsonObject);
        jsonObject.set(ITEM_TYPE_FIELD, itemType.toJson(registry));
        return jsonObject;
    }

    static IJsonSerializable convertToObject(IPersistedResourceRegistry registry, JsonNode json, boolean ordered)
            throws HyracksDataException {
        String typeName = json.get(TYPE_NAME_FIELD).asText();
        JsonNode itemTypeJson = json.get(ITEM_TYPE_FIELD);
        IAType itemType = (IAType) registry.deserialize(itemTypeJson);
        return ordered ? new AOrderedListType(itemType, typeName) : new AUnorderedListType(itemType, typeName);
    }
}
