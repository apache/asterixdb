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

package org.apache.asterix.om.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

public class JSONDeserializerForTypes {
    private static final Map<String, IAType> primitiveTypeMap;
    static {
        primitiveTypeMap = new HashMap<>();
        Class<?> buildInTypeClass = BuiltinType.class;
        Stream.of(buildInTypeClass.getDeclaredFields()).filter(f -> f.getType().isAssignableFrom(BuiltinType.class))
                .forEach(f -> {
                    try {
                        primitiveTypeMap.put(f.getName().toUpperCase(), (IAType) f.get(null));
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException(e);
                    }
                });
        // for backward-compatibility
        primitiveTypeMap.put("ASTERIX_TYPE", BuiltinType.ALL_TYPE);
    }

    private JSONDeserializerForTypes() {
    }

    /**
     * Deserialize an arbitrary JSON representation of a type.
     *
     * @param typeInJSON
     *            the JSON representation of the type.
     * @return an valid AsterixDB type.
     * @throws Exception
     */
    public static IAType convertFromJSON(JsonNode typeInJSON) throws Exception {
        String typeName = typeInJSON.get("type").asText();
        // Deals with ordered list.
        if (typeName.equals(AOrderedListType.class.getName())) {
            IAType itemType = convertFromJSON(typeInJSON.get("item-type"));
            return new AOrderedListType(itemType, "ordered-list");
        }

        // Deals with unordered list.
        if (typeName.equals(AUnorderedListType.class.getName())) {
            IAType itemType = convertFromJSON(typeInJSON.get("item-type"));
            return new AUnorderedListType(itemType, "unordered-list");
        }

        // Deals with Union Type.
        if (typeName.equals(AUnionType.class.getName())) {
            List<IAType> unionTypes = new ArrayList<>();
            JsonNode fields = typeInJSON.get("fields");
            for (int i = 0; i < fields.size(); i++) {
                JsonNode fieldType = fields.get(i);
                unionTypes.add(convertFromJSON(fieldType));
            }
            return new AUnionType(unionTypes, "union");
        }

        // Deals with record types.
        if (typeName.equals(ARecordType.class.getName())) {
            String name = typeInJSON.get("name").asText();
            boolean openType = typeInJSON.get("open").asBoolean();
            JsonNode fields = typeInJSON.get("fields");
            String[] fieldNames = new String[fields.size()];
            IAType[] fieldTypes = new IAType[fields.size()];
            for (int i = 0; i < fields.size(); ++i) {
                JsonNode field = fields.get(i);
                List<String> names = Lists.newArrayList(field.fieldNames());
                String fieldName = names.get(0);
                fieldNames[i] = fieldName;
                fieldTypes[i] = convertFromJSON(field.get(fieldName));
            }
            return new ARecordType(name, fieldNames, fieldTypes, openType);
        }

        // Deals with primitive types.
        return primitiveTypeMap.get(typeName.toUpperCase());
    }
}
