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

package org.apache.asterix.om.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class JSONDeserializerForTypes {

    /**
     * Deserialize an arbitrary JSON representation of a type.
     *
     * @param typeInJSON
     *            the JSON representation of the type.
     * @return an valid AsterixDB type.
     * @throws Exception
     */
    public static IAType convertFromJSON(JSONObject typeInJSON) throws Exception {
        String typeName = typeInJSON.getString("type");
        // Deals with ordered list.
        if (typeName.equals(AOrderedListType.class.getName())) {
            IAType itemType = convertFromJSON((JSONObject) typeInJSON.get("item-type"));
            return new AOrderedListType(itemType, "ordered-list");
        }

        // Deals with unordered list.
        if (typeName.equals(AUnorderedListType.class.getName())) {
            IAType itemType = convertFromJSON((JSONObject) typeInJSON.get("item-type"));
            return new AUnorderedListType(itemType, "unordered-list");
        }

        // Deals with Union Type.
        if (typeName.equals(AUnionType.class.getName())) {
            List<IAType> unionTypes = new ArrayList<IAType>();
            JSONArray fields = (JSONArray) typeInJSON.get("fields");
            for (int i = 0; i < fields.length(); i++) {
                JSONObject fieldType = (JSONObject) fields.get(i);
                unionTypes.add(convertFromJSON(fieldType));
            }
            return new AUnionType(unionTypes, "union");
        }

        // Deals with record types.
        if (typeName.equals(ARecordType.class.getName())) {
            String name = typeInJSON.getString("name");
            boolean openType = typeInJSON.getBoolean("open");
            JSONArray fields = typeInJSON.getJSONArray("fields");
            String[] fieldNames = new String[fields.length()];
            IAType[] fieldTypes = new IAType[fields.length()];
            for (int i = 0; i < fields.length(); ++i) {
                JSONObject field = (JSONObject) fields.get(i);
                JSONArray names = field.names();
                String fieldName = names.getString(0);
                fieldNames[i] = fieldName;
                fieldTypes[i] = convertFromJSON((JSONObject) field.get(fieldName));
            }
            return new ARecordType(name, fieldNames, fieldTypes, openType);
        }

        // Deals with primitive types.
        Class<?> cl = BuiltinType.class;
        Field typeField = cl.getDeclaredField(typeName.toUpperCase());
        return (IAType) typeField.get(null);
    }
}
