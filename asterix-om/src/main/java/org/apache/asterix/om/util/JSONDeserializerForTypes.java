/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.om.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

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
        boolean typeNameExists = typeInJSON.has("type");
        String typeName = typeNameExists ? typeInJSON.getString("type") : null;

        // Deals with ordered list.
        if (typeNameExists && typeName.equals(AOrderedListType.class.getName())) {
            IAType itemType = convertFromJSON((JSONObject) typeInJSON.get("item-type"));
            return new AOrderedListType(itemType, "ordered-list");
        }

        // Deals with unordered list.
        if (typeNameExists && typeName.equals(AUnorderedListType.class.getName())) {
            IAType itemType = convertFromJSON((JSONObject) typeInJSON.get("item-type"));
            return new AUnorderedListType(itemType, "unordered-list");
        }

        // Deals with Union Type.
        if (typeNameExists && typeName.equals(AUnionType.class.getName())) {
            List<IAType> unionTypes = new ArrayList<IAType>();
            JSONArray fields = (JSONArray) typeInJSON.get("fields");
            for (int i = 0; i < fields.length(); i++) {
                JSONObject fieldType = (JSONObject) fields.get(i);
                unionTypes.add(convertFromJSON(fieldType));
            }
            return new AUnionType(unionTypes, "union");
        }

        // Deals with primitive types.
        if (typeNameExists) {
            Class<?> cl = BuiltinType.class;
            Field typeField = cl.getDeclaredField(typeName.toUpperCase());
            return (IAType) typeField.get(null);
        }

        // Deals with record types.
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
        return new ARecordType("record", fieldNames, fieldTypes, openType);
    }

}
