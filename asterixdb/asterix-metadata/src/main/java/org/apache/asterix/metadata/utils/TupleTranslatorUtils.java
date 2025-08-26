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
package org.apache.asterix.metadata.utils;

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.PROPERTIES_NAME_FIELD_INDEX;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.PROPERTIES_VALUE_FIELD_INDEX;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class TupleTranslatorUtils {

    public static Map<String, String> getPropertiesFromIaCursor(IACursor cursor) {
        Map<String, String> properties = new HashMap<>();
        while (cursor.next()) {
            ARecord field = (ARecord) cursor.get();
            String key = ((AString) field.getValueByPos(PROPERTIES_NAME_FIELD_INDEX)).getStringValue();
            String value = ((AString) field.getValueByPos(PROPERTIES_VALUE_FIELD_INDEX)).getStringValue();
            properties.put(key, value);
        }
        return properties;
    }

    public static void writeCreator(Creator creator, IARecordBuilder recordBuilder, ArrayBackedValueStorage fieldName,
            ArrayBackedValueStorage fieldValue, AMutableString aString, ISerializerDeserializer<AString> stringSerde)
            throws HyracksDataException {
        RecordBuilder creatorObject = new RecordBuilder();
        creatorObject.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.FIELD_NAME_CREATOR_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(creator.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        creatorObject.addField(fieldName, fieldValue);

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.FIELD_NAME_CREATOR_UUID);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(creator.getUuid());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        creatorObject.addField(fieldName, fieldValue);

        fieldName.reset();
        aString.setValue(MetadataRecordTypes.CREATOR_ARECORD_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        creatorObject.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(fieldName, fieldValue);
    }
}
