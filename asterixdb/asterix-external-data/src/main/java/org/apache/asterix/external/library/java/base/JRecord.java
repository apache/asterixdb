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
package org.apache.asterix.external.library.java.base;

import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public final class JRecord implements IJObject {

    private static final AStringSerializerDeserializer aStringSerDer = AStringSerializerDeserializer.INSTANCE;
    private ARecordType recordType;
    private IJObject[] fields;
    private Map<String, IJObject> openFields;
    RecordBuilder recordBuilder = new RecordBuilder();
    ArrayBackedValueStorage fieldNameBuffer = new ArrayBackedValueStorage();
    ArrayBackedValueStorage fieldValueBuffer = new ArrayBackedValueStorage();
    AMutableString nameString = new AMutableString("");

    public JRecord(ARecordType recordType, IJObject[] fields) {
        this.recordType = recordType;
        this.fields = fields;
        this.openFields = new LinkedHashMap<>();
    }

    public JRecord(ARecordType recordType, IJObject[] fields, Map<String, IJObject> openFields) {
        this(recordType, fields);
        this.openFields = openFields;
    }

    public void addField(String fieldName, IJObject fieldValue) throws HyracksDataException {
        int pos = getFieldPosByName(fieldName);
        if (pos >= 0) {
            throw new RuntimeDataException(ErrorCode.LIBRARY_JAVA_JOBJECTS_FIELD_ALREADY_DEFINED, "closed");
        }
        if (openFields.get(fieldName) != null) {
            throw new RuntimeDataException(ErrorCode.LIBRARY_JAVA_JOBJECTS_FIELD_ALREADY_DEFINED, "open");
        }
        openFields.put(fieldName, fieldValue);
    }

    public IJObject getValueByName(String fieldName) throws HyracksDataException {
        // check closed part
        int fieldPos = getFieldPosByName(fieldName);
        if (fieldPos >= 0) {
            return fields[fieldPos];
        } else {
            // check open part
            IJObject fieldValue = openFields.get(fieldName);
            if (fieldValue == null) {
                throw new RuntimeDataException(ErrorCode.LIBRARY_JAVA_JOBJECTS_UNKNOWN_FIELD, fieldName);
            }
            return fieldValue;
        }
    }

    public void setValueAtPos(int pos, IJObject jObject) {
        fields[pos] = jObject;
    }

    public IAType getIAType() {
        return recordType;
    }

    public void setField(String fieldName, IJObject fieldValue) throws HyracksDataException {
        int pos = getFieldPosByName(fieldName);
        if (pos >= 0) {
            fields[pos] = fieldValue;
        } else {
            openFields.put(fieldName, fieldValue);
        }
    }

    private int getFieldPosByName(String fieldName) {
        int index = 0;
        String[] fieldNames = recordType.getFieldNames();
        for (String name : fieldNames) {
            if (name.equals(fieldName)) {
                return index;
            }
            index++;
        }
        return -1;
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    @Override
    public void serialize(DataOutput output, boolean writeTypeTag) throws HyracksDataException {
        recordBuilder.reset(recordType);
        int index = 0;
        for (IJObject jObject : fields) {
            fieldValueBuffer.reset();
            jObject.serialize(fieldValueBuffer.getDataOutput(), writeTypeTag);
            recordBuilder.addField(index, fieldValueBuffer);
            index++;
        }

        try {
            if (openFields != null && !openFields.isEmpty()) {
                for (Map.Entry<String, IJObject> entry : openFields.entrySet()) {
                    fieldNameBuffer.reset();
                    fieldValueBuffer.reset();
                    nameString.setValue(entry.getKey());
                    fieldNameBuffer.getDataOutput().write(ATypeTag.STRING.serialize());
                    aStringSerDer.serialize(nameString, fieldNameBuffer.getDataOutput());
                    entry.getValue().serialize(fieldValueBuffer.getDataOutput(), true);
                    recordBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                }
            }
        } catch (IOException ae) {
            throw HyracksDataException.create(ae);
        }
        recordBuilder.write(output, writeTypeTag);
    }

    @Override
    public IAObject getIAObject() {
        // As the open part can be changed any time, we cannot pre-allocate the arrays.
        int numberOfOpenFields = openFields.size();
        String[] openFieldNames = new String[numberOfOpenFields];
        IAType[] openFieldTypes = new IAType[numberOfOpenFields];
        IAObject[] openFieldValues = new IAObject[numberOfOpenFields];
        IAObject[] closedFieldValues = new IAObject[fields.length];
        int idx = 0;
        for (Map.Entry<String, IJObject> entry : openFields.entrySet()) {
            openFieldNames[idx] = entry.getKey();
            openFieldTypes[idx] = entry.getValue().getIAObject().getType();
            openFieldValues[idx] = entry.getValue().getIAObject();
        }
        for (int iter1 = 0; iter1 < fields.length; iter1++) {
            closedFieldValues[iter1] = fields[iter1].getIAObject();
        }
        ARecordType openPartRecType = new ARecordType(null, openFieldNames, openFieldTypes, true);
        ARecordType mergedRecordType = ARecordSerializerDeserializer.mergeRecordTypes(recordType, openPartRecType);
        IAObject[] mergedFields = ARecordSerializerDeserializer.mergeFields(closedFieldValues, openFieldValues);

        return new ARecord(mergedRecordType, mergedFields);
    }

    @Override
    public void reset() throws HyracksDataException {
        if (openFields != null && !openFields.isEmpty()) {
            openFields.clear();
        }
        if (fields != null) {
            for (IJObject field : fields) {
                if (field != null) {
                    field.reset();
                }
            }
        }
    }

    public void reset(IJObject[] fields, Map<String, IJObject> openFields) throws HyracksDataException {
        this.reset();
        this.fields = fields;
        this.openFields = openFields;
    }
}
