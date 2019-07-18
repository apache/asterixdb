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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class ARecordSerializerDeserializer implements ISerializerDeserializer<ARecord> {

    private static final long serialVersionUID = 1L;
    // TODO(ali): move PointableHelper to a lower package where this can see and reuse code from PointableHelper
    private static final byte[] NULL_BYTES = new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG };
    private static final byte[] MISSING_BYTES = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG };
    public static final ARecordSerializerDeserializer SCHEMALESS_INSTANCE = new ARecordSerializerDeserializer();
    private static final IAObject[] NO_FIELDS = new IAObject[0];
    private final ARecordType recordType;
    private final int numberOfSchemaFields;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] serializers;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] deserializers;

    private ARecordSerializerDeserializer() {
        this(null);
    }

    public ARecordSerializerDeserializer(ARecordType recordType) {
        if (recordType != null) {
            this.recordType = recordType;
            this.numberOfSchemaFields = recordType.getFieldNames().length;
            serializers = new ISerializerDeserializer[numberOfSchemaFields];
            deserializers = new ISerializerDeserializer[numberOfSchemaFields];
            for (int i = 0; i < numberOfSchemaFields; i++) {
                IAType t = recordType.getFieldTypes()[i];
                IAType t2 = (t.getTypeTag() == ATypeTag.UNION) ? ((AUnionType) t).getActualType() : t;
                serializers[i] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(t2);
                deserializers[i] = SerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(t2);
            }
        } else {
            this.recordType = null;
            this.numberOfSchemaFields = 0;
            this.serializers = null;
            this.deserializers = null;
        }
    }

    @Override
    public ARecord deserialize(DataInput in) throws HyracksDataException {
        try {
            boolean isExpanded = isExpandedRecord(in);
            IAObject[] schemaFields = getValuesForSchemaFields(in);

            if (isExpanded) {
                int numberOfOpenFields = in.readInt();
                String[] fieldNames = new String[numberOfOpenFields];
                IAType[] fieldTypes = new IAType[numberOfOpenFields];
                IAObject[] openFields = new IAObject[numberOfOpenFields];
                for (int i = 0; i < numberOfOpenFields; i++) {
                    in.readInt();
                    in.readInt();
                }
                for (int i = 0; i < numberOfOpenFields; i++) {
                    fieldNames[i] = AStringSerializerDeserializer.INSTANCE.deserialize(in).getStringValue();
                    openFields[i] = AObjectSerializerDeserializer.INSTANCE.deserialize(in);
                    fieldTypes[i] = openFields[i].getType();
                }
                ARecordType openPartRecType = new ARecordType(null, fieldNames, fieldTypes, true);
                if (numberOfSchemaFields > 0) {
                    ARecordType mergedRecordType = mergeRecordTypes(this.recordType, openPartRecType);
                    IAObject[] mergedFields = mergeFields(schemaFields, openFields);
                    return new ARecord(mergedRecordType, mergedFields);
                } else {
                    return new ARecord(openPartRecType, openFields);
                }
            } else {
                return new ARecord(this.recordType, schemaFields);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private boolean isExpandedRecord(DataInput in) throws IOException {
        in.readInt(); // recordSize
        if (recordType == null) {
            boolean exp = in.readBoolean();
            in.readInt(); // openPartOffset
            return exp;
        } else {
            if (recordType.isOpen()) {
                boolean exp = in.readBoolean();
                if (exp) {
                    in.readInt(); // openPartOffset
                }
                return exp;
            }
            return false;
        }
    }

    private IAObject[] getValuesForSchemaFields(DataInput in) throws IOException {
        if (numberOfSchemaFields <= 0) {
            return NO_FIELDS;
        }
        in.readInt(); // read number of schema fields.
        boolean hasOptionalFields = NonTaggedFormatUtil.hasOptionalField(this.recordType);
        byte[] nullBitMap = null;
        if (hasOptionalFields) {
            int nullBitMapSize = (int) (Math.ceil(numberOfSchemaFields / 4.0));
            nullBitMap = new byte[nullBitMapSize];
            in.readFully(nullBitMap);
        }
        for (int i = 0; i < numberOfSchemaFields; i++) {
            in.readInt();
        }
        IAObject[] schemaFields = new IAObject[numberOfSchemaFields];
        for (int fieldId = 0; fieldId < numberOfSchemaFields; fieldId++) {
            // TODO: null/missing formula is duplicated across the codebase. should be in a central place.
            if (hasOptionalFields && ((nullBitMap[fieldId / 4] & (1 << (7 - 2 * (fieldId % 4)))) == 0)) {
                schemaFields[fieldId] = ANull.NULL;
            } else if (hasOptionalFields && ((nullBitMap[fieldId / 4] & (1 << (7 - 2 * (fieldId % 4) - 1))) == 0)) {
                schemaFields[fieldId] = AMissing.MISSING;
            } else {
                schemaFields[fieldId] = (IAObject) deserializers[fieldId].deserialize(in);
            }
        }
        return schemaFields;
    }

    // This serialize method will NOT work if <code>recordType</code> is not equal to the type of the instance.
    @SuppressWarnings("unchecked")
    @Override
    public void serialize(ARecord instance, DataOutput out) throws HyracksDataException {
        if (recordType != null) {
            IARecordBuilder recordBuilder = new RecordBuilder();
            ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
            recordBuilder.reset(recordType);
            recordBuilder.init();
            IAType[] fieldTypes = recordType.getFieldTypes();
            for (int fieldIndex = 0; fieldIndex < recordType.getFieldNames().length; ++fieldIndex) {
                fieldValue.reset();
                IAObject value = instance.getValueByPos(fieldIndex);
                ATypeTag valueTag = value.getType().getTypeTag();
                boolean fieldIsOptional = NonTaggedFormatUtil.isOptional(fieldTypes[fieldIndex]);
                if (fieldIsOptional && valueTag == ATypeTag.NULL) {
                    fieldValue.set(NULL_BYTES, 0, NULL_BYTES.length);
                } else if (fieldIsOptional && valueTag == ATypeTag.MISSING) {
                    fieldValue.set(MISSING_BYTES, 0, MISSING_BYTES.length);
                } else {
                    serializers[fieldIndex].serialize(value, fieldValue.getDataOutput());
                }
                recordBuilder.addField(fieldIndex, fieldValue);
            }
            recordBuilder.write(out, false);
        } else {
            serializeSchemalessRecord(instance, out);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void serializeSchemalessRecord(ARecord record, DataOutput dataOutput) throws HyracksDataException {
        ISerializerDeserializer<AString> stringSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
        RecordBuilder confRecordBuilder = new RecordBuilder();
        confRecordBuilder.reset(RecordUtil.FULLY_OPEN_RECORD_TYPE);
        ArrayBackedValueStorage fieldNameBytes = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldValueBytes = new ArrayBackedValueStorage();
        AMutableString mutableString = new AMutableString(null);
        for (int i = 0; i < record.getType().getFieldNames().length; i++) {
            String fieldName = record.getType().getFieldNames()[i];
            fieldValueBytes.reset();
            fieldNameBytes.reset();
            mutableString.setValue(fieldName);
            stringSerde.serialize(mutableString, fieldNameBytes.getDataOutput());
            AObjectSerializerDeserializer.INSTANCE.serialize(record.getValueByPos(i), fieldValueBytes.getDataOutput());
            confRecordBuilder.addField(fieldNameBytes, fieldValueBytes);
        }
        confRecordBuilder.write(dataOutput, false);
    }

    public static IAObject[] mergeFields(IAObject[] closedFields, IAObject[] openFields) {
        IAObject[] fields = new IAObject[closedFields.length + openFields.length];
        int i = 0;
        for (; i < closedFields.length; i++) {
            fields[i] = closedFields[i];
        }
        for (int j = 0; j < openFields.length; j++) {
            fields[closedFields.length + j] = openFields[j];
        }
        return fields;
    }

    public static ARecordType mergeRecordTypes(ARecordType recType1, ARecordType recType2) {
        String[] fieldNames = new String[recType1.getFieldNames().length + recType2.getFieldNames().length];
        IAType[] fieldTypes = new IAType[recType1.getFieldTypes().length + recType2.getFieldTypes().length];

        int i = 0;
        for (; i < recType1.getFieldNames().length; i++) {
            fieldNames[i] = recType1.getFieldNames()[i];
            fieldTypes[i] = recType1.getFieldTypes()[i];
        }

        for (int j = 0; j < recType2.getFieldNames().length; i++, j++) {
            fieldNames[i] = recType2.getFieldNames()[j];
            fieldTypes[i] = recType2.getFieldTypes()[j];
        }
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    public static int getRecordLength(byte[] serRecord, int offset) {
        return AInt32SerializerDeserializer.getInt(serRecord, offset);
    }

    public static int getFieldOffsetById(byte[] serRecord, int offset, int fieldId, int nullBitmapSize,
            boolean isOpen) {
        final byte nullTestCode = (byte) (1 << (7 - 2 * (fieldId % 4)));
        final byte missingTestCode = (byte) (1 << (7 - 2 * (fieldId % 4) - 1));

        //early exit if not Record
        if (serRecord[offset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
            return -1;
        }

        //advance to isExpanded or numberOfSchemaFields
        int pointer = offset + 5;

        if (isOpen) {
            final boolean isExpanded = serRecord[pointer] == 1;
            //if isExpanded, advance to numberOfSchemaFields
            pointer += 1 + (isExpanded ? 4 : 0);
        }

        //advance to nullBitmap
        pointer += 4;

        if (nullBitmapSize > 0) {
            final int pos = pointer + fieldId / 4;
            if ((serRecord[pos] & nullTestCode) == 0) {
                // the field value is null
                return 0;
            }
            if ((serRecord[pos] & missingTestCode) == 0) {
                // the field value is missing
                return -1;
            }
        }

        return offset + AInt32SerializerDeserializer.getInt(serRecord, pointer + nullBitmapSize + (4 * fieldId));
    }

    public static int getFieldOffsetByName(byte[] serRecord, int start, int len, byte[] fieldName, int nstart,
            IBinaryHashFunction nameHashFunction, IBinaryComparator nameComparator) throws HyracksDataException {
        // 5 is the index of the byte that determines whether the record is expanded or not, i.e. it has an open part.
        if (hasNoFields(serRecord, start, len) || serRecord[start + 5] != 1) {
            return -1;
        }
        // 6 is the index of the first byte of the openPartOffset value.
        int openPartOffset = start + AInt32SerializerDeserializer.getInt(serRecord, start + 6);
        int numberOfOpenField = AInt32SerializerDeserializer.getInt(serRecord, openPartOffset);
        int fieldUtflength = UTF8StringUtil.getUTFLength(fieldName, nstart + 1);
        int fieldUtfMetaLen = UTF8StringUtil.getNumBytesToStoreLength(fieldUtflength);
        int fieldNameHashCode = nameHashFunction.hash(fieldName, nstart + 1, fieldUtflength + fieldUtfMetaLen);

        int offset = openPartOffset + 4;
        int fieldOffset = -1;
        int mid = 0;
        int high = numberOfOpenField - 1;
        int low = 0;
        while (low <= high) {
            mid = (high + low) / 2;
            // 8 = hash code (4) + offset to the (name + tag + value ) of the field (4).
            int h = AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * mid));
            if (h == fieldNameHashCode) {
                fieldOffset = start + AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * mid) + 4);
                // the utf8 comparator do not require to put the precise length, we can just pass a estimated limit.
                if (nameComparator.compare(serRecord, fieldOffset, len, fieldName, nstart + 1,
                        fieldUtflength + fieldUtfMetaLen) == 0) {
                    // since they are equal, we can directly use the meta length and the utf length.
                    return fieldOffset + fieldUtfMetaLen + fieldUtflength;
                } else { // this else part has not been tested yet
                    for (int j = mid + 1; j < numberOfOpenField; j++) {
                        h = AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * j));
                        if (h == fieldNameHashCode) {
                            fieldOffset = start + AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * j) + 4);
                            if (nameComparator.compare(serRecord, fieldOffset, len, fieldName, nstart + 1,
                                    fieldUtflength) == 0) {
                                return fieldOffset + fieldUtfMetaLen + fieldUtflength;
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
            if (fieldNameHashCode > h) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }

        }
        return -1; // no field with this name.
    }

    public static boolean hasNoFields(byte[] serRecord, int start, int len) {
        // a record with len <= 6 is empty
        return serRecord[start] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG || len <= 6;
    }

    @Override
    public String toString() {
        return recordType != null ? recordType.toString() : "";
    }
}
