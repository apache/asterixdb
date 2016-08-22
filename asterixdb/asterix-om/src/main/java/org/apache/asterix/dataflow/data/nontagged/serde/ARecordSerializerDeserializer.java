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
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class ARecordSerializerDeserializer implements ISerializerDeserializer<ARecord> {
    private static final long serialVersionUID = 1L;

    public static final ARecordSerializerDeserializer SCHEMALESS_INSTANCE = new ARecordSerializerDeserializer();

    private final ARecordType recordType;
    private final int numberOfSchemaFields;

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer serializers[];
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer deserializers[];

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
                IAType t2;
                if (t.getTypeTag() == ATypeTag.UNION) {
                    t2 = ((AUnionType) t).getActualType();
                } else {
                    t2 = t;
                }
                serializers[i] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(t2);
                deserializers[i] = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(t2);
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
            boolean isExpanded = false;
            in.readInt(); // recordSize
            if (recordType == null) {
                isExpanded = in.readBoolean();
                in.readInt(); // openPartOffset
            } else {
                if (recordType.isOpen()) {
                    isExpanded = in.readBoolean();
                    if (isExpanded) {
                        in.readInt(); // openPartOffset
                    }
                } else {
                    isExpanded = false;
                }
            }
            IAObject[] closedFields = null;
            if (numberOfSchemaFields > 0) {
                in.readInt(); // read number of closed fields.
                boolean hasOptionalFields = NonTaggedFormatUtil.hasOptionalField(this.recordType);
                byte[] nullBitMap = null;
                if (hasOptionalFields) {
                    int nullBitMapSize = (int) (Math.ceil(numberOfSchemaFields / 4.0));
                    nullBitMap = new byte[nullBitMapSize];
                    in.readFully(nullBitMap);
                }
                closedFields = new IAObject[numberOfSchemaFields];
                for (int i = 0; i < numberOfSchemaFields; i++) {
                    in.readInt();
                }
                for (int fieldId = 0; fieldId < numberOfSchemaFields; fieldId++) {
                    if (hasOptionalFields && ((nullBitMap[fieldId / 4] & (1 << (7 - 2 * (fieldId % 4)))) == 0)) {
                        closedFields[fieldId] = ANull.NULL;
                        continue;
                    }
                    if (hasOptionalFields && ((nullBitMap[fieldId / 4] & (1 << (7 - 2 * (fieldId % 4) - 1))) == 0)) {
                        closedFields[fieldId] = AMissing.MISSING;
                        continue;
                    }
                    closedFields[fieldId] = (IAObject) deserializers[fieldId].deserialize(in);
                }
            }

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
                    IAObject[] mergedFields = mergeFields(closedFields, openFields);
                    return new ARecord(mergedRecordType, mergedFields);
                } else {
                    return new ARecord(openPartRecType, openFields);
                }
            } else {
                return new ARecord(this.recordType, closedFields);
            }
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void serialize(ARecord instance, DataOutput out) throws HyracksDataException {
        this.serialize(instance, out, false);
    }

    // This serialize method will NOT work if <code>recordType</code> is not equal to the type of the instance.
    @SuppressWarnings("unchecked")
    public void serialize(ARecord instance, DataOutput out, boolean writeTypeTag) throws HyracksDataException {
        IARecordBuilder recordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        recordBuilder.reset(recordType);
        recordBuilder.init();
        if (recordType != null) {
            int fieldIndex = 0;
            for (; fieldIndex < recordType.getFieldNames().length; ++fieldIndex) {
                fieldValue.reset();
                serializers[fieldIndex].serialize(instance.getValueByPos(fieldIndex), fieldValue.getDataOutput());
                recordBuilder.addField(fieldIndex, fieldValue);
            }
            recordBuilder.write(out, writeTypeTag);
        } else {
            serializeSchemalessRecord(instance, out, writeTypeTag);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void serializeSchemalessRecord(ARecord record, DataOutput dataOutput, boolean writeTypeTag)
            throws HyracksDataException {
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);
        RecordBuilder confRecordBuilder = new RecordBuilder();
        confRecordBuilder.reset(ARecordType.FULLY_OPEN_RECORD_TYPE);
        ArrayBackedValueStorage fieldNameBytes = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldValueBytes = new ArrayBackedValueStorage();
        for (int i = 0; i < record.getType().getFieldNames().length; i++) {
            String fieldName = record.getType().getFieldNames()[i];
            fieldValueBytes.reset();
            fieldNameBytes.reset();
            stringSerde.serialize(new AString(fieldName), fieldNameBytes.getDataOutput());
            ISerializerDeserializer valueSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(record.getType().getFieldTypes()[i]);
            valueSerde.serialize(record.getValueByPos(i), fieldValueBytes.getDataOutput());
            confRecordBuilder.addField(fieldNameBytes, fieldValueBytes);
        }
        confRecordBuilder.write(dataOutput, writeTypeTag);
    }

    private IAObject[] mergeFields(IAObject[] closedFields, IAObject[] openFields) {
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

    private ARecordType mergeRecordTypes(ARecordType recType1, ARecordType recType2) throws AsterixException {

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

    public static final int getRecordLength(byte[] serRecord, int offset) {
        return AInt32SerializerDeserializer.getInt(serRecord, offset);
    }

    public static final int getFieldOffsetById(byte[] serRecord, int offset, int fieldId, int nullBitmapSize,
            boolean isOpen) {
        byte nullTestCode = (byte) (1 << (7 - 2 * (fieldId % 4)));
        byte missingTestCode = (byte) (1 << (7 - 2 * (fieldId % 4) - 1));
        if (isOpen) {
            if (serRecord[0 + offset] == ATypeTag.RECORD.serialize()) {
                // 5 is the index of the byte that determines whether the record
                // is expanded or not, i.e. it has an open part.
                if (serRecord[5 + offset] == 1) { // true
                    if (nullBitmapSize > 0) {
                        // 14 = tag (1) + record Size (4) + isExpanded (1) +
                        // offset of openPart (4) + number of closed fields (4)
                        int pos = 14 + offset + fieldId / 4;
                        if ((serRecord[pos] & nullTestCode) == 0) {
                            // the field value is null
                            return 0;
                        }
                        if ((serRecord[pos] & missingTestCode) == 0) {
                            // the field value is missing
                            return -1;
                        }
                    }
                    return offset + AInt32SerializerDeserializer.getInt(serRecord,
                            14 + offset + nullBitmapSize + (4 * fieldId));
                } else {
                    if (nullBitmapSize > 0) {
                        // 9 = tag (1) + record Size (4) + isExpanded (1) +
                        // number of closed fields (4)
                        int pos = 10 + offset + fieldId / 4;
                        if ((serRecord[pos] & nullTestCode) == 0) {
                            // the field value is null
                            return 0;
                        }
                        if ((serRecord[pos] & missingTestCode) == 0) {
                            // the field value is missing
                            return -1;
                        }
                    }
                    return offset + AInt32SerializerDeserializer.getInt(serRecord,
                            10 + offset + nullBitmapSize + (4 * fieldId));
                }
            } else {
                return -1;
            }
        } else {
            if (serRecord[offset] != ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
                return Integer.MIN_VALUE;
            }
            if (nullBitmapSize > 0) {
                // 9 = tag (1) + record Size (4) + number of closed fields
                // (4)
                int pos = 9 + offset + fieldId / 4;
                if ((serRecord[pos] & nullTestCode) == 0) {
                    // the field value is null
                    return 0;
                }
                if ((serRecord[pos] & missingTestCode) == 0) {
                    // the field value is missing
                    return -1;
                }
            }
            return offset + AInt32SerializerDeserializer.getInt(serRecord, 9 + offset + nullBitmapSize + (4 * fieldId));
        }
    }

    public static final int getFieldOffsetByName(byte[] serRecord, int start, int len, byte[] fieldName, int nstart)
            throws HyracksDataException {
        int openPartOffset;
        if (serRecord[start] == ATypeTag.SERIALIZED_RECORD_TYPE_TAG) {
            if (len <= 5) {
                // Empty record
                return -1;
            }
            // 5 is the index of the byte that determines whether the record is
            // expanded or not, i.e. it has an open part.
            if (serRecord[start + 5] == 1) { // true
                // 6 is the index of the first byte of the openPartOffset value.
                openPartOffset = start + AInt32SerializerDeserializer.getInt(serRecord, start + 6);
            } else {
                return -1; // this record does not have an open part
            }
        } else {
            return -1; // this record does not have an open part
        }

        int numberOfOpenField = AInt32SerializerDeserializer.getInt(serRecord, openPartOffset);
        int fieldUtflength = UTF8StringUtil.getUTFLength(fieldName, nstart + 1);
        int fieldUtfMetaLen = UTF8StringUtil.getNumBytesToStoreLength(fieldUtflength);

        IBinaryHashFunction utf8HashFunction = AqlBinaryHashFunctionFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
                .createBinaryHashFunction();

        IBinaryComparator utf8BinaryComparator = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
                .createBinaryComparator();

        int fieldNameHashCode = utf8HashFunction.hash(fieldName, nstart + 1, fieldUtflength + fieldUtfMetaLen);

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
                if (utf8BinaryComparator.compare(serRecord, fieldOffset, len, fieldName, nstart + 1,
                        fieldUtflength + fieldUtfMetaLen) == 0) {
                    // since they are equal, we can directly use the meta length and the utf length.
                    return fieldOffset + fieldUtfMetaLen + fieldUtflength;
                } else { // this else part has not been tested yet
                    for (int j = mid + 1; j < numberOfOpenField; j++) {
                        h = AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * j));
                        if (h == fieldNameHashCode) {
                            fieldOffset = start + AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * j) + 4);
                            if (utf8BinaryComparator.compare(serRecord, fieldOffset, len, fieldName, nstart + 1,
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

    @Override
    public String toString() {
        return " ";
    }
}
