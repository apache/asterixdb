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

package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class ARecordSerializerDeserializer implements ISerializerDeserializer<ARecord> {
    private static final long serialVersionUID = 1L;

    public static final ARecordSerializerDeserializer SCHEMALESS_INSTANCE = new ARecordSerializerDeserializer();

    private ARecordType recordType;
    private int numberOfSchemaFields = 0;

    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer serializers[];
    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer deserializers[];

    private ARecordSerializerDeserializer() {
    }

    public ARecordSerializerDeserializer(ARecordType recordType) {
        this.recordType = recordType;
        if (recordType != null) {
            this.numberOfSchemaFields = recordType.getFieldNames().length;
            serializers = new ISerializerDeserializer[numberOfSchemaFields];
            deserializers = new ISerializerDeserializer[numberOfSchemaFields];
            for (int i = 0; i < numberOfSchemaFields; i++) {
                IAType t = recordType.getFieldTypes()[i];
                IAType t2;
                if (t.getTypeTag() == ATypeTag.UNION) {
                    if (NonTaggedFormatUtil.isOptionalField((AUnionType) t)) {
                        t2 = ((AUnionType) recordType.getFieldTypes()[i]).getUnionList().get(
                                NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
                        serializers[i] = AqlSerializerDeserializerProvider.INSTANCE
                                .getSerializerDeserializer(((AUnionType) recordType.getFieldTypes()[i]).getUnionList()
                                        .get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST));
                    } else {
                        // union .. the general case
                        throw new NotImplementedException();
                    }
                } else {
                    t2 = t;
                }
                serializers[i] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(t2);
                deserializers[i] = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(t2);
            }
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
                    if (isExpanded)
                        in.readInt(); // openPartOffset
                } else
                    isExpanded = false;
            }

            IAObject[] closedFields = null;
            if (numberOfSchemaFields > 0) {
                in.readInt(); // read number of closed fields.
                boolean hasNullableFields = NonTaggedFormatUtil.hasNullableField(this.recordType);
                byte[] nullBitMap = null;
                if (hasNullableFields) {
                    int nullBitMapSize = (int) (Math.ceil(numberOfSchemaFields / 8.0));
                    nullBitMap = new byte[nullBitMapSize];
                    in.readFully(nullBitMap);
                }
                closedFields = new IAObject[numberOfSchemaFields];
                for (int i = 0; i < numberOfSchemaFields; i++) {
                    in.readInt();
                }
                for (int fieldId = 0; fieldId < numberOfSchemaFields; fieldId++) {
                    if (hasNullableFields && ((nullBitMap[fieldId / 8] & (1 << (7 - (fieldId % 8)))) == 0)) {
                        closedFields[fieldId] = (IAObject) ANull.NULL;
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
                    return new ARecord(this.recordType, openFields);
                }
            } else {
                return new ARecord(this.recordType, closedFields);
            }
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException(e);
        }
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

    @SuppressWarnings("unchecked")
    @Override
    public void serialize(ARecord instance, DataOutput out) throws HyracksDataException {
        IARecordBuilder recordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        recordBuilder.reset(recordType);
        recordBuilder.init();
        if (recordType != null) {
            for (int i = 0; i < recordType.getFieldNames().length; i++) {
                fieldValue.reset();
                serializers[i].serialize(instance.getValueByPos(i), fieldValue.getDataOutput());
                recordBuilder.addField(i, fieldValue);
            }
            try {
                recordBuilder.write(out, false);
            } catch (IOException | AsterixException e) {
                throw new HyracksDataException(e);
            }
        } else {
            throw new NotImplementedException("Serializer for schemaless records is not implemented.");
        }
    }

    public static final int getRecordLength(byte[] serRecord, int offset) {
        return AInt32SerializerDeserializer.getInt(serRecord, offset);
    }

    public static final int getFieldOffsetById(byte[] serRecord, int fieldId, int nullBitmapSize, boolean isOpen) {

        if (isOpen) {
            if (serRecord[0] == ATypeTag.RECORD.serialize()) {
                // 5 is the index of the byte that determines whether the record
                // is expanded or not, i.e. it has an open part.
                if (serRecord[5] == 1) { // true
                    if (nullBitmapSize > 0) {
                        // 14 = tag (1) + record Size (4) + isExpanded (1) +
                        // offset of openPart (4) + number of closed fields (4)
                        if ((serRecord[14 + fieldId / 8] & (1 << (7 - (fieldId % 8)))) == 0)
                            // the field value is null
                            return 0;
                    }
                    return AInt32SerializerDeserializer.getInt(serRecord, (int) (14 + nullBitmapSize + (4 * fieldId)));
                } else {
                    if (nullBitmapSize > 0) {
                        // 9 = tag (1) + record Size (4) + isExpanded (1) +
                        // number of closed fields (4)
                        if ((serRecord[10 + fieldId / 8] & (1 << (7 - (fieldId % 8)))) == 0)
                            // the field value is null
                            return 0;
                    }
                    return AInt32SerializerDeserializer.getInt(serRecord, (int) (10 + nullBitmapSize + (4 * fieldId)));
                }
            } else
                return -1;
        } else {
            if (serRecord[0] == ATypeTag.RECORD.serialize()) {
                if (nullBitmapSize > 0)
                    // 9 = tag (1) + record Size (4) + number of closed fields
                    // (4)
                    if ((serRecord[9 + fieldId / 8] & (1 << (7 - (fieldId % 8)))) == 0)
                        // the field value is null
                        return 0;
                return AInt32SerializerDeserializer.getInt(serRecord, (int) (9 + nullBitmapSize + (4 * fieldId)));
            } else
                return -1;
        }
    }

    public static final int getFieldOffsetByName(byte[] serRecord, byte[] fieldName) {

        int openPartOffset = 0;
        if (serRecord[0] == ATypeTag.RECORD.serialize())
            // 5 is the index of the byte that determines whether the record is
            // expanded or not, i.e. it has an open part.
            if (serRecord[5] == 1) // true
                // 6 is the index of the first byte of the openPartOffset value.
                openPartOffset = AInt32SerializerDeserializer.getInt(serRecord, 6);
            else
                return -1; // this record does not have an open part
        else
            return -1; // this record does not have an open part

        int numberOfOpenField = AInt32SerializerDeserializer.getInt(serRecord, openPartOffset);
        int utflength = UTF8StringPointable.getUTFLength(fieldName, 1);

        IBinaryHashFunction utf8HashFunction = AqlBinaryHashFunctionFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
                .createBinaryHashFunction();

        IBinaryComparator utf8BinaryComparator = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE
                .createBinaryComparator();

        int fieldNameHashCode = utf8HashFunction.hash(fieldName, 1, utflength);

        int offset = openPartOffset + 4;
        int fieldOffset = -1;
        short recordFieldNameLength = 0;
        int mid = 0;
        int high = numberOfOpenField - 1;
        int low = 0;
        while (low <= high) {
            mid = (high + low) / 2;
            // 8 = hash code (4) + offset to the (name + tag + value ) of the
            // field (4).
            int h = AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * mid));
            if (h == fieldNameHashCode) {
                fieldOffset = AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * mid) + 4);
                recordFieldNameLength = AInt16SerializerDeserializer.getShort(serRecord, fieldOffset);
                if (utf8BinaryComparator
                        .compare(serRecord, fieldOffset, recordFieldNameLength, fieldName, 1, utflength) == 0)
                    return fieldOffset + 2 + utflength;
                else { // this else part has not been tested yet
                    for (int j = mid + 1; j < numberOfOpenField; j++) {
                        h = AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * j));
                        if (h == fieldNameHashCode) {
                            fieldOffset = AInt32SerializerDeserializer.getInt(serRecord, offset + (8 * j) + 4);
                            recordFieldNameLength = AInt16SerializerDeserializer.getShort(serRecord, fieldOffset);
                            if (utf8BinaryComparator.compare(serRecord, fieldOffset, recordFieldNameLength, fieldName,
                                    1, utflength) == 0)
                                return fieldOffset + 2 + utflength;
                        } else
                            break;
                    }
                }
            }
            if (fieldNameHashCode > h)
                low = mid + 1;
            else
                high = mid - 1;

        }
        return -1; // no field with this name.
    }

    @Override
    public String toString() {
        return " ";
    }
}