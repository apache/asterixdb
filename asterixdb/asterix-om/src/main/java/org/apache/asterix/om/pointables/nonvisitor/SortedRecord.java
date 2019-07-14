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
package org.apache.asterix.om.pointables.nonvisitor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.om.util.container.ObjectFactories;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.util.string.UTF8StringWriter;

/**
 * This class is used to access the fields of a record in lexicographical order of the field names. The fields can be
 * retrieved by calling {@link #poll()} which will poll the field with the minimum field name. The field value can be
 * retrieved lazily by using the information stored in the {@link RecordField} returned from polling. Before
 * accessing the record information, {@link #reset(byte[], int)} has to be called first to point to the bytes of the
 * record in question.
 */
public final class SortedRecord {

    private static final IObjectFactory<RecordField, Void> FIELD_FACTORY = type -> new RecordField();
    // TODO(ali): copied from PointableHelper for now.
    private static final byte[] NULL_BYTES = new byte[] { ATypeTag.SERIALIZED_NULL_TYPE_TAG };
    private static final byte[] MISSING_BYTES = new byte[] { ATypeTag.SERIALIZED_MISSING_TYPE_TAG };
    private final IObjectPool<UTF8StringPointable, Void> utf8Pool = new ListObjectPool<>(ObjectFactories.UTF8_FACTORY);
    private final IObjectPool<RecordField, Void> recordFieldPool = new ListObjectPool<>(FIELD_FACTORY);
    private final PriorityQueue<RecordField> sortedFields = new PriorityQueue<>(RecordField.FIELD_NAME_COMP);
    private final ARecordType recordType;
    private final IAType[] fieldTypes;
    private final RecordField[] closedFields;
    private final int numSchemaFields;
    private final boolean hasOptionalFields;
    private final int nullBitMapSize;
    private byte[] bytes;

    public SortedRecord(ARecordType recordType) {
        String[] fieldNames = recordType.getFieldNames();
        this.numSchemaFields = fieldNames.length;
        this.recordType = recordType;
        this.fieldTypes = recordType.getFieldTypes();
        this.hasOptionalFields = NonTaggedFormatUtil.hasOptionalField(recordType);
        this.nullBitMapSize = RecordUtil.computeNullBitmapSize(hasOptionalFields, recordType);
        this.closedFields = new RecordField[numSchemaFields];
        UTF8StringWriter utf8Writer = new UTF8StringWriter();
        ByteArrayAccessibleOutputStream byteArrayStream = new ByteArrayAccessibleOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayStream);
        try {
            // write each closed field into a pointable, create a new field and add to the list of closedFields
            for (int i = 0; i < numSchemaFields; i++) {
                int nameStart = dataOutputStream.size();
                utf8Writer.writeUTF8(fieldNames[i], dataOutputStream);
                int nameEnd = dataOutputStream.size();
                UTF8StringPointable utf8Pointable = UTF8StringPointable.FACTORY.createPointable();
                utf8Pointable.set(byteArrayStream.getByteArray(), nameStart, nameEnd - nameStart);
                RecordField field = new RecordField();
                field.set(utf8Pointable, -1, -1, -1, null);
                closedFields[i] = field;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method should be called before using the record. It recalculates logical indices and offsets of fields,
     * closed and open. It populates the utf8 filed names for open.
     */
    public final void reset(byte[] data, int start) throws HyracksDataException {
        resetRecord(data, start, ARecordPointable.TAG_SIZE, 0);
    }

    public final void resetNonTagged(byte[] data, int start) throws HyracksDataException {
        resetRecord(data, start, 0, 1);
    }

    private void resetRecord(byte[] data, int start, int skipTag, int fixOffset) throws HyracksDataException {
        bytes = data;
        reset();
        boolean isExpanded = false;
        // advance to expanded byte if present
        int pointer = start + skipTag + ARecordPointable.RECORD_LENGTH_SIZE;
        if (recordType.isOpen()) {
            isExpanded = bytes[pointer] == 1;
            // advance to either open part offset or number of closed fields
            pointer += ARecordPointable.EXPANDED_SIZE;
        }
        int openPartOffset = 0;
        if (isExpanded) {
            openPartOffset = start + AInt32SerializerDeserializer.getInt(bytes, pointer) - fixOffset;
            // advance to number of closed fields
            pointer += ARecordPointable.OPEN_OFFSET_SIZE;
        }
        int fieldOffset;
        int length;
        int fieldIndex = 0;
        ATypeTag tag;
        // advance to where fields offsets are (or to null bit map if the schema has optional fields)
        if (numSchemaFields > 0) {
            pointer += ARecordPointable.CLOSED_COUNT_SIZE;
            int nullBitMapOffset = pointer;
            int fieldsOffsets = nullBitMapOffset + nullBitMapSize;
            // compute the offsets of each closed field value and whether it's missing or null
            for (int i = 0; i < numSchemaFields; i++, fieldIndex++) {
                fieldOffset = AInt32SerializerDeserializer.getInt(bytes, fieldsOffsets) + start - fixOffset;
                tag = TypeComputeUtils.getActualType(fieldTypes[i]).getTypeTag();
                if (hasOptionalFields) {
                    byte nullBits = bytes[nullBitMapOffset + i / 4];
                    if (RecordUtil.isNull(nullBits, i)) {
                        tag = ATypeTag.NULL;
                    } else if (RecordUtil.isMissing(nullBits, i)) {
                        tag = ATypeTag.MISSING;
                    }
                }
                length = NonTaggedFormatUtil.getFieldValueLength(bytes, fieldOffset, tag, false);
                closedFields[i].set(fieldIndex, fieldOffset, length, tag);
                if (tag != ATypeTag.MISSING) {
                    sortedFields.add(closedFields[i]);
                }
                fieldsOffsets += ARecordPointable.FIELD_OFFSET_SIZE;
            }
        }
        // then populate open fields info second, an open field has name + value (tagged)
        if (isExpanded) {
            int numberOpenFields = AInt32SerializerDeserializer.getInt(bytes, openPartOffset);
            fieldOffset = openPartOffset + ARecordPointable.OPEN_COUNT_SIZE + (8 * numberOpenFields);
            for (int i = 0; i < numberOpenFields; i++, fieldIndex++) {
                // get a pointable to the field name
                length = NonTaggedFormatUtil.getFieldValueLength(bytes, fieldOffset, ATypeTag.STRING, false);
                UTF8StringPointable openFieldName = utf8Pool.allocate(null);
                openFieldName.set(bytes, fieldOffset, length);
                // move to the value
                fieldOffset += length;
                tag = ATypeTag.VALUE_TYPE_MAPPING[bytes[fieldOffset]];
                // +1 to account for the tag included since the field is open
                length = NonTaggedFormatUtil.getFieldValueLength(bytes, fieldOffset, tag, true) + 1;
                RecordField openField = recordFieldPool.allocate(null);
                openField.set(openFieldName, fieldIndex, fieldOffset, length, tag);
                sortedFields.add(openField);
                // then skip the value to the next field name
                fieldOffset += length;
            }
        }
    }

    private void reset() {
        sortedFields.clear();
        utf8Pool.reset();
        recordFieldPool.reset();
    }

    public final boolean isEmpty() {
        return sortedFields.isEmpty();
    }

    public final RecordField poll() {
        return sortedFields.poll();
    }

    public final int size() {
        return sortedFields.size();
    }

    public final IAType getFieldType(RecordField field) throws HyracksDataException {
        return RecordUtil.getType(recordType, field.getIndex(), field.getValueTag());
    }

    public final void getFieldValue(RecordField field, TaggedValueReference fieldValueRef) {
        if (field.getIndex() >= numSchemaFields) {
            fieldValueRef.set(bytes, field.getValueOffset() + 1, field.getValueLength() - 1, field.getValueTag());
        } else {
            if (field.getValueTag() == ATypeTag.MISSING) {
                fieldValueRef.set(MISSING_BYTES, 0, 0, ATypeTag.MISSING);
            } else if (field.getValueTag() == ATypeTag.NULL) {
                fieldValueRef.set(NULL_BYTES, 0, 0, ATypeTag.NULL);
            } else {
                fieldValueRef.set(bytes, field.getValueOffset(), field.getValueLength(), field.getValueTag());
            }
        }
    }

    // TODO(ali): remove this method once hashing does not need the tag to be adjacent to the value
    public final void getFieldValue(RecordField field, IPointable pointable, ArrayBackedValueStorage storage)
            throws IOException {
        int fieldIdx = field.getIndex();
        if (fieldIdx >= numSchemaFields) {
            // open field
            pointable.set(bytes, field.getValueOffset(), field.getValueLength());
        } else {
            // closed field
            if (field.getValueTag() == ATypeTag.MISSING) {
                pointable.set(MISSING_BYTES, 0, MISSING_BYTES.length);
            } else if (field.getValueTag() == ATypeTag.NULL) {
                pointable.set(NULL_BYTES, 0, NULL_BYTES.length);
            } else {
                // TODO(ali): this is not ideal. should not need to copy the tag when tagged pointables are introduced
                int start = storage.getLength();
                storage.getDataOutput().writeByte(field.getValueTag().serialize());
                storage.getDataOutput().write(bytes, field.getValueOffset(), field.getValueLength());
                int end = storage.getLength();
                pointable.set(storage.getByteArray(), start, end - start);
            }
        }
    }
}
