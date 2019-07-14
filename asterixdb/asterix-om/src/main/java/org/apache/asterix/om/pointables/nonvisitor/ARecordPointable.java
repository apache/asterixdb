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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VarLengthTypeTrait;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.hyracks.util.string.UTF8StringWriter;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * This class interprets the binary data representation of a record.
 *
 * Record {
 *   byte tag;
 *   int length;
 *   byte isExpanded?;
 *   int openOffset?;
 *   int numberOfClosedFields?;
 *   byte[ceil (numberOfFields / 4)] nullBitMap?; // 2 bits per field, "1" means field is null, "2" field is missing
 *   int[numberOfClosedFields] closedFieldOffset?;
 *   IPointable[numberOfClosedFields] fieldValues?;
 *   int numberOfOpenFields?;
 *   OpenFieldLookup[numberOfOpenFields] lookup?;
 *   OpenField[numberOfOpenFields] openFields?;
 * }
 *
 * OpenFieldLookup {
 *   int hashCode;
 *   int Offset;
 * }
 *
 * OpenField {
 *   StringPointable fieldName;
 *   IPointable fieldValue;
 * }
 */
public class ARecordPointable extends AbstractPointable {

    public static final ARecordPointableFactory FACTORY = new ARecordPointableFactory();
    private final UTF8StringWriter utf8Writer = new UTF8StringWriter();

    public static final class ARecordPointableFactory implements IPointableFactory {

        private static final long serialVersionUID = 1L;

        private ARecordPointableFactory() {
        }

        @Override
        public ARecordPointable createPointable() {
            return new ARecordPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return VarLengthTypeTrait.INSTANCE;
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
            return registry.getClassIdentifier(getClass(), serialVersionUID);
        }

        @SuppressWarnings("squid:S1172") // unused parameter
        public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
            return FACTORY;
        }

    }

    public static final IObjectFactory<IPointable, ATypeTag> ALLOCATOR = type -> new ARecordPointable();
    static final int TAG_SIZE = 1;
    static final int RECORD_LENGTH_SIZE = 4;
    static final int EXPANDED_SIZE = 1;
    static final int OPEN_OFFSET_SIZE = 4;
    static final int CLOSED_COUNT_SIZE = 4;
    static final int FIELD_OFFSET_SIZE = 4;
    static final int OPEN_COUNT_SIZE = 4;
    private static final int OPEN_FIELD_HASH_SIZE = 4;
    private static final int OPEN_FIELD_OFFSET_SIZE = 4;
    private static final int OPEN_FIELD_HEADER = OPEN_FIELD_HASH_SIZE + OPEN_FIELD_OFFSET_SIZE;

    private static boolean isOpen(ARecordType recordType) {
        return recordType == null || recordType.isOpen();
    }

    public final int getSchemeFieldCount(ARecordType recordType) {
        return recordType.getFieldNames().length;
    }

    @Override
    public int getLength() {
        return IntegerPointable.getInteger(bytes, start + TAG_SIZE);
    }

    private boolean isExpanded(ARecordType recordType) {
        return isOpen(recordType) && BooleanPointable.getBoolean(bytes, start + TAG_SIZE + RECORD_LENGTH_SIZE);
    }

    private int getOpenPartOffset(ARecordType recordType) {
        return start + TAG_SIZE + RECORD_LENGTH_SIZE + (isOpen(recordType) ? EXPANDED_SIZE : 0);
    }

    private int getNullBitmapOffset(ARecordType recordType) {
        return getOpenPartOffset(recordType) + (isExpanded(recordType) ? OPEN_OFFSET_SIZE : 0) + CLOSED_COUNT_SIZE;
    }

    private int getNullBitmapSize(ARecordType recordType) {
        return RecordUtil.computeNullBitmapSize(recordType);
    }

    public boolean isClosedFieldNull(ARecordType recordType, int fieldId) {
        return getNullBitmapSize(recordType) > 0
                && RecordUtil.isNull(bytes[getNullBitmapOffset(recordType) + fieldId / 4], fieldId);
    }

    private boolean isClosedFieldMissing(ARecordType recordType, int fieldId) {
        return getNullBitmapSize(recordType) > 0
                && RecordUtil.isMissing(bytes[getNullBitmapOffset(recordType) + fieldId / 4], fieldId);
    }

    // -----------------------
    // Closed field accessors.
    // -----------------------

    public final void getClosedFieldValue(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException {
        if (isClosedFieldNull(recordType, fieldId)) {
            dOut.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
        } else if (isClosedFieldMissing(recordType, fieldId)) {
            dOut.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
        } else {
            dOut.write(getClosedFieldTag(recordType, fieldId));
            dOut.write(bytes, getClosedFieldOffset(recordType, fieldId), getClosedFieldSize(recordType, fieldId));
        }
    }

    /**
     * This is always untagged
     *
     * @param recordType
     * @param fieldId
     * @param pointable
     * @throws IOException
     */
    public final void getClosedFieldValue(ARecordType recordType, int fieldId, IPointable pointable)
            throws IOException {
        if (isClosedFieldNull(recordType, fieldId) || isClosedFieldMissing(recordType, fieldId)) {
            throw new IllegalStateException("Can't read a null or missing field");
        }
        pointable.set(bytes, getClosedFieldOffset(recordType, fieldId), getClosedFieldSize(recordType, fieldId));
    }

    private String getClosedFieldName(ARecordType recordType, int fieldId) {
        return recordType.getFieldNames()[fieldId];
    }

    public final void getClosedFieldName(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException {
        dOut.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        utf8Writer.writeUTF8(getClosedFieldName(recordType, fieldId), dOut);
    }

    public final byte getClosedFieldTag(ARecordType recordType, int fieldId) {
        return getClosedFieldType(recordType, fieldId).getTypeTag().serialize();
    }

    public final IAType getClosedFieldType(ARecordType recordType, int fieldId) {
        IAType aType = recordType.getFieldTypes()[fieldId];
        if (NonTaggedFormatUtil.isOptional(aType)) {
            // optional field: add the embedded non-null type tag
            aType = ((AUnionType) aType).getActualType();
        }
        return aType;
    }

    public final int getClosedFieldSize(ARecordType recordType, int fieldId) throws HyracksDataException {
        if (isClosedFieldNull(recordType, fieldId)) {
            return 0;
        }
        return NonTaggedFormatUtil.getFieldValueLength(bytes, getClosedFieldOffset(recordType, fieldId),
                getClosedFieldType(recordType, fieldId).getTypeTag(), false);
    }

    public final int getClosedFieldOffset(ARecordType recordType, int fieldId) {
        int offset = getNullBitmapOffset(recordType) + getNullBitmapSize(recordType) + fieldId * FIELD_OFFSET_SIZE;
        return start + IntegerPointable.getInteger(bytes, offset);
    }

    // -----------------------
    // Open field count.
    // -----------------------

    public final int getOpenFieldCount(ARecordType recordType) {
        return isExpanded(recordType) ? IntegerPointable.getInteger(bytes, getOpenFieldCountOffset(recordType)) : 0;
    }

    private int getOpenFieldCountSize(ARecordType recordType) {
        return isExpanded(recordType) ? OPEN_COUNT_SIZE : 0;
    }

    private int getOpenFieldCountOffset(ARecordType recordType) {
        return start + IntegerPointable.getInteger(bytes, getOpenPartOffset(recordType));
    }

    // -----------------------
    // Open field accessors.
    // -----------------------

    public final void getOpenFieldValue(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException {
        dOut.write(bytes, getOpenFieldValueOffset(recordType, fieldId), getOpenFieldValueSize(recordType, fieldId));
    }

    public final int getOpenFieldValueOffset(ARecordType recordType, int fieldId) {
        return getOpenFieldNameOffset(recordType, fieldId) + getOpenFieldNameSize(recordType, fieldId);
    }

    public final int getOpenFieldValueSize(ARecordType recordType, int fieldId) throws HyracksDataException {
        int offset = getOpenFieldValueOffset(recordType, fieldId);
        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(getOpenFieldTag(recordType, fieldId));
        return NonTaggedFormatUtil.getFieldValueLength(bytes, offset, tag, true);
    }

    public final void getOpenFieldName(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException {
        dOut.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        dOut.write(bytes, getOpenFieldNameOffset(recordType, fieldId), getOpenFieldNameSize(recordType, fieldId));
    }

    public final String getOpenFieldName(ARecordType recordType, int fieldId) {
        StringBuilder str = new StringBuilder();
        int offset = getOpenFieldNameOffset(recordType, fieldId);
        return UTF8StringUtil.toString(str, bytes, offset).toString();
    }

    private int getOpenFieldNameSize(ARecordType recordType, int fieldId) {
        int utfleng = UTF8StringUtil.getUTFLength(bytes, getOpenFieldNameOffset(recordType, fieldId));
        return utfleng + UTF8StringUtil.getNumBytesToStoreLength(utfleng);
    }

    private int getOpenFieldNameOffset(ARecordType recordType, int fieldId) {
        return getOpenFieldOffset(recordType, fieldId);
    }

    public final byte getOpenFieldTag(ARecordType recordType, int fieldId) {
        return bytes[getOpenFieldValueOffset(recordType, fieldId)];
    }

    private int getOpenFieldHashOffset(ARecordType recordType, int fieldId) {
        return getOpenFieldCountOffset(recordType) + getOpenFieldCountSize(recordType) + fieldId * OPEN_FIELD_HEADER;
    }

    private int getOpenFieldOffset(ARecordType recordType, int fieldId) {
        return start + IntegerPointable.getInteger(bytes, getOpenFieldOffsetOffset(recordType, fieldId));
    }

    private int getOpenFieldOffsetOffset(ARecordType recordType, int fieldId) {
        return getOpenFieldHashOffset(recordType, fieldId) + OPEN_FIELD_HASH_SIZE;
    }
}
