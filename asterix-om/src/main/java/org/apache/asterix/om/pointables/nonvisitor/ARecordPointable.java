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
import java.util.Arrays;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

/*
 * This class interprets the binary data representation of a record.
 *
 * Record {
 *   byte tag;
 *   int length;
 *   byte isExpanded;
 *   int openOffset?;
 *   int numberOfClosedFields;
 *   byte[ceil (numberOfFields / 8)] nullBitMap; // 1 bit per field, "1" means field is Null for this record
 *   int[numberOfClosedFields] closedFieldOffset;
 *   IPointable[numberOfClosedFields] fieldValue;
 *   int numberOfOpenFields?;
 *   OpenFieldLookup[numberOfOpenFields] lookup;
 *   OpenField[numberOfOpenFields] openFields;
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

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new ARecordPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static final IObjectFactory<IPointable, ATypeTag> ALLOCATOR = new IObjectFactory<IPointable, ATypeTag>() {
        @Override
        public IPointable create(ATypeTag type) {
            return new ARecordPointable();
        }
    };

    private static final int TAG_SIZE = 1;
    private static final int RECORD_LENGTH_SIZE = 4;
    private static final int EXPANDED_SIZE = 1;
    private static final int OPEN_OFFSET_SIZE = 4;
    private static final int CLOSED_COUNT_SIZE = 4;
    private static final int FIELD_OFFSET_SIZE = 4;
    private static final int OPEN_COUNT_SIZE = 4;
    private static final int OPEN_FIELD_HASH_SIZE = 4;
    private static final int OPEN_FIELD_OFFSET_SIZE = 4;
    private static final int OPEN_FIELD_HEADER = OPEN_FIELD_HASH_SIZE + OPEN_FIELD_OFFSET_SIZE;

    private static final int STRING_LENGTH_SIZE = 2;

    private static boolean isOpen(ARecordType recordType) {
        return recordType == null || recordType.isOpen();
    }

    public int getSchemeFieldCount(ARecordType recordType) {
        return recordType.getFieldNames().length;
    }

    public byte getTag() {
        return BytePointable.getByte(bytes, getTagOffset());
    }

    public int getTagOffset() {
        return start;
    }

    public int getTagSize() {
        return TAG_SIZE;
    }

    @Override
    public int getLength() {
        return IntegerPointable.getInteger(bytes, getLengthOffset());
    }

    public int getLengthOffset() {
        return getTagOffset() + getTagSize();
    }

    public int getLengthSize() {
        return RECORD_LENGTH_SIZE;
    }

    public boolean isExpanded(ARecordType recordType) {
        if (isOpen(recordType)) {
            return BooleanPointable.getBoolean(bytes, getExpendedOffset(recordType));
        }
        return false;
    }

    public int getExpendedOffset(ARecordType recordType) {
        return getLengthOffset() + getLengthSize();
    }

    public int getExpandedSize(ARecordType recordType) {
        return (isOpen(recordType)) ? EXPANDED_SIZE : 0;
    }

    public int getOpenPart(ARecordType recordType) {
        return IntegerPointable.getInteger(bytes, getOpenPartOffset(recordType));
    }

    public int getOpenPartOffset(ARecordType recordType) {
        return getExpendedOffset(recordType) + getExpandedSize(recordType);
    }

    public int getOpenPartSize(ARecordType recordType) {
        return (isExpanded(recordType)) ? OPEN_OFFSET_SIZE : 0;
    }

    public int getClosedFieldCount(ARecordType recordType) {
        return IntegerPointable.getInteger(bytes, getClosedFieldCountOffset(recordType));
    }

    public int getClosedFieldCountOffset(ARecordType recordType) {
        return getOpenPartOffset(recordType) + getOpenPartSize(recordType);
    }

    public int getClosedFieldCountSize(ARecordType recordType) {
        return CLOSED_COUNT_SIZE;
    }

    public byte[] getNullBitmap(ARecordType recordType) {
        if (getNullBitmapSize(recordType) > 0) {
            return Arrays.copyOfRange(bytes, getNullBitmapOffset(recordType), getNullBitmapSize(recordType));
        }
        return null;
    }

    public int getNullBitmapOffset(ARecordType recordType) {
        return getClosedFieldCountOffset(recordType) + getClosedFieldCountSize(recordType);
    }

    public int getNullBitmapSize(ARecordType recordType) {
        return ARecordType.computeNullBitmapSize(recordType);
    }

    public boolean isClosedFieldNull(ARecordType recordType, int fieldId) {
        if (getNullBitmapSize(recordType) > 0) {
            return ((bytes[getNullBitmapOffset(recordType) + fieldId / 8] & (1 << (7 - (fieldId % 8)))) == 0);
        }
        return false;
    }

    // -----------------------
    // Closed field accessors.
    // -----------------------

    public void getClosedFieldValue(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException,
            AsterixException {
        if (isClosedFieldNull(recordType, fieldId)) {
            dOut.writeByte(ATypeTag.NULL.serialize());
        } else {
            dOut.write(getClosedFieldTag(recordType, fieldId));
            dOut.write(bytes, getClosedFieldOffset(recordType, fieldId), getClosedFieldSize(recordType, fieldId));
        }
    }

    public String getClosedFieldName(ARecordType recordType, int fieldId) {
        return recordType.getFieldNames()[fieldId];
    }

    public void getClosedFieldName(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException {
        dOut.writeByte(ATypeTag.STRING.serialize());
        dOut.writeUTF(getClosedFieldName(recordType, fieldId));
    }

    public byte getClosedFieldTag(ARecordType recordType, int fieldId) {
        return getClosedFieldType(recordType, fieldId).getTypeTag().serialize();
    }

    public IAType getClosedFieldType(ARecordType recordType, int fieldId) {
        IAType aType = recordType.getFieldTypes()[fieldId];
        if (NonTaggedFormatUtil.isOptional(aType)) {
            // optional field: add the embedded non-null type tag
            aType = ((AUnionType) aType).getNullableType();
        }
        return aType;
    }

    public int getClosedFieldSize(ARecordType recordType, int fieldId) throws AsterixException {
        if (isClosedFieldNull(recordType, fieldId)) {
            return 0;
        }
        return NonTaggedFormatUtil.getFieldValueLength(bytes, getClosedFieldOffset(recordType, fieldId),
                getClosedFieldType(recordType, fieldId).getTypeTag(), false);
    }

    public int getClosedFieldOffset(ARecordType recordType, int fieldId) {
        int offset = getNullBitmapOffset(recordType) + getNullBitmapSize(recordType) + fieldId * FIELD_OFFSET_SIZE;
        return IntegerPointable.getInteger(bytes, offset);
    }

    // -----------------------
    // Open field count.
    // -----------------------

    public int getOpenFieldCount(ARecordType recordType) {
        return isExpanded(recordType) ? IntegerPointable.getInteger(bytes, getOpenFieldCountOffset(recordType)) : 0;
    }

    public int getOpenFieldCountSize(ARecordType recordType) {
        return (isExpanded(recordType)) ? OPEN_COUNT_SIZE : 0;
    }

    public int getOpenFieldCountOffset(ARecordType recordType) {
        return getOpenPart(recordType);
    }

    // -----------------------
    // Open field accessors.
    // -----------------------

    public void getOpenFieldValue(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException,
            AsterixException {
        dOut.write(bytes, getOpenFieldValueOffset(recordType, fieldId), getOpenFieldValueSize(recordType, fieldId));
    }

    public int getOpenFieldValueOffset(ARecordType recordType, int fieldId) {
        return getOpenFieldNameOffset(recordType, fieldId) + getOpenFieldNameSize(recordType, fieldId);
    }

    public int getOpenFieldValueSize(ARecordType recordType, int fieldId) throws AsterixException {
        int offset = getOpenFieldValueOffset(recordType, fieldId);
        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(getOpenFieldTag(recordType, fieldId));
        return NonTaggedFormatUtil.getFieldValueLength(bytes, offset, tag, true);
    }

    public void getOpenFieldName(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException {
        dOut.writeByte(ATypeTag.STRING.serialize());
        dOut.write(bytes, getOpenFieldNameOffset(recordType, fieldId), getOpenFieldNameSize(recordType, fieldId));
    }

    public int getOpenFieldNameSize(ARecordType recordType, int fieldId) {
        return UTF8StringPointable.getUTFLength(bytes, getOpenFieldNameOffset(recordType, fieldId))
                + STRING_LENGTH_SIZE;
    }

    public int getOpenFieldNameOffset(ARecordType recordType, int fieldId) {
        return getOpenFieldOffset(recordType, fieldId);
    }

    public byte getOpenFieldTag(ARecordType recordType, int fieldId) {
        return bytes[getOpenFieldValueOffset(recordType, fieldId)];
    }

    public int getOpenFieldHash(ARecordType recordType, int fieldId) {
        return IntegerPointable.getInteger(bytes, getOpenFieldHashOffset(recordType, fieldId));
    }

    public int getOpenFieldHashOffset(ARecordType recordType, int fieldId) {
        return getOpenFieldCountOffset(recordType) + getOpenFieldCountSize(recordType) + fieldId * OPEN_FIELD_HEADER;
    }

    public int getOpenFieldHashSize(ARecordType recordType, int fieldId) {
        return OPEN_FIELD_HASH_SIZE;
    }

    public int getOpenFieldOffset(ARecordType recordType, int fieldId) {
        return IntegerPointable.getInteger(bytes, getOpenFieldOffsetOffset(recordType, fieldId));
    }

    public int getOpenFieldOffsetOffset(ARecordType recordType, int fieldId) {
        return getOpenFieldHashOffset(recordType, fieldId) + getOpenFieldHashSize(recordType, fieldId);
    }

    public int getOpenFieldOffsetSize(ARecordType recordType, int fieldId) {
        return OPEN_FIELD_HASH_SIZE;
    }

}
