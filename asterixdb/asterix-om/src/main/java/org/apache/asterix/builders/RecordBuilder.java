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

package org.apache.asterix.builders;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class RecordBuilder implements IARecordBuilder {
    private final static int DEFAULT_NUM_OPEN_FIELDS = 10;
    private final UTF8StringSerializerDeserializer utf8SerDer = new UTF8StringSerializerDeserializer();
    private int openPartOffsetArraySize;
    private byte[] openPartOffsetArray;
    private int offsetPosition;
    private int headerSize;
    private boolean isOpen;
    private boolean containsOptionalField;
    private int numberOfSchemaFields;
    private int openPartOffset;
    private ARecordType recType;
    private final IBinaryHashFunction utf8HashFunction;
    private final IBinaryComparator utf8Comparator;
    private final ByteArrayAccessibleOutputStream closedPartOutputStream;
    private int[] closedPartOffsets;
    private int numberOfClosedFields;
    private byte[] nullBitMap;
    private int nullBitMapSize;
    private final ByteArrayAccessibleOutputStream openPartOutputStream;
    private long[] openPartOffsets;
    private int[] openFieldNameLengths;
    private int numberOfOpenFields;
    private final RuntimeRecordTypeInfo recTypeInfo;
    private final IntOpenHashSet fieldNamesHashes;

    public RecordBuilder() {
        this.closedPartOutputStream = new ByteArrayAccessibleOutputStream();
        this.numberOfClosedFields = 0;
        this.openPartOutputStream = new ByteArrayAccessibleOutputStream();
        this.openPartOffsets = new long[DEFAULT_NUM_OPEN_FIELDS];
        this.openFieldNameLengths = new int[DEFAULT_NUM_OPEN_FIELDS];
        this.numberOfOpenFields = 0;
        this.utf8HashFunction =
                new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY).createBinaryHashFunction();
        this.utf8Comparator = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
        this.openPartOffsetArray = null;
        this.openPartOffsetArraySize = 0;
        this.offsetPosition = 0;
        this.recTypeInfo = new RuntimeRecordTypeInfo();
        this.fieldNamesHashes = new IntOpenHashSet();
    }

    @Override
    public void init() {
        this.numberOfClosedFields = 0;
        this.closedPartOutputStream.reset();
        this.openPartOutputStream.reset();
        this.numberOfOpenFields = 0;
        this.offsetPosition = 0;
        this.fieldNamesHashes.clear();
        if (nullBitMap != null) {
            // A default null byte is 10101010 (0xAA):
            // the null bit is 1, which means the field is not a null,
            // the missing bit is 0, means the field is missing (by default).
            Arrays.fill(nullBitMap, (byte) 0xAA);
        }
    }

    @Override
    public void reset(ARecordType recType) {
        this.recType = recType;
        this.recTypeInfo.reset(recType);
        this.closedPartOutputStream.reset();
        this.openPartOutputStream.reset();
        this.numberOfClosedFields = 0;
        this.numberOfOpenFields = 0;
        this.offsetPosition = 0;
        this.fieldNamesHashes.clear();
        if (recType != null) {
            this.isOpen = recType.isOpen();
            this.containsOptionalField = NonTaggedFormatUtil.hasOptionalField(recType);
            this.numberOfSchemaFields = recType.getFieldNames().length;
        } else {
            this.isOpen = true;
            this.containsOptionalField = false;
            this.numberOfSchemaFields = 0;
        }
        // the header of the record consists of tag + record length (in bytes) +
        // boolean to indicates whether or not the record is expanded + the
        // offset of the open part of the record + the number of closed fields +
        // null bit map for closed fields.

        headerSize = 5;
        // this is the record tag (1) + record size (4)

        if (isOpen) {
            // to tell whether the record has an open part or not.
            headerSize += 1;
        }

        if (numberOfSchemaFields > 0) {
            // there is a schema associated with the record.

            headerSize += 4;
            // 4 = four bytes to store the number of closed fields.
            if (closedPartOffsets == null || closedPartOffsets.length < numberOfSchemaFields) {
                closedPartOffsets = new int[numberOfSchemaFields];
            }

            if (containsOptionalField) {
                nullBitMapSize = (int) Math.ceil(numberOfSchemaFields / 4.0);
                if (nullBitMap == null || nullBitMap.length < nullBitMapSize) {
                    this.nullBitMap = new byte[nullBitMapSize];
                }
                // A default null byte is 10101010 (0xAA):
                // the null bit is 1, which means the field is not a null,
                // the missing bit is 0, means the field is missing (by default).
                Arrays.fill(nullBitMap, 0, nullBitMapSize, (byte) 0xAA);
                headerSize += nullBitMapSize;
            }
        }
    }

    @Override
    public void addField(int fid, IValueReference value) {
        closedPartOffsets[fid] = closedPartOutputStream.size();
        int len = value.getLength() - 1;
        // +1 because we do not store the value tag.
        closedPartOutputStream.write(value.getByteArray(), value.getStartOffset() + 1, len);
        numberOfClosedFields++;
        addNullOrMissingField(fid, value.getByteArray(), value.getStartOffset());
    }

    private void addNullOrMissingField(int fid, byte[] data, int offset) {
        if (containsOptionalField) {
            byte nullByte = (byte) (1 << (7 - 2 * (fid % 4)));
            if (data[offset] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                // unset the null bit, 0 means is null.
                nullBitMap[fid / 4] &= ~nullByte;
            }
            if (data[offset] != ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                nullBitMap[fid / 4] |= (byte) (1 << (7 - 2 * (fid % 4) - 1));
            }
        }
    }

    @Override
    public void addField(IValueReference name, IValueReference value) throws HyracksDataException {
        byte[] data = value.getByteArray();
        int offset = value.getStartOffset();

        // MISSING for an open field means the field does not exist.
        if (data[offset] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
            return;
        }
        // ignore adding duplicate fields
        byte[] nameBytes = name.getByteArray();
        int nameStart = name.getStartOffset() + 1;
        int nameLength = name.getLength() - 1;
        if (recType != null && recTypeInfo.getFieldIndex(nameBytes, nameStart, nameLength) >= 0) {
            // TODO(ali): issue a warning
            return;
        }
        int fieldNameHashCode = utf8HashFunction.hash(nameBytes, nameStart, nameLength);
        if (!fieldNamesHashes.add(fieldNameHashCode)) {
            for (int i = 0; i < numberOfOpenFields; i++) {
                if (isDuplicate(nameBytes, nameStart, nameLength, fieldNameHashCode, i)) {
                    // TODO(ali): issue a warning
                    return;
                }
            }
        }
        if (numberOfOpenFields == openPartOffsets.length) {
            openPartOffsets = Arrays.copyOf(openPartOffsets, openPartOffsets.length + DEFAULT_NUM_OPEN_FIELDS);
            openFieldNameLengths =
                    Arrays.copyOf(openFieldNameLengths, openFieldNameLengths.length + DEFAULT_NUM_OPEN_FIELDS);
        }
        openPartOffsets[this.numberOfOpenFields] = fieldNameHashCode;
        openPartOffsets[this.numberOfOpenFields] = openPartOffsets[numberOfOpenFields] << 32;
        openPartOffsets[numberOfOpenFields] += openPartOutputStream.size();
        openFieldNameLengths[numberOfOpenFields++] = nameLength;
        openPartOutputStream.write(nameBytes, nameStart, nameLength);
        openPartOutputStream.write(data, offset, value.getLength());
    }

    @Override
    public void write(DataOutput out, boolean writeTypeTag) throws HyracksDataException {
        int h = headerSize;
        int recordLength;
        // prepare the open part
        if (numberOfOpenFields > 0) {
            h += 4;
            // 4 = four bytes to store the offset to the open part.
            openPartOffsetArraySize = numberOfOpenFields * 8;
            if (openPartOffsetArray == null || openPartOffsetArray.length < openPartOffsetArraySize) {
                openPartOffsetArray = new byte[openPartOffsetArraySize];
            }

            // field names with the same hash code should be adjacent to each other after sorting
            Arrays.sort(this.openPartOffsets, 0, numberOfOpenFields);
            openPartOffset = h + numberOfSchemaFields * 4 + closedPartOutputStream.size();
            int fieldNameHashCode;
            for (int i = 0; i < numberOfOpenFields; i++) {
                fieldNameHashCode = (int) (openPartOffsets[i] >> 32);
                SerializerDeserializerUtil.writeIntToByteArray(openPartOffsetArray, fieldNameHashCode, offsetPosition);
                int fieldOffset = (int) openPartOffsets[i];
                SerializerDeserializerUtil.writeIntToByteArray(openPartOffsetArray,
                        fieldOffset + openPartOffset + 4 + openPartOffsetArraySize, offsetPosition + 4);
                offsetPosition += 8;
            }
            recordLength = openPartOffset + 4 + openPartOffsetArraySize + openPartOutputStream.size();
        } else {
            recordLength = h + numberOfSchemaFields * 4 + closedPartOutputStream.size();
        }
        writeRecord(out, writeTypeTag, h, recordLength);
    }

    private void writeRecord(DataOutput out, boolean writeTypeTag, int headerSize, int recordLength)
            throws HyracksDataException {
        try {
            // write the record header
            if (writeTypeTag) {
                out.writeByte(ATypeTag.SERIALIZED_RECORD_TYPE_TAG);
            }
            out.writeInt(recordLength);
            if (isOpen) {
                if (this.numberOfOpenFields > 0) {
                    out.writeBoolean(true);
                    out.writeInt(openPartOffset);
                } else {
                    out.writeBoolean(false);
                }
            }

            // write the closed part
            if (numberOfSchemaFields > 0) {
                out.writeInt(numberOfClosedFields);
                if (containsOptionalField) {
                    out.write(nullBitMap, 0, nullBitMapSize);
                }
                for (int i = 0; i < numberOfSchemaFields; i++) {
                    out.writeInt(closedPartOffsets[i] + headerSize + (numberOfSchemaFields * 4));
                }
                out.write(closedPartOutputStream.getByteArray(), 0, closedPartOutputStream.getLength());
            }

            // write the open part
            if (numberOfOpenFields > 0) {
                out.writeInt(numberOfOpenFields);
                out.write(openPartOffsetArray, 0, openPartOffsetArraySize);
                out.write(openPartOutputStream.getByteArray(), 0, openPartOutputStream.getLength());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public int getFieldId(String fieldName) {
        return recType.getFieldIndex(fieldName);
    }

    public IBinaryHashFunction getFieldNameHashFunction() {
        return utf8HashFunction;
    }

    public IBinaryComparator getFieldNameComparator() {
        return utf8Comparator;
    }

    private boolean isDuplicate(byte[] fName, int fStart, int fLen, int fNameHash, int otherFieldIdx)
            throws HyracksDataException {
        return ((int) (openPartOffsets[otherFieldIdx] >>> 32) == fNameHash)
                && (utf8Comparator.compare(openPartOutputStream.getByteArray(), (int) openPartOffsets[otherFieldIdx],
                        openFieldNameLengths[otherFieldIdx], fName, fStart, fLen) == 0);
    }
}
