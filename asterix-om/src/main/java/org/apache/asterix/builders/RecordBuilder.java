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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.runtime.RuntimeRecordTypeInfo;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class RecordBuilder implements IARecordBuilder {
    private final static int DEFAULT_NUM_OPEN_FIELDS = 10;
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final static byte RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();
    private final UTF8StringSerializerDeserializer utf8SerDer = new UTF8StringSerializerDeserializer();

    private int openPartOffsetArraySize;
    private byte[] openPartOffsetArray;
    private int offsetPosition;
    private int headerSize;
    private boolean isOpen;
    private boolean isNullable;
    private int numberOfSchemaFields;

    private int openPartOffset;
    private ARecordType recType;

    private final IBinaryHashFunction utf8HashFunction;
    private final IBinaryComparator utf8Comparator;

    private final ByteArrayOutputStream closedPartOutputStream;
    private int[] closedPartOffsets;
    private int numberOfClosedFields;
    private byte[] nullBitMap;
    private int nullBitMapSize;

    private final ByteArrayAccessibleOutputStream openPartOutputStream;
    private long[] openPartOffsets;
    private int[] openFieldNameLengths;

    private int numberOfOpenFields;
    private RuntimeRecordTypeInfo recTypeInfo;

    public RecordBuilder() {
        this.closedPartOutputStream = new ByteArrayOutputStream();
        this.numberOfClosedFields = 0;

        this.openPartOutputStream = new ByteArrayAccessibleOutputStream();
        this.openPartOffsets = new long[DEFAULT_NUM_OPEN_FIELDS];
        this.openFieldNameLengths = new int[DEFAULT_NUM_OPEN_FIELDS];
        this.numberOfOpenFields = 0;

        this.utf8HashFunction = new PointableBinaryHashFunctionFactory(UTF8StringPointable.FACTORY)
                .createBinaryHashFunction();
        this.utf8Comparator = new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY)
                .createBinaryComparator();

        this.openPartOffsetArray = null;
        this.openPartOffsetArraySize = 0;
        this.offsetPosition = 0;

        this.recTypeInfo = new RuntimeRecordTypeInfo();
    }

    @Override
    public void init() {
        this.numberOfClosedFields = 0;
        this.closedPartOutputStream.reset();
        this.openPartOutputStream.reset();
        this.numberOfOpenFields = 0;
        this.offsetPosition = 0;
        if (nullBitMap != null) {
            Arrays.fill(nullBitMap, (byte) 0);
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
        if (recType != null) {
            this.isOpen = recType.isOpen();
            this.isNullable = NonTaggedFormatUtil.hasNullableField(recType);
            this.numberOfSchemaFields = recType.getFieldNames().length;
        } else {
            this.isOpen = true;
            this.isNullable = false;
            this.numberOfSchemaFields = 0;
        }
        // the header of the record consists of tag + record length (in bytes) +
        // boolean to indicates whether or not the record is expanded + the
        // offset of the open part of the record + the number of closed fields +
        // null bit map for closed fields.

        headerSize = 5;
        // this is the record tag (1) + record size (4)

        if (isOpen)
            // to tell whether the record has an open part or not.
            headerSize += 1;

        if (numberOfSchemaFields > 0) {
            // there is a schema associated with the record.

            headerSize += 4;
            // 4 = four bytes to store the number of closed fields.
            if (closedPartOffsets == null || closedPartOffsets.length < numberOfSchemaFields)
                closedPartOffsets = new int[numberOfSchemaFields];

            if (isNullable) {
                nullBitMapSize = (int) Math.ceil(numberOfSchemaFields / 8.0);
                if (nullBitMap == null || nullBitMap.length < nullBitMapSize)
                    this.nullBitMap = new byte[nullBitMapSize];
                Arrays.fill(nullBitMap, 0, nullBitMapSize, (byte) 0);
                headerSize += nullBitMap.length;
            }
        }
    }

    @Override
    public void addField(int id, IValueReference value) {
        closedPartOffsets[id] = closedPartOutputStream.size();
        int len = value.getLength() - 1;
        // +1 because we do not store the value tag.
        closedPartOutputStream.write(value.getByteArray(), value.getStartOffset() + 1, len);
        numberOfClosedFields++;
        if (isNullable && value.getByteArray()[value.getStartOffset()] != SER_NULL_TYPE_TAG) {
            nullBitMap[id / 8] |= (byte) (1 << (7 - (id % 8)));
        }
    }

    public void addField(int id, byte[] value) {
        closedPartOffsets[id] = closedPartOutputStream.size();
        // We assume the tag is not included (closed field)
        closedPartOutputStream.write(value, 0, value.length);
        numberOfClosedFields++;
        if (isNullable && value[0] != SER_NULL_TYPE_TAG) {
            nullBitMap[id / 8] |= (byte) (1 << (7 - (id % 8)));
        }
    }

    @Override
    public void addField(IValueReference name, IValueReference value) throws AsterixException {
        if (numberOfOpenFields == openPartOffsets.length) {
            openPartOffsets = Arrays.copyOf(openPartOffsets, openPartOffsets.length + DEFAULT_NUM_OPEN_FIELDS);
            openFieldNameLengths = Arrays.copyOf(openFieldNameLengths,
                    openFieldNameLengths.length + DEFAULT_NUM_OPEN_FIELDS);
        }
        int fieldNameHashCode;
        try {
            fieldNameHashCode = utf8HashFunction.hash(name.getByteArray(), name.getStartOffset() + 1,
                    name.getLength() - 1);
        } catch (HyracksDataException e1) {
            throw new AsterixException(e1);
        }
        if (recType != null) {
            int cFieldPos;
            try {
                cFieldPos = recTypeInfo.getFieldIndex(name.getByteArray(), name.getStartOffset() + 1,
                        name.getLength() - 1);
            } catch (HyracksDataException e) {
                throw new AsterixException(e);
            }
            if (cFieldPos >= 0) {
                throw new AsterixException("Open field \"" + recType.getFieldNames()[cFieldPos]
                        + "\" has the same field name as closed field at index " + cFieldPos);
            }
        }
        openPartOffsets[this.numberOfOpenFields] = fieldNameHashCode;
        openPartOffsets[this.numberOfOpenFields] = (openPartOffsets[numberOfOpenFields] << 32);
        openPartOffsets[numberOfOpenFields] += openPartOutputStream.size();
        openFieldNameLengths[numberOfOpenFields++] = name.getLength() - 1;
        openPartOutputStream.write(name.getByteArray(), name.getStartOffset() + 1, name.getLength() - 1);
        openPartOutputStream.write(value.getByteArray(), value.getStartOffset(), value.getLength());
    }

    @Override
    public void write(DataOutput out, boolean writeTypeTag) throws IOException, AsterixException {
        int h = headerSize;
        int recordLength;
        // prepare the open part
        if (numberOfOpenFields > 0) {
            h += 4;
            // 4 = four bytes to store the offset to the open part.
            openPartOffsetArraySize = numberOfOpenFields * 8;
            if (openPartOffsetArray == null || openPartOffsetArray.length < openPartOffsetArraySize)
                openPartOffsetArray = new byte[openPartOffsetArraySize];

            Arrays.sort(this.openPartOffsets, 0, numberOfOpenFields);
            if (numberOfOpenFields > 1) {
                byte[] openBytes = openPartOutputStream.getByteArray();
                for (int i = 1; i < numberOfOpenFields; i++) {
                    if (utf8Comparator.compare(openBytes, (int) openPartOffsets[i - 1], openFieldNameLengths[i - 1],
                            openBytes, (int) openPartOffsets[i], openFieldNameLengths[i]) == 0) {
                        String field = utf8SerDer.deserialize(new DataInputStream(new ByteArrayInputStream(openBytes,
                                (int) openPartOffsets[i], openFieldNameLengths[i])));
                        throw new AsterixException(
                                "Open fields " + (i - 1) + " and " + i + " have the same field name \"" + field + "\"");
                    }
                }
            }

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
        } else
            recordLength = h + numberOfSchemaFields * 4 + closedPartOutputStream.size();

        // write the record header
        if (writeTypeTag) {
            out.writeByte(RECORD_TYPE_TAG);
        }
        out.writeInt(recordLength);
        if (isOpen) {
            if (this.numberOfOpenFields > 0) {
                out.writeBoolean(true);
                out.writeInt(openPartOffset);
            } else
                out.writeBoolean(false);
        }

        // write the closed part
        if (numberOfSchemaFields > 0) {
            out.writeInt(numberOfClosedFields);
            if (isNullable)
                out.write(nullBitMap, 0, nullBitMapSize);
            for (int i = 0; i < numberOfSchemaFields; i++)
                out.writeInt(closedPartOffsets[i] + h + (numberOfSchemaFields * 4));
            out.write(closedPartOutputStream.toByteArray());
        }

        // write the open part
        if (numberOfOpenFields > 0) {
            out.writeInt(numberOfOpenFields);
            out.write(openPartOffsetArray, 0, openPartOffsetArraySize);
            out.write(openPartOutputStream.toByteArray());
        }
    }

    @Override
    public int getFieldId(String fieldName) {
        for (int i = 0; i < recType.getFieldNames().length; i++) {
            if (recType.getFieldNames()[i].equals(fieldName))
                return i;
        }
        return -1;
    }

}