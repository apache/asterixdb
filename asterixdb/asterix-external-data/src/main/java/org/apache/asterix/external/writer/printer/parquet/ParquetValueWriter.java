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
package org.apache.asterix.external.writer.printer.parquet;

import java.io.IOException;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.printers.PrintTools;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.om.lazy.FlatLazyVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ResettableByteArrayOutputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetValueWriter {
    public static final String LIST_FIELD = "list";
    public static final String ELEMENT_FIELD = "element";

    public static final String GROUP_TYPE_ERROR_FIELD = "group";
    public static final String PRIMITIVE_TYPE_ERROR_FIELD = "primitive";

    private final VoidPointable voidPointable;
    private final ResettableByteArrayOutputStream byteArrayOutputStream;

    ParquetValueWriter() {
        this.voidPointable = VoidPointable.FACTORY.createPointable();
        this.byteArrayOutputStream = new ResettableByteArrayOutputStream();
    }

    private void addIntegerType(long value, PrimitiveType.PrimitiveTypeName primitiveTypeName, ATypeTag typeTag,
            RecordConsumer recordConsumer) throws HyracksDataException {
        switch (primitiveTypeName) {
            case INT32:
                recordConsumer.addInteger((int) value);
                break;
            case INT64:
                recordConsumer.addLong(value);
                break;
            case FLOAT:
                recordConsumer.addFloat(value);
                break;
            case DOUBLE:
                recordConsumer.addDouble(value);
                break;
            default:
                throw RuntimeDataException.create(ErrorCode.TYPE_MISMATCH_GENERIC, primitiveTypeName, typeTag);
        }
    }

    public void addValueToColumn(RecordConsumer recordConsumer, FlatLazyVisitablePointable pointable,
            PrimitiveType type) throws HyracksDataException {

        ATypeTag typeTag = pointable.getTypeTag();
        byte[] b = pointable.getByteArray();
        int s, l;

        if (pointable.isTagged()) {
            s = pointable.getStartOffset() + 1;
            l = pointable.getLength() - 1;
        } else {
            s = pointable.getStartOffset();
            l = pointable.getLength();
        }
        voidPointable.set(b, s, l);

        PrimitiveType.PrimitiveTypeName primitiveTypeName = type.getPrimitiveTypeName();

        switch (typeTag) {
            case TINYINT:
                byte tinyIntValue = AInt8SerializerDeserializer.getByte(b, s);
                addIntegerType(tinyIntValue, primitiveTypeName, typeTag, recordConsumer);
                break;
            case SMALLINT:
                short smallIntValue = AInt16SerializerDeserializer.getShort(b, s);
                addIntegerType(smallIntValue, primitiveTypeName, typeTag, recordConsumer);
                break;
            case INTEGER:
                int intValue = AInt32SerializerDeserializer.getInt(b, s);
                addIntegerType(intValue, primitiveTypeName, typeTag, recordConsumer);
                break;
            case BIGINT:
                long bigIntValue = AInt64SerializerDeserializer.getLong(b, s);
                addIntegerType(bigIntValue, primitiveTypeName, typeTag, recordConsumer);
                break;
            case FLOAT:
                float floatValue = AFloatSerializerDeserializer.getFloat(b, s);
                switch (primitiveTypeName) {
                    case INT32:
                        recordConsumer.addInteger((int) floatValue);
                        break;
                    case INT64:
                        recordConsumer.addLong((long) floatValue);
                        break;
                    case FLOAT:
                        recordConsumer.addFloat(floatValue);
                        break;
                    case DOUBLE:
                        recordConsumer.addDouble(floatValue);
                        break;
                    default:
                        throw RuntimeDataException.create(ErrorCode.TYPE_MISMATCH_GENERIC, primitiveTypeName, typeTag);
                }
                break;
            case DOUBLE:
                double doubleValue = ADoubleSerializerDeserializer.getDouble(b, s);
                switch (primitiveTypeName) {
                    case INT32:
                        recordConsumer.addInteger((int) doubleValue);
                        break;
                    case INT64:
                        recordConsumer.addLong((long) doubleValue);
                        break;
                    case FLOAT:
                        recordConsumer.addFloat((float) doubleValue);
                        break;
                    case DOUBLE:
                        recordConsumer.addDouble(doubleValue);
                        break;
                    default:
                        throw RuntimeDataException.create(ErrorCode.TYPE_MISMATCH_GENERIC, primitiveTypeName, typeTag);
                }
                break;
            case STRING:
                int utfLength = UTF8StringUtil.getUTFLength(b, s);
                if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY) {
                    byteArrayOutputStream.reset();
                    try {
                        PrintTools.writeUTF8StringAsJSONUnquoted(b, s, l, utfLength, byteArrayOutputStream);
                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                    recordConsumer.addBinary(Binary.fromReusedByteArray(byteArrayOutputStream.getByteArray(), 0,
                            byteArrayOutputStream.getLength()));

                } else {
                    throw RuntimeDataException.create(ErrorCode.TYPE_MISMATCH_GENERIC, primitiveTypeName, typeTag);
                }
                break;
            case BOOLEAN:
                boolean booleanValue = ABooleanSerializerDeserializer.getBoolean(b, s);
                if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                    recordConsumer.addBoolean(booleanValue);
                } else {
                    throw RuntimeDataException.create(ErrorCode.TYPE_MISMATCH_GENERIC, primitiveTypeName, typeTag);
                }
                break;
            case DATE:
                int dateValue = ADateSerializerDeserializer.getChronon(b, s);
                addIntegerType(dateValue, primitiveTypeName, typeTag, recordConsumer);
                break;
            case TIME:
                int timeValue = ATimeSerializerDeserializer.getChronon(b, s);
                addIntegerType(timeValue, primitiveTypeName, typeTag, recordConsumer);
                break;
            case DATETIME:
                long dateTimeValue = ADateTimeSerializerDeserializer.getChronon(b, s);
                addIntegerType(dateTimeValue, primitiveTypeName, typeTag, recordConsumer);
            case NULL:
            case MISSING:
                break;
            default:
                throw RuntimeDataException.create(ErrorCode.TYPE_MISMATCH_GENERIC, primitiveTypeName, typeTag);
        }
    }
}
