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
package org.apache.asterix.external.parser;

import static org.apache.avro.Schema.Type.NULL;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;

public class AvroDataParser extends AbstractDataParser implements IRecordDataParser<GenericRecord> {
    private final ParserContext parserContext;

    public AvroDataParser(IExternalDataRuntimeContext context) {
        parserContext = new ParserContext();
    }

    @Override
    public boolean parse(IRawRecord<? extends GenericRecord> record, DataOutput out) throws HyracksDataException {
        try {
            parseObject(record.get(), out);
            return true;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private final void parseObject(GenericRecord record, DataOutput out) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        Schema schema = record.getSchema();
        for (Schema.Field field : schema.getFields()) {
            if (record.get(field.name()) != null) {
                valueBuffer.reset();
                parseValue(field.schema(), record.get(field.name()), valueBuffer.getDataOutput());
                objectBuilder.addField(parserContext.getSerializedFieldName(field.name()), valueBuffer);
            }
        }
        objectBuilder.write(out, true);
        parserContext.exitObject(valueBuffer, null, objectBuilder);
    }

    private final void parseArray(Schema arraySchema, Collection<?> elements, DataOutput out) throws IOException {
        Schema elementSchema = arraySchema.getElementType();
        final IMutableValueStorage valueBuffer = parserContext.enterCollection();
        final IAsterixListBuilder arrayBuilder =
                parserContext.getCollectionBuilder(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (Object element : elements) {
            valueBuffer.reset();
            parseValue(elementSchema, element, valueBuffer.getDataOutput());
            arrayBuilder.addItem(valueBuffer);
        }
        arrayBuilder.write(out, true);
        parserContext.exitCollection(valueBuffer, arrayBuilder);
    }

    private void parseMap(Schema mapSchema, Map<String, ?> map, DataOutput out) throws IOException {
        Schema valueSchema = mapSchema.getValueType();
        final IMutableValueStorage valueBuffer = parserContext.enterCollection();
        final IMutableValueStorage keyBuffer = parserContext.enterCollection();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            keyBuffer.reset();
            valueBuffer.reset();
            serializeString(entry.getKey(), Schema.Type.STRING, keyBuffer.getDataOutput());
            parseValue(valueSchema, entry.getValue(), valueBuffer.getDataOutput());
            objectBuilder.addField(keyBuffer, valueBuffer);
        }
        objectBuilder.write(out, true);
        parserContext.exitObject(valueBuffer, null, objectBuilder);
    }

    private final void parseUnion(Schema unionSchema, Object value, DataOutput out) throws IOException {
        List<Schema> possibleTypes = unionSchema.getTypes();
        for (Schema possibleType : possibleTypes) {
            Schema.Type schemaType = possibleType.getType();
            if (possibleType.getType() != NULL) {
                if (matchesType(value, schemaType)) {
                    parseValue(possibleType, value, out);
                    return;
                }
            }
        }
    }

    private boolean matchesType(Object value, Schema.Type schemaType) {
        switch (schemaType) {
            case INT:
                return value instanceof Integer;
            case STRING:
                return value instanceof CharSequence;
            case LONG:
                return value instanceof Long;
            case FLOAT:
                return value instanceof Float;
            case DOUBLE:
                return value instanceof Double;
            case BOOLEAN:
                return value instanceof Boolean;
            case BYTES:
                return value instanceof Byte;
            case RECORD:
                return value instanceof GenericData.Record;
            default:
                return false;
        }
    }

    private void parseValue(Schema schema, Object value, DataOutput out) throws IOException {
        Schema.Type type = schema.getType();
        switch (type) {
            case RECORD:
                parseObject((GenericRecord) value, out);
                break;
            case ARRAY:
                parseArray(schema, (Collection<?>) value, out);
                break;
            case UNION:
                parseUnion(schema, value, out);
                break;
            case MAP:
                parseMap(schema, (Map<String, ?>) value, out);
                break;
            case ENUM:
            case FIXED:
            case NULL:
                nullSerde.serialize(ANull.NULL, out);
                break;
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                serializeNumeric(value, type, out);
                break;
            case STRING:
                serializeString(value, type, out);
                break;
            case BYTES:
                serializeBinary(value, type, out);
                break;
            case BOOLEAN:
                if ((Boolean) value) {
                    booleanSerde.serialize(ABoolean.TRUE, out);
                } else {
                    booleanSerde.serialize(ABoolean.FALSE, out);
                }
                break;
            default:
                throw new RuntimeDataException(ErrorCode.PARSE_ERROR, value.toString());
        }
    }

    private void serializeNumeric(Object value, Schema.Type type, DataOutput out) throws IOException {
        switch (type) {
            case INT:
                aInt32.setValue((Integer) value);
                int32Serde.serialize(aInt32, out);
                break;
            case LONG:
                aInt64.setValue((Long) value);
                int64Serde.serialize(aInt64, out);
                break;
            case FLOAT:
                aFloat.setValue((Float) value);
                floatSerde.serialize(aFloat, out);
                break;
            case DOUBLE:
                aDouble.setValue((Double) value);
                doubleSerde.serialize(aDouble, out);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Error");
        }
    }

    private void serializeString(Object value, Schema.Type type, DataOutput out) throws IOException {
        switch (type) {
            case STRING:
                aString.setValue(value.toString());
                stringSerde.serialize(aString, out);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Error");
        }
    }

    private void serializeBinary(Object value, Schema.Type type, DataOutput out) throws IOException {
        switch (type) {
            case BYTES:
                aBinary.setValue(((ByteBuffer) value).array(), 0, ((ByteBuffer) value).array().length);
                binarySerde.serialize(aBinary, out);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Error");
        }
    }
}
