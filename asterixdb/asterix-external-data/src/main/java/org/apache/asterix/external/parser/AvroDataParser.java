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
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;

public class AvroDataParser extends AbstractDataParser implements IRecordDataParser<GenericRecord> {
    private final ParserContext parserContext;
    private final IExternalFilterValueEmbedder valueEmbedder;

    public AvroDataParser(IExternalDataRuntimeContext context) {
        parserContext = new ParserContext();
        valueEmbedder = context.getValueEmbedder();
    }

    @Override
    public boolean parse(IRawRecord<? extends GenericRecord> record, DataOutput out) throws HyracksDataException {
        try {
            parseObject(record.get(), out);
            valueEmbedder.reset();
            return true;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void parseObject(GenericRecord record, DataOutput out) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        Schema schema = record.getSchema();
        valueEmbedder.enterObject();
        for (Schema.Field field : schema.getFields()) {
            Schema fieldSchema = field.schema();
            String fieldName = field.name();
            Object fieldValue = record.get(fieldName);
            ATypeTag typeTag = getTypeTag(fieldSchema, fieldValue);

            IValueReference value = null;
            if (valueEmbedder.shouldEmbed(fieldName, typeTag)) {
                value = valueEmbedder.getEmbeddedValue();
            } else if (fieldValue != null) {
                valueBuffer.reset();
                parseValue(fieldSchema, fieldValue, valueBuffer.getDataOutput());
                value = valueBuffer;
            }

            if (value != null) {
                // Ignore missing values
                objectBuilder.addField(parserContext.getSerializedFieldName(fieldName), value);
            }
        }

        embedMissingValues(objectBuilder, parserContext, valueEmbedder);
        objectBuilder.write(out, true);
        valueEmbedder.exitObject();
        parserContext.exitObject(valueBuffer, null, objectBuilder);
    }

    private void parseArray(Schema arraySchema, Collection<?> elements, DataOutput out) throws IOException {
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
        final IMutableValueStorage item = parserContext.enterCollection();
        final IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        IAsterixListBuilder listBuilder =
                parserContext.getCollectionBuilder(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);

        for (Map.Entry<String, ?> entry : map.entrySet()) {
            objectBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
            valueBuffer.reset();
            serializeString(entry.getKey(), valueBuffer.getDataOutput());
            objectBuilder.addField(parserContext.getSerializedFieldName("key"), valueBuffer);
            valueBuffer.reset();
            parseValue(mapSchema.getValueType(), entry.getValue(), valueBuffer.getDataOutput());
            objectBuilder.addField(parserContext.getSerializedFieldName("value"), valueBuffer);
            item.reset();
            objectBuilder.write(item.getDataOutput(), true);
            listBuilder.addItem(item);
        }

        listBuilder.write(out, true);
        parserContext.exitObject(valueBuffer, null, objectBuilder);
        parserContext.exitCollection(item, listBuilder);
    }

    private void parseUnion(Schema unionSchema, Object value, DataOutput out) throws IOException {
        Schema actualSchema = getActualSchema(unionSchema, value);
        if (actualSchema != null) {
            parseValue(actualSchema, value, out);
        } else {
            throw new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, unionSchema.getType());
        }
    }

    private Schema getActualSchema(Schema unionSchema, Object value) {
        List<Schema> possibleTypes = unionSchema.getTypes();
        for (Schema possibleType : possibleTypes) {
            Schema.Type schemaType = possibleType.getType();
            if (schemaType != NULL) {
                if (matchesType(value, schemaType)) {
                    return possibleType;
                }
            }
        }
        return null;
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
                return value instanceof ByteBuffer;
            case RECORD:
                return value instanceof GenericData.Record;
            default:
                return false;
        }
    }

    private ATypeTag getTypeTag(Schema schema, Object value) throws HyracksDataException {
        Schema.Type schemaType = schema.getType();
        if (value == null) {
            // The 'value' is missing
            return ATypeTag.MISSING;
        }

        switch (schemaType) {
            case NULL:
                return ATypeTag.NULL;
            case BOOLEAN:
                return ATypeTag.BOOLEAN;
            case INT:
            case LONG:
                return ATypeTag.BIGINT;
            case FLOAT:
            case DOUBLE:
                return ATypeTag.DOUBLE;
            case STRING:
                return ATypeTag.STRING;
            case BYTES:
                return ATypeTag.BINARY;
            case RECORD:
                return ATypeTag.OBJECT;
            case ARRAY:
            case MAP:
                return ATypeTag.ARRAY;
            case UNION:
                Schema actualSchema = getActualSchema(schema, value);
                if (actualSchema != null) {
                    return getTypeTag(actualSchema, value);
                }
            default:
                throw createUnsupportedException(schema);

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
            case NULL:
                nullSerde.serialize(ANull.NULL, out);
                break;
            case INT:
            case LONG:
                serializeLong(value, out);
                break;
            case FLOAT:
            case DOUBLE:
                serializeDouble(value, out);
                break;
            case STRING:
                serializeString(value, out);
                break;
            case BYTES:
                aBinary.setValue(((ByteBuffer) value).array(), 0, ((ByteBuffer) value).array().length);
                binarySerde.serialize(aBinary, out);
                break;
            case BOOLEAN:
                if ((Boolean) value) {
                    booleanSerde.serialize(ABoolean.TRUE, out);
                } else {
                    booleanSerde.serialize(ABoolean.FALSE, out);
                }
                break;
            default:
                throw createUnsupportedException(schema);
        }
    }

    private void serializeLong(Object value, DataOutput out) throws HyracksDataException {
        long intValue = ((Number) value).longValue();
        aInt64.setValue(intValue);
        int64Serde.serialize(aInt64, out);
    }

    private void serializeDouble(Object value, DataOutput out) throws HyracksDataException {
        double doubleValue = ((Number) value).doubleValue();
        aDouble.setValue(doubleValue);
        doubleSerde.serialize(aDouble, out);
    }

    private void serializeString(Object value, DataOutput out) throws IOException {
        aString.setValue(value.toString());
        stringSerde.serialize(aString, out);
    }

    private static HyracksDataException createUnsupportedException(Schema schema) throws HyracksDataException {
        return new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Avro Parser", schema);
    }
}
