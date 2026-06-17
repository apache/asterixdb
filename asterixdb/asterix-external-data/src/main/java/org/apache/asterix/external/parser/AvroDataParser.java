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
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.stream.AvroConverterContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.util.annotations.AiProvenance;

public class AvroDataParser extends AbstractDataParser implements IRecordDataParser<GenericRecord> {
    private final AvroConverterContext parserContext;
    private final IExternalFilterValueEmbedder valueEmbedder;

    public AvroDataParser(IExternalDataRuntimeContext context, Map<String, String> conf) {
        List<Warning> warnings = new ArrayList<>();
        parserContext = new AvroConverterContext(conf, warnings);
        valueEmbedder = context.getValueEmbedder();
    }

    @Override
    public boolean parse(IRawRecord<? extends GenericRecord> record, DataOutput out) throws HyracksDataException {
        try {
            parseObject(record.get(), out);
            valueEmbedder.reset();
            return true;
        } catch (AvroRuntimeException | IOException e) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Add the field even when its value is NULL (previously only non-null/non-missing values were added, silently dropping null fields)")
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

            IValueReference value;
            if (valueEmbedder.shouldEmbed(fieldName, typeTag)) {
                value = valueEmbedder.getEmbeddedValue();
            } else {
                valueBuffer.reset();
                parseValue(fieldSchema, fieldValue, valueBuffer.getDataOutput());
                value = valueBuffer;
            }

            if (value != null || typeTag == ATypeTag.NULL) {
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

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Throw a well-formed unsupported-type error instead of a 1-arg message that crashes formatting")
    private void parseUnion(Schema unionSchema, Object value, DataOutput out) throws IOException {
        Schema actualSchema = getActualSchema(unionSchema, value);
        if (actualSchema != null) {
            parseValue(actualSchema, value, out);
        } else {
            throw createUnsupportedException(unionSchema);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Return the null branch of a nullable union for null values instead of returning null (no match)")
    private Schema getActualSchema(Schema unionSchema, Object value) {
        List<Schema> possibleTypes = unionSchema.getTypes();
        Schema nullSchema = null;
        for (Schema possibleType : possibleTypes) {
            Schema.Type schemaType = possibleType.getType();
            if (schemaType == NULL) {
                nullSchema = possibleType;
            } else if (matchesType(value, schemaType)) {
                return possibleType;
            }
        }
        // value is null — return the null branch of the union if present
        return value == null ? nullSchema : null;
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
            case ARRAY:
                return value instanceof GenericArray;
            case MAP:
                return value instanceof Map;
            default:
                return false;
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Map null values to NULL instead of MISSING; make union fall-through explicit")
    private ATypeTag getTypeTag(Schema schema, Object value) throws HyracksDataException {
        Schema.Type schemaType = schema.getType();
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Uuid) {
            if (parserContext.isUuidAsString()) {
                return ATypeTag.STRING;
            }
            return ATypeTag.UUID;
        }
        if (logicalType instanceof LogicalTypes.Decimal) {
            ensureDecimalToDoubleEnabled(logicalType, parserContext);
            return ATypeTag.DOUBLE;
        } else if (logicalType instanceof LogicalTypes.Date) {
            if (parserContext.isDateAsInt()) {
                return ATypeTag.INTEGER;
            }
            return ATypeTag.DATE;
        } else if (logicalType instanceof LogicalTypes.TimeMicros) {
            if (parserContext.isTimeAsLong()) {
                return ATypeTag.BIGINT;
            }
            return ATypeTag.TIME;
        } else if (logicalType instanceof LogicalTypes.TimeMillis) {
            if (parserContext.isTimeAsLong()) {
                return ATypeTag.BIGINT;
            }
            return ATypeTag.TIME;
        } else if (logicalType instanceof LogicalTypes.TimestampMicros
                || logicalType instanceof LogicalTypes.TimestampMillis
                || logicalType instanceof LogicalTypes.LocalTimestampMicros
                || logicalType instanceof LogicalTypes.LocalTimestampMillis) {
            if (parserContext.isTimestampAsLong()) {
                return ATypeTag.BIGINT;
            }
            return ATypeTag.DATETIME;
        }

        if (value == null) {
            // A null value (e.g. the null branch of a nullable union) maps to NULL, not MISSING.
            // Avro records always carry every declared field, so there is no true "missing" here.
            return ATypeTag.NULL;
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
                throw createUnsupportedException(schema);
            default:
                throw createUnsupportedException(schema);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Only apply the timezone offset to UTC-instant timestamps (timestamp-millis/micros); "
            + "stop double-applying it to time-micros, and stop applying it at all to time-millis/micros "
            + "and local-timestamp-millis/micros, none of which carry an associated timezone")
    private void parseLogicalValue(LogicalType logicalType, Object value, DataOutput out) throws IOException {
        if (logicalType instanceof LogicalTypes.Uuid) {
            if (parserContext.isUuidAsString()) {
                serializeString(value, out);
            } else {
                parserContext.serializeUUID(value, out);
            }
        } else if (logicalType instanceof LogicalTypes.Decimal) {
            LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
            int scale = decimalType.getScale();
            parserContext.serializeDecimal(value, out, scale);
        } else if (logicalType instanceof LogicalTypes.Date) {
            if (parserContext.isDateAsInt()) {
                serializeLong(value, out);
            } else {
                parserContext.serializeDate(value, out);
            }
        } else if (logicalType instanceof LogicalTypes.TimeMicros) {
            // time-micros is a time of day with no reference to any timezone; it must not be shifted
            // by the configured offset (unlike timestamp-millis/micros, which are UTC instants).
            int timeInMillis = (int) TimeUnit.MICROSECONDS.toMillis(((Number) value).longValue());
            if (parserContext.isTimeAsLong()) {
                serializeLong(timeInMillis, out);
            } else {
                parserContext.serializeTime(timeInMillis, out);
            }
        } else if (logicalType instanceof LogicalTypes.TimeMillis) {
            // time-millis is a time of day with no reference to any timezone; it must not be shifted
            // by the configured offset (unlike timestamp-millis/micros, which are UTC instants).
            int timeInMillis = ((Number) value).intValue();
            if (parserContext.isTimeAsLong()) {
                serializeLong(timeInMillis, out);
            } else {
                parserContext.serializeTime(timeInMillis, out);
            }
        } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
            long timeStampInMicros = ((Number) value).longValue();
            int offset = parserContext.getTimeZoneOffset();
            long timeStampInMillis = TimeUnit.MICROSECONDS.toMillis(timeStampInMicros);
            timeStampInMillis = timeStampInMillis + offset;
            if (parserContext.isTimestampAsLong()) {
                serializeLong(timeStampInMillis, out);
            } else {
                parserContext.serializeDateTime(timeStampInMillis, out);
            }
        } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
            long timeStampInMillis = ((Number) value).longValue();
            int offset = parserContext.getTimeZoneOffset();
            timeStampInMillis = timeStampInMillis + offset;
            if (parserContext.isTimestampAsLong()) {
                serializeLong(timeStampInMillis, out);
            } else {
                parserContext.serializeDateTime(timeStampInMillis, out);
            }
        } else if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
            // local-timestamp-micros is already wall-clock time with no associated timezone;
            // unlike timestamp-micros (a UTC instant), it must not be shifted by the configured offset.
            long timeStampInMicros = ((Number) value).longValue();
            long timeStampInMillis = TimeUnit.MICROSECONDS.toMillis(timeStampInMicros);
            if (parserContext.isTimestampAsLong()) {
                serializeLong(timeStampInMillis, out);
            } else {
                parserContext.serializeDateTime(timeStampInMillis, out);
            }
        } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
            // local-timestamp-millis is already wall-clock time with no associated timezone;
            // unlike timestamp-millis (a UTC instant), it must not be shifted by the configured offset.
            long timeStampInMillis = ((Number) value).longValue();
            if (parserContext.isTimestampAsLong()) {
                serializeLong(timeStampInMillis, out);
            } else {
                parserContext.serializeDateTime(timeStampInMillis, out);
            }
        } else {
            throw createUnsupportedException(logicalType.getName());
        }
    }

    private void parseValue(Schema schema, Object value, DataOutput out) throws IOException {
        Schema.Type type = schema.getType();
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            parseLogicalValue(logicalType, value, out);
            return;
        }
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
                serializeInt(value, out);
                break;
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

    private void serializeInt(Object value, DataOutput out) throws HyracksDataException {
        int intValue = ((Number) value).intValue();
        aInt32.setValue(intValue);
        int32Serde.serialize(aInt32, out);
    }

    private void serializeDouble(Object value, DataOutput out) throws HyracksDataException {
        double doubleValue = ((Number) value).doubleValue();
        aDouble.setValue(doubleValue);
        doubleSerde.serialize(aDouble, out);
    }

    private void serializeString(Object value, DataOutput out) throws HyracksDataException {
        aString.setValue(value.toString());
        stringSerde.serialize(aString, out);
    }

    private static void ensureDecimalToDoubleEnabled(LogicalType type, AvroConverterContext context)
            throws RuntimeDataException {
        if (!context.isDecimalToDoubleEnabled()) {
            throw new RuntimeDataException(ErrorCode.AVRO_SUPPORTED_TYPE_WITH_OPTION, type.toString(),
                    ExternalDataConstants.AvroOptions.DECIMAL_TO_DOUBLE);
        }
    }

    private static HyracksDataException createUnsupportedException(Schema schema) {
        return new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Avro Parser", schema);
    }

    private static HyracksDataException createUnsupportedException(String logicalType) {
        return new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Avro Parser, Invalid Logical Type: ", logicalType);
    }

}
