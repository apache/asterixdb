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

import static org.apache.asterix.om.pointables.base.DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.aws.iceberg.converter.IcebergConverterContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.avro.AvroRuntimeException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

public class IcebergParquetDataParser extends AbstractDataParser implements IRecordDataParser<Record> {
    private final IcebergConverterContext parserContext;
    private final IExternalFilterValueEmbedder valueEmbedder;
    private final Schema projectedSchema;
    private final TimestampZoneProjector timestampZoneProjector;
    private final IWarningCollector warningCollector;
    private boolean decimalOverflowWarned = false;

    public IcebergParquetDataParser(IExternalDataRuntimeContext context, Map<String, String> conf,
            Schema projectedSchema) {
        parserContext = new IcebergConverterContext(conf);
        valueEmbedder = context.getValueEmbedder();
        this.projectedSchema = projectedSchema;
        timestampZoneProjector = new TimestampZoneProjector(parserContext.getTimeZoneId());
        warningCollector = context.getTaskContext().getWarningCollector();
    }

    @Override
    public boolean parse(IRawRecord<? extends Record> record, DataOutput out) throws HyracksDataException {
        try {
            parseRootObject(record.get(), out);
            valueEmbedder.reset();
            return true;
        } catch (AvroRuntimeException | IOException e) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
        }
    }

    private void parseRootObject(Record record, DataOutput out) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        valueEmbedder.enterObject();
        for (int i = 0; i < projectedSchema.columns().size(); i++) {
            NestedField field = projectedSchema.columns().get(i);
            String fieldName = field.name();
            Type fieldType = field.type();
            Object fieldValue = record.getField(fieldName);
            ATypeTag typeTag = getTypeTag(fieldType, fieldValue == null, parserContext);
            IValueReference value;
            if (valueEmbedder.shouldEmbed(fieldName, typeTag)) {
                value = valueEmbedder.getEmbeddedValue();
            } else {
                valueBuffer.reset();
                parseValue(fieldType, fieldValue, valueBuffer.getDataOutput());
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

    private void parseValue(Type fieldType, Object value, DataOutput out) throws IOException {
        if (value == null) {
            nullSerde.serialize(ANull.NULL, out);
            return;
        }

        switch (fieldType.typeId()) {
            case BOOLEAN:
                booleanSerde.serialize((Boolean) value ? ABoolean.TRUE : ABoolean.FALSE, out);
                return;
            case INTEGER:
                serializeInteger(value, out);
                return;
            case LONG:
                serializeLong(value, out);
                return;
            case FLOAT:
                serializeFloat(value, out);
                return;
            case DOUBLE:
                serializeDouble(value, out);
                return;
            case STRING:
                serializeString(value, out);
                return;
            case UUID:
                serializeUuid(value, out);
                return;
            case FIXED:
                serializeFixedBinary(value, out);
                return;
            case BINARY:
                serializeBinary(value, out);
                return;
            case DECIMAL:
                ensureDecimalToDoubleEnabled(fieldType, parserContext);
                serializeDecimal((BigDecimal) value, out);
                return;
            case LIST:
                Types.ListType listType = fieldType.asListType();
                parseArray(listType, (List<?>) value, out);
                return;
            case STRUCT:
                parseObject((StructType) fieldType, (StructLike) value, out);
                return;
            case MAP:
                Types.MapType mapType = fieldType.asMapType();
                parseMap(mapType, (Map<?, ?>) value, out);
                return;
            case DATE:
                serializeDate(value, out);
                return;
            case TIME:
                serializeTime(value, out);
                return;
            case TIMESTAMP:
            case TIMESTAMP_NANO:
                serializeTimestamp(fieldType, value, out);
                return;
            case GEOMETRY:
            case GEOGRAPHY:
            case VARIANT:
            case UNKNOWN:
            default:
                throw createUnsupportedException(fieldType);

        }
    }

    private void parseArray(Types.ListType listType, List<?> listValues, DataOutput out) throws IOException {
        if (listValues == null) {
            nullSerde.serialize(ANull.NULL, out);
            return;
        }

        Type elementType = listType.elementType();
        final IMutableValueStorage valueBuffer = parserContext.enterCollection();
        final IAsterixListBuilder arrayBuilder = parserContext.getCollectionBuilder(NESTED_OPEN_AORDERED_LIST_TYPE);
        for (Object listValue : listValues) {
            valueBuffer.reset();
            parseValue(elementType, listValue, valueBuffer.getDataOutput());
            arrayBuilder.addItem(valueBuffer);
        }
        arrayBuilder.write(out, true);
        parserContext.exitCollection(valueBuffer, arrayBuilder);
    }

    private void parseObject(StructType schema, StructLike structLike, DataOutput out) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        valueEmbedder.enterObject();
        for (int i = 0; i < schema.fields().size(); i++) {
            NestedField field = schema.fields().get(i);
            String fieldName = field.name();
            Type fieldType = field.type();
            Object fieldValue = structLike.get(i, Object.class);
            parseValueAndAddObjectField(valueBuffer, objectBuilder, fieldType, fieldName, fieldValue);
        }

        embedMissingValues(objectBuilder, parserContext, valueEmbedder);
        objectBuilder.write(out, true);
        valueEmbedder.exitObject();
        parserContext.exitObject(valueBuffer, null, objectBuilder);
    }

    private void parseMap(Types.MapType mapSchema, Map<?, ?> map, DataOutput out) throws IOException {
        final IMutableValueStorage item = parserContext.enterCollection();
        final IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        IAsterixListBuilder listBuilder =
                parserContext.getCollectionBuilder(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);

        Type keyType = mapSchema.keyType();
        Type valueType = mapSchema.valueType();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            objectBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
            valueBuffer.reset();
            parseValue(keyType, entry.getKey(), valueBuffer.getDataOutput());
            objectBuilder.addField(parserContext.getSerializedFieldName("key"), valueBuffer);
            valueBuffer.reset();
            parseValue(valueType, entry.getValue(), valueBuffer.getDataOutput());
            objectBuilder.addField(parserContext.getSerializedFieldName("value"), valueBuffer);
            item.reset();
            objectBuilder.write(item.getDataOutput(), true);
            listBuilder.addItem(item);
        }

        listBuilder.write(out, true);
        parserContext.exitObject(valueBuffer, null, objectBuilder);
        parserContext.exitCollection(item, listBuilder);
    }

    private void parseValueAndAddObjectField(IMutableValueStorage valueBuffer, IARecordBuilder objectBuilder,
            Type valueType, String fieldName, Object fieldValue) throws IOException {
        ATypeTag typeTag = getTypeTag(valueType, fieldValue == null, parserContext);
        IValueReference value;
        if (valueEmbedder.shouldEmbed(fieldName, typeTag)) {
            value = valueEmbedder.getEmbeddedValue();
        } else {
            valueBuffer.reset();
            parseValue(valueType, fieldValue, valueBuffer.getDataOutput());
            value = valueBuffer;
        }

        if (value != null) {
            // Ignore missing values
            objectBuilder.addField(parserContext.getSerializedFieldName(fieldName), value);
        }
    }

    private void serializeInteger(Object value, DataOutput out) throws HyracksDataException {
        int intValue = (Integer) value;
        aInt64.setValue(intValue);
        int64Serde.serialize(aInt64, out);
    }

    private void serializeLong(Object value, DataOutput out) throws HyracksDataException {
        long longValue = (Long) value;
        aInt64.setValue(longValue);
        int64Serde.serialize(aInt64, out);
    }

    private void serializeFloat(Object value, DataOutput out) throws HyracksDataException {
        Float floatValue = (Float) value;
        aFloat.setValue(floatValue);
        floatSerde.serialize(aFloat, out);
    }

    private void serializeDouble(Object value, DataOutput out) throws HyracksDataException {
        double doubleValue = (Double) value;
        aDouble.setValue(doubleValue);
        doubleSerde.serialize(aDouble, out);
    }

    private void serializeUuid(Object value, DataOutput out) throws HyracksDataException {
        UUID uuid = (UUID) value;
        String uuidValue = uuid.toString();
        char[] buffer = uuidValue.toCharArray();
        aUUID.parseUUIDString(buffer, 0, uuidValue.length());
        uuidSerde.serialize(aUUID, out);
    }

    private void serializeString(Object value, DataOutput out) throws HyracksDataException {
        aString.setValue(value.toString());
        stringSerde.serialize(aString, out);
    }

    private void serializeDecimal(BigDecimal value, DataOutput out) throws HyracksDataException {
        double doubleValue = value.doubleValue();
        if (warningCollector.shouldWarn() && !Double.isFinite(doubleValue) && !decimalOverflowWarned) {
            warningCollector.warn(Warning.of(null, ErrorCode.EXTERNAL_SOURCE_ERROR,
                    "decimal value overflows double representation; infinity will be stored"));
            decimalOverflowWarned = true;
        }
        serializeDouble(doubleValue, out);
    }

    private void serializeBinary(Object value, DataOutput out) throws HyracksDataException {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        if (byteBuffer.hasArray()) {
            aBinary.setValue(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(),
                    byteBuffer.remaining());
        } else {
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.duplicate().get(bytes);
            aBinary.setValue(bytes, 0, bytes.length);
        }
        binarySerde.serialize(aBinary, out);
    }

    private void serializeFixedBinary(Object value, DataOutput out) throws HyracksDataException {
        byte[] bytes = (byte[]) value;
        aBinary.setValue(bytes, 0, bytes.length);
        binarySerde.serialize(aBinary, out);
    }

    private void serializeDate(Object value, DataOutput output) throws HyracksDataException {
        LocalDate localDate = (LocalDate) value;
        if (parserContext.isDateAsInt()) {
            serializeInteger((int) localDate.toEpochDay(), output);
        } else {
            aDate.setValue((int) localDate.toEpochDay());
            dateSerde.serialize(aDate, output);
        }
    }

    private void serializeTime(Object value, DataOutput output) throws HyracksDataException {
        LocalTime localTime = (LocalTime) value;
        int timeInMillis = (int) TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay());
        if (parserContext.isTimeAsInt()) {
            serializeInteger(timeInMillis, output);
        } else {
            aTime.setValue(timeInMillis);
            timeSerde.serialize(aTime, output);
        }
    }

    private void serializeTimestamp(Type type, Object value, DataOutput output) throws HyracksDataException {
        switch (value) {
            case LocalDateTime localDateTime -> serializeWallClockTimestamp(type, localDateTime, output);
            case OffsetDateTime offsetDateTime -> serializeUtcAdjustedTimestamp(type, offsetDateTime, output);
            case null, default -> throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR,
                    value == null ? "unexpected null value for field type (" + type + ")"
                            : "unexpected value type (" + value.getClass() + ") for field type (" + type + ")");
        }
    }

    private void serializeWallClockTimestamp(Type type, LocalDateTime localDateTime, DataOutput output)
            throws HyracksDataException {
        long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
        int nano = localDateTime.getNano();
        if (parserContext.isTimestampAsLong()) {
            long value = isTimestampNano(type) ? toNanos(epochSecond, nano) : toMicros(epochSecond, nano);
            serializeLong(value, output);
        } else {
            aDateTime.setValue(toMillis(epochSecond, nano));
            datetimeSerde.serialize(aDateTime, output);
        }
    }

    private void serializeUtcAdjustedTimestamp(Type type, OffsetDateTime offsetDateTime, DataOutput output)
            throws HyracksDataException {
        long epochSecond = offsetDateTime.toEpochSecond();
        int nano = offsetDateTime.getNano();
        if (parserContext.isTimestampAsLong()) {
            TimestampUnit unit = getTimestampUnit(type);
            long value = unit == TimestampUnit.NANOS ? toNanos(epochSecond, nano) : toMicros(epochSecond, nano);
            value = timestampZoneProjector.projectEpochValue(value, unit);
            serializeLong(value, output);
        } else {
            long epochMillis = toMillis(epochSecond, nano);
            epochMillis = timestampZoneProjector.projectEpochMillis(epochMillis);
            aDateTime.setValue(epochMillis);
            datetimeSerde.serialize(aDateTime, output);
        }
    }

    private static TimestampUnit getTimestampUnit(Type type) {
        return isTimestampNano(type) ? TimestampUnit.NANOS : TimestampUnit.MICROS;
    }

    private static boolean isTimestampNano(Type type) {
        return type.typeId() == Type.TypeID.TIMESTAMP_NANO;
    }

    private static long toMillis(long epochSecond, int nano) throws HyracksDataException {
        try {
            return Math.addExact(Math.multiplyExact(epochSecond, 1_000L), nano / 1_000_000L);
        } catch (ArithmeticException ex) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                    "timestamp value overflows long representation in milliseconds");
        }
    }

    private static long toMicros(long epochSecond, int nano) throws HyracksDataException {
        try {
            return Math.addExact(Math.multiplyExact(epochSecond, 1_000_000L), nano / 1_000L);
        } catch (ArithmeticException ex) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                    "timestamp value overflows long representation in microseconds");
        }
    }

    // Epoch nanoseconds stored in a long overflow outside roughly 1677-09-21 to 2262-04-11
    private static long toNanos(long epochSecond, int nano) throws HyracksDataException {
        try {
            return Math.addExact(Math.multiplyExact(epochSecond, 1_000_000_000L), nano);
        } catch (ArithmeticException ex) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                    "timestamp value overflows long representation in nanoseconds");
        }
    }

    private static HyracksDataException createUnsupportedException(Type type) {
        return new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Iceberg Parser", type.toString());
    }

    private static void ensureDecimalToDoubleEnabled(Type type, IcebergConverterContext context)
            throws RuntimeDataException {
        if (!context.isDecimalToDoubleEnabled()) {
            throw new RuntimeDataException(ErrorCode.PARQUET_SUPPORTED_TYPE_WITH_OPTION, type.toString(),
                    ExternalDataConstants.ParquetOptions.DECIMAL_TO_DOUBLE);
        }
    }

    private static ATypeTag getTypeTag(Type type, boolean isNull, IcebergConverterContext parserContext)
            throws HyracksDataException {
        if (isNull) {
            return ATypeTag.NULL;
        }

        return switch (type.typeId()) {
            case BOOLEAN -> ATypeTag.BOOLEAN;
            case INTEGER, LONG -> ATypeTag.BIGINT;
            case FLOAT -> ATypeTag.FLOAT;
            case DOUBLE -> ATypeTag.DOUBLE;
            case STRING -> ATypeTag.STRING;
            case UUID -> ATypeTag.UUID;
            case FIXED, BINARY -> ATypeTag.BINARY;
            case DECIMAL -> {
                ensureDecimalToDoubleEnabled(type, parserContext);
                yield ATypeTag.DOUBLE;
            }
            case STRUCT -> ATypeTag.OBJECT;
            case LIST, MAP -> ATypeTag.ARRAY;
            case DATE -> {
                if (parserContext.isDateAsInt()) {
                    yield ATypeTag.INTEGER;
                } else {
                    yield ATypeTag.DATE;
                }
            }
            case TIME -> {
                if (parserContext.isTimeAsInt()) {
                    yield ATypeTag.INTEGER;
                } else {
                    yield ATypeTag.TIME;
                }
            }
            case TIMESTAMP, TIMESTAMP_NANO -> {
                if (parserContext.isTimestampAsLong()) {
                    yield ATypeTag.BIGINT;
                } else {
                    yield ATypeTag.DATETIME;
                }
            }
            default -> throw createUnsupportedException(type);
        };
    }

    private enum TimestampUnit {
        MILLIS,
        MICROS,
        NANOS
    }

    // Not thread-safe by design: one instance per parser, parsers are per-task/per-thread.
    private static final class TimestampZoneProjector {
        private static final long MILLIS_PER_SECOND = 1_000L;
        private static final long MICROS_PER_SECOND = 1_000_000L;
        private static final long NANOS_PER_SECOND = 1_000_000_000L;

        private final boolean enabled;
        private final ZoneRules zoneRules;
        private final boolean fixedOffsetZone;
        private final int fixedOffsetSeconds;

        private long validFromEpochSecond = Long.MIN_VALUE;
        private long validUntilEpochSecond = Long.MIN_VALUE;
        private int cachedOffsetSeconds;

        private TimestampZoneProjector(ZoneId zoneId) {
            enabled = zoneId != null;

            if (enabled) {
                zoneRules = zoneId.getRules();
                fixedOffsetZone = zoneRules.isFixedOffset();
                fixedOffsetSeconds = fixedOffsetZone ? zoneRules.getOffset(Instant.EPOCH).getTotalSeconds() : 0;
            } else {
                zoneRules = null;
                fixedOffsetZone = true;
                fixedOffsetSeconds = 0;
            }
        }

        private long projectEpochMillis(long epochMillis) throws HyracksDataException {
            return projectEpochValue(epochMillis, TimestampUnit.MILLIS);
        }

        private long projectEpochValue(long epochValue, TimestampUnit unit) throws HyracksDataException {
            if (!enabled) {
                return epochValue;
            }

            int offsetSeconds = getOffsetSeconds(epochValue, unit);

            try {
                return switch (unit) {
                    case MILLIS -> Math.addExact(epochValue, offsetSeconds * MILLIS_PER_SECOND);
                    case MICROS -> Math.addExact(epochValue, offsetSeconds * MICROS_PER_SECOND);
                    case NANOS -> Math.addExact(epochValue, offsetSeconds * NANOS_PER_SECOND);
                };
            } catch (ArithmeticException ex) {
                throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                        "timestamp value overflows long representation after applying timezone configuration");
            }
        }

        private int getOffsetSeconds(long epochValue, TimestampUnit unit) {
            if (fixedOffsetZone) {
                return fixedOffsetSeconds;
            }

            long epochSecond = toEpochSecond(epochValue, unit);

            if (epochSecond >= validFromEpochSecond && epochSecond < validUntilEpochSecond) {
                return cachedOffsetSeconds;
            }

            return refreshOffsetCache(epochSecond);
        }

        private int refreshOffsetCache(long epochSecond) {
            Instant instant = Instant.ofEpochSecond(epochSecond);

            ZoneOffset offset = zoneRules.getOffset(instant);
            // Use epochSecond + 1ns so that if the record falls exactly on a transition boundary,
            // previousTransition captures that transition as the start of the current offset period.
            ZoneOffsetTransition previous = zoneRules.previousTransition(Instant.ofEpochSecond(epochSecond, 1L));
            ZoneOffsetTransition next = zoneRules.nextTransition(instant);

            validFromEpochSecond = previous == null ? Long.MIN_VALUE : previous.getInstant().getEpochSecond();
            validUntilEpochSecond = next == null ? Long.MAX_VALUE : next.getInstant().getEpochSecond();
            cachedOffsetSeconds = offset.getTotalSeconds();

            return cachedOffsetSeconds;
        }

        private static long toEpochSecond(long epochValue, TimestampUnit unit) {
            return switch (unit) {
                case MILLIS -> Math.floorDiv(epochValue, MILLIS_PER_SECOND);
                case MICROS -> Math.floorDiv(epochValue, MICROS_PER_SECOND);
                case NANOS -> Math.floorDiv(epochValue, NANOS_PER_SECOND);
            };
        }
    }
}
