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
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;

public class IcebergParquetDataParser extends AbstractDataParser implements IRecordDataParser<Record> {
    private final IcebergConverterContext parserContext;
    private final IExternalFilterValueEmbedder valueEmbedder;
    private final Schema projectedSchema;
    private final TimestampZoneProjector timestampZoneProjector;

    public IcebergParquetDataParser(IExternalDataRuntimeContext context, Map<String, String> conf,
            Schema projectedSchema) {
        parserContext = new IcebergConverterContext(conf);
        valueEmbedder = context.getValueEmbedder();
        this.projectedSchema = projectedSchema;
        timestampZoneProjector = new TimestampZoneProjector(parserContext.getTimeZoneId());
    }

    @Override
    public boolean parse(IRawRecord<? extends Record> record, DataOutput out) throws HyracksDataException {
        try {
            parseRootObject(record.get(), out);
            valueEmbedder.reset();
            return true;
        } catch (HyracksDataException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
        }
    }

    // Look up root fields by name, not index: when a scan task has delete files, records are materialized
    // against IcebergFileRecordReader's deleteFilter.requiredSchema() (a superset of projectedSchema with
    // extra equality/positional columns), so positional access would misalign. Nested structs (parseObject)
    // are not augmented this way, so index access is fine there.
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Hoisted projectedSchema.columns() out of the loop condition (was re-evaluated per column per row) and added the comment above explaining why name-based lookup here must not be changed to index-based")
    private void parseRootObject(Record record, DataOutput out) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        valueEmbedder.enterObject();
        List<NestedField> fields = projectedSchema.columns();
        for (NestedField field : fields) {
            String fieldName = field.name();
            Type fieldType = field.type();
            Object fieldValue = record.getField(fieldName);
            ATypeTag typeTag = getTypeTag(fieldType, fieldValue, parserContext);
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

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Added VARIANT case")
    private void parseValue(Type fieldType, Object value, DataOutput out) throws IOException {
        if (value == null) {
            serializeNull(out);
            return;
        }

        switch (fieldType.typeId()) {
            case BOOLEAN:
                serializeBoolean(value, out);
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
                serializeDecimal(value, out);
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
            case VARIANT:
                parseVariant((Variant) value, out);
                return;
            case GEOMETRY:
            case GEOGRAPHY:
            case UNKNOWN:
            default:
                throw createUnsupportedException(fieldType);

        }
    }

    private void parseArray(Types.ListType listType, List<?> listValues, DataOutput out) throws IOException {
        if (listValues == null) {
            serializeNull(out);
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

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Hoisted schema.fields() out of the loop condition, was re-evaluated per field per row")
    private void parseObject(StructType schema, StructLike structLike, DataOutput out) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        valueEmbedder.enterObject();
        List<NestedField> fields = schema.fields();
        for (int i = 0; i < fields.size(); i++) {
            NestedField field = fields.get(i);
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

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Entry point for VARIANT column: unwraps Iceberg Variant and delegates to parseVariantValue")
    private void parseVariant(Variant variant, DataOutput out) throws IOException {
        parseVariantValue(variant.value(), out, 1);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Dispatches on PhysicalType and serializes each Variant primitive/object/array to the correct AsterixDB type. Includes a configurable max-depth guard (variantDepth WITH-clause option, default 500, since Variant nesting has no schema bound), a correct UUID read (java.util.UUID via UUIDUtil.convert, not the String that PhysicalType.UUID.javaClass() claims), the decimal-to-double opt-in gate for DECIMAL4/8/16, and timezone projection for TIMESTAMPTZ/TIMESTAMPTZ_NANOS")
    // Keep the PhysicalType -> serialized-shape mapping here in sync with getVariantTypeTag's
    // PhysicalType -> ATypeTag mapping: getVariantTypeTag predicts what this method will write, and
    // nothing enforces that the two switches agree beyond this comment.
    private void parseVariantValue(VariantValue variantValue, DataOutput out, int depth) throws IOException {
        int maxVariantDepth = parserContext.getMaxVariantDepth();
        if (depth > maxVariantDepth) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR,
                    "Variant nesting depth exceeds the maximum supported depth of " + maxVariantDepth);
        }
        PhysicalType physicalType = variantValue.type();
        switch (physicalType) {
            case NULL:
                serializeNull(out);
                return;
            case BOOLEAN_TRUE:
                serializeBoolean(true, out);
                return;
            case BOOLEAN_FALSE:
                serializeBoolean(false, out);
                return;
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                serializeLong(((Number) variantValue.asPrimitive().get()).longValue(), out);
                return;
            case FLOAT:
                serializeFloat(variantValue.asPrimitive().get(), out);
                return;
            case DOUBLE:
                serializeDouble(variantValue.asPrimitive().get(), out);
                return;
            case DECIMAL4:
            case DECIMAL8:
            case DECIMAL16:
                ensureDecimalToDoubleEnabled(physicalType.toString(), parserContext);
                serializeDecimal(variantValue.asPrimitive().get(), out);
                return;
            case STRING:
                serializeString(variantValue.asPrimitive().get(), out);
                return;
            case BINARY:
                serializeBinary(variantValue.asPrimitive().get(), out);
                return;
            case DATE:
                serializeDateEpochDay((Integer) variantValue.asPrimitive().get(), out);
                return;
            case TIME:
                serializeTimeMicros((Long) variantValue.asPrimitive().get(), out);
                return;
            case TIMESTAMPTZ:
            case TIMESTAMPNTZ: {
                long epochMicros = (Long) variantValue.asPrimitive().get();
                if (physicalType == PhysicalType.TIMESTAMPTZ) {
                    epochMicros = timestampZoneProjector.projectEpochValue(epochMicros, TimestampUnit.MICROS);
                }
                serializeTimestampMicros(epochMicros, out);
                return;
            }
            case TIMESTAMPTZ_NANOS:
            case TIMESTAMPNTZ_NANOS: {
                long epochNanos = (Long) variantValue.asPrimitive().get();
                if (physicalType == PhysicalType.TIMESTAMPTZ_NANOS) {
                    epochNanos = timestampZoneProjector.projectEpochValue(epochNanos, TimestampUnit.NANOS);
                }
                serializeTimestampNanos(epochNanos, out);
                return;
            }
            case UUID:
                serializeUuid(variantValue.asPrimitive().get(), out);
                return;
            case OBJECT:
                parseVariantObject(variantValue.asObject(), out, depth);
                return;
            case ARRAY:
                parseVariantArray(variantValue.asArray(), out, depth);
                return;
            default:
                throw createVariantUnsupportedException(physicalType);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Serializes a VariantObject as an open AsterixDB record")
    private void parseVariantObject(VariantObject variantObject, DataOutput out, int depth) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        for (String fieldName : variantObject.fieldNames()) {
            valueBuffer.reset();
            parseVariantValue(variantObject.get(fieldName), valueBuffer.getDataOutput(), depth + 1);
            objectBuilder.addField(parserContext.getSerializedFieldName(fieldName), valueBuffer);
        }
        objectBuilder.write(out, true);
        parserContext.exitObject(valueBuffer, null, objectBuilder);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Serializes a VariantArray as an AsterixDB ordered list")
    private void parseVariantArray(VariantArray variantArray, DataOutput out, int depth) throws IOException {
        final IMutableValueStorage valueBuffer = parserContext.enterCollection();
        final IAsterixListBuilder arrayBuilder = parserContext.getCollectionBuilder(NESTED_OPEN_AORDERED_LIST_TYPE);
        for (int i = 0; i < variantArray.numElements(); i++) {
            valueBuffer.reset();
            parseVariantValue(variantArray.get(i), valueBuffer.getDataOutput(), depth + 1);
            arrayBuilder.addItem(valueBuffer);
        }
        arrayBuilder.write(out, true);
        parserContext.exitCollection(valueBuffer, arrayBuilder);
    }

    private void parseValueAndAddObjectField(IMutableValueStorage valueBuffer, IARecordBuilder objectBuilder,
            Type valueType, String fieldName, Object fieldValue) throws IOException {
        ATypeTag typeTag = getTypeTag(valueType, fieldValue, parserContext);
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

    private void serializeNull(DataOutput out) throws HyracksDataException {
        nullSerde.serialize(ANull.NULL, out);
    }

    private void serializeBoolean(Object value, DataOutput out) throws HyracksDataException {
        boolean booleanValue = (Boolean) value;
        booleanSerde.serialize(booleanValue ? ABoolean.TRUE : ABoolean.FALSE, out);
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
        String uuidStr = value.toString();
        aUUID.parseUUIDString(uuidStr.toCharArray(), 0, uuidStr.length());
        uuidSerde.serialize(aUUID, out);
    }

    private void serializeString(Object value, DataOutput out) throws HyracksDataException {
        aString.setValue(value.toString());
        stringSerde.serialize(aString, out);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Removed the decimal-overflow-to-Infinity warning branch (and the now-unused warningCollector/decimalOverflowWarned state): decimal precision is capped at 38 significant digits, so doubleValue() can never actually return an infinite result for any value the type system can produce")
    private void serializeDecimal(Object value, DataOutput out) throws HyracksDataException {
        // Decimal precision is capped at 38 (DECIMAL16/Iceberg's max), so magnitude is bounded by ~10^38,
        // nowhere near Double.MAX_VALUE (~1.8x10^308) — doubleValue() can never return an infinite result here.
        double doubleValue = ((BigDecimal) value).doubleValue();
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
        serializeDateEpochDay((int) ((LocalDate) value).toEpochDay(), output);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Core date serializer accepting days-since-epoch; shared by serializeDate and Variant DATE handling")
    private void serializeDateEpochDay(int epochDay, DataOutput out) throws HyracksDataException {
        if (parserContext.isDateAsInt()) {
            serializeInteger(epochDay, out);
        } else {
            aDate.setValue(epochDay);
            dateSerde.serialize(aDate, out);
        }
    }

    private void serializeTime(Object value, DataOutput output) throws HyracksDataException {
        serializeTimeMillis((int) TimeUnit.NANOSECONDS.toMillis(((LocalTime) value).toNanoOfDay()), output);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Core time serializer accepting milliseconds-of-day; shared by serializeTime and serializeTimeMicros")
    private void serializeTimeMillis(int timeInMillis, DataOutput out) throws HyracksDataException {
        if (parserContext.isTimeAsInt()) {
            serializeInteger(timeInMillis, out);
        } else {
            aTime.setValue(timeInMillis);
            timeSerde.serialize(aTime, out);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Converts Variant TIME (microseconds-of-day) to milliseconds and delegates to serializeTimeMillis")
    private void serializeTimeMicros(long timeMicros, DataOutput out) throws HyracksDataException {
        serializeTimeMillis((int) TimeUnit.MICROSECONDS.toMillis(timeMicros), out);
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
        if (isTimestampNano(type)) {
            serializeTimestampNanos(toNanos(epochSecond, nano), output);
        } else {
            serializeTimestampMicros(toMicros(epochSecond, nano), output);
        }
    }

    private void serializeUtcAdjustedTimestamp(Type type, OffsetDateTime offsetDateTime, DataOutput output)
            throws HyracksDataException {
        long epochSecond = offsetDateTime.toEpochSecond();
        int nano = offsetDateTime.getNano();
        if (isTimestampNano(type)) {
            serializeTimestampNanos(
                    timestampZoneProjector.projectEpochValue(toNanos(epochSecond, nano), TimestampUnit.NANOS), output);
        } else {
            serializeTimestampMicros(
                    timestampZoneProjector.projectEpochValue(toMicros(epochSecond, nano), TimestampUnit.MICROS),
                    output);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Handles isTimestampAsLong flag for microsecond-precision timestamps; shared by Iceberg TIMESTAMP and Variant TIMESTAMPTZ/TIMESTAMPNTZ")
    private void serializeTimestampMicros(long epochMicros, DataOutput out) throws HyracksDataException {
        if (parserContext.isTimestampAsLong()) {
            serializeLong(epochMicros, out);
        } else {
            serializeDatetimeMillis(TimeUnit.MICROSECONDS.toMillis(epochMicros), out);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Handles isTimestampAsLong flag for nanosecond-precision timestamps; shared by Iceberg TIMESTAMP_NANO and Variant TIMESTAMPTZ_NANOS/TIMESTAMPNTZ_NANOS")
    private void serializeTimestampNanos(long epochNanos, DataOutput out) throws HyracksDataException {
        if (parserContext.isTimestampAsLong()) {
            serializeLong(epochNanos, out);
        } else {
            serializeDatetimeMillis(TimeUnit.NANOSECONDS.toMillis(epochNanos), out);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_4_6, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Core datetime serializer accepting epoch milliseconds; shared by timestamp helpers and Variant TIMESTAMP handling")
    private void serializeDatetimeMillis(long epochMillis, DataOutput out) throws HyracksDataException {
        aDateTime.setValue(epochMillis);
        datetimeSerde.serialize(aDateTime, out);
    }

    private static boolean isTimestampNano(Type type) {
        return type.typeId() == Type.TypeID.TIMESTAMP_NANO;
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

    private static HyracksDataException createVariantUnsupportedException(PhysicalType type) {
        return new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Iceberg Variant Parser", type.toString());
    }

    private static void ensureDecimalToDoubleEnabled(Type type, IcebergConverterContext context)
            throws RuntimeDataException {
        ensureDecimalToDoubleEnabled(type.toString(), context);
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "String-typed overload so the decimal-to-double gate can also be enforced for Variant DECIMAL4/8/16, which have a PhysicalType rather than an Iceberg Type")
    private static void ensureDecimalToDoubleEnabled(String typeDescription, IcebergConverterContext context)
            throws RuntimeDataException {
        if (!context.isDecimalToDoubleEnabled()) {
            throw new RuntimeDataException(ErrorCode.PARQUET_SUPPORTED_TYPE_WITH_OPTION, typeDescription,
                    ExternalDataConstants.IcebergOptions.DECIMAL_TO_DOUBLE);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Added the VARIANT case, taking the actual field value (not just an isNull flag) and delegating to getVariantTypeTag so the reported tag reflects the value's real PhysicalType. Also reports ATypeTag.BIGINT instead of INTEGER for DATE/TIME-as-int, matching serializeInteger's actual int64 output")
    private static ATypeTag getTypeTag(Type type, Object value, IcebergConverterContext parserContext)
            throws HyracksDataException {
        if (value == null) {
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
            case VARIANT -> getVariantTypeTag((Variant) value, parserContext);
            case LIST, MAP -> ATypeTag.ARRAY;
            case DATE -> {
                if (parserContext.isDateAsInt()) {
                    // Despite the "date-to-int" option name, serializeDateEpochDay actually writes via
                    // serializeInteger, which serializes as BIGINT (int64), not INTEGER (int32). Reporting
                    // BIGINT here to match the real on-wire type.
                    yield ATypeTag.BIGINT;
                } else {
                    yield ATypeTag.DATE;
                }
            }
            case TIME -> {
                if (parserContext.isTimeAsInt()) {
                    // Same as the DATE case above: "time-to-int" is serialized as BIGINT (int64) via
                    // serializeInteger, not INTEGER (int32) — reporting BIGINT to match reality.
                    yield ATypeTag.BIGINT;
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

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Resolves the real tag for a VARIANT column's value by inspecting its PhysicalType, mirroring the exact dispatch/option-flag logic in parseVariantValue")
    // Keep this PhysicalType -> ATypeTag mapping in sync with parseVariantValue's PhysicalType -> serialized-shape
    // mapping: this method predicts what that method will write, and nothing enforces that the two switches agree
    // beyond this comment.
    private static ATypeTag getVariantTypeTag(Variant variant, IcebergConverterContext parserContext)
            throws HyracksDataException {
        PhysicalType physicalType = variant.value().type();
        return switch (physicalType) {
            case NULL -> ATypeTag.NULL;
            case BOOLEAN_TRUE, BOOLEAN_FALSE -> ATypeTag.BOOLEAN;
            case INT8, INT16, INT32, INT64 -> ATypeTag.BIGINT;
            case FLOAT -> ATypeTag.FLOAT;
            case DOUBLE -> ATypeTag.DOUBLE;
            case DECIMAL4, DECIMAL8, DECIMAL16 -> {
                ensureDecimalToDoubleEnabled(physicalType.toString(), parserContext);
                yield ATypeTag.DOUBLE;
            }
            case STRING -> ATypeTag.STRING;
            case BINARY -> ATypeTag.BINARY;
            case UUID -> ATypeTag.UUID;
            case DATE -> parserContext.isDateAsInt() ? ATypeTag.BIGINT : ATypeTag.DATE;
            case TIME -> parserContext.isTimeAsInt() ? ATypeTag.BIGINT : ATypeTag.TIME;
            case TIMESTAMPTZ, TIMESTAMPNTZ, TIMESTAMPTZ_NANOS, TIMESTAMPNTZ_NANOS ->
                parserContext.isTimestampAsLong() ? ATypeTag.BIGINT : ATypeTag.DATETIME;
            case OBJECT -> ATypeTag.OBJECT;
            case ARRAY -> ATypeTag.ARRAY;
            default -> throw createVariantUnsupportedException(physicalType);
        };
    }

    private enum TimestampUnit {
        MICROS,
        NANOS
    }

    // Not thread-safe by design: one instance per parser, parsers are per-task/per-thread.
    private static final class TimestampZoneProjector {
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

        private long projectEpochValue(long epochValue, TimestampUnit unit) throws HyracksDataException {
            if (!enabled) {
                return epochValue;
            }

            int offsetSeconds = getOffsetSeconds(epochValue, unit);

            try {
                return switch (unit) {
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
                case MICROS -> Math.floorDiv(epochValue, MICROS_PER_SECOND);
                case NANOS -> Math.floorDiv(epochValue, NANOS_PER_SECOND);
            };
        }
    }
}
