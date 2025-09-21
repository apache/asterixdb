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
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    public IcebergParquetDataParser(IExternalDataRuntimeContext context, Map<String, String> conf,
            Schema projectedSchema) {
        List<Warning> warnings = new ArrayList<>();
        parserContext = new IcebergConverterContext(conf, warnings);
        valueEmbedder = context.getValueEmbedder();
        this.projectedSchema = projectedSchema;
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
            ATypeTag typeTag = getTypeTag(fieldType, record.get(i) == null, parserContext);
            IValueReference value;
            if (valueEmbedder.shouldEmbed(fieldName, typeTag)) {
                value = valueEmbedder.getEmbeddedValue();
            } else {
                valueBuffer.reset();
                parseValue(fieldType, record.get(i), valueBuffer.getDataOutput());
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
            ATypeTag typeTag =
                    getTypeTag(fieldType, structLike.get(i, fieldType.typeId().javaClass()) == null, parserContext);
            IValueReference value;
            if (valueEmbedder.shouldEmbed(fieldName, typeTag)) {
                value = valueEmbedder.getEmbeddedValue();
            } else {
                valueBuffer.reset();
                parseValue(fieldType, structLike.get(i, fieldType.typeId().javaClass()), valueBuffer.getDataOutput());
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
        float floatValue = (Float) value;
        aDouble.setValue(floatValue);
        doubleSerde.serialize(aDouble, out);
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
        serializeDouble(value.doubleValue(), out);
    }

    private void serializeBinary(Object value, DataOutput out) throws HyracksDataException {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        aBinary.setValue(byteBuffer.array(), 0, byteBuffer.array().length);
        binarySerde.serialize(aBinary, out);
    }

    private void serializeFixedBinary(Object value, DataOutput out) throws HyracksDataException {
        byte[] bytes = (byte[]) value;
        aBinary.setValue(bytes, 0, bytes.length);
        binarySerde.serialize(aBinary, out);
    }

    public void serializeDate(Object value, DataOutput output) throws HyracksDataException {
        LocalDate localDate = (LocalDate) value;
        aDate.setValue((int) localDate.toEpochDay());
        dateSerde.serialize(aDate, output);
    }

    public void serializeTime(Object value, DataOutput output) throws HyracksDataException {
        LocalTime localTime = (LocalTime) value;
        aTime.setValue((int) (localTime.toNanoOfDay() / 1_000_000));
        timeSerde.serialize(aTime, output);
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

    public static ATypeTag getTypeTag(Type type, boolean isNull, IcebergConverterContext parserContext)
            throws HyracksDataException {
        if (isNull) {
            return ATypeTag.NULL;
        }

        return switch (type.typeId()) {
            case BOOLEAN -> ATypeTag.BOOLEAN;
            case INTEGER, LONG -> ATypeTag.BIGINT;
            case FLOAT, DOUBLE -> ATypeTag.DOUBLE;
            case STRING -> ATypeTag.STRING;
            case UUID -> ATypeTag.UUID;
            case FIXED, BINARY -> ATypeTag.BINARY;
            case DECIMAL -> {
                ensureDecimalToDoubleEnabled(type, parserContext);
                yield ATypeTag.DOUBLE;
            }
            case STRUCT -> ATypeTag.OBJECT;
            case LIST, MAP -> ATypeTag.ARRAY;
            case DATE -> ATypeTag.DATE;
            case TIME -> ATypeTag.TIME;
            default -> throw createUnsupportedException(type);
        };
    }
}
