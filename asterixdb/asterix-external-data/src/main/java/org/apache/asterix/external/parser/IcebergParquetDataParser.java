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
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

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
            parseObject(record.get(), out);
            valueEmbedder.reset();
            return true;
        } catch (AvroRuntimeException | IOException e) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
        }
    }

    private void parseObject(Record record, DataOutput out) throws IOException {
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
                parseValue(fieldType, record, i, valueBuffer.getDataOutput());
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

    private void parseArray(Type arrayType, boolean isOptional, List<?> listValues, DataOutput out) throws IOException {
        if (listValues == null) {
            nullSerde.serialize(ANull.NULL, out);
            return;
        }
        final IMutableValueStorage valueBuffer = parserContext.enterCollection();
        final IAsterixListBuilder arrayBuilder = parserContext.getCollectionBuilder(NESTED_OPEN_AORDERED_LIST_TYPE);
        for (int i = 0; i < listValues.size(); i++) {
            valueBuffer.reset();
            //parseValue(elementSchema, elements, i, valueBuffer.getDataOutput());
            arrayBuilder.addItem(valueBuffer);
        }
        arrayBuilder.write(out, true);
        parserContext.exitCollection(valueBuffer, arrayBuilder);
    }

    public static ATypeTag getTypeTag(Type type, boolean isNull, IcebergConverterContext parserContext)
            throws HyracksDataException {
        if (isNull) {
            return ATypeTag.NULL;
        }

        switch (type.typeId()) {
            case BOOLEAN:
                return ATypeTag.BOOLEAN;
            case INTEGER:
            case LONG:
                return ATypeTag.BIGINT;
            case FLOAT:
                return ATypeTag.FLOAT;
            case DOUBLE:
                return ATypeTag.DOUBLE;
            case STRING:
                return ATypeTag.STRING;
            case UUID:
                return ATypeTag.UUID;
            case BINARY:
                return ATypeTag.BINARY;
            case DECIMAL:
                ensureDecimalToDoubleEnabled(type, parserContext);
                return ATypeTag.DOUBLE;
            case STRUCT:
                return ATypeTag.OBJECT;
            case LIST:
                return ATypeTag.ARRAY;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_NANO:
            case FIXED:
            case GEOMETRY:
            case GEOGRAPHY:
            case MAP:
            case VARIANT:
            case UNKNOWN:
                throw new NotImplementedException();
            default:
                throw createUnsupportedException(type);

        }
    }

    private void parseValue(Type fieldType, Record record, int index, DataOutput out) throws IOException {
        Object value = record.get(index);
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
                // TODO: should this be parsed as double?
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
            case BINARY:
                serializeBinary(value, out);
                return;
            case DECIMAL:
                ensureDecimalToDoubleEnabled(fieldType, parserContext);
                serializeDecimal((BigDecimal) value, out);
                return;
            case STRUCT:
                parseObject((Record) value, out);
                return;
            case LIST:
                Types.ListType listType = fieldType.asListType();
                parseArray(listType.elementType(), listType.isElementOptional(), (List<?>) value, out);
                return;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_NANO:
            case FIXED:
            case GEOMETRY:
            case GEOGRAPHY:
            case MAP:
            case VARIANT:
            case UNKNOWN:
                throw new NotImplementedException();

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
        float floatValue = (Float) value;
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
        serializeDouble(value.doubleValue(), out);
    }

    private void serializeBinary(Object value, DataOutput out) throws HyracksDataException {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        aBinary.setValue(byteBuffer.array(), 0, byteBuffer.array().length);
        binarySerde.serialize(aBinary, out);
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
}
