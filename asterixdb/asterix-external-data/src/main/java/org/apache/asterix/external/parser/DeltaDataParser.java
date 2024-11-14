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

import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
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
import org.apache.asterix.external.input.record.reader.aws.delta.converter.DeltaConverterContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.avro.AvroRuntimeException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IValueReference;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

public class DeltaDataParser extends AbstractDataParser implements IRecordDataParser<Row> {
    private final DeltaConverterContext parserContext;
    private final IExternalFilterValueEmbedder valueEmbedder;

    public DeltaDataParser(IExternalDataRuntimeContext context, Map<String, String> conf) {
        parserContext = new DeltaConverterContext(conf);
        valueEmbedder = context.getValueEmbedder();
    }

    @Override
    public boolean parse(IRawRecord<? extends Row> record, DataOutput out) throws HyracksDataException {
        try {
            parseObject(record.get(), out);
            valueEmbedder.reset();
            return true;
        } catch (AvroRuntimeException | IOException e) {
            throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
        }
    }

    private void parseObject(Row record, DataOutput out) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        StructType schema = record.getSchema();
        valueEmbedder.enterObject();
        for (int i = 0; i < schema.fields().size(); i++) {
            DataType fieldSchema = schema.fields().get(i).getDataType();
            String fieldName = schema.fieldNames().get(i);
            ATypeTag typeTag = getTypeTag(fieldSchema, record.isNullAt(i));
            IValueReference value = null;
            if (valueEmbedder.shouldEmbed(fieldName, typeTag)) {
                value = valueEmbedder.getEmbeddedValue();
            } else {
                valueBuffer.reset();
                parseValue(fieldSchema, record, i, valueBuffer.getDataOutput());
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

    private void parseObject(StructType schema, ColumnVector column, int index, DataOutput out) throws IOException {
        IMutableValueStorage valueBuffer = parserContext.enterObject();
        IARecordBuilder objectBuilder = parserContext.getObjectBuilder(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        valueEmbedder.enterObject();
        for (int i = 0; i < schema.fields().size(); i++) {
            DataType fieldSchema = schema.fields().get(i).getDataType();
            String fieldName = schema.fieldNames().get(i);
            ATypeTag typeTag = getTypeTag(fieldSchema, column.getChild(i).isNullAt(index));
            IValueReference value = null;
            if (valueEmbedder.shouldEmbed(fieldName, typeTag)) {
                value = valueEmbedder.getEmbeddedValue();
            } else {
                valueBuffer.reset();
                parseValue(fieldSchema, column.getChild(i), index, valueBuffer.getDataOutput());
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

    private void parseArray(ArrayType arraySchema, ArrayValue arrayValue, DataOutput out) throws IOException {
        DataType elementSchema = arraySchema.getElementType();
        ColumnVector elements = arrayValue.getElements();
        final IMutableValueStorage valueBuffer = parserContext.enterCollection();
        final IAsterixListBuilder arrayBuilder =
                parserContext.getCollectionBuilder(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (int i = 0; i < elements.getSize(); i++) {
            valueBuffer.reset();
            parseValue(elementSchema, elements, i, valueBuffer.getDataOutput());
            arrayBuilder.addItem(valueBuffer);
        }
        arrayBuilder.write(out, true);
        parserContext.exitCollection(valueBuffer, arrayBuilder);
    }

    private ATypeTag getTypeTag(DataType schema, boolean isNull) throws HyracksDataException {
        if (isNull) {
            return ATypeTag.NULL;
        }
        if (schema instanceof BooleanType) {
            return ATypeTag.BOOLEAN;
        } else if (schema instanceof ShortType || schema instanceof IntegerType || schema instanceof LongType) {
            return ATypeTag.BIGINT;
        } else if (schema instanceof DoubleType) {
            return ATypeTag.DOUBLE;
        } else if (schema instanceof StringType) {
            return ATypeTag.STRING;
        } else if (schema instanceof DateType) {
            if (parserContext.isDateAsInt()) {
                return ATypeTag.INTEGER;
            }
            return ATypeTag.DATE;
        } else if (schema instanceof TimestampType || schema instanceof TimestampNTZType) {
            if (parserContext.isTimestampAsLong()) {
                return ATypeTag.BIGINT;
            }
            return ATypeTag.DATETIME;
        } else if (schema instanceof BinaryType) {
            return ATypeTag.BINARY;
        } else if (schema instanceof ArrayType) {
            return ATypeTag.ARRAY;
        } else if (schema instanceof StructType) {
            return ATypeTag.OBJECT;
        } else if (schema instanceof DecimalType) {
            ensureDecimalToDoubleEnabled(schema, parserContext);
            return ATypeTag.DOUBLE;
        } else {
            throw createUnsupportedException(schema);
        }
    }

    private void parseValue(DataType schema, Row row, int index, DataOutput out) throws IOException {
        if (row.isNullAt(index)) {
            nullSerde.serialize(ANull.NULL, out);
        } else if (schema instanceof BooleanType) {
            if (row.getBoolean(index)) {
                booleanSerde.serialize(ABoolean.TRUE, out);
            } else {
                booleanSerde.serialize(ABoolean.FALSE, out);
            }
        } else if (schema instanceof ShortType) {
            serializeLong(row.getShort(index), out);
        } else if (schema instanceof IntegerType) {
            serializeLong(row.getInt(index), out);
        } else if (schema instanceof LongType) {
            serializeLong(row.getLong(index), out);
        } else if (schema instanceof DoubleType) {
            serializeDouble(row.getDouble(index), out);
        } else if (schema instanceof StringType) {
            serializeString(row.getString(index), out);
        } else if (schema instanceof DateType) {
            if (parserContext.isDateAsInt()) {
                serializeLong(row.getInt(index), out);
            } else {
                parserContext.serializeDate(row.getInt(index), out);
            }
        } else if (schema instanceof TimestampType) {
            long timeStampInMillis = TimeUnit.MICROSECONDS.toMillis(row.getLong(index));
            int offset = parserContext.getTimeZoneOffset();
            if (parserContext.isTimestampAsLong()) {
                serializeLong(timeStampInMillis + offset, out);
            } else {
                parserContext.serializeDateTime(timeStampInMillis + offset, out);
            }
        } else if (schema instanceof TimestampNTZType) {
            long timeStampInMillis = TimeUnit.MICROSECONDS.toMillis(row.getLong(index));
            if (parserContext.isTimestampAsLong()) {
                serializeLong(timeStampInMillis, out);
            } else {
                parserContext.serializeDateTime(timeStampInMillis, out);
            }
        } else if (schema instanceof StructType) {
            parseObject(row.getStruct(index), out);
        } else if (schema instanceof ArrayType) {
            parseArray((ArrayType) schema, row.getArray(index), out);
        } else if (schema instanceof DecimalType) {
            serializeDecimal(row.getDecimal(index), out);
        } else {
            throw createUnsupportedException(schema);
        }
    }

    private void parseValue(DataType schema, ColumnVector column, int index, DataOutput out) throws IOException {
        if (column.isNullAt(index)) {
            nullSerde.serialize(ANull.NULL, out);
        } else if (schema instanceof BooleanType) {
            if (column.getBoolean(index)) {
                booleanSerde.serialize(ABoolean.TRUE, out);
            } else {
                booleanSerde.serialize(ABoolean.FALSE, out);
            }
        } else if (schema instanceof ShortType) {
            serializeLong(column.getShort(index), out);
        } else if (schema instanceof IntegerType) {
            serializeLong(column.getInt(index), out);
        } else if (schema instanceof LongType) {
            serializeLong(column.getLong(index), out);
        } else if (schema instanceof DoubleType) {
            serializeDouble(column.getDouble(index), out);
        } else if (schema instanceof StringType) {
            serializeString(column.getString(index), out);
        } else if (schema instanceof DateType) {
            if (parserContext.isDateAsInt()) {
                serializeLong(column.getInt(index), out);
            } else {
                parserContext.serializeDate(column.getInt(index), out);
            }
        } else if (schema instanceof TimestampType) {
            long timeStampInMillis = TimeUnit.MICROSECONDS.toMillis(column.getLong(index));
            int offset = parserContext.getTimeZoneOffset();
            if (parserContext.isTimestampAsLong()) {
                serializeLong(timeStampInMillis + offset, out);
            } else {
                parserContext.serializeDateTime(timeStampInMillis + offset, out);
            }
        } else if (schema instanceof TimestampNTZType) {
            long timeStampInMillis = TimeUnit.MICROSECONDS.toMillis(column.getLong(index));
            if (parserContext.isTimestampAsLong()) {
                serializeLong(timeStampInMillis, out);
            } else {
                parserContext.serializeDateTime(timeStampInMillis, out);
            }
        } else if (schema instanceof ArrayType) {
            parseArray((ArrayType) schema, column.getArray(index), out);
        } else if (schema instanceof StructType) {
            parseObject((StructType) schema, column, index, out);
        } else if (schema instanceof DecimalType) {
            serializeDecimal(column.getDecimal(index), out);
        } else {
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

    private void serializeString(Object value, DataOutput out) throws HyracksDataException {
        aString.setValue(value.toString());
        stringSerde.serialize(aString, out);
    }

    private void serializeDecimal(BigDecimal value, DataOutput out) throws HyracksDataException {
        serializeDouble(value.doubleValue(), out);
    }

    private static HyracksDataException createUnsupportedException(DataType schema) {
        return new RuntimeDataException(ErrorCode.TYPE_UNSUPPORTED, "Delta Parser", schema.toString());
    }

    private static void ensureDecimalToDoubleEnabled(DataType type, DeltaConverterContext context)
            throws RuntimeDataException {
        if (!context.isDecimalToDoubleEnabled()) {
            throw new RuntimeDataException(ErrorCode.PARQUET_SUPPORTED_TYPE_WITH_OPTION, type.toString(),
                    ExternalDataConstants.ParquetOptions.DECIMAL_TO_DOUBLE);
        }
    }
}
