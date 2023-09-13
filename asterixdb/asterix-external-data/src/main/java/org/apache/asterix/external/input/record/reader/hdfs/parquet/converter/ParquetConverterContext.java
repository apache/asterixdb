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
package org.apache.asterix.external.input.record.reader.hdfs.parquet.converter;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.TimeZone;

import org.apache.asterix.dataflow.data.nontagged.serde.ABinarySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.external.input.filter.NoOpFilterValueEmbedder;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.stream.StandardUTF8ToModifiedUTF8DataOutput;
import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.external.util.ExternalDataConstants.ParquetOptions;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableTime;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.apache.parquet.io.api.Binary;

public class ParquetConverterContext extends ParserContext {
    /*
     * ************************************************************************
     * Serializers/Deserializers
     * ************************************************************************
     */
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ABoolean> booleanSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> int64SerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADouble> doubleSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ABinary> binarySerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADate> dateSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ATime> timeSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ATIME);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADateTime> datetimeSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);

    //Issued warnings
    private final List<Warning> warnings;

    /*
     * ************************************************************************
     * Binary values members
     * ************************************************************************
     */
    private final StandardUTF8ToModifiedUTF8DataOutput modifiedUTF8DataOutput;
    private byte[] lengthBytes;

    /*
     * ************************************************************************
     * Mutable Values
     * ************************************************************************
     */

    private final AMutableInt64 mutableInt64 = new AMutableInt64(0);
    private final AMutableDouble mutableDouble = new AMutableDouble(0.0);
    private final AMutableDate mutableDate = new AMutableDate(0);
    private final AMutableTime mutableTime = new AMutableTime(0);
    private final AMutableDateTime mutableDateTime = new AMutableDateTime(0);

    /*
     * ************************************************************************
     * Type knobs
     * ************************************************************************
     */
    private final boolean parseJson;
    private final boolean decimalToDouble;

    /*
     * ************************************************************************
     * Temporal Configuration
     * ************************************************************************
     */
    private final String timeZoneId;
    private final int timeZoneOffset;

    /*
     * ************************************************************************
     * Value Embedder
     * ************************************************************************
     */
    private final IExternalFilterValueEmbedder valueEmbedder;

    public ParquetConverterContext(Configuration configuration, List<Warning> warnings) {
        this(configuration, NoOpFilterValueEmbedder.INSTANCE, warnings);
    }

    public ParquetConverterContext(Configuration configuration, IExternalFilterValueEmbedder valueEmbedder,
            List<Warning> warnings) {
        this.warnings = warnings;
        this.valueEmbedder = valueEmbedder;
        modifiedUTF8DataOutput = new StandardUTF8ToModifiedUTF8DataOutput(
                new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader()));

        parseJson = configuration.getBoolean(ParquetOptions.HADOOP_PARSE_JSON_STRING, false);
        decimalToDouble = configuration.getBoolean(ParquetOptions.HADOOP_DECIMAL_TO_DOUBLE, false);

        String configuredTimeZoneId = configuration.get(ParquetOptions.HADOOP_TIMEZONE);
        if (!configuredTimeZoneId.isEmpty()) {
            timeZoneId = configuredTimeZoneId;
            timeZoneOffset = TimeZone.getTimeZone(timeZoneId).getRawOffset();
        } else {
            timeZoneId = "";
            timeZoneOffset = 0;
        }
    }

    public IExternalFilterValueEmbedder getValueEmbedder() {
        return valueEmbedder;
    }

    public List<Warning> getWarnings() {
        return warnings;
    }

    public boolean isParseJsonEnabled() {
        return parseJson;
    }

    public boolean isDecimalToDoubleEnabled() {
        return decimalToDouble;
    }

    public String getTimeZoneId() {
        return timeZoneId;
    }

    public int getTimeZoneOffset() {
        return timeZoneOffset;
    }

    /*
     * ************************************************************************
     * Serialization methods
     * All methods throws IllegalStateException as Parquet's converters methods
     * do not throw any exceptions
     * ************************************************************************
     */

    @Override
    public IMutableValueStorage getSerializedFieldName(String fieldName) throws IOException {
        if (fieldName == null) {
            // Could happen in the context of Parquet's converters (i.e., in array item converter)
            return null;
        }

        return super.getSerializedFieldName(fieldName);
    }

    public void serializeBoolean(boolean value, DataOutput output) {
        try {
            booleanSerDer.serialize(value ? ABoolean.TRUE : ABoolean.FALSE, output);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    public void serializeInt64(long value, DataOutput output) {
        try {
            mutableInt64.setValue(value);
            int64SerDer.serialize(mutableInt64, output);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    public void serializeDouble(double value, DataOutput output) {
        try {
            mutableDouble.setValue(value);
            doubleSerDer.serialize(mutableDouble, output);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * String here is a binary UTF-8 String (not Java string) and not a modified-UTF8
     *
     * @param value  Parquet binary value
     * @param output output to write the converted string
     */
    public void serializeString(Binary value, DataOutput output) {
        //Set the destination to where to write the final modified UTF-8
        modifiedUTF8DataOutput.setDataOutput(output);
        try {
            //Write the type tag
            output.writeByte(ATypeTag.STRING.serialize());
            //Write the binary UTF-8 string as
            value.writeTo(modifiedUTF8DataOutput);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void serializeUUID(Binary value, DataOutput output) {
        try {
            output.writeByte(ATypeTag.UUID.serialize());
            value.writeTo(output);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * To avoid object creation when writing a binary value, we do not use {@link ABinarySerializerDeserializer}
     * as calls to {@link Binary#getBytes()} could create new buffer each time we call this method
     *
     * @param value  Parquet binary value
     * @param output output to write the binary value
     */
    public void serializeBinary(Binary value, DataOutput output) {
        try {
            output.writeByte(ATypeTag.BINARY.serialize());
            writeLength(value.length(), output);
            value.writeTo(output);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void serializeDate(int value, DataOutput output) {
        try {
            mutableDate.setValue(value);
            dateSerDer.serialize(mutableDate, output);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    public void serializeTime(int value, DataOutput output) {
        try {
            mutableTime.setValue(value);
            timeSerDer.serialize(mutableTime, output);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    public void serializeDateTime(long timestamp, DataOutput output) {
        try {
            mutableDateTime.setValue(timestamp);
            datetimeSerDer.serialize(mutableDateTime, output);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    private void writeLength(int length, DataOutput out) throws IOException {
        int requiredLength = VarLenIntEncoderDecoder.getBytesRequired(length);
        if (lengthBytes == null || requiredLength > lengthBytes.length) {
            lengthBytes = new byte[requiredLength];
        }
        VarLenIntEncoderDecoder.encode(length, lengthBytes, 0);
        out.write(lengthBytes, 0, requiredLength);
    }
}
