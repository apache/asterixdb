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
package org.apache.asterix.external.input.record.reader.stream;

import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableTime;
import org.apache.asterix.om.base.AMutableUUID;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;

public class AvroConverterContext extends ParserContext {
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADate> dateSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADateTime> datetimeSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
    private final ISerializerDeserializer<ATime> timeSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ATIME);
    private final ISerializerDeserializer<ADouble> doubleSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
    protected ISerializerDeserializer<AUUID> uuidSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AUUID);

    private final boolean decimalToDouble;
    private final boolean timestampAsLong;
    private final boolean dateAsInt;
    private final boolean timeAsLong;
    private final boolean uuidAsString;

    private final int timeZoneOffset;
    private final AMutableDate mutableDate = new AMutableDate(0);
    private final AMutableDateTime mutableDateTime = new AMutableDateTime(0);
    private final AMutableDouble mutableDouble = new AMutableDouble(0.0);
    private final AMutableTime mutableTime = new AMutableTime(0);
    protected AMutableUUID aUUID = new AMutableUUID();
    private final List<Warning> warnings;

    public AvroConverterContext(Map<String, String> configuration, List<Warning> warnings) {
        this.warnings = warnings;
        decimalToDouble = Boolean.parseBoolean(configuration
                .getOrDefault(ExternalDataConstants.AvroOptions.DECIMAL_TO_DOUBLE, ExternalDataConstants.FALSE));
        timestampAsLong = Boolean.parseBoolean(configuration
                .getOrDefault(ExternalDataConstants.AvroOptions.TIMESTAMP_AS_LONG, ExternalDataConstants.TRUE));
        dateAsInt = Boolean.parseBoolean(
                configuration.getOrDefault(ExternalDataConstants.AvroOptions.DATE_AS_INT, ExternalDataConstants.TRUE));
        timeAsLong = Boolean.parseBoolean(
                configuration.getOrDefault(ExternalDataConstants.AvroOptions.TIME_AS_LONG, ExternalDataConstants.TRUE));
        uuidAsString = Boolean.parseBoolean(configuration.getOrDefault(ExternalDataConstants.AvroOptions.UUID_AS_STRING,
                ExternalDataConstants.TRUE));
        String configuredTimeZoneId = configuration.get(ExternalDataConstants.AvroOptions.TIMEZONE);
        if (configuredTimeZoneId != null && !configuredTimeZoneId.isEmpty()) {
            timeZoneOffset = TimeZone.getTimeZone(configuredTimeZoneId).getRawOffset();
        } else {
            timeZoneOffset = 0;
        }
    }

    public void serializeDate(Object value, DataOutput output) {
        try {
            int intValue = (int) ((Number) value).longValue();
            mutableDate.setValue(intValue);
            dateSerDer.serialize(mutableDate, output);
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

    public void serializeTime(int value, DataOutput output) {
        try {
            mutableTime.setValue(value);
            timeSerDer.serialize(mutableTime, output);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    public void serializeDecimal(Object value, DataOutput output, int scale) throws IOException {
        if (value instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) value;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            BigInteger unscaledValue = new BigInteger(bytes);
            BigDecimal bigDecimal = new BigDecimal(unscaledValue, scale);
            serializeDouble(bigDecimal.doubleValue(), output);
        } else {
            throw new IOException(
                    "Expected ByteBuffer for Decimal logical type, but got: " + value.getClass().getName());
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

    public void serializeUUID(Object value, DataOutput output) throws HyracksDataException {
        String uuidValue = value.toString();
        char[] buffer = uuidValue.toCharArray();
        aUUID.parseUUIDString(buffer, 0, uuidValue.length());
        uuidSerde.serialize(aUUID, output);
    }

    public boolean isDecimalToDoubleEnabled() {
        return decimalToDouble;
    }

    public int getTimeZoneOffset() {
        return timeZoneOffset;
    }

    public boolean isTimestampAsLong() {
        return timestampAsLong;
    }

    public boolean isDateAsInt() {
        return dateAsInt;
    }

    public boolean isUuidAsString() {
        return uuidAsString;
    }

    public boolean isTimeAsLong() {
        return timeAsLong;
    }

    public List<Warning> getWarnings() {
        return warnings;
    }
}
