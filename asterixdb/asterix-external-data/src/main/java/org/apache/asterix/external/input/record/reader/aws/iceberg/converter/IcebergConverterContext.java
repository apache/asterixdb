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
package org.apache.asterix.external.input.record.reader.aws.iceberg.converter;

import java.io.DataOutput;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;

public class IcebergConverterContext extends ParserContext {
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADate> dateSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADateTime> datetimeSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
    private final boolean decimalToDouble;
    private final boolean timestampAsLong;
    private final boolean dateAsInt;
    private final boolean timeAsInt;

    private final ZoneId timeZoneId;
    private final AMutableDate mutableDate = new AMutableDate(0);
    private final AMutableDateTime mutableDateTime = new AMutableDateTime(0);
    private final List<Warning> warnings;

    public IcebergConverterContext(Map<String, String> configuration, List<Warning> warnings) {
        this.warnings = warnings;
        decimalToDouble = Boolean.parseBoolean(configuration
                .getOrDefault(ExternalDataConstants.IcebergOptions.DECIMAL_TO_DOUBLE, ExternalDataConstants.FALSE));
        timestampAsLong = Boolean.parseBoolean(configuration
                .getOrDefault(ExternalDataConstants.IcebergOptions.TIMESTAMP_AS_LONG, ExternalDataConstants.TRUE));
        timeAsInt = Boolean.parseBoolean(configuration.getOrDefault(ExternalDataConstants.IcebergOptions.TIME_AS_INT,
                ExternalDataConstants.TRUE));
        dateAsInt = Boolean.parseBoolean(configuration.getOrDefault(ExternalDataConstants.IcebergOptions.DATE_AS_INT,
                ExternalDataConstants.TRUE));
        String configuredTimeZoneId = configuration.get(ExternalDataConstants.IcebergOptions.TIMEZONE);
        if (configuredTimeZoneId != null && !configuredTimeZoneId.isEmpty()) {
            timeZoneId = TimeZone.getTimeZone(configuredTimeZoneId).toZoneId();
        } else {
            timeZoneId = ZoneOffset.UTC;
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

    public void serializeDateTime(long timestamp, DataOutput output) {
        try {
            mutableDateTime.setValue(timestamp);
            datetimeSerDer.serialize(mutableDateTime, output);
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean isDecimalToDoubleEnabled() {
        return decimalToDouble;
    }

    public ZoneId getTimeZoneId() {
        return timeZoneId;
    }

    public boolean isTimestampAsLong() {
        return timestampAsLong;
    }

    public boolean isTimeAsInt() {
        return timeAsInt;
    }

    public boolean isDateAsInt() {
        return dateAsInt;
    }

    public List<Warning> getWarnings() {
        return warnings;
    }
}
