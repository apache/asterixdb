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
package org.apache.asterix.external.input.record.reader.aws.delta.converter;

import java.io.DataOutput;
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

public class DeltaConverterContext extends ParserContext {
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADate> dateSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADateTime> datetimeSerDer =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
    private final boolean decimalToDouble;
    private final boolean timestampAsLong;
    private final boolean dateAsInt;

    private final int timeZoneOffset;
    private final AMutableDate mutableDate = new AMutableDate(0);
    private final AMutableDateTime mutableDateTime = new AMutableDateTime(0);

    public DeltaConverterContext(Map<String, String> configuration) {
        decimalToDouble = Boolean.parseBoolean(configuration
                .getOrDefault(ExternalDataConstants.DeltaOptions.DECIMAL_TO_DOUBLE, ExternalDataConstants.FALSE));
        timestampAsLong = Boolean.parseBoolean(configuration
                .getOrDefault(ExternalDataConstants.DeltaOptions.TIMESTAMP_AS_LONG, ExternalDataConstants.TRUE));
        dateAsInt = Boolean.parseBoolean(
                configuration.getOrDefault(ExternalDataConstants.DeltaOptions.DATE_AS_INT, ExternalDataConstants.TRUE));
        String configuredTimeZoneId = configuration.get(ExternalDataConstants.DeltaOptions.TIMEZONE);
        if (configuredTimeZoneId != null && !configuredTimeZoneId.isEmpty()) {
            timeZoneOffset = TimeZone.getTimeZone(configuredTimeZoneId).getRawOffset();
        } else {
            timeZoneOffset = 0;
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

    public int getTimeZoneOffset() {
        return timeZoneOffset;
    }

    public boolean isTimestampAsLong() {
        return timestampAsLong;
    }

    public boolean isDateAsInt() {
        return dateAsInt;
    }
}
