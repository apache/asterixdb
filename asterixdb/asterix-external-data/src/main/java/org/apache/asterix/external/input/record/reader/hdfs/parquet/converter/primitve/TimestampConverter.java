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
package org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.primitve;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.AbstractComplexConverter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;

class TimestampConverter extends GenericPrimitiveConverter {
    private static final long JULIAN_DAY_OF_EPOCH = 2440588;
    private static final long MILLIS_PER_DAY = 86400000L;
    private static final long NANOS_PER_MILLIS = 1000000L;

    private final LogicalTypeAnnotation.TimeUnit timeUnit;
    private final int timeZoneOffset;

    TimestampConverter(AbstractComplexConverter parent, String stringFieldName, int index,
            ParquetConverterContext context, LogicalTypeAnnotation.TimeUnit timeUnit, int timeZoneOffset)
            throws IOException {
        super(ATypeTag.DATETIME, parent, stringFieldName, index, context);
        this.timeUnit = timeUnit;
        this.timeZoneOffset = timeZoneOffset;
    }

    /**
     * Timestamp is an INT96 (Little Endian)
     * INT96 timestamps are not adjusted to UTC and always considered as local timestamp
     *
     * @param value binary representation of INT96
     */
    @Override
    public void addBinary(Binary value) {
        ByteBuffer buffer = value.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        long timeOfDayNanos = buffer.getLong();
        int julianDay = buffer.getInt();
        long timestamp = fromJulian(julianDay, timeOfDayNanos);
        addLong(timestamp);
    }

    /**
     * Timestamp is an INT64
     *
     * @param value long value
     */
    @Override
    public void addLong(long value) {
        long convertedTime = TimeConverter.getConvertedTime(timeUnit, value);
        context.serializeDateTime(convertedTime + timeZoneOffset, parent.getDataOutput());
        parent.addValue(this);
    }

    private static long fromJulian(int days, long nanos) {
        return (days - JULIAN_DAY_OF_EPOCH) * MILLIS_PER_DAY + nanos / NANOS_PER_MILLIS;
    }
}
