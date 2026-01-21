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

import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.AbstractComplexConverter;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.parquet.io.api.Binary;

/**
 * Converts Parquet's logical INTERVAL type (months:int32, days:int32, millis:int32) into Asterix {@link ATypeTag#DURATION}
 * (months:int32, millis:int64) by folding days into milliseconds (days * 86400000 + millis).
 */
public class IntervalConverter extends GenericPrimitiveConverter {
    private static final long MILLIS_PER_DAY = 86_400_000L;

    IntervalConverter(AbstractComplexConverter parent, String stringFieldName, int index,
            ParquetConverterContext context) throws IOException {
        super(ATypeTag.DURATION, parent, stringFieldName, index, context);
    }

    @Override
    public void addBinary(Binary value) {
        byte[] bytes = value.getBytesUnsafe();
        int months = readIntLE(bytes, 0);
        int days = readIntLE(bytes, 4);
        int millis = readIntLE(bytes, 8);

        long dayTimeMillis = days * MILLIS_PER_DAY + millis;
        context.serializeDuration(months, dayTimeMillis, parent.getDataOutput());
        parent.addValue(this);
    }

    private static int readIntLE(byte[] b, int o) {
        return (b[o] & 0xFF) | ((b[o + 1] & 0xFF) << 8) | ((b[o + 2] & 0xFF) << 16) | ((b[o + 3] & 0xFF) << 24);
    }
}
