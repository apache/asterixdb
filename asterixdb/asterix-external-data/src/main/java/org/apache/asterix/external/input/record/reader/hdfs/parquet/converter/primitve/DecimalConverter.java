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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested.AbstractComplexConverter;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.parquet.io.api.Binary;

/**
 * The decimal converter relies on java {@link BigDecimal} to convert decimal values. The converter could pressure
 * the GC as we need to create {@link BigDecimal} object / value
 */
public class DecimalConverter extends GenericPrimitiveConverter {
    public static final int LONG_MAX_PRECISION = 20;
    private final int precision;
    private final int scale;

    DecimalConverter(AbstractComplexConverter parent, String stringFieldName, int index,
            ParquetConverterContext context, int precision, int scale) throws IOException {
        super(ATypeTag.DOUBLE, parent, stringFieldName, index, context);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void addInt(int value) {
        addLong(value);
    }

    @Override
    public void addLong(long value) {
        addConvertedDouble(BigDecimal.valueOf(value, scale).doubleValue());
    }

    @Override
    public void addBinary(Binary value) {
        if (precision <= LONG_MAX_PRECISION) {
            addLong(getUnscaledLong(value.toByteBuffer()));
        } else {
            //Unlimited precision
            addConvertedDouble(new BigDecimal(new BigInteger(value.getBytes()), scale).doubleValue());
        }
    }

    private void addConvertedDouble(double value) {
        context.serializeDouble(value, parent.getDataOutput());
        parent.addValue(this);
    }

    private static long getUnscaledLong(ByteBuffer buffer) {
        byte[] bytes = buffer.array();
        int start = buffer.arrayOffset() + buffer.position();
        int end = buffer.arrayOffset() + buffer.limit();

        long value = 0L;
        for (int i = start; i < end; i++) {
            value = (value << 8) | (bytes[i] & 0xFF);
        }
        int bits = 8 * (end - start);
        return (value << (64 - bits)) >> (64 - bits);
    }
}
