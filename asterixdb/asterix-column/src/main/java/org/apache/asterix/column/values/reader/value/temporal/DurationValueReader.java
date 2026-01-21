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
package org.apache.asterix.column.values.reader.value.temporal;

import java.io.IOException;

import org.apache.asterix.column.bytes.decoder.ParquetDeltaBinaryPackingValuesReader;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.values.reader.value.AbstractValueReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.parquet.bytes.BytesUtils;

public class DurationValueReader extends AbstractValueReader {
    private final ParquetDeltaBinaryPackingValuesReader monthsReader;
    private final ParquetDeltaBinaryPackingValuesReader millisReader;
    private final ArrayBackedValueStorage value;

    public DurationValueReader() {
        // Decoded value is non-tagged: months:int32 + millis:int64 (12 bytes)
        value = new ArrayBackedValueStorage(Integer.BYTES + Long.BYTES);
        value.setSize(Integer.BYTES + Long.BYTES);
        monthsReader = new ParquetDeltaBinaryPackingValuesReader();
        millisReader = new ParquetDeltaBinaryPackingValuesReader();
    }

    @Override
    public void init(AbstractBytesInputStream in, int tupleCount) throws IOException {
        int monthsStreamLength = BytesUtils.readUnsignedVarInt(in);
        AbstractBytesInputStream monthsStream = in.sliceStream(monthsStreamLength);
        monthsReader.initFromPage(monthsStream);
        // remaining bytes are the millis stream
        millisReader.initFromPage(in);
    }

    @Override
    public void nextValue() throws HyracksDataException {
        int months = monthsReader.readInteger();
        long millis = millisReader.readLong();
        byte[] bytes = value.getByteArray();
        int s = value.getStartOffset();
        IntegerPointable.setInteger(bytes, s, months);
        LongPointable.setLong(bytes, s + Integer.BYTES, millis);
    }

    @Override
    public IValueReference getBytes() {
        return value;
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.DURATION;
    }

    @Override
    public int compareTo(AbstractValueReader o) {
        // Compare using Asterix duration ordering: months first, then day-time milliseconds.
        IValueReference other = o.getBytes();

        byte[] a = value.getByteArray();
        int ao = value.getStartOffset();
        int monthsA = IntegerPointable.getInteger(a, ao);

        byte[] b = other.getByteArray();
        int bo = other.getStartOffset();
        int monthsB = IntegerPointable.getInteger(b, bo);

        int cmp = Integer.compare(monthsA, monthsB);
        if (cmp != 0) {
            return cmp;
        }

        long millisA = LongPointable.getLong(a, ao + Integer.BYTES);
        long millisB = LongPointable.getLong(b, bo + Integer.BYTES);
        return Long.compare(millisA, millisB);
    }
}
