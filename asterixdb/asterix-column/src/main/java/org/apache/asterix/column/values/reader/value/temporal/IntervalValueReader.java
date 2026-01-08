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
import org.apache.asterix.column.bytes.decoder.ParquetRunLengthBitPackingHybridDecoder;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.values.reader.value.AbstractValueReader;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.parquet.bytes.BytesUtils;

public final class IntervalValueReader extends AbstractValueReader {
    private static final int TYPE_BIT_WIDTH = Byte.SIZE;
    private final ParquetRunLengthBitPackingHybridDecoder typeReader;
    private final ParquetDeltaBinaryPackingValuesReader startReader;
    private final ParquetDeltaBinaryPackingValuesReader endReader;
    private final ArrayBackedValueStorage value;

    public IntervalValueReader() {
        typeReader = new ParquetRunLengthBitPackingHybridDecoder(TYPE_BIT_WIDTH);
        startReader = new ParquetDeltaBinaryPackingValuesReader();
        endReader = new ParquetDeltaBinaryPackingValuesReader();
        value = new ArrayBackedValueStorage();
    }

    @Override
    public void init(AbstractBytesInputStream in, int tupleCount) throws IOException {
        int typeStreamLength = BytesUtils.readUnsignedVarInt(in);
        AbstractBytesInputStream typeStream = in.sliceStream(typeStreamLength);
        typeReader.reset(typeStream);

        int startStreamLength = BytesUtils.readUnsignedVarInt(in);
        AbstractBytesInputStream startStream = in.sliceStream(startStreamLength);
        startReader.initFromPage(startStream);

        // remaining bytes are the end stream
        endReader.initFromPage(in);
    }

    @Override
    public void nextValue() {
        final int intervalTimeType;
        try {
            intervalTimeType = typeReader.readInt();
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
        long start = startReader.readLong();
        long end = endReader.readLong();

        int payloadLen = intervalTimeType == ATypeTag.DATETIME.serialize() ? (Byte.BYTES + Long.BYTES + Long.BYTES)
                : (Byte.BYTES + Integer.BYTES + Integer.BYTES);
        value.reset();
        value.setSize(payloadLen);
        byte[] bytes = value.getByteArray();
        int s = value.getStartOffset();
        bytes[s] = (byte) intervalTimeType;
        if (intervalTimeType == ATypeTag.DATETIME.serialize()) {
            LongPointable.setLong(bytes, s + 1, start);
            LongPointable.setLong(bytes, s + 1 + Long.BYTES, end);
        } else {
            IntegerPointable.setInteger(bytes, s + 1, (int) start);
            IntegerPointable.setInteger(bytes, s + 1 + Integer.BYTES, (int) end);
        }
    }

    @Override
    public IValueReference getBytes() {
        return value;
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.INTERVAL;
    }

    @Override
    public int compareTo(AbstractValueReader o) {
        // Defensive ordering:
        // compare internal interval time type first, then (start,end) within same type.
        IValueReference other = o.getBytes();
        byte[] a = value.getByteArray();
        int ao = value.getStartOffset();
        byte[] b = other.getByteArray();
        int bo = other.getStartOffset();
        int t1 = AIntervalSerializerDeserializer.getIntervalTimeType(a, ao);
        int t2 = AIntervalSerializerDeserializer.getIntervalTimeType(b, bo);
        int typeCmp = Integer.compare(t1, t2);
        if (typeCmp != 0) {
            return typeCmp;
        }
        long s1 = AIntervalSerializerDeserializer.getIntervalStart(a, ao);
        long s2 = AIntervalSerializerDeserializer.getIntervalStart(b, bo);
        int cmp = Long.compare(s1, s2);
        if (cmp != 0) {
            return cmp;
        }
        long e1 = AIntervalSerializerDeserializer.getIntervalEnd(a, ao);
        long e2 = AIntervalSerializerDeserializer.getIntervalEnd(b, bo);
        return Long.compare(e1, e2);
    }
}