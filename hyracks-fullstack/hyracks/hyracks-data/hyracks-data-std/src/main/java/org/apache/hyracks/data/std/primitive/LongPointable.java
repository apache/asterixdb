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
package org.apache.hyracks.data.std.primitive;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.IHashable;
import org.apache.hyracks.data.std.api.INumeric;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.util.DataUtils;

import com.fasterxml.jackson.databind.JsonNode;

public final class LongPointable extends AbstractPointable implements IHashable, IComparable, INumeric {

    private static final int SIZE = 8;
    public static final LongPointableFactory FACTORY = new LongPointableFactory();
    public static final ITypeTraits TYPE_TRAITS = new FixedLengthTypeTrait(SIZE);

    public static class LongPointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        private LongPointableFactory() {
        }

        @Override
        public LongPointable createPointable() {
            return new LongPointable();
        }

        public LongPointable createPointable(long value) {
            LongPointable pointable = new LongPointable();
            pointable.setLong(value);
            return pointable;
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
            return registry.getClassIdentifier(getClass(), serialVersionUID);
        }

        @SuppressWarnings("squid:S1172") // unused parameter
        public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
            return FACTORY;
        }
    }

    public static long getLong(byte[] bytes, int start) {
        return (((long) (bytes[start] & 0xff)) << 56) + (((long) (bytes[start + 1] & 0xff)) << 48)
                + (((long) (bytes[start + 2] & 0xff)) << 40) + (((long) (bytes[start + 3] & 0xff)) << 32)
                + (((long) (bytes[start + 4] & 0xff)) << 24) + (((long) (bytes[start + 5] & 0xff)) << 16)
                + (((long) (bytes[start + 6] & 0xff)) << 8) + (((long) (bytes[start + 7] & 0xff)) << 0);
    }

    public static void setLong(byte[] bytes, int start, long value) {
        bytes[start] = (byte) ((value >>> 56) & 0xFF);
        bytes[start + 1] = (byte) ((value >>> 48) & 0xFF);
        bytes[start + 2] = (byte) ((value >>> 40) & 0xFF);
        bytes[start + 3] = (byte) ((value >>> 32) & 0xFF);
        bytes[start + 4] = (byte) ((value >>> 24) & 0xFF);
        bytes[start + 5] = (byte) ((value >>> 16) & 0xFF);
        bytes[start + 6] = (byte) ((value >>> 8) & 0xFF);
        bytes[start + 7] = (byte) ((value >>> 0) & 0xFF);
    }

    public long getLong() {
        return getLong(bytes, start);
    }

    public void setLong(long value) {
        if (bytes == null) {
            start = 0;
            length = TYPE_TRAITS.getFixedLength();
            bytes = new byte[length];
        }
        setLong(bytes, start, value);
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        return compare(this.bytes, this.start, this.length, bytes, start, length);
    }

    public static int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        DataUtils.ensureLengths(SIZE, l1, l2);
        return Long.compare(getLong(b1, s1), getLong(b2, s2));
    }

    @Override
    public int hash() {
        long v = getLong();
        return (int) (v ^ (v >>> 32));
    }

    @Override
    public byte byteValue() {
        return (byte) getLong();
    }

    @Override
    public short shortValue() {
        return (short) getLong();
    }

    @Override
    public int intValue() {
        return (int) getLong();
    }

    @Override
    public long longValue() {
        return getLong();
    }

    @Override
    public float floatValue() {
        return getLong();
    }

    @Override
    public double doubleValue() {
        return getLong();
    }

    public static byte[] toByteArray(long value) {
        byte[] bytes = new byte[Long.BYTES];
        setLong(bytes, 0, value);
        return bytes;
    }
}
