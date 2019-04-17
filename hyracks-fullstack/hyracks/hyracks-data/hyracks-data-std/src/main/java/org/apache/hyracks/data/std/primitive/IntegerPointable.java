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

public final class IntegerPointable extends AbstractPointable implements IHashable, IComparable, INumeric {

    private static final int SIZE = 4;
    public static final IntegerPointableFactory FACTORY = new IntegerPointableFactory();
    public static final ITypeTraits TYPE_TRAITS = new FixedLengthTypeTrait(SIZE) {
        private static final long serialVersionUID = -7178318032449879790L;

        //TODO fix RTREE logic based on class comparision in LSMRTreeUtils#proposeBestLinearizer
        @Override
        public boolean isFixedLength() {
            return super.isFixedLength();
        }
    };

    public static final class IntegerPointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public IntegerPointable createPointable() {
            return new IntegerPointable();
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

    public static int getInteger(byte[] bytes, int start) {
        return ((bytes[start] & 0xff) << 24) + ((bytes[start + 1] & 0xff) << 16) + ((bytes[start + 2] & 0xff) << 8)
                + ((bytes[start + 3] & 0xff) << 0);
    }

    public static void setInteger(byte[] bytes, int start, int value) {
        bytes[start] = (byte) ((value >>> 24) & 0xFF);
        bytes[start + 1] = (byte) ((value >>> 16) & 0xFF);
        bytes[start + 2] = (byte) ((value >>> 8) & 0xFF);
        bytes[start + 3] = (byte) ((value >>> 0) & 0xFF);
    }

    public int getInteger() {
        return getInteger(bytes, start);
    }

    public void setInteger(int value) {
        setInteger(bytes, start, value);
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
        return Integer.compare(getInteger(b1, s1), getInteger(b2, s2));
    }

    @Override
    public int hash() {
        return getInteger();
    }

    @Override
    public byte byteValue() {
        return (byte) getInteger();
    }

    @Override
    public short shortValue() {
        return (short) getInteger();
    }

    @Override
    public int intValue() {
        return getInteger();
    }

    @Override
    public long longValue() {
        return getInteger();
    }

    @Override
    public float floatValue() {
        return getInteger();
    }

    @Override
    public double doubleValue() {
        return getInteger();
    }
}
