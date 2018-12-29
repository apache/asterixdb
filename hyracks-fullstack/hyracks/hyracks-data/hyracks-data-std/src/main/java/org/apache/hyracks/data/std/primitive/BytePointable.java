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

import com.fasterxml.jackson.databind.JsonNode;

public final class BytePointable extends AbstractPointable implements IHashable, IComparable, INumeric {

    public static final BytePointableFactory FACTORY = new BytePointableFactory();
    public static final ITypeTraits TYPE_TRAITS = new FixedLengthTypeTrait(1);

    public static final class BytePointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new BytePointable();
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

    public static byte getByte(byte[] bytes, int start) {
        return bytes[start];
    }

    public static void setByte(byte[] bytes, int start, byte value) {
        bytes[start] = value;
    }

    public byte getByte() {
        return getByte(bytes, start);
    }

    public void setByte(byte value) {
        setByte(bytes, start, value);
    }

    public byte preIncrement() {
        byte v = getByte();
        ++v;
        setByte(v);
        return v;
    }

    public byte postIncrement() {
        byte v = getByte();
        byte ov = v++;
        setByte(v);
        return ov;
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        byte b = getByte();
        byte ob = getByte(bytes, start);
        return Byte.compare(b, ob);
    }

    @Override
    public int hash() {
        return getByte();
    }

    @Override
    public byte byteValue() {
        return getByte();
    }

    @Override
    public short shortValue() {
        return getByte();
    }

    @Override
    public int intValue() {
        return getByte();
    }

    @Override
    public long longValue() {
        return getByte();
    }

    @Override
    public float floatValue() {
        return getByte();
    }

    @Override
    public double doubleValue() {
        return getByte();
    }
}
