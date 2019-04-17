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
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.util.DataUtils;

import com.fasterxml.jackson.databind.JsonNode;

public final class BooleanPointable extends AbstractPointable implements IHashable, IComparable {

    private static final int SIZE = 1;
    public static final BooleanPointableFactory FACTORY = new BooleanPointableFactory();
    public static final FixedLengthTypeTrait TYPE_TRAITS = new FixedLengthTypeTrait(SIZE);

    public static class BooleanPointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public BooleanPointable createPointable() {
            return new BooleanPointable();
        }

        public BooleanPointable createPointable(boolean value) {
            BooleanPointable pointable = new BooleanPointable();
            pointable.setBoolean(value);
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

    public static boolean getBoolean(byte[] bytes, int start) {
        return bytes[start] != 0;
    }

    public static void setBoolean(byte[] bytes, int start, boolean value) {
        bytes[start] = (byte) (value ? 1 : 0);
    }

    public boolean getBoolean() {
        return getBoolean(bytes, start);
    }

    public void setBoolean(boolean value) {
        if (bytes == null) {
            start = 0;
            length = TYPE_TRAITS.getFixedLength();
            bytes = new byte[length];
        }
        setBoolean(bytes, start, value);
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
        return Boolean.compare(getBoolean(b1, s1), getBoolean(b2, s2));
    }

    @Override
    public int hash() {
        return getBoolean() ? Boolean.TRUE.hashCode() : Boolean.FALSE.hashCode();
    }
}
