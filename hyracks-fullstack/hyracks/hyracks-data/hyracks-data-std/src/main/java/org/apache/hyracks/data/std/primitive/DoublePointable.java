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

public final class DoublePointable extends AbstractPointable implements IHashable, IComparable, INumeric {

    private static final int SIZE = 8;
    public static final DoublePointableFactory FACTORY = new DoublePointableFactory();
    public static final ITypeTraits TYPE_TRAITS = new FixedLengthTypeTrait(SIZE) {
        private static final long serialVersionUID = 7348262203696059687L;

        //TODO fix RTREE logic based on class comparision in LSMRTreeUtils#proposeBestLinearizer
        @Override
        public boolean isFixedLength() {
            return super.isFixedLength();
        }
    };
    private static final double MACHINE_EPSILON = getMachineEpsilon();

    private static double getMachineEpsilon() {
        float epsilon = 1.0f;
        do {
            epsilon /= 2.0f;
        } while ((float) (1.0 + (epsilon / 2.0)) != 1.0);
        return epsilon;
    }

    public static class DoublePointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new DoublePointable();
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

    public static long getLongBits(byte[] bytes, int start) {
        return LongPointable.getLong(bytes, start);
    }

    public static double getDouble(byte[] bytes, int start) {
        long bits = getLongBits(bytes, start);
        return Double.longBitsToDouble(bits);
    }

    public static void setDouble(byte[] bytes, int start, double value) {
        long bits = Double.doubleToLongBits(value);
        LongPointable.setLong(bytes, start, bits);
    }

    public double getDouble() {
        return getDouble(bytes, start);
    }

    public void setDouble(double value) {
        setDouble(bytes, start, value);
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
        return Double.compare(getDouble(b1, s1), getDouble(b2, s2));
    }

    @Override
    public int hash() {
        long bits = getLongBits(bytes, start);
        return (int) (bits ^ (bits >>> 32));
    }

    @Override
    public byte byteValue() {
        return (byte) getDouble();
    }

    @Override
    public short shortValue() {
        return (short) getDouble();
    }

    @Override
    public int intValue() {
        return (int) getDouble();
    }

    @Override
    public long longValue() {
        return (long) getDouble();
    }

    @Override
    public float floatValue() {
        return (float) getDouble();
    }

    @Override
    public double doubleValue() {
        return getDouble();
    }

    public static double getEpsilon() {
        return MACHINE_EPSILON;
    }
}
