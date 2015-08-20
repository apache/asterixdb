/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.data.std.primitive;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public final class FloatPointable extends AbstractPointable implements IHashable, IComparable, INumeric {
    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 4;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new FloatPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    private static int getIntBits(byte[] bytes, int start) {
        return IntegerPointable.getInteger(bytes, start);
    }

    public static float getFloat(byte[] bytes, int start) {
        int bits = getIntBits(bytes, start);
        return Float.intBitsToFloat(bits);
    }

    public static void setFloat(byte[] bytes, int start, float value) {
        int bits = Float.floatToIntBits(value);
        IntegerPointable.setInteger(bytes, start, bits);
    }

    public float getFloat() {
        return getFloat(bytes, start);
    }

    public void setFloat(float value) {
        setFloat(bytes, start, value);
    }

    public float preIncrement() {
        float v = getFloat();
        ++v;
        setFloat(v);
        return v;
    }

    public float postIncrement() {
        float v = getFloat();
        float ov = v++;
        setFloat(v);
        return ov;
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        float v = getFloat();
        float ov = getFloat(bytes, start);
        return v < ov ? -1 : (v > ov ? 1 : 0);
    }

    @Override
    public int hash() {
        return getIntBits(bytes, start);
    }

    @Override
    public byte byteValue() {
        return (byte) getFloat();
    }

    @Override
    public short shortValue() {
        return (short) getFloat();
    }

    @Override
    public int intValue() {
        return (int) getFloat();
    }

    @Override
    public long longValue() {
        return (long) getFloat();
    }

    @Override
    public float floatValue() {
        return getFloat();
    }

    @Override
    public double doubleValue() {
        return getFloat();
    }
}