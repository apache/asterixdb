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

public final class IntegerPointable extends AbstractPointable implements IHashable, IComparable, INumeric {
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
            return new IntegerPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

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

    public int preIncrement() {
        int v = getInteger();
        ++v;
        setInteger(v);
        return v;
    }

    public int postIncrement() {
        int v = getInteger();
        int ov = v++;
        setInteger(v);
        return ov;
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        int v = getInteger();
        int ov = getInteger(bytes, start);
        return v < ov ? -1 : (v > ov ? 1 : 0);
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