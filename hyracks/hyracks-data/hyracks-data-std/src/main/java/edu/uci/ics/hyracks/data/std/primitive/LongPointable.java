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

public final class LongPointable extends AbstractPointable implements IHashable, IComparable, INumeric {
    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 8;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new LongPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

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
        setLong(bytes, start, value);
    }

    public long preIncrement() {
        long v = getLong();
        ++v;
        setLong(v);
        return v;
    }

    public long postIncrement() {
        long v = getLong();
        long ov = v++;
        setLong(v);
        return ov;
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        long v = getLong();
        long ov = getLong(bytes, start);
        return v < ov ? -1 : (v > ov ? 1 : 0);
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
}