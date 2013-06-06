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

public final class ShortPointable extends AbstractPointable implements IHashable, IComparable, INumeric {
    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 2;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new ShortPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static short getShort(byte[] bytes, int start) {
        return (short) (((bytes[start] & 0xff) << 8) + (bytes[start + 1] & 0xff));
    }

    public static void setShort(byte[] bytes, int start, short value) {
        bytes[start] = (byte) ((value >>> 8) & 0xFF);
        bytes[start + 1] = (byte) ((value >>> 0) & 0xFF);
    }

    public short getShort() {
        return getShort(bytes, start);
    }

    public void setShort(short value) {
        setShort(bytes, start, value);
    }

    public short preIncrement() {
        short v = getShort();
        ++v;
        setShort(v);
        return v;
    }

    public short postIncrement() {
        short v = getShort();
        short ov = v++;
        setShort(v);
        return ov;
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        short v = getShort();
        short ov = getShort(bytes, start);
        return v < ov ? -1 : (v > ov ? 1 : 0);
    }

    @Override
    public int hash() {
        return getShort();
    }

    @Override
    public byte byteValue() {
        return (byte) getShort();
    }

    @Override
    public short shortValue() {
        return getShort();
    }

    @Override
    public int intValue() {
        return getShort();
    }

    @Override
    public long longValue() {
        return getShort();
    }

    @Override
    public float floatValue() {
        return getShort();
    }

    @Override
    public double doubleValue() {
        return getShort();
    }
}