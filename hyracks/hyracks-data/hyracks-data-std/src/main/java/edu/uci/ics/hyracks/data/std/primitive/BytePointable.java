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

public final class BytePointable extends AbstractPointable implements IHashable, IComparable, INumeric {
    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 1;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new BytePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

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
        return b < ob ? -1 : (b > ob ? 1 : 0);
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