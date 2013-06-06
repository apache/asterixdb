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

public final class DoublePointable extends AbstractPointable implements IHashable, IComparable, INumeric {
	private final static double machineEpsilon;
	static {
		float epsilon = 1.0f;

        do {
           epsilon /= 2.0f;
        }
        while ((float)(1.0 + (epsilon/2.0)) != 1.0);
        machineEpsilon = epsilon;
	}
	
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
            return new DoublePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

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

    public double preIncrement() {
        double v = getDouble();
        ++v;
        setDouble(v);
        return v;
    }

    public double postIncrement() {
        double v = getDouble();
        double ov = v++;
        setDouble(v);
        return ov;
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        double v = getDouble();
        double ov = getDouble(bytes, start);
        return v < ov ? -1 : (v > ov ? 1 : 0);
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
		return machineEpsilon;
	}
}