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
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public final class BooleanPointable extends AbstractPointable implements IHashable, IComparable {
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
            return new BooleanPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static boolean getBoolean(byte[] bytes, int start) {
        return bytes[start] == 0 ? false : true;
    }

    public static void setBoolean(byte[] bytes, int start, boolean value) {
        bytes[start] = (byte) (value ? 1 : 0);
    }

    public boolean getBoolean() {
        return getBoolean(bytes, start);
    }

    public void setBoolean(boolean value) {
        setBoolean(bytes, start, value);
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        boolean b = getBoolean();
        boolean ob = getBoolean(bytes, start);
        return b == ob ? 0 : (b ? 1 : -1);
    }

    @Override
    public int hash() {
        return getBoolean() ? Boolean.TRUE.hashCode() : Boolean.FALSE.hashCode();
    }
}