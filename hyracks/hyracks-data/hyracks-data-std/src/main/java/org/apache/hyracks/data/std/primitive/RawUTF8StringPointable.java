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

/**
 * This class provides the raw bytes-based comparison and hash function for UTF8 strings.
 * Note that the comparison may not deliver the correct ordering for certain languages that include 2 or 3 bytes characters.
 * But it works for single-byte character languages.
 */
public final class RawUTF8StringPointable extends AbstractPointable implements IHashable, IComparable {
    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new RawUTF8StringPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        int utflen1 = UTF8StringPointable.getUTFLength(this.bytes, this.start);
        int utflen2 = UTF8StringPointable.getUTFLength(bytes, start);

        int c1 = 0;
        int c2 = 0;

        int s1Start = this.start + 2;
        int s2Start = start + 2;

        while (c1 < utflen1 && c2 < utflen2) {
            char ch1 = (char) this.bytes[s1Start + c1];
            char ch2 = (char) bytes[s2Start + c2];

            if (ch1 != ch2) {
                return ch1 - ch2;
            }
            c1++;
            c2++;
        }
        return utflen1 - utflen2;
    }

    @Override
    public int hash() {
        int h = 0;
        int utflen = UTF8StringPointable.getUTFLength(bytes, start);
        int sStart = start + 2;
        int c = 0;

        while (c < utflen) {
            char ch = (char) bytes[sStart + c];
            h = 31 * h + ch;
            c++;
        }
        return h;
    }

    public void toString(StringBuilder buffer) {
        UTF8StringPointable.toString(buffer, bytes, start);
    }
}
