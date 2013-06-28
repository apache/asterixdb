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
package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;

// TODO: Perhaps this class should go into hyracks.
public final class UTF8StringLowercasePointable extends AbstractPointable implements IHashable, IComparable {
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
            return new UTF8StringLowercasePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static int getUTFLen(byte[] b, int s) {
        return ((b[s] & 0xff) << 8) + ((b[s + 1] & 0xff) << 0);
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        int utflen1 = getUTFLen(this.bytes, this.start);
        int utflen2 = getUTFLen(bytes, start);

        int c1 = 0;
        int c2 = 0;

        int s1Start = this.start + 2;
        int s2Start = start + 2;

        while (c1 < utflen1 && c2 < utflen2) {
            char ch1 = Character.toLowerCase(UTF8StringPointable.charAt(this.bytes, s1Start + c1));
            char ch2 = Character.toLowerCase(UTF8StringPointable.charAt(bytes, s2Start + c2));

            if (ch1 != ch2) {
                return ch1 - ch2;
            }
            c1 += UTF8StringPointable.charSize(this.bytes, s1Start + c1);
            c2 += UTF8StringPointable.charSize(bytes, s2Start + c2);
        }
        return utflen1 - utflen2;
    }

    @Override
    public int hash() {
        int h = 0;
        int utflen = getUTFLen(bytes, start);
        int sStart = start + 2;
        int c = 0;

        while (c < utflen) {
            char ch = Character.toLowerCase(UTF8StringPointable.charAt(bytes, sStart + c));
            h = 31 * h + ch;
            c += UTF8StringPointable.charSize(bytes, sStart + c);
        }
        return h;
    }
}
