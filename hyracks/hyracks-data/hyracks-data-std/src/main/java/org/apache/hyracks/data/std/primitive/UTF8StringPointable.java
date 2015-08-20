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

public final class UTF8StringPointable extends AbstractPointable implements IHashable, IComparable {
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
            return new UTF8StringPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    /**
     * Returns the character at the given byte offset. The caller is responsible for making sure that
     * the provided offset is within bounds and points to the beginning of a valid UTF8 character.
     * 
     * @param offset
     *            - Byte offset
     * @return Character at the given offset.
     */
    public char charAt(int offset) {
        return charAt(bytes, start + offset);
    }

    public static char charAt(byte[] b, int s) {
        int c = b[s] & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return (char) c;

            case 12:
            case 13:
                return (char) (((c & 0x1F) << 6) | ((b[s + 1]) & 0x3F));

            case 14:
                return (char) (((c & 0x0F) << 12) | (((b[s + 1]) & 0x3F) << 6) | (((b[s + 2]) & 0x3F) << 0));

            default:
                throw new IllegalArgumentException();
        }
    }

    public int charSize(int offset) {
        return charSize(bytes, start + offset);
    }

    public static int charSize(byte[] b, int s) {
        int c = b[s] & 0xff;
        switch (c >> 4) {
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                return 1;

            case 12:
            case 13:
                return 2;

            case 14:
                return 3;
        }
        throw new IllegalStateException();
    }

    public static int getModifiedUTF8Len(char c) {
        if (c >= 0x0000 && c <= 0x007F) {
            return 1;
        } else if (c <= 0x07FF) {
            return 2;
        } else {
            return 3;
        }
    }

    /**
     * Gets the length of the string in characters.
     * 
     * @return length of string in characters
     */
    public int getStringLength() {
        return getStringLength(bytes, start);
    }

    public static int getStringLength(byte[] b, int s) {
        int pos = s + 2;
        int end = pos + getUTFLength(b, s);
        int charCount = 0;
        while (pos < end) {
            charCount++;
            pos += charSize(b, pos);
        }
        return charCount;
    }

    /**
     * Gets the length of the UTF-8 encoded string in bytes.
     * 
     * @return length of UTF-8 encoded string in bytes
     */
    public int getUTFLength() {
        return getUTFLength(bytes, start);
    }

    public static int getUTFLength(byte[] b, int s) {
        return ((b[s] & 0xff) << 8) + ((b[s + 1] & 0xff) << 0);
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        int utflen1 = getUTFLength(this.bytes, this.start);
        int utflen2 = getUTFLength(bytes, start);

        int c1 = 0;
        int c2 = 0;

        int s1Start = this.start + 2;
        int s2Start = start + 2;

        while (c1 < utflen1 && c2 < utflen2) {
            char ch1 = charAt(this.bytes, s1Start + c1);
            char ch2 = charAt(bytes, s2Start + c2);

            if (ch1 != ch2) {
                return ch1 - ch2;
            }
            c1 += charSize(this.bytes, s1Start + c1);
            c2 += charSize(bytes, s2Start + c2);
        }
        return utflen1 - utflen2;
    }

    @Override
    public int hash() {
        int h = 0;
        int utflen = getUTFLength(bytes, start);
        int sStart = start + 2;
        int c = 0;

        while (c < utflen) {
            char ch = charAt(bytes, sStart + c);
            h = 31 * h + ch;
            c += charSize(bytes, sStart + c);
        }
        return h;
    }

    public static void toString(StringBuilder buffer, byte[] bytes, int start) {
        int utfLen = getUTFLength(bytes, start);
        int offset = 2;
        while (utfLen > 0) {
            char c = charAt(bytes, start + offset);
            buffer.append(c);
            int cLen = UTF8StringPointable.getModifiedUTF8Len(c);
            offset += cLen;
            utfLen -= cLen;
        }
    }

    public void toString(StringBuilder buffer) {
        toString(buffer, bytes, start);
    }
}
