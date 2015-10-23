/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.data.std.primitive;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.IHashable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.util.string.UTF8StringUtil;

public final class UTF8StringPointable extends AbstractPointable implements IHashable, IComparable {

    // These values are cached to speed up the length data access.
    // Since we are using the variable-length encoding, we can save the repeated decoding efforts.
    // WARNING: must call the resetConstants() method after each reset().
    private int utf8Length;
    private int metaLength;
    private int hashValue;
    private int stringLength;

    /**
     * reset those meta length.
     * Since the {@code utf8Length} and the {@code metaLength} are often used, we compute those two values in advance.
     * As for the {@code stringLength} and the {@code hashValue}, they will be lazily initialized after the first call.
     */
    @Override
    protected void afterReset() {
        utf8Length = UTF8StringUtil.getUTFLength(bytes, start);
        metaLength = UTF8StringUtil.getNumBytesToStoreLength(getUTF8Length());
        hashValue = 0;
        stringLength = -1;
    }

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

    public static UTF8StringPointable generateUTF8Pointable(String string) {
        byte[] bytes;
        bytes = UTF8StringUtil.writeStringToBytes(string);
        UTF8StringPointable ptr = new UTF8StringPointable();
        ptr.set(bytes, 0, bytes.length);
        return ptr;
    }

    /**
     * Returns the character at the given byte offset. The caller is responsible for making sure that
     * the provided offset is within bounds and points to the beginning of a valid UTF8 character.
     *
     * @param offset - Byte offset
     * @return Character at the given offset.
     */
    public char charAt(int offset) {
        return UTF8StringUtil.charAt(bytes, start + offset);
    }

    public int charSize(int offset) {
        return UTF8StringUtil.charSize(bytes, start + offset);
    }

    /**
     * Gets the length of the string in characters.
     * The first time call will need to go through the entire string, the following call will just return the pre-caculated result
     *
     * @return length of string in characters
     */
    public int getStringLength() {
        if (stringLength < 0) {
            stringLength = UTF8StringUtil.getStringLength(bytes, start);
        }
        return stringLength;
    }

    /**
     * Gets the length of the UTF-8 encoded string in bytes.
     *
     * @return length of UTF-8 encoded string in bytes
     */
    public int getUTF8Length() {
        return utf8Length;
    }

    public int getMetaDataLength() {
        return metaLength;
    }

    public int getCharStartOffset() {
        return getStartOffset() + getMetaDataLength();
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        return UTF8StringUtil.compareTo(this.bytes, this.start, bytes, start);
    }

    @Override
    public int hash() {
        if (hashValue == 0) {
            hashValue = UTF8StringUtil.hash(this.bytes, this.start);
        }
        return hashValue;
    }

    public void toString(StringBuilder buffer) {
        UTF8StringUtil.toString(buffer, bytes, start);
    }

    public String toString() {
        return new String(this.bytes, this.getCharStartOffset(), this.getUTF8Length(), Charset.forName("UTF-8"));
    }

    /****
     * String functions
     */

    public int ignoreCaseCompareTo(UTF8StringPointable other) {
        return UTF8StringUtil.lowerCaseCompareTo(this.getByteArray(), this.getStartOffset(),
                other.getByteArray(), other.getStartOffset());
    }

    public int find(UTF8StringPointable pattern, boolean ignoreCase) {
        return find(this, pattern, ignoreCase);
    }

    /**
     * return the byte offset of the first character of the matching string. Not including the MetaLength
     *
     * @param src
     * @param pattern
     * @param ignoreCase
     * @return
     */
    public static int find(UTF8StringPointable src, UTF8StringPointable pattern, boolean ignoreCase) {
        final int srcUtfLen = src.getUTF8Length();
        final int pttnUtfLen = pattern.getUTF8Length();
        final int srcStart = src.getMetaDataLength();
        final int pttnStart = pattern.getMetaDataLength();

        int startMatch = 0;
        int maxStart = srcUtfLen - pttnUtfLen;
        while (startMatch <= maxStart) {
            int c1 = startMatch;
            int c2 = 0;
            while (c1 < srcUtfLen && c2 < pttnUtfLen) {
                char ch1 = src.charAt(srcStart + c1);
                char ch2 = pattern.charAt(pttnStart + c2);

                if (ch1 != ch2) {
                    if (!ignoreCase || ignoreCase && Character.toLowerCase(ch1) != Character.toLowerCase(ch2)) {
                        break;
                    }
                }
                c1 += src.charSize(srcStart + c1);
                c2 += pattern.charSize(pttnStart + c2);
            }
            if (c2 == pttnUtfLen) {
                return startMatch;
            }
            startMatch += src.charSize(srcStart + startMatch);
        }
        return -1;
    }

    public boolean contains(UTF8StringPointable pattern, boolean ignoreCase) {
        return contains(this, pattern, ignoreCase);
    }

    public static boolean contains(UTF8StringPointable src, UTF8StringPointable pattern, boolean ignoreCase) {
        return find(src, pattern, ignoreCase) >= 0;
    }

    public boolean startsWith(UTF8StringPointable pattern, boolean ignoreCase) {
        return startsWith(this, pattern, ignoreCase);
    }

    public static boolean startsWith(UTF8StringPointable src, UTF8StringPointable pattern, boolean ignoreCase) {
        int utflen1 = src.getUTF8Length();
        int utflen2 = pattern.getUTF8Length();
        if (utflen2 > utflen1)
            return false;

        int s1Start = src.getMetaDataLength();
        int s2Start = pattern.getMetaDataLength();

        int c1 = 0;
        int c2 = 0;
        while (c1 < utflen1 && c2 < utflen2) {
            char ch1 = src.charAt(s1Start + c1);
            char ch2 = pattern.charAt(s2Start + c2);
            if (ch1 != ch2) {
                if (!ignoreCase || ignoreCase && Character.toLowerCase(ch1) != Character.toLowerCase(ch2)) {
                    break;
                }
            }
            c1 += src.charSize(s1Start + c1);
            c2 += pattern.charSize(s2Start + c2);
        }
        return (c2 == utflen2);
    }

    public boolean endsWith(UTF8StringPointable pattern, boolean ignoreCase) {
        return endsWith(this, pattern, ignoreCase);
    }

    public static boolean endsWith(UTF8StringPointable src, UTF8StringPointable pattern, boolean ignoreCase) {
        int len1 = src.getUTF8Length();
        int len2 = pattern.getUTF8Length();
        if (len2 > len1)
            return false;

        int s1Start = src.getMetaDataLength();
        int s2Start = pattern.getMetaDataLength();

        int c1 = len1 - len2;
        int c2 = 0;
        while (c1 < len1 && c2 < len2) {
            char ch1 = src.charAt(s1Start + c1);
            char ch2 = pattern.charAt(s2Start + c2);

            if (ch1 != ch2) {
                if (!ignoreCase || ignoreCase && Character.toLowerCase(ch1) != Character.toLowerCase(ch2)) {
                    break;
                }
            }
            c1 += src.charSize(s1Start + c1);
            c2 += pattern.charSize(s2Start + c2);
        }
        return (c2 == len2);
    }

    public void concat(UTF8StringPointable next, UTF8StringBuilder builder, GrowableArray out) throws IOException {
        concat(this, next, builder, out);
    }

    public static void concat(UTF8StringPointable first, UTF8StringPointable next, UTF8StringBuilder builder,
            GrowableArray out) throws IOException {
        int firstUtfLen = first.getUTF8Length();
        int nextUtfLen = next.getUTF8Length();

        builder.reset(out, firstUtfLen + nextUtfLen);
        builder.appendUtf8StringPointable(first);
        builder.appendUtf8StringPointable(next);
        builder.finish();
    }

    public void substr(int charOffset, int charLength, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        substr(this, charOffset, charLength, builder, out);
    }

    public static void substr(UTF8StringPointable src, int charOffset, int charLength, UTF8StringBuilder builder,
            GrowableArray out) throws IOException {
        // Really don't understand why we need to support the charOffset < 0 case.
        // At this time, usually there is mistake on user side, we'd better give him a warning.
        // assert charOffset >= 0;
        if (charOffset < 0) {
            charOffset = 0;
        }
        if (charLength < 0) {
            charLength = 0;
        }

        int utfLen = src.getUTF8Length();
        int chIdx = 0;
        int byteIdx = 0;
        while (byteIdx < utfLen && chIdx < charOffset) {
            byteIdx += src.charSize(src.getMetaDataLength() + byteIdx);
            chIdx++;
        }
        if (byteIdx >= utfLen) {
            // Again, why do we tolerant this kind of mistakes?
            // throw new StringIndexOutOfBoundsException(charOffset);
            builder.reset(out, 0);
            builder.finish();
            return;
        }

        builder.reset(out, Math.min(utfLen - byteIdx, (int) (charLength * 1.0 * byteIdx / chIdx)));
        chIdx = 0;
        while (byteIdx < utfLen && chIdx < charLength) {
            builder.appendChar(src.charAt(src.getMetaDataLength() + byteIdx));
            chIdx++;
            byteIdx += src.charSize(src.getMetaDataLength() + byteIdx);
        }
        builder.finish();
    }

    public void substrBefore(UTF8StringPointable match, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        substrBefore(this, match, builder, out);
    }

    /**
     * Write the substring before the given pattern. It will write a empty string if the matching fails.
     *
     * @param src
     * @param match
     * @param builder
     * @param out
     * @throws IOException
     */
    public static void substrBefore(
            UTF8StringPointable src,
            UTF8StringPointable match,
            UTF8StringBuilder builder,
            GrowableArray out) throws IOException {

        int byteOffset = find(src, match, false);
        if (byteOffset < 0) {
            builder.reset(out, 0);
            builder.finish();
            return;
        }

        final int srcMetaLen = src.getMetaDataLength();

        builder.reset(out, byteOffset);
        for (int idx = 0; idx < byteOffset; ) {
            builder.appendChar(src.charAt(srcMetaLen + idx));
            idx += src.charSize(srcMetaLen + idx);
        }
        builder.finish();
    }

    public void substrAfter(UTF8StringPointable match, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        substrAfter(this, match, builder, out);
    }

    /**
     * Write the substring after the given pattern. It will write a empty string if the matching fails.
     *
     * @param src
     * @param match
     * @param builder
     * @param out
     */
    public static void substrAfter(
            UTF8StringPointable src,
            UTF8StringPointable match,
            UTF8StringBuilder builder,
            GrowableArray out) throws IOException {

        int byteOffset = find(src, match, false);
        if (byteOffset < 0) {
            builder.reset(out, 0);
            builder.finish();
            return;
        }

        final int srcUtfLen = src.getUTF8Length();
        final int matchUtfLen = match.getUTF8Length();

        final int resultLen = srcUtfLen - byteOffset - matchUtfLen;
        builder.reset(out, resultLen);
        builder.appendUtf8StringPointable(src, src.getCharStartOffset() + byteOffset + matchUtfLen, resultLen);
        builder.finish();
    }

    public void lowercase(UTF8StringBuilder builder, GrowableArray out) throws IOException {
        lowercase(this, builder, out);
    }

    public static void lowercase(UTF8StringPointable src, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        final int srcUtfLen = src.getUTF8Length();
        final int srcStart = src.getMetaDataLength();

        builder.reset(out, srcUtfLen);
        int byteIndex = 0;
        while (byteIndex < srcUtfLen) {
            builder.appendChar(Character.toLowerCase(src.charAt(srcStart + byteIndex)));
            byteIndex += src.charSize(srcStart + byteIndex);
        }
        builder.finish();
    }

    public void uppercase(UTF8StringBuilder builder, GrowableArray out) throws IOException {
        uppercase(this, builder, out);
    }

    public static void uppercase(UTF8StringPointable src, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        final int srcUtfLen = src.getUTF8Length();
        final int srcStart = src.getMetaDataLength();

        builder.reset(out, srcUtfLen);
        int byteIndex = 0;
        while (byteIndex < srcUtfLen) {
            builder.appendChar(Character.toUpperCase(src.charAt(srcStart + byteIndex)));
            byteIndex += src.charSize(srcStart + byteIndex);
        }
        builder.finish();
    }

}
