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
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.CharSet;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.IHashable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.util.string.UTF8StringUtil;

import com.fasterxml.jackson.databind.JsonNode;

public final class UTF8StringPointable extends AbstractPointable implements IHashable, IComparable {

    public static final UTF8StringPointableFactory FACTORY = new UTF8StringPointableFactory();
    public static final ITypeTraits TYPE_TRAITS = VarLengthTypeTrait.INSTANCE;
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

    public static class UTF8StringPointableFactory implements IPointableFactory {
        private static final long serialVersionUID = 1L;

        private UTF8StringPointableFactory() {
        }

        @Override
        public UTF8StringPointable createPointable() {
            return new UTF8StringPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
            return registry.getClassIdentifier(getClass(), serialVersionUID);
        }

        @SuppressWarnings("squid:S1172") // unused parameter
        public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
            return FACTORY;
        }
    }

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
     * @param offset
     *            - Byte offset
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

    @Override
    public String toString() {
        try {
            return new String(bytes, getCharStartOffset(), getUTF8Length(), StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    public int ignoreCaseCompareTo(UTF8StringPointable other) {
        return UTF8StringUtil.lowerCaseCompareTo(this.getByteArray(), this.getStartOffset(), other.getByteArray(),
                other.getStartOffset());
    }

    public int find(UTF8StringPointable pattern, boolean ignoreCase) {
        return find(this, pattern, ignoreCase);
    }

    @SuppressWarnings("squid:S1172") // unused parameter
    public static int compare(byte[] bytes, int start, int length, byte[] thatBytes, int thatStart, int thatLength) {
        return UTF8StringUtil.compareTo(bytes, start, thatBytes, thatStart);
    }

    // takes advantage of cached utf8 length and meta length
    public static int compare(UTF8StringPointable pointable1, UTF8StringPointable pointable2) {
        return UTF8StringUtil.compareTo(pointable1.bytes, pointable1.start + pointable1.metaLength,
                pointable1.utf8Length, pointable2.bytes, pointable2.start + pointable2.metaLength,
                pointable2.utf8Length);
    }

    /**
     * @param src,
     *            the source string.
     * @param pattern,
     *            the pattern string.
     * @param ignoreCase,
     *            to ignore case or not.
     * @return the byte offset of the first character of the matching string. Not including the MetaLength.
     */
    public static int find(UTF8StringPointable src, UTF8StringPointable pattern, boolean ignoreCase) {
        return find(src, pattern, ignoreCase, 0);
    }

    /**
     * @param src,
     *            the source string.
     * @param pattern,
     *            the pattern string.
     * @param ignoreCase,
     *            to ignore case or not.
     * @param startMatch,
     *            the start offset.
     * @return the byte offset of the first character of the matching string after <code>startMatchPos}</code>.
     *         Not including the MetaLength.
     */
    public static int find(UTF8StringPointable src, UTF8StringPointable pattern, boolean ignoreCase, int startMatch) {
        int startMatchPos = startMatch;
        final int srcUtfLen = src.getUTF8Length();
        final int pttnUtfLen = pattern.getUTF8Length();
        final int srcStart = src.getMetaDataLength();
        final int pttnStart = pattern.getMetaDataLength();

        int maxStart = srcUtfLen - pttnUtfLen;
        while (startMatchPos <= maxStart) {
            int c1 = startMatchPos;
            int c2 = 0;
            while (c1 < srcUtfLen && c2 < pttnUtfLen) {
                char ch1 = src.charAt(srcStart + c1);
                char ch2 = pattern.charAt(pttnStart + c2);

                if (ch1 != ch2) {
                    if (!ignoreCase || Character.toLowerCase(ch1) != Character.toLowerCase(ch2)) {
                        break;
                    }
                }
                c1 += src.charSize(srcStart + c1);
                c2 += pattern.charSize(pttnStart + c2);
            }
            if (c2 == pttnUtfLen) {
                return startMatchPos;
            }
            startMatchPos += src.charSize(srcStart + startMatchPos);
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
        if (utflen2 > utflen1) {
            return false;
        }

        int s1Start = src.getMetaDataLength();
        int s2Start = pattern.getMetaDataLength();

        int c1 = 0;
        int c2 = 0;
        while (c1 < utflen1 && c2 < utflen2) {
            char ch1 = src.charAt(s1Start + c1);
            char ch2 = pattern.charAt(s2Start + c2);
            if (ch1 != ch2) {
                if (!ignoreCase || Character.toLowerCase(ch1) != Character.toLowerCase(ch2)) {
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
        if (len2 > len1) {
            return false;
        }

        int s1Start = src.getMetaDataLength();
        int s2Start = pattern.getMetaDataLength();

        int c1 = len1 - len2;
        int c2 = 0;
        while (c1 < len1 && c2 < len2) {
            char ch1 = src.charAt(s1Start + c1);
            char ch2 = pattern.charAt(s2Start + c2);

            if (ch1 != ch2) {
                if (!ignoreCase || Character.toLowerCase(ch1) != Character.toLowerCase(ch2)) {
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

    /**
     * @return {@code true} if substring was successfully written into given {@code out}, or
     *         {@code false} if substring could not be obtained ({@code charOffset} or {@code charLength}
     *         are less than 0 or starting position is greater than the input length)
     */
    public boolean substr(int charOffset, int charLength, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        return substr(this, charOffset, charLength, builder, out);
    }

    /**
     * @return {@code true} if substring was successfully written into given {@code out}, or
     *         {@code false} if substring could not be obtained ({@code charOffset} or {@code charLength}
     *         are less than 0 or starting position is greater than the input length)
     */
    public static boolean substr(UTF8StringPointable src, int charOffset, int charLength, UTF8StringBuilder builder,
            GrowableArray out) throws IOException {
        if (charOffset < 0 || charLength < 0) {
            return false;
        }

        int utfLen = src.getUTF8Length();
        int chIdx = 0;
        int byteIdx = 0;
        while (byteIdx < utfLen && chIdx < charOffset) {
            byteIdx += src.charSize(src.getMetaDataLength() + byteIdx);
            chIdx++;
        }
        if (byteIdx >= utfLen) {
            return false;
        }

        builder.reset(out, Math.min(utfLen - byteIdx, (int) (charLength * 1.0 * byteIdx / chIdx)));
        chIdx = 0;
        while (byteIdx < utfLen && chIdx < charLength) {
            builder.appendChar(src.charAt(src.getMetaDataLength() + byteIdx));
            chIdx++;
            byteIdx += src.charSize(src.getMetaDataLength() + byteIdx);
        }
        builder.finish();
        return true;
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
    public static void substrBefore(UTF8StringPointable src, UTF8StringPointable match, UTF8StringBuilder builder,
            GrowableArray out) throws IOException {

        int byteOffset = find(src, match, false);
        if (byteOffset < 0) {
            builder.reset(out, 0);
            builder.finish();
            return;
        }

        final int srcMetaLen = src.getMetaDataLength();

        builder.reset(out, byteOffset);
        for (int idx = 0; idx < byteOffset;) {
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
    public static void substrAfter(UTF8StringPointable src, UTF8StringPointable match, UTF8StringBuilder builder,
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

    /**
     * Generates a lower case string of an input string.
     *
     * @param src
     *            , the input source string.
     * @param builder
     *            , a builder for the resulting string.
     * @param out
     *            , the storage for a result string.
     * @throws IOException
     */
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

    /**
     * Generates an upper case string of an input string.
     *
     * @param src
     *            , the input source string.
     * @param builder
     *            , a builder for the resulting string.
     * @param out
     *            , the storage for a result string.
     * @throws IOException
     */
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

    public void initCap(UTF8StringBuilder builder, GrowableArray out) throws IOException {
        initCap(this, builder, out);
    }

    /**
     * Generates a "title" format string from an input source string, i.e., the first letter of each word
     * is in the upper case while the other letter is in the lower case.
     *
     * @param src
     *            , the input source string.
     * @param builder
     *            , a builder for the resulting string.
     * @param out
     *            , the storage for a result string.
     * @throws IOException
     */
    public static void initCap(UTF8StringPointable src, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        final int srcUtfLen = src.getUTF8Length();
        final int srcStart = src.getMetaDataLength();

        builder.reset(out, srcUtfLen);
        boolean toUpperCase = true;
        int byteIndex = 0;
        while (byteIndex < srcUtfLen) {
            char originalChar = src.charAt(srcStart + byteIndex);
            boolean isLetter = Character.isLetter(originalChar);

            // Make the first character into upper case while the later ones into lower case.
            char resultChar = toUpperCase && isLetter ? Character.toUpperCase(originalChar)
                    : (isLetter ? Character.toLowerCase(originalChar) : originalChar);
            builder.appendChar(resultChar);
            byteIndex += src.charSize(srcStart + byteIndex);

            // Whether the next letter needs to switch to the upper case.
            toUpperCase = !isLetter;
        }
        builder.finish();
    }

    public void trim(UTF8StringBuilder builder, GrowableArray out, boolean left, boolean right, CharSet charSet)
            throws IOException {
        trim(this, builder, out, left, right, charSet);
    }

    /**
     * Generates a trimmed string of an input source string.
     *
     * @param srcPtr
     *            , the input source string.
     * @param builder
     *            , the result string builder.
     * @param out
     *            , the storage for the output string.
     * @param left
     *            , whether to trim the left side.
     * @param right
     *            , whether to trim the right side.
     * @param charSet
     *            , the chars that should be trimmed.
     * @throws IOException
     */
    public static void trim(UTF8StringPointable srcPtr, UTF8StringBuilder builder, GrowableArray out, boolean left,
            boolean right, CharSet charSet) throws IOException {
        final int srcUtfLen = srcPtr.getUTF8Length();
        final int srcStart = srcPtr.getMetaDataLength();
        // Finds the start Index (inclusive).
        int startIndex = 0;
        if (left) {
            while (startIndex < srcUtfLen) {
                char ch = srcPtr.charAt(srcStart + startIndex);
                if (!charSet.contains(ch)) {
                    break;
                }
                startIndex += srcPtr.charSize(srcStart + startIndex);
            }
        }

        // Finds the end index (exclusive).
        int endIndex = srcUtfLen;
        if (right) {
            endIndex = startIndex;
            int cursorIndex = startIndex;
            while (cursorIndex < srcUtfLen) {
                char ch = srcPtr.charAt(srcStart + cursorIndex);
                cursorIndex += srcPtr.charSize(srcStart + cursorIndex);
                if (!charSet.contains(ch)) {
                    endIndex = cursorIndex;
                }
            }
        }

        // Outputs the desired substring.
        int len = endIndex - startIndex;
        builder.reset(out, len);
        builder.appendUtf8StringPointable(srcPtr, srcPtr.getStartOffset() + srcStart + startIndex, len);
        builder.finish();
    }

    /**
     * Generates a reversed string from an input source string
     *
     * @param srcPtr
     *            , the input source string.
     * @param builder
     *            , a builder for the resulting string.
     * @param out
     *            , the storage for a result string.
     * @throws IOException
     */
    public static void reverse(UTF8StringPointable srcPtr, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        builder.reset(out, srcPtr.getUTF8Length());
        int srcStart = srcPtr.getCharStartOffset();
        int srcEnd = srcPtr.getStartOffset() + srcPtr.getLength() - 1;
        for (int cursorIndex = srcEnd; cursorIndex >= srcStart; cursorIndex--) {
            if (UTF8StringUtil.isCharStart(srcPtr.bytes, cursorIndex)) {
                int charSize = UTF8StringUtil.charSize(srcPtr.bytes, cursorIndex);
                builder.appendUtf8StringPointable(srcPtr, cursorIndex, charSize);
            }
        }
        builder.finish();
    }

    public boolean findAndReplace(UTF8StringPointable searchPtr, UTF8StringPointable replacePtr, int replaceLimit,
            UTF8StringBuilder builder, GrowableArray out) throws IOException {
        return findAndReplace(this, searchPtr, replacePtr, replaceLimit, builder, out);
    }

    public static boolean findAndReplace(UTF8StringPointable srcPtr, UTF8StringPointable searchPtr,
            UTF8StringPointable replacePtr, int replaceLimit, UTF8StringBuilder builder, GrowableArray out)
            throws IOException {
        if (replaceLimit == 0) {
            return false;
        }
        if (replaceLimit < 0) {
            replaceLimit = Integer.MAX_VALUE;
        }
        int curIdx = find(srcPtr, searchPtr, false);
        if (curIdx < 0) {
            return false;
        }
        int searchUtfLen = searchPtr.getUTF8Length();
        int replaceUtfLen = replacePtr.getUTF8Length();
        int estimatedLen = searchUtfLen > 0 && replaceUtfLen > searchUtfLen
                ? (int) (((long) srcPtr.getUTF8Length()) * replaceUtfLen / searchUtfLen) : srcPtr.getUTF8Length();
        builder.reset(out, estimatedLen);
        builder.appendUtf8StringPointable(srcPtr, srcPtr.getCharStartOffset(), curIdx);
        builder.appendUtf8StringPointable(replacePtr);

        curIdx += searchUtfLen;
        int limit = replaceLimit - 1;

        int nextIdx;
        while (limit > 0 && (nextIdx = find(srcPtr, searchPtr, false, curIdx)) > 0) {
            builder.appendUtf8StringPointable(srcPtr, srcPtr.getCharStartOffset() + curIdx, nextIdx - curIdx);
            builder.appendUtf8StringPointable(replacePtr);
            curIdx = nextIdx + searchUtfLen;
            limit--;
        }
        builder.appendUtf8StringPointable(srcPtr, srcPtr.getCharStartOffset() + curIdx,
                srcPtr.getUTF8Length() - curIdx);

        builder.finish();

        return true;
    }
}
