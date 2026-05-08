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

import static org.apache.hyracks.data.std.primitive.UTF8StringPointable.generateUTF8Pointable;
import static org.apache.hyracks.util.string.UTF8StringSample.EMOJI_BASKETBALL;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_EMOJI_FAMILY_OF_2;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_EMOJI_FAMILY_OF_4;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.util.string.UTF8StringSample;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntCollection;

public class UTF8StringPointableTest {

    @Test
    public void testFindWithKMP() throws Exception {
        // Standard matching (returns byte pos)
        assertTrue(findReturnPos("hello world", "world") >= 0);
        assertEquals(0, findReturnPos("hello world", "hello"));
        assertTrue(findReturnPos("hello world", "o w") >= 0);

        // Exact same pattern boundary overlaps (KMP jump testing)
        assertEquals(0, findReturnPos("abacababc", "abacab"));
        assertTrue(findReturnPos("abacababc", "ababc") >= 0);

        // Match not found
        assertEquals(-1, findReturnPos("hello world", "xyz"));

        // Multi-byte Unicode Characters (length checking bytes vs codepoints)
        // 'é' (U+00E9) is 2 bytes in UTF-8, 1 code point
        // '€' (U+20AC) is 3 bytes in UTF-8, 1 code point
        // '😀' (U+1F600) is a surrogate pair in Java, encoded as 6 bytes in Modified UTF-8!

        // Testing 2-byte character: expected w byte pos = 5(hello) + 2(é) = 7
        assertEquals(7, findReturnPos("helloéworld", "w"));
        assertEquals(6, findReturnCodePointPos("helloéworld", "w")); // 5(hello) + 1(é) = 6

        // Testing 3-byte character: expected w byte pos = 5(hello) + 3(€) = 8
        assertEquals(8, findReturnPos("hello€world", "w"));
        assertEquals(6, findReturnCodePointPos("hello€world", "w"));

        // Testing Surrogate Pair (Modified UTF-8 encodes as 2x 3-byte characters = 6 bytes)
        assertEquals(11, findReturnPos("hello😀world", "w")); // 5 + 6 = 11
        assertEquals(6, findReturnCodePointPos("hello😀world", "w")); // 5 + 1 = 6

        // Find the emoji itself
        assertTrue(findReturnPos("hello😀world", "😀") > 0);
        assertEquals(0, findReturnCodePointPos("😀😀😀", "😀😀"));

        // Case insensitivity
        assertTrue(findReturnPosIgnoreCase("hello WORLD", "world") >= 0);
        assertTrue(findReturnPosIgnoreCase("hello WORLD", "WOrLd") >= 0);
        assertEquals(-1, findReturnPosIgnoreCase("hello WORLD", "XYZ"));
    }

    private int findReturnPos(String srcStr, String patternStr) throws Exception {
        return findReturnPosInternal(srcStr, patternStr, false, true);
    }

    private int findReturnCodePointPos(String srcStr, String patternStr) throws Exception {
        return findReturnPosInternal(srcStr, patternStr, false, false);
    }

    private int findReturnPosIgnoreCase(String srcStr, String patternStr) throws Exception {
        return findReturnPosInternal(srcStr, patternStr, true, true);
    }

    private int findReturnPosInternal(String srcStr, String patternStr, boolean ignoreCase, boolean resultInByte)
            throws Exception {
        UTF8StringPointable src = generateUTF8Pointable(srcStr);
        UTF8StringPointable pattern = generateUTF8Pointable(patternStr);
        if (resultInByte) {
            return UTF8StringPointable.find(src, pattern, ignoreCase, 0);
        } else {
            return UTF8StringPointable.findInCodePoint(src, pattern, ignoreCase, 0);
        }
    }

    public static UTF8StringPointable STRING_EMPTY = generateUTF8Pointable(UTF8StringSample.EMPTY_STRING);
    public static UTF8StringPointable STRING_UTF8_MIX = generateUTF8Pointable(UTF8StringSample.STRING_UTF8_MIX);
    public static UTF8StringPointable STRING_UTF8_MIX_LOWERCASE =
            generateUTF8Pointable(UTF8StringSample.STRING_UTF8_MIX_LOWERCASE);

    public static UTF8StringPointable STRING_LEN_127 = generateUTF8Pointable(UTF8StringSample.STRING_LEN_127);
    public static UTF8StringPointable STRING_LEN_128 = generateUTF8Pointable(UTF8StringSample.STRING_LEN_128);

    public static UTF8StringPointable STRING_POINTABLE_EMOJI_FAMILY_OF_4 =
            generateUTF8Pointable(STRING_EMOJI_FAMILY_OF_4);
    public static UTF8StringPointable STRING_POINTABLE_EMOJI_FAMILY_OF_2 =
            generateUTF8Pointable(STRING_EMOJI_FAMILY_OF_2);

    @Test
    public void testGetStringUTF8Length() throws Exception {
        UTF8StringPointable utf8Ptr = generateUTF8Pointable(UTF8StringSample.STRING_LEN_127);
        assertEquals(127, utf8Ptr.getUTF8Length());
        assertEquals(1, utf8Ptr.getMetaDataLength());
        assertEquals(127, utf8Ptr.getStringLength());

        byte[] bytes = UTF8StringUtil.writeStringToBytes(UTF8StringSample.STRING_LEN_128);
        utf8Ptr.set(bytes, 0, bytes.length);
        assertEquals(128, utf8Ptr.getUTF8Length());
        assertEquals(2, utf8Ptr.getMetaDataLength());
        assertEquals(128, utf8Ptr.getStringLength());
    }

    @Test
    public void testFindInCodePoint() throws HyracksDataException {
        UTF8StringPointable strp = generateUTF8Pointable(STRING_EMOJI_FAMILY_OF_4 + EMOJI_BASKETBALL);
        UTF8StringPointable pattern = generateUTF8Pointable(EMOJI_BASKETBALL);

        assertEquals(7, UTF8StringPointable.findInCodePoint(strp, pattern, false));

        assertEquals(7, UTF8StringPointable.findInCodePoint(strp, pattern, true));
    }

    @Test
    public void testFindWithOffset() throws HyracksDataException {
        UTF8StringPointable src = generateUTF8Pointable("this is the king's palace");
        UTF8StringPointable pattern = generateUTF8Pointable("'s");

        // startMatchPos points to 'i' at byte offset 2 (index 2)
        assertEquals(16, UTF8StringPointable.find(src, pattern, false, 2));
        // code point difference: from index 2 to index 16 -> 14
        assertEquals(14, UTF8StringPointable.findInCodePoint(src, pattern, false, 2));
    }

    @Test
    public void testContains() throws Exception {
        assertTrue(STRING_UTF8_MIX.contains(STRING_UTF8_MIX, false));
        assertTrue(STRING_UTF8_MIX.contains(STRING_UTF8_MIX, true));
        assertTrue(STRING_UTF8_MIX.contains(STRING_EMPTY, true));

        assertTrue(STRING_UTF8_MIX.contains(STRING_UTF8_MIX_LOWERCASE, true));
        assertTrue(STRING_UTF8_MIX_LOWERCASE.contains(STRING_UTF8_MIX, true));
    }

    @Test
    public void testStartsWith() throws Exception {
        assertTrue(STRING_LEN_128.startsWith(STRING_LEN_127, true));
        assertFalse(STRING_LEN_127.startsWith(STRING_LEN_128, true));

        assertTrue(STRING_LEN_127.startsWith(STRING_EMPTY, true));
    }

    @Test
    public void testEndsWith() throws Exception {
        assertTrue(STRING_LEN_128.endsWith(STRING_LEN_127, true));
        assertFalse(STRING_LEN_127.endsWith(STRING_LEN_128, true));

        assertTrue(STRING_LEN_127.startsWith(STRING_EMPTY, true));
    }

    @Test
    public void testConcat() throws Exception {
        UTF8StringPointable expected = generateUTF8Pointable(
                UTF8StringSample.generateStringRepeatBy(UTF8StringSample.ONE_ASCII_CHAR, 127 + 128));

        GrowableArray storage = new GrowableArray();
        UTF8StringBuilder builder = new UTF8StringBuilder();
        STRING_LEN_127.concat(STRING_LEN_128, builder, storage);

        UTF8StringPointable actual = new UTF8StringPointable();
        actual.set(storage.getByteArray(), 0, storage.getLength());

        assertEquals(0, expected.compareTo(actual));

        storage.reset();
        STRING_LEN_127.concat(STRING_EMPTY, builder, storage);
        actual.set(storage.getByteArray(), 0, storage.getLength());

        assertEquals(0, STRING_LEN_127.compareTo(actual));
    }

    @Test
    public void testSubstr() throws Exception {
        GrowableArray storage = new GrowableArray();
        UTF8StringBuilder builder = new UTF8StringBuilder();

        STRING_LEN_128.substr(1, 127, builder, storage);
        UTF8StringPointable result = new UTF8StringPointable();
        result.set(storage.getByteArray(), 0, storage.getLength());

        assertEquals(0, STRING_LEN_127.compareTo(result));

        storage.reset();
        STRING_UTF8_MIX.substr(0, UTF8StringSample.STRING_UTF8_MIX.length(), builder, storage);
        result.set(storage.getByteArray(), 0, storage.getLength());
        assertEquals(0, STRING_UTF8_MIX.compareTo(result));
    }

    @Test
    public void testSubstrWithMultiCodePointCharacter() throws IOException {
        GrowableArray storage = new GrowableArray();
        UTF8StringBuilder builder = new UTF8StringBuilder();

        // The middle 2 people of the 4-people-family is a 2-people-family
        STRING_POINTABLE_EMOJI_FAMILY_OF_4.substr(2, 3, builder, storage);
        UTF8StringPointable result = new UTF8StringPointable();
        result.set(storage.getByteArray(), 0, storage.getLength());

        assertEquals(0, STRING_POINTABLE_EMOJI_FAMILY_OF_2.compareTo(result));
    }

    @Test
    public void testSubstrBefore() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();

        STRING_LEN_128.substrBefore(STRING_LEN_127, builder, storage);
        UTF8StringPointable result = new UTF8StringPointable();
        result.set(storage.getByteArray(), 0, storage.getLength());

        assertEquals(0, STRING_EMPTY.compareTo(result));

        storage.reset();
        UTF8StringPointable testPtr = generateUTF8Pointable("Mix中文123");
        UTF8StringPointable pattern = generateUTF8Pointable("文");
        UTF8StringPointable expect = generateUTF8Pointable("Mix中");
        testPtr.substrBefore(pattern, builder, storage);
        result.set(storage.getByteArray(), 0, storage.getLength());
        assertEquals(0, expect.compareTo(result));
    }

    @Test
    public void testSubstrAfter() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();

        STRING_LEN_128.substrAfter(STRING_LEN_127, builder, storage);
        UTF8StringPointable result = new UTF8StringPointable();
        result.set(storage.getByteArray(), 0, storage.getLength());

        UTF8StringPointable expect = generateUTF8Pointable(Character.toString(UTF8StringSample.ONE_ASCII_CHAR));
        assertEquals(0, expect.compareTo(result));

        storage.reset();
        UTF8StringPointable testPtr = generateUTF8Pointable("Mix中文123");
        UTF8StringPointable pattern = generateUTF8Pointable("文");
        expect = generateUTF8Pointable("123");
        testPtr.substrAfter(pattern, builder, storage);
        result.set(storage.getByteArray(), 0, storage.getLength());
        assertEquals(0, expect.compareTo(result));
    }

    @Test
    public void testLowercase() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();

        UTF8StringPointable result = new UTF8StringPointable();
        STRING_UTF8_MIX.lowercase(builder, storage);

        result.set(storage.getByteArray(), 0, storage.getLength());

        assertEquals(0, STRING_UTF8_MIX_LOWERCASE.compareTo(result));
    }

    @Test
    public void testUppercase() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();

        UTF8StringPointable result = new UTF8StringPointable();
        STRING_UTF8_MIX_LOWERCASE.uppercase(builder, storage);

        result.set(storage.getByteArray(), 0, storage.getLength());

        UTF8StringPointable expected = generateUTF8Pointable(UTF8StringSample.STRING_UTF8_MIX_LOWERCASE.toUpperCase());
        assertEquals(0, expected.compareTo(result));
    }

    @Test
    public void testInitCap() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();

        UTF8StringPointable result = new UTF8StringPointable();
        UTF8StringPointable input = generateUTF8Pointable("this is it.i am;here.");
        input.initCap(builder, storage);

        result.set(storage.getByteArray(), 0, storage.getLength());

        UTF8StringPointable expected = generateUTF8Pointable("This Is It.I Am;Here.");
        assertEquals(0, expected.compareTo(result));
    }

    @Test
    public void testTrim() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();
        UTF8StringPointable result = new UTF8StringPointable();
        UTF8StringPointable input = generateUTF8Pointable("  this is it.i am;here.  ");
        IntCollection spaceCodePointSet = new IntArraySet(Arrays.asList((int) ' '));

        // Trims both sides.
        input.trim(builder, storage, true, true, spaceCodePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        UTF8StringPointable expected = generateUTF8Pointable("this is it.i am;here.");
        assertEquals(0, expected.compareTo(result));

        // Only trims the right side.
        storage.reset();
        input.trim(builder, storage, false, true, spaceCodePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        expected = generateUTF8Pointable("  this is it.i am;here.");
        assertEquals(0, expected.compareTo(result));

        // Only trims the left side.
        storage.reset();
        input.trim(builder, storage, true, false, spaceCodePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        expected = generateUTF8Pointable("this is it.i am;here.  ");
        assertEquals(0, expected.compareTo(result));

        // Only trims the left side in case of emoji
        input = STRING_POINTABLE_EMOJI_FAMILY_OF_4;
        storage.reset();
        input.trim(builder, storage, true, false, spaceCodePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        expected = STRING_POINTABLE_EMOJI_FAMILY_OF_4;
        assertEquals(0, expected.compareTo(result));
    }

    @Test
    public void testTrimWithPattern() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();
        UTF8StringPointable result = new UTF8StringPointable();
        UTF8StringPointable input = generateUTF8Pointable("  this is it.i am;here.  ");

        String pattern = " hert.";
        UTF8StringPointable patternPointable = generateUTF8Pointable(pattern);
        IntCollection codePointSet = new IntArraySet();
        codePointSet.clear();
        patternPointable.getCodePoints(codePointSet);

        // Trims both sides.
        input.trim(builder, storage, true, true, codePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        UTF8StringPointable expected = generateUTF8Pointable("is is it.i am;");
        assertEquals(0, expected.compareTo(result));

        // Only trims the right side.
        storage.reset();
        input.trim(builder, storage, false, true, codePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        expected = generateUTF8Pointable("  this is it.i am;");
        assertEquals(0, expected.compareTo(result));

        // Only trims the left side.
        storage.reset();
        input.trim(builder, storage, true, false, codePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        expected = generateUTF8Pointable("is is it.i am;here.  ");
        assertEquals(0, expected.compareTo(result));

        // Test Emoji trim
        input = STRING_POINTABLE_EMOJI_FAMILY_OF_4;
        pattern = "👨👦";
        patternPointable = generateUTF8Pointable(pattern);
        codePointSet.clear();
        patternPointable.getCodePoints(codePointSet);

        // Trim left
        storage.reset();
        input.trim(builder, storage, true, false, codePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        expected = generateUTF8Pointable("\u200D" + "👨‍👦‍👦");
        assertEquals(0, expected.compareTo(result));

        // Trim right
        storage.reset();
        input.trim(builder, storage, false, true, codePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        expected = generateUTF8Pointable("👨‍👨‍👦" + "\u200D");
        assertEquals(0, expected.compareTo(result));

        // Trim left and right
        storage.reset();
        input.trim(builder, storage, true, true, codePointSet);
        result.set(storage.getByteArray(), 0, storage.getLength());
        expected = generateUTF8Pointable("\u200D" + "👨‍👦" + "\u200D");
        assertEquals(0, expected.compareTo(result));
    }

    @Test
    public void testReverse() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();
        UTF8StringPointable result = new UTF8StringPointable();
        UTF8StringPointable input = generateUTF8Pointable(" I'd like to reverse ");
        UTF8StringPointable expected = generateUTF8Pointable(" esrever ot ekil d'I ");

        UTF8StringPointable.reverse(input, builder, storage);
        result.set(storage.getByteArray(), 0, storage.getLength());
        assertEquals(0, expected.compareTo(result));
    }

    @Test
    public void testReverseWithEmoji() throws IOException {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray storage = new GrowableArray();
        UTF8StringPointable result = new UTF8StringPointable();
        UTF8StringPointable input = generateUTF8Pointable("\uD83C\uDDE8\uD83C\uDDF3"); // CN flag
        UTF8StringPointable expected = generateUTF8Pointable("\uD83C\uDDF3\uD83C\uDDE8"); // NC flag

        UTF8StringPointable.reverse(input, builder, storage);
        result.set(storage.getByteArray(), 0, storage.getLength());
        assertEquals(0, expected.compareTo(result));
    }

    @Test
    public void testStringBuilder() throws Exception {
        UTF8StringBuilder builder = new UTF8StringBuilder();
        GrowableArray array = new GrowableArray();
        UTF8StringPointable stringPointable = new UTF8StringPointable();
        String writtenString;
        int startIdx;

        array.append(STRING_UTF8_MIX.getByteArray(), STRING_UTF8_MIX.getStartOffset(), STRING_UTF8_MIX.getLength());
        String chunk = "ABC";
        String originalString = chunk.repeat(699051);

        // test grow path
        startIdx = array.getLength();
        builder.reset(array, 2);
        builder.appendString(originalString);
        builder.finish();
        stringPointable.set(array.getByteArray(), startIdx, array.getLength());
        writtenString = stringPointable.toString();
        assertEquals(originalString, writtenString);

        // test shrink path
        array.reset();
        array.append(STRING_UTF8_MIX.getByteArray(), STRING_UTF8_MIX.getStartOffset(), STRING_UTF8_MIX.getLength());
        startIdx = array.getLength();
        builder.reset(array, 699051);
        builder.appendString(chunk);
        builder.finish();
        stringPointable.set(array.getByteArray(), startIdx, array.getLength());
        writtenString = stringPointable.toString();
        assertEquals(chunk, writtenString);
    }

}
