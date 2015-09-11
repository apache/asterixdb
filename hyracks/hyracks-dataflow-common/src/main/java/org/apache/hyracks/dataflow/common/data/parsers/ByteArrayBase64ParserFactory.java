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

package org.apache.hyracks.dataflow.common.data.parsers;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class ByteArrayBase64ParserFactory implements IValueParserFactory {

    public static final ByteArrayBase64ParserFactory INSTANCE = new ByteArrayBase64ParserFactory();

    private ByteArrayBase64ParserFactory() {
    }

    @Override public IValueParser createValueParser() {
        return new IValueParser() {
            private byte[] buffer;
            private byte[] quadruplet = new byte[4];

            @Override public void parse(char[] input, int start, int length, DataOutput out)
                    throws HyracksDataException {
                if (length % 4 != 0) {
                    throw new HyracksDataException(
                            "Invalid Base64 string, the length of the string should be a multiple of 4");
                }
                buffer = extractPointableArrayFromBase64String(input, start, length, buffer, quadruplet);
                try {
                    out.write(buffer, 0, ByteArrayPointable.getFullLength(buffer, 0));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

    // The following base64 related implementation is copied/changed base on javax.xml.bind.DatatypeConverterImpl.java
    private static final byte[] decodeMap = initDecodeMap();
    private static final byte PADDING = 127;

    private static byte[] initDecodeMap() {
        byte[] map = new byte[128];
        Arrays.fill(map, (byte) -1);

        int i;
        for (i = 'A'; i <= 'Z'; i++) {
            map[i] = (byte) (i - 'A');
        }
        for (i = 'a'; i <= 'z'; i++) {
            map[i] = (byte) (i - 'a' + 26);
        }
        for (i = '0'; i <= '9'; i++) {
            map[i] = (byte) (i - '0' + 52);
        }
        map['+'] = 62;
        map['/'] = 63;
        map['='] = PADDING;

        return map;
    }

    /**
     * computes the length of binary data speculatively.
     * Our requirement is to create byte[] of the exact length to store the binary data.
     * If we do this in a straight-forward way, it takes two passes over the data.
     * Experiments show that this is a non-trivial overhead (35% or so is spent on
     * the first pass in calculating the length.)
     * So the approach here is that we compute the length speculatively, without looking
     * at the whole contents. The obtained speculative value is never less than the
     * actual length of the binary data, but it may be bigger. So if the speculation
     * goes wrong, we'll pay the cost of reallocation and buffer copying.
     * If the base64 text is tightly packed with no indentation nor illegal char
     * (like what most web services produce), then the speculation of this method
     * will be correct, so we get the performance benefit.
     */
    private static int guessLength(char[] chars, int start, int length) {

        // compute the tail '=' chars
        int j = length - 1;
        for (; j >= 0; j--) {
            byte code = decodeMap[chars[start + j]];
            if (code == PADDING) {
                continue;
            }
            if (code == -1) // most likely this base64 text is indented. go with the upper bound
            {
                return length / 4 * 3;
            }
            break;
        }

        j++;    // text.charAt(j) is now at some base64 char, so +1 to make it the size
        int padSize = length - j;
        if (padSize > 2) // something is wrong with base64. be safe and go with the upper bound
        {
            return length / 4 * 3;
        }

        // so far this base64 looks like it's unindented tightly packed base64.
        // take a chance and create an array with the expected size
        return length / 4 * 3 - padSize;
    }

    private static int guessLength(byte[] chars, int start, int length) {

        // compute the tail '=' chars
        int j = length - 1;
        for (; j >= 0; j--) {
            byte code = decodeMap[chars[start + j]];
            if (code == PADDING) {
                continue;
            }
            if (code == -1) // most likely this base64 text is indented. go with the upper bound
            {
                return length / 4 * 3;
            }
            break;
        }

        j++;    // text.charAt(j) is now at some base64 char, so +1 to make it the size
        int padSize = length - j;
        if (padSize > 2) // something is wrong with base64. be safe and go with the upper bound
        {
            return length / 4 * 3;
        }

        // so far this base64 looks like it's unindented tightly packed base64.
        // take a chance and create an array with the expected size
        return length / 4 * 3 - padSize;
    }

    public static byte[] extractPointableArrayFromBase64String(byte[] input, int start, int length,
            byte[] bufferNeedToReset, byte[] quadruplet)
            throws HyracksDataException {
        int contentOffset = ByteArrayPointable.SIZE_OF_LENGTH;
        final int buflen = guessLength(input, start, length) + contentOffset;
        bufferNeedToReset = ByteArrayHexParserFactory.ensureCapacity(buflen, bufferNeedToReset);
        int byteArrayLength = parseBase64String(input, start, length, bufferNeedToReset, contentOffset,
                quadruplet);
        if (byteArrayLength > ByteArrayPointable.MAX_LENGTH) {
            throw new HyracksDataException("The decoded byte array is too long.");
        }
        ByteArrayPointable.putLength(byteArrayLength, bufferNeedToReset, 0);
        return bufferNeedToReset;
    }

    public static byte[] extractPointableArrayFromBase64String(char[] input, int start, int length,
            byte[] bufferNeedToReset, byte[] quadruplet)
            throws HyracksDataException {
        int contentOffset = ByteArrayPointable.SIZE_OF_LENGTH;
        final int buflen = guessLength(input, start, length) + contentOffset;
        bufferNeedToReset = ByteArrayHexParserFactory.ensureCapacity(buflen, bufferNeedToReset);
        int byteArrayLength = parseBase64String(input, start, length, bufferNeedToReset, contentOffset,
                quadruplet);
        if (byteArrayLength > ByteArrayPointable.MAX_LENGTH) {
            throw new HyracksDataException("The decoded byte array is too long.");
        }
        ByteArrayPointable.putLength(byteArrayLength, bufferNeedToReset, 0);
        return bufferNeedToReset;
    }

    static int parseBase64String(char[] input, int start, int length, byte[] out, int offset,
            byte[] quadruplet) throws HyracksDataException {
        int outLength = 0;

        int i;
        int q = 0;

        // convert each quadruplet to three bytes.
        for (i = 0; i < length; i++) {
            char ch = input[start + i];
            byte v = decodeMap[ch];

            if (v == -1) {
                throw new HyracksDataException("Invalid Base64 character");
            }
            quadruplet[q++] = v;

            if (q == 4) {
                // quadruplet is now filled.
                out[offset + outLength++] = (byte) ((quadruplet[0] << 2) | (quadruplet[1] >> 4));
                if (quadruplet[2] != PADDING) {
                    out[offset + outLength++] = (byte) ((quadruplet[1] << 4) | (quadruplet[2] >> 2));
                }
                if (quadruplet[3] != PADDING) {
                    out[offset + outLength++] = (byte) ((quadruplet[2] << 6) | (quadruplet[3]));
                }
                q = 0;
            }
        }

        return outLength;
    }

    static int parseBase64String(byte[] input, int start, int length, byte[] out, int offset,
            byte[] quadruplet) throws HyracksDataException {
        int outLength = 0;

        int i;
        int q = 0;

        // convert each quadruplet to three bytes.
        for (i = 0; i < length; i++) {
            char ch = (char)input[start + i];
            byte v = decodeMap[ch];

            if (v == -1) {
                throw new HyracksDataException("Invalid Base64 character");
            }
            quadruplet[q++] = v;

            if (q == 4) {
                // quadruplet is now filled.
                out[offset + outLength++] = (byte) ((quadruplet[0] << 2) | (quadruplet[1] >> 4));
                if (quadruplet[2] != PADDING) {
                    out[offset + outLength++] = (byte) ((quadruplet[1] << 4) | (quadruplet[2] >> 2));
                }
                if (quadruplet[3] != PADDING) {
                    out[offset + outLength++] = (byte) ((quadruplet[2] << 6) | (quadruplet[3]));
                }
                q = 0;
            }
        }

        return outLength;
    }
}
