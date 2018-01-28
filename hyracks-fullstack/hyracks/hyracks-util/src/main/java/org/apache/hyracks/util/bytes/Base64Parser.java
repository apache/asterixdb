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

package org.apache.hyracks.util.bytes;

import java.util.Arrays;

public class Base64Parser {
    private static final byte[] DECODE_MAP = initDecodeMap();
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

    private byte[] quadruplet = new byte[4];
    private byte[] storage;
    private int length = 0;

    /**
     * Parse the Base64 sequence from {@code input} into {@code out}
     * Note, the out should have enough space by checking the {@link #guessLength(char[], int, int)} first
     *
     * @param input
     * @param start
     * @param length
     * @param out
     * @param offset
     * @return
     */
    public int parseBase64String(char[] input, int start, int length, byte[] out, int offset) {
        int outLength = 0;

        int i;
        int q = 0;

        // convert each quadruplet to three bytes.
        for (i = 0; i < length; i++) {
            char ch = input[start + i];
            byte v = DECODE_MAP[ch];

            if (v == -1) {
                throw new IllegalArgumentException("Invalid Base64 character");
            }
            quadruplet[q++] = v;

            if (q == 4) {
                outLength += dumpQuadruplet(out, offset + outLength);
                q = 0;
            }
        }

        return outLength;
    }

    /**
     * Parse the Base64 sequence from {@code input} into {@code out}
     * Note, the out should have enough space by checking the {@link #guessLength(byte[], int, int)} first
     *
     * @param input
     * @param start
     * @param length
     * @param out
     * @param offset
     * @return the number of written bytes
     */
    public int parseBase64String(byte[] input, int start, int length, byte[] out, int offset) {
        int outLength = 0;

        int i;
        int q = 0;

        // convert each quadruplet to three bytes.
        for (i = 0; i < length; i++) {
            char ch = (char) input[start + i];
            byte v = DECODE_MAP[ch];

            if (v == -1) {
                throw new IllegalArgumentException("Invalid Base64 character");
            }
            quadruplet[q++] = v;

            if (q == 4) {
                outLength += dumpQuadruplet(out, offset + outLength);
                q = 0;
            }
        }

        return outLength;
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
    public static int guessLength(char[] chars, int start, int length) {

        // compute the tail '=' chars
        int j = length - 1;
        for (; j >= 0; j--) {
            byte code = DECODE_MAP[chars[start + j]];
            if (code == PADDING) {
                continue;
            }
            if (code == -1) // most likely this base64 text is indented. go with the upper bound
            {
                return length / 4 * 3;
            }
            break;
        }

        j++; // text.charAt(j) is now at some base64 char, so +1 to make it the size
        int padSize = length - j;
        if (padSize > 2) // something is wrong with base64. be safe and go with the upper bound
        {
            return length / 4 * 3;
        }

        // so far this base64 looks like it's unindented tightly packed base64.
        // take a chance and create an array with the expected size
        return length / 4 * 3 - padSize;
    }

    public static int guessLength(byte[] chars, int start, int length) {

        // compute the tail '=' chars
        int j = length - 1;
        for (; j >= 0; j--) {
            byte code = DECODE_MAP[chars[start + j]];
            if (code == PADDING) {
                continue;
            }
            if (code == -1) // most likely this base64 text is indented. go with the upper bound
            {
                return length / 4 * 3;
            }
            break;
        }

        j++; // text.charAt(j) is now at some base64 char, so +1 to make it the size
        int padSize = length - j;
        if (padSize > 2) // something is wrong with base64. be safe and go with the upper bound
        {
            return length / 4 * 3;
        }

        // so far this base64 looks like it's unindented tightly packed base64.
        // take a chance and create an array with the expected size
        return length / 4 * 3 - padSize;
    }

    public byte[] getByteArray() {
        return storage;
    }

    public int getLength() {
        return length;
    }

    /**
     * Same as {@link #parseBase64String(byte[], int, int, byte[], int)}, but we will provide the storage for caller
     *
     * @param input
     * @param start
     * @param length
     */
    public void generatePureByteArrayFromBase64String(byte[] input, int start, int length) {
        // The base64 character length equals to utf8length
        if (length % 4 != 0) {
            throw new IllegalArgumentException(
                    "Invalid Base64 string, the length of the string should be a multiple of 4");
        }
        final int buflen = guessLength(input, start, length);
        ensureCapacity(buflen);
        this.length = parseBase64String(input, start, length, storage, 0);
    }

    public void generatePureByteArrayFromBase64String(char[] input, int start, int length) {
        if (length % 4 != 0) {
            throw new IllegalArgumentException(
                    "Invalid Base64 string, the length of the string should be a multiple of 4");
        }
        final int buflen = guessLength(input, start, length);
        ensureCapacity(buflen);
        this.length = parseBase64String(input, start, length, storage, 0);
    }

    private void ensureCapacity(int length) {
        if (storage == null || storage.length < length) {
            storage = new byte[length];
        }
    }

    private int dumpQuadruplet(byte[] out, int offset) {
        int outLength = 0;
        // quadruplet is now filled.
        out[offset + outLength++] = (byte) ((quadruplet[0] << 2) | (quadruplet[1] >> 4));
        if (quadruplet[2] != PADDING) {
            out[offset + outLength++] = (byte) ((quadruplet[1] << 4) | (quadruplet[2] >> 2));
        }
        if (quadruplet[3] != PADDING) {
            out[offset + outLength++] = (byte) ((quadruplet[2] << 6) | (quadruplet[3]));
        }
        return outLength;
    }

}
