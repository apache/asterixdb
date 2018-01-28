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

import java.io.IOException;

public class Base64Printer {
    /**
     * Encodes a byte array into a {@code Appendable} stream by doing base64 encoding.
     *
     * @return the same input stream.
     */
    public static Appendable printBase64Binary(byte[] input, int offset, int len, Appendable appendable)
            throws IOException {
        // encode elements until only 1 or 2 elements are left to encode
        int remaining = len;
        int i;
        for (i = offset; remaining >= 3; remaining -= 3, i += 3) {
            appendable.append(encode(input[i] >> 2));
            appendable.append(encode(((input[i] & 0x3) << 4) | ((input[i + 1] >> 4) & 0xF)));
            appendable.append(encode(((input[i + 1] & 0xF) << 2) | ((input[i + 2] >> 6) & 0x3)));
            appendable.append(encode(input[i + 2] & 0x3F));
        }
        // encode when exactly 1 element (left) to encode
        if (remaining == 1) {
            appendable.append(encode(input[i] >> 2));
            appendable.append(encode(((input[i]) & 0x3) << 4));
            appendable.append('=');
            appendable.append('=');
        }
        // encode when exactly 2 elements (left) to encode
        if (remaining == 2) {
            appendable.append(encode(input[i] >> 2));
            appendable.append(encode(((input[i] & 0x3) << 4) | ((input[i + 1] >> 4) & 0xF)));
            appendable.append(encode((input[i + 1] & 0xF) << 2));
            appendable.append('=');
        }
        return appendable;
    }

    /**
     * Encodes a byte array into a char array by doing base64 encoding.
     * The caller must supply a big enough buffer.
     *
     * @return the value of {@code ptr+((len+2)/3)*4}, which is the new offset
     * in the output buffer where the further bytes should be placed.
     */
    public static int printBase64Binary(byte[] input, int offset, int len, char[] buf, int ptr) {
        // encode elements until only 1 or 2 elements are left to encode
        int remaining = len;
        int i;
        for (i = offset; remaining >= 3; remaining -= 3, i += 3) {
            buf[ptr++] = encode(input[i] >> 2);
            buf[ptr++] = encode(((input[i] & 0x3) << 4) | ((input[i + 1] >> 4) & 0xF));
            buf[ptr++] = encode(((input[i + 1] & 0xF) << 2) | ((input[i + 2] >> 6) & 0x3));
            buf[ptr++] = encode(input[i + 2] & 0x3F);
        }
        // encode when exactly 1 element (left) to encode
        if (remaining == 1) {
            buf[ptr++] = encode(input[i] >> 2);
            buf[ptr++] = encode(((input[i]) & 0x3) << 4);
            buf[ptr++] = '=';
            buf[ptr++] = '=';
        }
        // encode when exactly 2 elements (left) to encode
        if (remaining == 2) {
            buf[ptr++] = encode(input[i] >> 2);
            buf[ptr++] = encode(((input[i] & 0x3) << 4) | ((input[i + 1] >> 4) & 0xF));
            buf[ptr++] = encode((input[i + 1] & 0xF) << 2);
            buf[ptr++] = '=';
        }
        return ptr;
    }

    private static final char[] encodeMap = initEncodeMap();

    private static char[] initEncodeMap() {
        char[] map = new char[64];
        int i;
        for (i = 0; i < 26; i++) {
            map[i] = (char) ('A' + i);
        }
        for (i = 26; i < 52; i++) {
            map[i] = (char) ('a' + (i - 26));
        }
        for (i = 52; i < 62; i++) {
            map[i] = (char) ('0' + (i - 52));
        }
        map[62] = '+';
        map[63] = '/';

        return map;
    }

    public static char encode(int i) {
        return encodeMap[i & 0x3F];
    }
}
