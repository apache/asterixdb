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

package org.apache.hyracks.util.string;

import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.BOUND_THREE_BYTE;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.BOUND_TWO_BYTE;

import java.util.Arrays;

/**
 * Util class to provide the sample test string
 */
public class UTF8StringSample {
    public static final String EMPTY_STRING = "";

    public static final char ONE_ASCII_CHAR = 'x';
    public static final char ONE_UTF8_CHAR = 'à';

    public static final String STRING_LEN_3 = "xyz";
    public static final String STRING_UTF8_3 = "锟斤拷";
    public static final String STRING_UTF8_MIX = "\uD841\uDF0E\uD841\uDF31锟X斤Y拷Zà"; // one, two, three, and four bytes
    public static final String STRING_UTF8_MIX_LOWERCASE = "\uD841\uDF0E\uD841\uDF31锟x斤y拷zà";
    public static final String STRING_NEEDS_2_JAVA_CHARS_1 = "\uD83D\uDE22\uD83D\uDE22\uD83D\uDC89\uD83D\uDC89";
    public static final String STRING_NEEDS_2_JAVA_CHARS_2 = "😢😢💉💉";

    public static final String STRING_LEN_127 = generateStringRepeatBy(ONE_ASCII_CHAR, 127);
    public static final String STRING_LEN_128 = generateStringRepeatBy(ONE_ASCII_CHAR, 128);

    public static final String STRING_LEN_MEDIUM_SUB_1 = generateStringRepeatBy(ONE_ASCII_CHAR, BOUND_TWO_BYTE - 1);
    public static final String STRING_LEN_MEDIUM = generateStringRepeatBy(ONE_ASCII_CHAR, BOUND_TWO_BYTE);

    public static final String STRING_LEN_LARGE_SUB_1 = generateStringRepeatBy(ONE_ASCII_CHAR, BOUND_THREE_BYTE - 1);
    public static final String STRING_LEN_LARGE = generateStringRepeatBy(ONE_ASCII_CHAR, BOUND_THREE_BYTE);

    public static String generateStringRepeatBy(char c, int times) {
        char[] chars = new char[times];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    private UTF8StringSample() {
    }
}
