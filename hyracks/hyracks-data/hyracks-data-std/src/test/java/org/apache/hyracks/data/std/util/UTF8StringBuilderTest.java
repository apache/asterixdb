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

package org.apache.hyracks.data.std.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.hyracks.util.string.UTF8StringSample;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.junit.Test;

public class UTF8StringBuilderTest {

    UTF8StringBuilder utf8StringBuilder = new UTF8StringBuilder();
    GrowableArray storage = new GrowableArray();

    @Test
    public void testNormalBuilder() throws IOException {
        testOneString(UTF8StringSample.EMPTY_STRING, 0);
        testOneString(UTF8StringSample.EMPTY_STRING, 127);

        testOneString(UTF8StringSample.STRING_UTF8_MIX, 127);
        testOneString(UTF8StringSample.STRING_LEN_128, 128);

        testOneString(UTF8StringSample.STRING_LEN_MEDIUM, VarLenIntEncoderDecoder.BOUND_TWO_BYTE);
        testOneString(UTF8StringSample.STRING_LEN_LARGE, VarLenIntEncoderDecoder.BOUND_THREE_BYTE);
    }

    @Test
    public void testShrinkAfterFinish() throws IOException {
        testOneString(UTF8StringSample.STRING_LEN_127, VarLenIntEncoderDecoder.BOUND_TWO_BYTE);
        testOneString(UTF8StringSample.STRING_LEN_127, VarLenIntEncoderDecoder.BOUND_THREE_BYTE);
        testOneString(UTF8StringSample.STRING_LEN_127, VarLenIntEncoderDecoder.BOUND_FOUR_BYTE);
    }

    @Test
    public void testIncreaseAfterFinish() throws IOException {
        testOneString(UTF8StringSample.STRING_LEN_128, VarLenIntEncoderDecoder.BOUND_ONE_BYTE);
        testOneString(UTF8StringSample.STRING_LEN_MEDIUM, VarLenIntEncoderDecoder.BOUND_ONE_BYTE);
        testOneString(UTF8StringSample.STRING_LEN_LARGE, VarLenIntEncoderDecoder.BOUND_TWO_BYTE);
    }

    public void testOneString(String testString, int estimateLength) throws IOException {
        storage.reset();
        utf8StringBuilder.reset(storage, estimateLength);
        for (char c : testString.toCharArray()) {
            utf8StringBuilder.appendChar(c);
        }
        utf8StringBuilder.finish();
        assertEquals(testString, UTF8StringUtil.toString(new StringBuilder(), storage.getByteArray(), 0).toString());

        UTF8StringPointable hyracksUtf = new UTF8StringPointable();
        hyracksUtf.set(storage.getByteArray(), 0, storage.getLength());

        GrowableArray storage2 = new GrowableArray();
        utf8StringBuilder.reset(storage2, estimateLength);
        utf8StringBuilder.appendUtf8StringPointable(hyracksUtf);
        utf8StringBuilder.finish();
        assertEquals(testString, UTF8StringUtil.toString(new StringBuilder(), storage.getByteArray(), 0).toString());
    }
}
