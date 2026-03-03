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

package org.apache.asterix.dataflow.data.nontagged.printers;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.apache.hyracks.util.string.UTF8StringUtil;
import org.junit.Test;

public class PrintToolsTest {

    private static final char QUOTE = '"';
    private static final char ESCAPE = '"';
    private static final char DELIMITER = ',';

    public static String csvOf(String input) throws Exception {
        byte[] bytes = UTF8StringUtil.writeStringToBytes(input);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
        PrintTools.writeUTF8StringAsCSV(bytes, 0, bytes.length, ps, QUOTE, false, ESCAPE, DELIMITER);
        return baos.toString(StandardCharsets.UTF_8);
    }

    public static String jsonOf(String input) throws Exception {
        byte[] bytes = UTF8StringUtil.writeStringToBytes(input);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintTools.writeUTF8StringAsJSON(bytes, 0, bytes.length, baos);
        return baos.toString(StandardCharsets.UTF_8);
    }

    @Test
    public void testCsvAsciiPlain() throws Exception {
        assertEquals("hello", csvOf("hello"));
    }

    @Test
    public void testCsvAsciiEmpty() throws Exception {
        assertEquals("", csvOf(""));
    }

    @Test
    public void testCsvAsciiQuotingOnComma() throws Exception {
        assertEquals("\"a,b\"", csvOf("a,b"));
    }

    @Test
    public void testCsvAsciiQuotingOnNewline() throws Exception {
        assertEquals("\"a\nb\"", csvOf("a\nb"));
    }

    @Test
    public void testCsvAsciiEscapeQuote() throws Exception {
        assertEquals("\"say \"\"hi\"\"\"", csvOf("say \"hi\""));
    }

    @Test
    public void testCsvTwoByteChar() throws Exception {
        assertEquals("café", csvOf("café"));
    }

    @Test
    public void testCsvTwoByteCharWithComma() throws Exception {
        assertEquals("\"café,café\"", csvOf("café,café"));
    }

    @Test
    public void testCsvThreeByteChar() throws Exception {
        assertEquals("中文", csvOf("中文"));
    }

    @Test
    public void testCsvEmoji() throws Exception {
        assertEquals("💪", csvOf("💪"));
    }

    @Test
    public void testCsvMixedEmojiString() throws Exception {
        String input = "No more 💪🏻🦋";
        assertEquals(input, csvOf(input));
    }

    @Test
    public void testCsvMixedMultibyteAndAscii() throws Exception {
        String input = "Hello 中文 café 😀!";
        assertEquals(input, csvOf(input));
    }

    @Test
    public void testCsvEmojiWithComma() throws Exception {
        assertEquals("\"💪,💪\"", csvOf("💪,💪"));
    }

    @Test
    public void testCsvEmojiWithNewline() throws Exception {
        assertEquals("\"💪\n💪\"", csvOf("💪\n💪"));
    }

    @Test
    public void testCsvEmojiWithQuote() throws Exception {
        assertEquals("\"💪\"\"💪\"", csvOf("💪\"💪"));
    }

    @Test
    public void testJsonAsciiPlain() throws Exception {
        assertEquals("\"hello\"", jsonOf("hello"));
    }

    @Test
    public void testJsonAsciiEmpty() throws Exception {
        assertEquals("\"\"", jsonOf(""));
    }

    @Test
    public void testJsonAsciiEscapeNewline() throws Exception {
        assertEquals("\"line1\\nline2\"", jsonOf("line1\nline2"));
    }

    @Test
    public void testJsonAsciiEscapeTab() throws Exception {
        assertEquals("\"col1\\tcol2\"", jsonOf("col1\tcol2"));
    }

    @Test
    public void testJsonAsciiEscapeBackslash() throws Exception {
        assertEquals("\"a\\\\b\"", jsonOf("a\\b"));
    }

    @Test
    public void testJsonTwoByteChar() throws Exception {
        assertEquals("\"café\"", jsonOf("café"));
    }

    @Test
    public void testJsonThreeByteChar() throws Exception {
        assertEquals("\"中文\"", jsonOf("中文"));
    }

    @Test
    public void testJsonEmoji() throws Exception {
        assertEquals("\"💪\"", jsonOf("💪"));
    }

    @Test
    public void testJsonMixedMultibyteAndAscii() throws Exception {
        String input = "Hello 中文 café 😀!";
        assertEquals("\"" + input + "\"", jsonOf(input));
    }

    @Test
    public void testJsonMixedEmojiString() throws Exception {
        String input = "No more 💪🏻🦋";
        assertEquals("\"" + input + "\"", jsonOf(input));
    }
}
