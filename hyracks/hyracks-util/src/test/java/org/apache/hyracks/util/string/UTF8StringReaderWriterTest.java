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

import static org.apache.hyracks.util.string.UTF8StringSample.EMPTY_STRING;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_127;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_128;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_3;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_LARGE;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_LARGE_SUB_1;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_MEDIUM;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_LEN_MEDIUM_SUB_1;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_UTF8_3;
import static org.apache.hyracks.util.string.UTF8StringSample.STRING_UTF8_MIX;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class UTF8StringReaderWriterTest {

    UTF8StringWriter writer = new UTF8StringWriter();
    UTF8StringReader reader = new UTF8StringReader();

    @Test
    public void testWriterReader() throws IOException {
        writeAndReadOneString(EMPTY_STRING);
        writeAndReadOneString(STRING_LEN_3);

        writeAndReadOneString(STRING_LEN_127);
        writeAndReadOneString(STRING_LEN_128);
        writeAndReadOneString(STRING_LEN_MEDIUM_SUB_1);
    }

    @Test
    public void testMedium() throws IOException {
        writeAndReadOneString(STRING_LEN_MEDIUM);
        writeAndReadOneString(STRING_LEN_LARGE_SUB_1);
    }

    @Test
    public void testLarge() throws IOException {
        writeAndReadOneString(STRING_LEN_LARGE);
    }

    @Test
    public void testUTF8() throws IOException {
        writeAndReadOneString(STRING_UTF8_3);
        writeAndReadOneString(STRING_UTF8_MIX);
    }

    private void writeAndReadOneString(String testString) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        writer.writeUTF8(testString, dos);

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray(), 0, bos.size());
        assertEquals(testString, reader.readUTF(new DataInputStream(bis)));

        int lastOffset = bos.size();
        char[] charArray = testString.toCharArray();
        writer.writeUTF8(charArray, 0, charArray.length, dos);

        bis = new ByteArrayInputStream(bos.toByteArray(), lastOffset, bos.size());
        assertEquals(testString, reader.readUTF(new DataInputStream(bis)));
    }

}
