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

import static org.apache.hyracks.data.std.primitive.ByteArrayPointable.copyContent;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.junit.Test;

import junit.framework.TestCase;

public class ByteArrayBase64ParserFactoryTest extends TestCase {

    @Test
    public void testParseBase64String() throws HyracksDataException {
        testOneString("");

        StringBuilder everyChar = new StringBuilder();
        for (char c = 'a'; c <= 'z'; c++) {
            everyChar.append(c);
        }
        for (char c = 'A'; c <= 'Z'; c++) {
            everyChar.append(c);
        }
        for (char c = '0'; c <= '9'; c++) {
            everyChar.append(c);
        }
        everyChar.append("+/");

        testOneString(everyChar.toString());

        byte[] longBytes = new byte[65536];
        Arrays.fill(longBytes, (byte) 0xff);
        String maxString = DatatypeConverter.printBase64Binary(longBytes);

        testOneString(maxString);
    }

    void testOneString(String test) throws HyracksDataException {
        IValueParser parser = ByteArrayBase64ParserFactory.INSTANCE.createValueParser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(bos);
        ByteArrayPointable bytePtr = new ByteArrayPointable();

        parser.parse(test.toCharArray(), 0, test.length(), outputStream);
        bytePtr.set(bos.toByteArray(), 0, bos.size());

        byte[] answer = DatatypeConverter.parseBase64Binary(test);
        assertTrue(bytePtr.getContentLength() == answer.length);
        assertTrue(Arrays.equals(answer, copyContent(bytePtr)));
    }
}
