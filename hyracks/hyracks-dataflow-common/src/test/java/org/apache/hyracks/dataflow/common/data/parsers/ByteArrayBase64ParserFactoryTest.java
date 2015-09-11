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
import junit.framework.TestCase;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import static org.apache.hyracks.dataflow.common.data.parsers.ByteArrayHexParserFactoryTest.subArray;

public class ByteArrayBase64ParserFactoryTest extends TestCase {

    @Test
    public void testParseBase64String() throws HyracksDataException {
        IValueParser parser = ByteArrayBase64ParserFactory.INSTANCE.createValueParser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(bos);
        String empty = "";

        parser.parse(empty.toCharArray(), 0, empty.length(), outputStream);

        byte[] cache = bos.toByteArray();
        assertTrue(ByteArrayPointable.getLength(cache, 0) == 0);
        assertTrue(DatatypeConverter.printBase64Binary(subArray(cache, 2)).equalsIgnoreCase(empty));

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

        bos.reset();
        parser.parse(everyChar.toString().toCharArray(), 0, everyChar.length(), outputStream);
        cache = bos.toByteArray();
        byte[] answer = DatatypeConverter.parseBase64Binary(everyChar.toString());
        assertTrue(ByteArrayPointable.getLength(cache, 0) == answer.length);
        assertTrue(Arrays.equals(answer, subArray(cache, 2)));

        byte[] maxBytes = new byte[ByteArrayPointable.MAX_LENGTH];
        Arrays.fill(maxBytes, (byte) 0xff);
        String maxString = DatatypeConverter.printBase64Binary(maxBytes);
        bos.reset();
        parser.parse(maxString.toCharArray(), 0, maxString.length(), outputStream);
        cache = bos.toByteArray();
        assertTrue(ByteArrayPointable.getLength(cache, 0) == maxBytes.length);
        assertTrue(Arrays.equals(maxBytes, subArray(cache, 2)));
    }

}