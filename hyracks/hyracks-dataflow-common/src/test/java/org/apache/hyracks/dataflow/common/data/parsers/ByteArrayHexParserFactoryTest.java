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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.junit.Test;

public class ByteArrayHexParserFactoryTest {

    @Test
    public void testExtractPointableArrayFromHexString() throws Exception {
        testOneString("");
        testOneString("ABCDEF0123456789");

        testOneString("0123456789abcdef");

        char[] maxChars = new char[65540 * 2];
        Arrays.fill(maxChars, 'f');
        String maxString = new String(maxChars);

        testOneString(maxString);
    }

    void testOneString(String test) throws HyracksDataException {
        IValueParser parser = ByteArrayHexParserFactory.INSTANCE.createValueParser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(bos);
        ByteArrayPointable bytePtr = new ByteArrayPointable();

        parser.parse(test.toCharArray(), 0, test.length(), outputStream);

        bytePtr.set(bos.toByteArray(), 0, bos.size());

        assertTrue(bytePtr.getContentLength() == test.length() / 2);
        assertEquals(DatatypeConverter.printHexBinary(ByteArrayPointable.copyContent(bytePtr)).toLowerCase(),
                test.toLowerCase());
    }

}
