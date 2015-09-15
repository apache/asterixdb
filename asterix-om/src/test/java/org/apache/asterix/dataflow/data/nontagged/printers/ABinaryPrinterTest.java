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

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import static org.junit.Assert.*;

public class ABinaryPrinterTest {

    public static byte[] generateABinaryBytesByStringContent(String content){
        String padding = "000000" + content;
        byte [] bytes = DatatypeConverter.parseHexBinary(padding);
        ByteArrayPointable.putLength(content.length() / 2, bytes, 1);
        return bytes;
    }

    public static void testOneInputString(String input) throws AlgebricksException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        byte [] bytes = generateABinaryBytesByStringContent(input);
        ABinaryHexPrinter.INSTANCE.print(bytes, 0, bytes.length, ps);

        String pureHex = baos.toString();
        assertTrue(pureHex.startsWith("hex("));
        assertTrue(pureHex.endsWith("\")"));
        assertTrue(pureHex.substring(5,pureHex.length()-2).equalsIgnoreCase(input));
    }

    @Test
    public void testPrintEveryChar() throws Exception {
        testOneInputString("ABCDEF0123456789");
        testOneInputString("0A0B0C0D0E0F00010203040506070809");
        testOneInputString("A0B0C0D0E0F000102030405060708090");
    }

    @Test
    public void testSpecialString() throws Exception{
        testOneInputString("");
        char [] chars = new char[(ByteArrayPointable.MAX_LENGTH ) * 2];
        Arrays.fill(chars, 'F');
        testOneInputString(new String(chars));
    }
}