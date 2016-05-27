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

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import org.apache.asterix.dataflow.data.nontagged.printers.adm.ABinaryHexPrinterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.junit.Test;

public class ABinaryPrinterTest {

    public static byte[] generateABinaryBytesByStringContent(String content) {
        byte[] bytes = DatatypeConverter.parseHexBinary(content);
        ByteArrayPointable ptr = ByteArrayPointable.generatePointableFromPureBytes(bytes);
        byte[] ret = new byte[ptr.getLength() + 1];
        System.arraycopy(ptr.getByteArray(), ptr.getStartOffset(), ret, 1, ptr.getLength());
        return ret;
    }

    public static void testOneInputString(String input) throws HyracksDataException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);

        byte[] bytes = generateABinaryBytesByStringContent(input);
        ABinaryHexPrinterFactory.PRINTER.print(bytes, 0, bytes.length, ps);

        String pureHex = baos.toString();
        assertTrue(pureHex.startsWith("hex("));
        assertTrue(pureHex.endsWith("\")"));
        assertTrue(pureHex.substring(5, pureHex.length() - 2).equalsIgnoreCase(input));
    }

    @Test
    public void testPrintEveryChar() throws Exception {
        testOneInputString("ABCDEF0123456789");
        testOneInputString("0A0B0C0D0E0F00010203040506070809");
        testOneInputString("A0B0C0D0E0F000102030405060708090");
    }

    @Test
    public void testLongString() throws Exception {
        testOneInputString("");
        int aVeryLongLength = 65540;
        char[] aVeryLongHexString = new char[aVeryLongLength * 2];
        Arrays.fill(aVeryLongHexString, 'F');
        testOneInputString(new String(aVeryLongHexString));
    }
}
