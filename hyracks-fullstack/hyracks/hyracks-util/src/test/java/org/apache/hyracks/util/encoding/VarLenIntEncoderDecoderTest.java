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

package org.apache.hyracks.util.encoding;

import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.BOUND_FIVE_BYTE;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.BOUND_FOUR_BYTE;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.BOUND_ONE_BYTE;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.BOUND_THREE_BYTE;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.BOUND_TWO_BYTE;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.VarLenIntDecoder;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.createDecoder;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.decode;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.encode;
import static org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.getBytesRequired;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.Test;

public class VarLenIntEncoderDecoderTest {

    int[] bounds = new int[] { 0, BOUND_ONE_BYTE, BOUND_TWO_BYTE, BOUND_THREE_BYTE, BOUND_FOUR_BYTE, BOUND_FIVE_BYTE };

    @Test
    public void testGetBytesRequired() throws Exception {
        for (int bound = 0; bound < bounds.length - 1; bound++) {
            assertEquals(bound + 1, getBytesRequired(bounds[bound]));
            assertEquals(bound + 1, getBytesRequired(bounds[bound + 1] - 1));
        }
    }

    @Test
    public void testEncodeDecode() throws Exception {
        byte[] bytes = new byte[10];
        int startPos = 3;
        for (int i = 1; i < bounds.length - 1; i++) {
            testEncodeDecode(i, bounds[i] - 1, bytes, startPos);
            testEncodeDecode(i + 1, bounds[i], bytes, startPos);
            testEncodeDecode(i + 1, bounds[i] + 1, bytes, startPos);
        }
        // Integer.Max
        testEncodeDecode(5, BOUND_FIVE_BYTE, bytes, startPos);
    }

    @Test
    public void testCreateDecoder() throws Exception {
        VarLenIntDecoder decoder = createDecoder();
        byte[] bytes = new byte[100];
        int pos = 1;
        for (int b : bounds) {
            pos += encode(b, bytes, pos);
        }
        decoder.reset(bytes, 1);
        for (int b : bounds) {
            assertEquals(b, decoder.decode());
        }
    }

    protected void testEncodeDecode(int expectedBytes, int value, byte[] bytes, int startPos) throws IOException {
        assertEquals(expectedBytes, encode(value, bytes, startPos));
        assertEquals(value, decode(bytes, startPos));

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes, startPos, bytes.length - startPos);
        DataInputStream dis = new DataInputStream(bis);
        assertEquals(value, decode(dis));
    }
}
