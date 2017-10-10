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
package org.apache.asterix.external.parser.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ParserPerformanceTest {

    public void benchmark() throws Exception {
        int seed = 2;
        int levels = 26;
        int[] rounds = getRounds(seed, levels);
        for (int round : rounds) {
            round(round);
        }
    }

    private int[] getRounds(int seed, int levels) {
        int[] rounds = new int[levels];
        rounds[0] = seed;
        for (int i = 1; i < rounds.length; i++) {
            seed *= 2;
            rounds[i] = seed;
        }
        return rounds;
    }

    public void round(int round) throws Exception {
        System.err.println("Running a parse round of size = " + round + " records");
        ObjectMapper om = new ObjectMapper();
        byte[] json = "{\"name\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        long start = System.nanoTime();
        for (int i = 0; i < round; i++) {
            om.readTree(json);
        }
        long end = System.nanoTime();
        long jacksonTime = end - start;
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        CharBuffer chars = CharBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
        CharArrayRecord record = new CharArrayRecord();
        set(json, decoder, chars, record);
        ADMDataParser parser = new ADMDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, false);
        ArrayBackedValueStorage output = new ArrayBackedValueStorage();
        start = System.nanoTime();
        for (int i = 0; i < round; i++) {
            output.reset();
            parser.parse(record, output.getDataOutput());
        }
        end = System.nanoTime();
        long admParserTime = end - start;
        System.err.println("Jackson time = " + jacksonTime);
        System.err.println("AdmParser time = " + admParserTime);
    }

    private static void set(final byte[] content, final CharsetDecoder decoder, final CharBuffer chars,
            final CharArrayRecord record) throws IOException {
        chars.clear();
        ByteBuffer bytes = ByteBuffer.wrap(content);
        CoderResult result = decoder.decode(bytes, chars, true);
        if (result.isError() || (result.isUnderflow() && bytes.remaining() > 0)) {
            throw new IOException(result.toString());
        }
        chars.flip();
        record.append(chars);
        record.endRecord();
    }
}
