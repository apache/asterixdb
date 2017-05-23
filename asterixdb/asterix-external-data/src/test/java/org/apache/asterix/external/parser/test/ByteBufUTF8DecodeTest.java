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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.record.converter.DCPMessageToRecordConverter;
import org.apache.asterix.external.input.record.reader.stream.SemiStructuredRecordReader;
import org.apache.asterix.external.input.stream.LocalFSInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FileSystemWatcher;
import org.junit.Assert;
import org.junit.Test;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.UnpooledByteBufAllocator;

public class ByteBufUTF8DecodeTest {

    private final int BUFFER_SIZE = 8; // Small buffer size to ensure multiple loop execution in the decode call
    private final int KB32 = 32768;
    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private final ByteBuffer bytes = ByteBuffer.allocate(BUFFER_SIZE);
    private final CharBuffer chars = CharBuffer.allocate(BUFFER_SIZE);
    private final CharArrayRecord value = new CharArrayRecord();
    private final ByteBuf nettyBuffer = UnpooledByteBufAllocator.DEFAULT.heapBuffer(KB32, Integer.MAX_VALUE);

    @Test
    public void eatGlass() {
        try {
            String fileName = getClass().getResource("/ICanEatGlass.txt").toURI().getPath();
            try (BufferedReader br = new BufferedReader(new FileReader(new File(fileName)))) {
                for (String line; (line = br.readLine()) != null;) {
                    process(line);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testDecodingJsonRecords() throws URISyntaxException, IOException {
        String jsonFileName = "/record.json";
        List<Path> paths = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(ExternalDataConstants.KEY_RECORD_START, "{");
        config.put(ExternalDataConstants.KEY_RECORD_END, "}");
        paths.add(Paths.get(getClass().getResource(jsonFileName).toURI()));
        FileSystemWatcher watcher = new FileSystemWatcher(paths, null, false);
        LocalFSInputStream in = new LocalFSInputStream(watcher);
        try (SemiStructuredRecordReader recordReader = new SemiStructuredRecordReader()) {
            recordReader.configure(in, config);
            while (recordReader.hasNext()) {
                try {
                    IRawRecord<char[]> record = recordReader.next();
                    process(record.toString());
                } catch (Throwable th) {
                    th.printStackTrace();
                    Assert.fail(th.getMessage());
                }
            }
        }
    }

    private void process(String input) throws IOException {
        value.reset();
        nettyBuffer.clear();
        nettyBuffer.writeBytes(input.getBytes(StandardCharsets.UTF_8));
        DCPMessageToRecordConverter.set(nettyBuffer, decoder, bytes, chars, value);
        Assert.assertEquals(input, value.toString());
    }
}
