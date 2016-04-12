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

package org.apache.hyracks.dataflow.common.data.marshalling;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.util.string.UTF8StringSample;
import org.junit.Test;

public class ByteArraySerializerDeserializerTest {

    ByteArrayPointable bytePtr = new ByteArrayPointable();
    ByteArraySerializerDeserializer serder = ByteArraySerializerDeserializer.INSTANCE;

    @Test
    public void testSerializeDeserializeRandomBytes() throws Exception {
        testOneByteArray(UTF8StringSample.EMPTY_STRING.getBytes());
        testOneByteArray(UTF8StringSample.STRING_UTF8_MIX.getBytes());
        testOneByteArray(UTF8StringSample.STRING_LEN_128.getBytes());
        testOneByteArray(UTF8StringSample.STRING_LEN_MEDIUM.getBytes());
        testOneByteArray(UTF8StringSample.STRING_LEN_LARGE.getBytes());
    }

    void testOneByteArray(byte[] testBytes) throws HyracksDataException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        serder.serialize(testBytes, new DataOutputStream(outputStream));

        bytePtr.set(outputStream.toByteArray(), 0, outputStream.size());
        assertTrue(Arrays.equals(testBytes, ByteArrayPointable.copyContent(bytePtr)));

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        assertTrue(Arrays.equals(testBytes, serder.deserialize(new DataInputStream(inputStream))));

    }

}
