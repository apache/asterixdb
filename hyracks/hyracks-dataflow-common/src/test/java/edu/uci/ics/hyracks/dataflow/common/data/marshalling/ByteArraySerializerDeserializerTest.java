/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.dataflow.common.data.marshalling;

import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class ByteArraySerializerDeserializerTest {
    Random random = new Random();

    public static byte[] generateRandomBytes(int maxSize, Random random) {
        int size = random.nextInt(maxSize);
        byte[] bytes = new byte[size + ByteArrayPointable.SIZE_OF_LENGTH];
        random.nextBytes(bytes);
        ByteArrayPointable.putLength(size, bytes, 0);
        return bytes;
    }

    @Test
    public void testSerializeDeserializeRandomBytes() throws Exception {
        for (int i = 0; i < 10; ++i) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] randomBytes = generateRandomBytes(ByteArrayPointable.MAX_LENGTH + 1, random);

            ByteArraySerializerDeserializer.INSTANCE.serialize(randomBytes, new DataOutputStream(outputStream));
            byte[] result = outputStream.toByteArray();
            assertTrue(Arrays.equals(randomBytes, result));

            ByteArrayInputStream inputStream = new ByteArrayInputStream(result);
            assertTrue(Arrays.equals(randomBytes,
                    ByteArraySerializerDeserializer.INSTANCE.deserialize(new DataInputStream(inputStream))));
        }

    }

    @Test
    public void testPutGetLength() throws Exception {
        final int size = 5;
        byte[] newBytes = new byte[size];
        for (int i = 0; i < 10; ++i) {
            int length = random.nextInt(ByteArrayPointable.MAX_LENGTH +1);
            for (int j = 0; j < size - 1; ++j) {
                ByteArrayPointable.putLength(length, newBytes, j);
                int result = ByteArrayPointable.getLength(newBytes, j);
                assertTrue(result == length);
            }
        }
    }

}