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

package org.apache.hyracks.dataflow.common.data.normalizers;

import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializerTest;
import org.junit.Test;

import java.util.Random;

import static junit.framework.Assert.assertTrue;

public class ByteArrayNormalizedKeyComputerFactoryTest {

    Random random = new Random();

    INormalizedKeyComputer computer = ByteArrayNormalizedKeyComputerFactory.INSTANCE.createNormalizedKeyComputer();

    public static ByteArrayPointable generateRandomByteArrayPointable(int maxSize, Random random) {
        byte[] bytes = ByteArraySerializerDeserializerTest
                .generateRandomBytes(maxSize, random);
        ByteArrayPointable pointable = new ByteArrayPointable();
        pointable.set(bytes, 0, bytes.length);
        return pointable;
    }

    @Test
    public void testRandomNormalizedKey() {
        for (int i = 0; i < 10; ++i) {
            ByteArrayPointable pointable1 = generateRandomByteArrayPointable(ByteArrayPointable.MAX_LENGTH + 1,
                    random);

            ByteArrayPointable pointable2 = generateRandomByteArrayPointable(ByteArrayPointable.MAX_LENGTH + 1,
                    random);
            assertNormalizeValue(pointable1, pointable2, computer);
        }
    }

    public static ByteArrayPointable generateRandomByteArrayPointableWithFixLength(int length, Random random) {
        byte[] bytes = new byte[length + ByteArrayPointable.SIZE_OF_LENGTH];
        random.nextBytes(bytes);
        ByteArrayPointable pointable = new ByteArrayPointable();
        ByteArrayPointable.putLength(length, bytes, 0);
        pointable.set(bytes, 0, bytes.length);
        return pointable;
    }

    public static void assertNormalizeValue(ByteArrayPointable pointable1, ByteArrayPointable pointable2,
            INormalizedKeyComputer computer) {
        int n1 = computer.normalize(pointable1.getByteArray(), pointable1.getStartOffset(), pointable1.getLength());
        int n2 = computer.normalize(pointable2.getByteArray(), pointable2.getStartOffset(), pointable2.getLength());
        if (n1 < n2) {
            assertTrue(pointable1.compareTo(pointable2) < 0);
        } else if (n1 > n2) {
            assertTrue(pointable1.compareTo(pointable2) > 0);
        }
    }

    @Test
    public void testCornerCase() {
        for (int len = 0; len < 4; ++len) {
            ByteArrayPointable pointable1 = generateRandomByteArrayPointableWithFixLength(len, random);
            ByteArrayPointable pointable2 = generateRandomByteArrayPointableWithFixLength(len, random);
            assertNormalizeValue(pointable1, pointable2, computer);
        }

        byte[] bytes1 = new byte[] { 0, 4, 0, 25, 34, 42 };
        byte[] bytes2 = new byte[] { 0, 4, (byte) 130, 25, 34, 42 };

        int n1 = computer.normalize(bytes1, 0, bytes1.length);
        int n2 = computer.normalize(bytes2, 0, bytes2.length);
        assertTrue(n1 < n2);

    }
}