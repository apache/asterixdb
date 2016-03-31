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
package org.apache.hyracks.data.std.accessors;

/**
 * An implementation of the Murmur3 hash family. The code is implemented based
 * on the original <a
 * href=http://code.google.com/p/guava-libraries/source/browse
 * /guava/src/com/google/common/hash/Murmur3_32HashFunction.java>guava
 * implementation</a> from Google Guava library.
 */
public class MurmurHash3BinaryHash {

    private MurmurHash3BinaryHash() {
    }

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int C3 = 5;
    private static final int C4 = 0xe6546b64;
    private static final int C5 = 0x85ebca6b;
    private static final int C6 = 0xc2b2ae35;

    public static int hash(byte[] bytes, int offset, int length, int seed) {
        int h = seed;
        int p = offset;
        int remain = length;

        while (remain >= 4) {
            int k = (bytes[p] & 0xff) | ((bytes[p + 1] & 0xff) << 8) | ((bytes[p + 2] & 0xff) << 16)
                    | ((bytes[p + 3] & 0xff) << 24);
            k *= C1;
            k = Integer.rotateLeft(k, 15);
            k *= C2;
            h ^= k;
            h = Integer.rotateLeft(h, 13);
            h = h * C3 + C4;
            p += 4;
            remain -= 4;
        }
        if (remain > 0) {
            int k = 0;
            for (int i = 0; remain > 0; i += 8) {
                k ^= (bytes[p++] & 0xff) << i;
                remain--;
            }
            k *= C1;
            k = Integer.rotateLeft(k, 15);
            k *= C2;
            h ^= k;
        }
        h ^= length;
        h ^= (h >>> 16);
        h *= C5;
        h ^= (h >>> 13);
        h *= C6;
        h ^= (h >>> 16);
        return h;
    }
}
