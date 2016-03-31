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

package org.apache.hyracks.storage.am.bloomfilter.impls;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * The idea of this class is borrowed from http://murmurhash.googlepages.com/ and cassandra source code.
 * We changed the hash function to operate on ITupleReference instead of a byte array.
 **/
public class MurmurHash128Bit {

    private final static int DUMMY_FIELD = 0;

    public static long rotl64(long v, int n) {
        return ((v << n) | (v >>> (64 - n)));
    }

    public static long fmix(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;

        return k;
    }

    public static void hash3_x64_128(ITupleReference tuple, int[] keyFields, long seed, long[] hashes) {
        int length = 0;
        for (int i = 0; i < keyFields.length; ++i) {
            length += tuple.getFieldLength(keyFields[i]);
        }
        final int nblocks = length >> 4; // Process as 128-bit blocks.

        long h1 = seed;
        long h2 = seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        int currentFieldIndex = 0;
        int bytePos = 0;
        for (int i = 0; i < nblocks; ++i) {

            long k1 = 0L;
            for (int j = 0; j < 8; ++j) {
                k1 += (((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos] & 0xff) << (j << 3));
                ++bytePos;
                if (tuple.getFieldLength(keyFields[currentFieldIndex]) == bytePos) {
                    ++currentFieldIndex;
                    bytePos = 0;
                }
            }
            long k2 = 0L;
            for (int j = 0; j < 8; ++j) {
                k2 += (((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos] & 0xff) << (j << 3));
                ++bytePos;
                if (tuple.getFieldLength(keyFields[currentFieldIndex]) == bytePos) {
                    ++currentFieldIndex;
                    bytePos = 0;
                }
            }

            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        //----------
        // tail

        long k1 = 0L;
        long k2 = 0L;

        currentFieldIndex = keyFields.length - 1;
        bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
        switch (length & 15) {
            case 15:
                k2 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 48;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 14:
                k2 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 40;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 13:
                k2 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 32;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 12:
                k2 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 24;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 11:
                k2 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 16;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 10:
                k2 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 8;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 9:
                k2 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]);
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
                k2 *= c2;
                k2 = rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;

            case 8:
                k1 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 56;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 7:
                k1 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 48;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 6:
                k1 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 40;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 5:
                k1 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 32;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 4:
                k1 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 24;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 3:
                k1 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 16;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 2:
                k1 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]) << 8;
                --bytePos;
                if (bytePos == -1) {
                    --currentFieldIndex;
                    bytePos = tuple.getFieldLength(keyFields[currentFieldIndex]) - 1;
                }
            case 1:
                k1 ^= ((long) tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex])
                        + bytePos]);
                k1 *= c1;
                k1 = rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
        };

        //----------
        // finalization

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;

        hashes[0] = h1;
        hashes[1] = h2;
    }

}
