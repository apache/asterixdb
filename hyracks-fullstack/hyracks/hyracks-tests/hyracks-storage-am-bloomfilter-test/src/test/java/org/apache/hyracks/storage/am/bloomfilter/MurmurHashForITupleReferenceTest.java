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

package org.apache.hyracks.storage.am.bloomfilter;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;
import org.apache.hyracks.storage.am.bloomfilter.util.AbstractBloomFilterTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class MurmurHashForITupleReferenceTest extends AbstractBloomFilterTest {
    private final static int NUM_LONG_VARS_FOR_128_BIT_HASH = 2;
    private final static int DUMMY_FIELD = 0;
    private final Random rnd = new Random(50);

    @Before
    public void setUp() throws HyracksDataException {
        super.setUp();
    }

    @Test
    public void murmurhashONEIntegerFieldTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING MURMUR HASH ONE INTEGER FIELD");
        }

        int fieldCount = 2;
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(tupleBuilder, tuple, rnd.nextInt());
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

        int keyFields[] = { 0 };
        int length = getTupleSize(tuple, keyFields);

        long actuals[] = new long[NUM_LONG_VARS_FOR_128_BIT_HASH];
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, 0L, actuals);

        ByteBuffer buffer;
        byte[] array = new byte[length];
        fillArrayWithData(array, keyFields, tuple, length);
        buffer = ByteBuffer.wrap(array);

        long[] expecteds = hash3_x64_128(buffer, 0, length, 0L);
        Assert.assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void murmurhashTwoIntegerFieldsTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING MURMUR HASH TWO INTEGER FIELDS");
        }

        int fieldCount = 2;
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(tupleBuilder, tuple, rnd.nextInt(), rnd.nextInt());
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

        int keyFields[] = { 0, 1 };
        int length = getTupleSize(tuple, keyFields);

        long actuals[] = new long[NUM_LONG_VARS_FOR_128_BIT_HASH];
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, 0L, actuals);

        ByteBuffer buffer;
        byte[] array = new byte[length];
        fillArrayWithData(array, keyFields, tuple, length);
        buffer = ByteBuffer.wrap(array);

        long[] expecteds = hash3_x64_128(buffer, 0, length, 0L);
        Assert.assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void murmurhashOneStringFieldTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING MURMUR HASH ONE STRING FIELD");
        }

        int fieldCount = 2;
        ISerializerDeserializer[] fieldSerdes = { new UTF8StringSerializerDeserializer() };
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        String s = randomString(100, rnd);
        TupleUtils.createTuple(tupleBuilder, tuple, fieldSerdes, s);

        int keyFields[] = { 0 };
        int length = getTupleSize(tuple, keyFields);

        long actuals[] = new long[NUM_LONG_VARS_FOR_128_BIT_HASH];
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, 0L, actuals);

        byte[] array = new byte[length];
        ByteBuffer buffer;
        fillArrayWithData(array, keyFields, tuple, length);
        buffer = ByteBuffer.wrap(array);

        long[] expecteds = hash3_x64_128(buffer, 0, length, 0L);
        Assert.assertArrayEquals(expecteds, actuals);
    }

    @Test
    public void murmurhashThreeStringFieldsTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING MURMUR HASH THREE STRING FIELDS");
        }

        int fieldCount = 3;
        ISerializerDeserializer[] fieldSerdes = { new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        String s1 = randomString(40, rnd);
        String s2 = randomString(60, rnd);
        String s3 = randomString(20, rnd);
        TupleUtils.createTuple(tupleBuilder, tuple, fieldSerdes, s1, s2, s3);

        int keyFields[] = { 2, 0, 1 };
        int length = getTupleSize(tuple, keyFields);

        long actuals[] = new long[NUM_LONG_VARS_FOR_128_BIT_HASH];
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, 0L, actuals);

        byte[] array = new byte[length];
        ByteBuffer buffer;
        fillArrayWithData(array, keyFields, tuple, length);
        buffer = ByteBuffer.wrap(array);

        long[] expecteds = hash3_x64_128(buffer, 0, length, 0L);
        Assert.assertArrayEquals(expecteds, actuals);
    }

    private void fillArrayWithData(byte[] array, int[] keyFields, ITupleReference tuple, int length) {
        int currentFieldIndex = 0;
        int bytePos = 0;
        for (int i = 0; i < length; ++i) {
            array[i] = tuple.getFieldData(DUMMY_FIELD)[tuple.getFieldStart(keyFields[currentFieldIndex]) + bytePos];
            ++bytePos;
            if (tuple.getFieldLength(keyFields[currentFieldIndex]) == bytePos) {
                ++currentFieldIndex;
                bytePos = 0;
            }
        }
    }

    private int getTupleSize(ITupleReference tuple, int[] keyFields) {
        int length = 0;
        for (int i = 0; i < keyFields.length; ++i) {
            length += tuple.getFieldLength(keyFields[i]);
        }
        return length;
    }

    /**
     * The hash3_x64_128 and getblock functions are borrowed from cassandra source code for testing purpose
     **/
    protected static long getblock(ByteBuffer key, int offset, int index) {
        int i_8 = index << 3;
        int blockOffset = offset + i_8;
        return ((long) key.get(blockOffset + 0) & 0xff) + (((long) key.get(blockOffset + 1) & 0xff) << 8)
                + (((long) key.get(blockOffset + 2) & 0xff) << 16) + (((long) key.get(blockOffset + 3) & 0xff) << 24)
                + (((long) key.get(blockOffset + 4) & 0xff) << 32) + (((long) key.get(blockOffset + 5) & 0xff) << 40)
                + (((long) key.get(blockOffset + 6) & 0xff) << 48) + (((long) key.get(blockOffset + 7) & 0xff) << 56);
    }

    public static long[] hash3_x64_128(ByteBuffer key, int offset, int length, long seed) {
        final int nblocks = length >> 4; // Process as 128-bit blocks.

        long h1 = seed;
        long h2 = seed;

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        for (int i = 0; i < nblocks; i++) {
            long k1 = getblock(key, offset, i * 2 + 0);
            long k2 = getblock(key, offset, i * 2 + 1);

            k1 *= c1;
            k1 = MurmurHash128Bit.rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = MurmurHash128Bit.rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = MurmurHash128Bit.rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = MurmurHash128Bit.rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        //----------
        // tail

        // Advance offset to the unprocessed tail of the data.
        offset += nblocks * 16;

        long k1 = 0;
        long k2 = 0;

        switch (length & 15) {
            case 15:
                k2 ^= ((long) key.get(offset + 14)) << 48;
            case 14:
                k2 ^= ((long) key.get(offset + 13)) << 40;
            case 13:
                k2 ^= ((long) key.get(offset + 12)) << 32;
            case 12:
                k2 ^= ((long) key.get(offset + 11)) << 24;
            case 11:
                k2 ^= ((long) key.get(offset + 10)) << 16;
            case 10:
                k2 ^= ((long) key.get(offset + 9)) << 8;
            case 9:
                k2 ^= ((long) key.get(offset + 8)) << 0;
                k2 *= c2;
                k2 = MurmurHash128Bit.rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;

            case 8:
                k1 ^= ((long) key.get(offset + 7)) << 56;
            case 7:
                k1 ^= ((long) key.get(offset + 6)) << 48;
            case 6:
                k1 ^= ((long) key.get(offset + 5)) << 40;
            case 5:
                k1 ^= ((long) key.get(offset + 4)) << 32;
            case 4:
                k1 ^= ((long) key.get(offset + 3)) << 24;
            case 3:
                k1 ^= ((long) key.get(offset + 2)) << 16;
            case 2:
                k1 ^= ((long) key.get(offset + 1)) << 8;
            case 1:
                k1 ^= ((long) key.get(offset));
                k1 *= c1;
                k1 = MurmurHash128Bit.rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
        }

        //----------
        // finalization

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = MurmurHash128Bit.fmix(h1);
        h2 = MurmurHash128Bit.fmix(h2);

        h1 += h2;
        h2 += h1;

        return (new long[] { h1, h2 });
    }
}
