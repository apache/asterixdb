/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.bloomfilter;

import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.bloomfilter.util.AbstractBloomFilterTest;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public class BloomFilterTest extends AbstractBloomFilterTest {
    private final Random rnd = new Random(50);

    @Before
    public void setUp() throws HyracksDataException {
        super.setUp();
    }

    @Test
    public void basicTest() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("TESTING BLOOM FILTER");
        }

        IBufferCache bufferCache = harness.getBufferCache();

        long numElements = 100L;
        int[] keyFields = { 0 };
        int numHashes = 5;

        BloomFilter bf = new BloomFilter(bufferCache, harness.getFileMapProvider(), harness.getFileReference(),
                keyFields, numElements, numHashes);

        bf.create();
        bf.activate();

        int fieldCount = 2;
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        // generate keys
        int maxKey = 1000;
        TreeSet<Integer> uniqueKeys = new TreeSet<Integer>();
        ArrayList<Integer> keys = new ArrayList<Integer>();
        while (uniqueKeys.size() < numElements) {
            int key = rnd.nextInt() % maxKey;
            uniqueKeys.add(key);
        }
        for (Integer i : uniqueKeys) {
            keys.add(i);
        }

        long[] hashes = new long[2];
        // Check against an empty bloom filter
        for (int i = 0; i < keys.size(); ++i) {
            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            Assert.assertFalse(bf.contains(tuple, hashes));
        }

        // Check all the inserted tuples can be found
        for (int i = 0; i < keys.size(); ++i) {
            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            bf.add(tuple, hashes);
            Assert.assertTrue(bf.contains(tuple, hashes));
        }

        // Deactivate the bllom filter
        bf.deactivate();

        // Activate the bloom filter and check the tuples again
        bf.activate();
        for (int i = 0; i < keys.size(); ++i) {
            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            bf.add(tuple, hashes);
            Assert.assertTrue(bf.contains(tuple, hashes));
        }

        bf.deactivate();
        bf.destroy();
    }

    @Test
    public void multiFieldTest() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("TESTING BLOOM FILTER");
        }

        IBufferCache bufferCache = harness.getBufferCache();

        long numElements = 10000L;
        int[] keyFields = { 2, 4, 1 };
        int numHashes = 10;

        BloomFilter bf = new BloomFilter(bufferCache, harness.getFileMapProvider(), harness.getFileReference(),
                keyFields, numElements, numHashes);

        bf.create();
        bf.activate();

        int fieldCount = 5;
        ISerializerDeserializer[] fieldSerdes = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        long[] hashes = new long[2];
        int maxLength = 20;
        for (int i = 0; i < numElements; ++i) {
            String s1 = randomString(rnd.nextInt() % maxLength, rnd);
            String s2 = randomString(rnd.nextInt() % maxLength, rnd);
            String s3 = randomString(rnd.nextInt() % maxLength, rnd);
            String s4 = randomString(rnd.nextInt() % maxLength, rnd);
            TupleUtils.createTuple(tupleBuilder, tuple, fieldSerdes, s1, s2, rnd.nextInt(), s3, s4);

            bf.add(tuple, hashes);
            Assert.assertTrue(bf.contains(tuple, hashes));
        }

        bf.deactivate();
        bf.destroy();
    }
}
