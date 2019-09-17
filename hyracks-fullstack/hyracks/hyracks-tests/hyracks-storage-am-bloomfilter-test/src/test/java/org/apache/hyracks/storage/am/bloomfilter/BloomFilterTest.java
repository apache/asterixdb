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

import java.util.ArrayList;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomCalculations;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.bloomfilter.util.AbstractBloomFilterTest;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class BloomFilterTest extends AbstractBloomFilterTest {
    private final Random rnd = new Random(50);

    @Override
    @Before
    public void setUp() throws HyracksDataException {
        super.setUp();
    }

    @Test
    public void singleFieldTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING BLOOM FILTER");
        }

        IBufferCache bufferCache = harness.getBufferCache();

        int numElements = 100;
        int[] keyFields = { 0 };

        BloomFilter bf = new BloomFilter(bufferCache, harness.getFileReference(), keyFields);

        double acceptanleFalsePositiveRate = 0.1;
        int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomFilterSpecification bloomFilterSpec =
                BloomCalculations.computeBloomSpec(maxBucketsPerElement, acceptanleFalsePositiveRate);

        bf.create();
        bf.activate();
        IIndexBulkLoader builder = bf.createBuilder(numElements, bloomFilterSpec.getNumHashes(),
                bloomFilterSpec.getNumBucketsPerElements(), NoOpPageWriteCallback.INSTANCE);

        int fieldCount = 2;
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        // generate keys
        int maxKey = 1000;
        TreeSet<Integer> uniqueKeys = new TreeSet<>();
        ArrayList<Integer> keys = new ArrayList<>();
        while (uniqueKeys.size() < numElements) {
            int key = rnd.nextInt() % maxKey;
            uniqueKeys.add(key);
        }
        for (Integer i : uniqueKeys) {
            keys.add(i);
        }

        // Insert tuples in the bloom filter
        for (int i = 0; i < keys.size(); ++i) {
            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            builder.add(tuple);
        }
        builder.end();

        bf.pinAllPages();
        // Check all the inserted tuples can be found.

        long[] hashes = BloomFilter.createHashArray();
        for (int i = 0; i < keys.size(); ++i) {
            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            Assert.assertTrue(bf.contains(tuple, hashes));
        }
        bf.unpinAllPages();
        bf.deactivate();
        bf.destroy();
    }

    @Test
    public void multiFieldTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING BLOOM FILTER");
        }

        IBufferCache bufferCache = harness.getBufferCache();

        int numElements = 10000;
        int[] keyFields = { 2, 4, 1 };

        BloomFilter bf = new BloomFilter(bufferCache, harness.getFileReference(), keyFields);

        double acceptanleFalsePositiveRate = 0.1;
        int maxBucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomFilterSpecification bloomFilterSpec =
                BloomCalculations.computeBloomSpec(maxBucketsPerElement, acceptanleFalsePositiveRate);

        bf.create();
        bf.activate();
        IIndexBulkLoader builder = bf.createBuilder(numElements, bloomFilterSpec.getNumHashes(),
                bloomFilterSpec.getNumBucketsPerElements(), NoOpPageWriteCallback.INSTANCE);

        int fieldCount = 5;
        ISerializerDeserializer[] fieldSerdes = { new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        int maxLength = 20;
        ArrayList<String> s1 = new ArrayList<>();
        ArrayList<String> s2 = new ArrayList<>();
        ArrayList<String> s3 = new ArrayList<>();
        ArrayList<String> s4 = new ArrayList<>();
        for (int i = 0; i < numElements; ++i) {
            s1.add(randomString(rnd.nextInt() % maxLength, rnd));
            s2.add(randomString(rnd.nextInt() % maxLength, rnd));
            s3.add(randomString(rnd.nextInt() % maxLength, rnd));
            s4.add(randomString(rnd.nextInt() % maxLength, rnd));
        }

        for (int i = 0; i < numElements; ++i) {
            TupleUtils.createTuple(tupleBuilder, tuple, fieldSerdes, s1.get(i), s2.get(i), i, s3.get(i), s4.get(i));
            builder.add(tuple);
        }
        builder.end();

        bf.pinAllPages();
        long[] hashes = BloomFilter.createHashArray();
        for (int i = 0; i < numElements; ++i) {
            TupleUtils.createTuple(tupleBuilder, tuple, fieldSerdes, s1.get(i), s2.get(i), i, s3.get(i), s4.get(i));
            Assert.assertTrue(bf.contains(tuple, hashes));
        }

        bf.unpinAllPages();
        bf.deactivate();
        bf.destroy();
    }
}
