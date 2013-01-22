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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.bloomfilter.util.AbstractBloomFilterTest;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public class BloomFilterTest extends AbstractBloomFilterTest {
    private final int fieldCount = 2;
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
        int numHashes = 10;

        BloomFilter bf = new BloomFilter(bufferCache, harness.getFileMapProvider(), harness.getFileReference(),
                numElements, keyFields, numHashes);

        bf.create();
        bf.activate();

        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        // generate keys
        int numKeys = 50;
        int maxKey = 1000;
        TreeSet<Integer> uniqueKeys = new TreeSet<Integer>();
        ArrayList<Integer> keys = new ArrayList<Integer>();
        while (uniqueKeys.size() < numKeys) {
            int key = rnd.nextInt() % maxKey;
            uniqueKeys.add(key);
        }
        for (Integer i : uniqueKeys) {
            keys.add(i);
        }

        for (int i = 0; i < keys.size(); ++i) {

            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

            bf.add(tuple);

            Assert.assertTrue(bf.contains(tuple));
        }

        bf.deactivate();
        bf.destroy();
    }
}
