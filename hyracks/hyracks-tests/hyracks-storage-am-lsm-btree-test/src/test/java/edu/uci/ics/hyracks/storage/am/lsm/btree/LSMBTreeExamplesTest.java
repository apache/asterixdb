/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.btree;

import java.util.logging.Level;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexExamplesTest;
import edu.uci.ics.hyracks.storage.am.common.TestOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;

public class LSMBTreeExamplesTest extends OrderedIndexExamplesTest {
    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    @Override
    protected ITreeIndex createTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields) throws TreeIndexException {
        return LSMBTreeUtils.createLSMTree(harness.getVirtualBufferCaches(), harness.getFileReference(),
                harness.getDiskBufferCache(), harness.getDiskFileMapProvider(), typeTraits, cmpFactories,
                bloomFilterKeyFields, harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallback(), true,
                filterTypeTraits, filterCmpFactories, btreeFields, filterFields);
    }

    @Before
    public void setUp() throws HyracksException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /**
     * Test the LSM component filters.
     */
    @Test
    public void additionalFilteringingExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Testing LSMBTree component filters.");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;

        ITypeTraits[] filterTypeTraits = { IntegerPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] filterCmpFactories = { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) };
        int[] filterFields = { 1 };
        int[] btreeFields = { 1 };
        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, filterTypeTraits,
                filterCmpFactories, btreeFields, filterFields);
        treeIndex.create();
        treeIndex.activate();

        long start = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = (IIndexAccessor) treeIndex.createAccessor(TestOperationCallback.INSTANCE,
                TestOperationCallback.INSTANCE);
        int numInserts = 10000;
        for (int i = 0; i < numInserts; i++) {
            int f0 = rnd.nextInt() % numInserts;
            int f1 = i;
            TupleUtils.createIntegerTuple(tb, tuple, f0, f1);
            if (LOGGER.isLoggable(Level.INFO)) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + i + " : " + f0 + " " + f1);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        orderedScan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build low key.
        ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(keyFieldCount);
        ArrayTupleReference lowKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(lowKeyTb, lowKey, -1000);

        // Build high key.
        ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(keyFieldCount);
        ArrayTupleReference highKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(highKeyTb, highKey, 1000);

        // Build min filter key.
        ArrayTupleBuilder minFilterTb = new ArrayTupleBuilder(filterFields.length);
        ArrayTupleReference minTuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(minFilterTb, minTuple, 400);

        // Build max filter key.
        ArrayTupleBuilder maxFilterTb = new ArrayTupleBuilder(filterFields.length);
        ArrayTupleReference maxTuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(maxFilterTb, maxTuple, 500);

        rangeSearch(cmpFactories, indexAccessor, fieldSerdes, lowKey, highKey, minTuple, maxTuple);

        treeIndex.validate();
        treeIndex.deactivate();
        treeIndex.destroy();
    }
}
