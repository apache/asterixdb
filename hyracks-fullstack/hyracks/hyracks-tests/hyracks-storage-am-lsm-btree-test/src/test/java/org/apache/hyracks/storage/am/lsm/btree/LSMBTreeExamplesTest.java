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

package org.apache.hyracks.storage.am.lsm.btree;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.OrderedIndexExamplesTest;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.btree.utils.LSMBTreeUtil;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.util.trace.ITracer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LSMBTreeExamplesTest extends OrderedIndexExamplesTest {
    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    @Override
    protected ITreeIndex createTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields) throws HyracksDataException {
        return createTreeIndex(harness, typeTraits, cmpFactories, bloomFilterKeyFields, filterTypeTraits,
                filterCmpFactories, btreeFields, filterFields);
    }

    public static LSMBTree createTreeIndex(LSMBTreeTestHarness harness, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] cmpFactories, int[] bloomFilterKeyFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] btreeFields, int[] filterFields)
            throws HyracksDataException {
        return LSMBTreeUtil.createLSMTree(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), typeTraits, cmpFactories,
                bloomFilterKeyFields, harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), true, filterTypeTraits, filterCmpFactories, btreeFields,
                filterFields, true, harness.getMetadataPageManagerFactory(), false, ITracer.NONE,
                NoOpCompressorDecompressorFactory.INSTANCE, bloomFilterKeyFields != null);
    }

    @Before
    public void setUp() throws HyracksDataException {
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
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Testing LSMBTree component filters.");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;

        ITypeTraits[] filterTypeTraits = { IntegerPointable.TYPE_TRAITS };
        IBinaryComparatorFactory[] filterCmpFactories = { IntegerBinaryComparatorFactory.INSTANCE };
        int[] filterFields = { 1 };
        int[] btreeFields = { 1 };
        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, filterTypeTraits,
                filterCmpFactories, btreeFields, filterFields);
        treeIndex.create();
        treeIndex.activate();

        long start = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        IIndexAccessor indexAccessor = treeIndex.createAccessor(actx);
        int numInserts = 10000;
        for (int i = 0; i < numInserts; i++) {
            int f0 = rnd.nextInt() % numInserts;
            int f1 = i;
            TupleUtils.createIntegerTuple(tb, tuple, f0, f1);
            if (LOGGER.isInfoEnabled()) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + i + " : " + f0 + " " + f1);
                }
            }
            indexAccessor.insert(tuple);
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
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
