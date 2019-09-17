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

package org.apache.hyracks.storage.am.btree;

import static org.junit.Assert.fail;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public abstract class OrderedIndexExamplesTest {
    protected static final Logger LOGGER = LogManager.getLogger();
    protected final Random rnd = new Random(50);

    protected abstract ITreeIndex createTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            int[] bloomFilterKeyFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] btreeFields, int[] filterFields) throws HyracksDataException;

    /**
     * Fixed-Length Key,Value Example. Create a tree index with one fixed-length
     * key field and one fixed-length value field. Fill index with random values
     * using insertions (not bulk load). Perform scans and range search.
     */
    @Test
    public void fixedLengthKeyValueExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Fixed-Length Key,Value Example.");
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

        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, null, null, null, null);
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
            int f1 = 5;
            TupleUtils.createIntegerTuple(tb, tuple, f0, f1);
            if (LOGGER.isInfoEnabled()) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + i + " : " + f0 + " " + f1);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
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

        rangeSearch(cmpFactories, indexAccessor, fieldSerdes, lowKey, highKey, null, null);

        treeIndex.validate();
        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * This test the btree page split. Originally this test didn't pass since
     * the btree was spliting by cardinality and not size. Thus, we might end up
     * with a situation where there is not enough space to insert the new tuple
     * after the split which will throw an error and the split won't be
     * propagated to upper level; thus, the tree is corrupted. Now, it split
     * page by size. The correct behavior on abnormally large keys/values.
     */
    @Test
    public void pageSplitTestExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("BTree page split test.");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        typeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;

        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, null, null, null, null);
        treeIndex.create();
        treeIndex.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        IIndexAccessor indexAccessor = treeIndex.createAccessor(actx);

        String key = "111";
        String data = "XXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, key, data);
        indexAccessor.insert(tuple);

        key = "222";
        data = "XXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, key, data);
        indexAccessor.insert(tuple);

        key = "333";
        data = "XXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, key, data);
        indexAccessor.insert(tuple);

        key = "444";
        data = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, key, data);
        indexAccessor.insert(tuple);

        key = "555";
        data = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, key, data);
        indexAccessor.insert(tuple);

        key = "666";
        data = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, key, data);
        indexAccessor.insert(tuple);

        treeIndex.validate();
        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Composite Key Example (Non-Unique Index). Create a tree index with two
     * fixed-length key fields and one fixed-length value field. Fill index with
     * random values using insertions (not bulk load) Perform scans and range
     * search.
     */
    @Test
    public void twoFixedLengthKeysOneFixedLengthValueExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Composite Key Test");
        }

        // Declare fields.
        int fieldCount = 3;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        cmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;
        bloomFilterKeyFields[1] = 1;

        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, null, null, null, null);
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
        for (int i = 0; i < 10000; i++) {
            int f0 = rnd.nextInt() % 2000;
            int f1 = rnd.nextInt() % 1000;
            int f2 = 5;
            TupleUtils.createIntegerTuple(tb, tuple, f0, f1, f2);
            if (LOGGER.isInfoEnabled()) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + i + " : " + f0 + " " + f1 + " " + f2);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        orderedScan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build low key.
        ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference lowKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(lowKeyTb, lowKey, -3);

        // Build high key.
        ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference highKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(highKeyTb, highKey, 3);

        // Prefix-Range search in [-3, 3]
        rangeSearch(cmpFactories, indexAccessor, fieldSerdes, lowKey, highKey, null, null);

        treeIndex.validate();
        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Variable-Length Example. Create a BTree with one variable-length key
     * field and one variable-length value field. Fill BTree with random values
     * using insertions (not bulk load) Perform ordered scans and range search.
     */
    @Test
    public void varLenKeyValueExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Variable-Length Key,Value Example");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        typeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;

        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, null, null, null, null);
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
        // Max string length to be generated.
        int maxLength = 10;
        int numInserts = 10000;
        for (int i = 0; i < 10000; i++) {
            String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            TupleUtils.createTuple(tb, tuple, fieldSerdes, f0, f1);
            if (LOGGER.isInfoEnabled()) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting[" + i + "] " + f0 + " " + f1);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        orderedScan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build low key.
        ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference lowKey = new ArrayTupleReference();
        TupleUtils.createTuple(lowKeyTb, lowKey, fieldSerdes, "cbf");

        // Build high key.
        ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference highKey = new ArrayTupleReference();
        TupleUtils.createTuple(highKeyTb, highKey, fieldSerdes, "cc7");

        rangeSearch(cmpFactories, indexAccessor, fieldSerdes, lowKey, highKey, null, null);

        treeIndex.validate();
        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Deletion Example. Create a BTree with one variable-length key field and
     * one variable-length value field. Fill B-tree with random values using
     * insertions, then delete entries one-by-one. Repeat procedure a few times
     * on same BTree.
     */
    @Test
    public void deleteExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Deletion Example");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        typeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;

        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, null, null, null, null);
        treeIndex.create();
        treeIndex.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        IIndexAccessor indexAccessor = treeIndex.createAccessor(actx);
        // Max string length to be generated.
        int runs = 3;
        for (int run = 0; run < runs; run++) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Deletion example run: " + (run + 1) + "/" + runs);
                LOGGER.info("Inserting into tree...");
            }
            int maxLength = 10;
            int ins = 10000;
            String[] f0s = new String[ins];
            String[] f1s = new String[ins];
            int insDone = 0;
            int[] insDoneCmp = new int[ins];
            for (int i = 0; i < ins; i++) {
                String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                TupleUtils.createTuple(tb, tuple, fieldSerdes, f0, f1);
                f0s[i] = f0;
                f1s[i] = f1;
                if (LOGGER.isInfoEnabled()) {
                    if (i % 1000 == 0) {
                        LOGGER.info("Inserting " + i);
                    }
                }
                try {
                    indexAccessor.insert(tuple);
                    insDone++;
                } catch (HyracksDataException e) {
                    if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                        throw e;
                    }
                }
                insDoneCmp[i] = insDone;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Deleting from tree...");
            }
            int delDone = 0;
            for (int i = 0; i < ins; i++) {
                TupleUtils.createTuple(tb, tuple, fieldSerdes, f0s[i], f1s[i]);
                if (LOGGER.isInfoEnabled()) {
                    if (i % 1000 == 0) {
                        LOGGER.info("Deleting " + i);
                    }
                }
                try {
                    indexAccessor.delete(tuple);
                    delDone++;
                } catch (HyracksDataException e) {
                    if (e.getErrorCode() != ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY) {
                        throw e;
                    }
                }
                if (insDoneCmp[i] != delDone) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("INCONSISTENT STATE, ERROR IN DELETION EXAMPLE.");
                        LOGGER.info("INSDONECMP: " + insDoneCmp[i] + " " + delDone);
                    }
                    break;
                }
            }
            if (insDone != delDone) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("ERROR! INSDONE: " + insDone + " DELDONE: " + delDone);
                }
                break;
            }
        }
        treeIndex.validate();
        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Update example. Create a BTree with one variable-length key field and one
     * variable-length value field. Fill B-tree with random values using
     * insertions, then update entries one-by-one. Repeat procedure a few times
     * on same BTree.
     */
    @Test
    public void updateExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Update example");
        }

        // Declare fields.
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        typeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };

        // Declare keys.
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;

        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, null, null, null, null);
        treeIndex.create();
        treeIndex.activate();

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Inserting into tree...");
        }
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        IIndexAccessor indexAccessor = treeIndex.createAccessor(actx);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        int maxLength = 10;
        int ins = 10000;
        String[] keys = new String[10000];
        for (int i = 0; i < ins; i++) {
            String f0 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
            TupleUtils.createTuple(tb, tuple, fieldSerdes, f0, f1);
            keys[i] = f0;
            if (LOGGER.isInfoEnabled()) {
                if (i % 1000 == 0) {
                    LOGGER.info("Inserting " + i);
                }
            }
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
        }
        // Print before doing any updates.
        orderedScan(indexAccessor, fieldSerdes);

        int runs = 3;
        for (int run = 0; run < runs; run++) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Update test run: " + (run + 1) + "/" + runs);
                LOGGER.info("Updating BTree");
            }
            for (int i = 0; i < ins; i++) {
                // Generate a new random value for f1.
                String f1 = randomString(Math.abs(rnd.nextInt()) % maxLength + 1, rnd);
                TupleUtils.createTuple(tb, tuple, fieldSerdes, keys[i], f1);
                if (LOGGER.isInfoEnabled()) {
                    if (i % 1000 == 0) {
                        LOGGER.info("Updating " + i);
                    }
                }
                indexAccessor.update(tuple);
            }
            // Do another scan after a round of updates.
            orderedScan(indexAccessor, fieldSerdes);
        }
        treeIndex.validate();
        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Bulk load example. Load a tree with 100,000 tuples. BTree has a composite
     * key to "simulate" non-unique index creation.
     */
    @Test
    public void bulkLoadExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Bulk load example");
        }
        // Declare fields.
        int fieldCount = 3;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        cmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;
        bloomFilterKeyFields[1] = 1;

        ITreeIndex treeIndex = createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, null, null, null, null);
        treeIndex.create();
        treeIndex.activate();

        // Load sorted records.
        int ins = 100000;
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Bulk loading " + ins + " tuples");
        }
        long start = System.currentTimeMillis();
        IIndexBulkLoader bulkLoader =
                treeIndex.createBulkLoader(0.7f, false, ins, true, NoOpPageWriteCallback.INSTANCE);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        for (int i = 0; i < ins; i++) {
            TupleUtils.createIntegerTuple(tb, tuple, i, i, 5);
            bulkLoader.add(tuple);
        }
        bulkLoader.end();
        long end = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(ins + " tuples loaded in " + (end - start) + "ms");
        }

        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        IIndexAccessor indexAccessor = treeIndex.createAccessor(actx);

        // Build low key.
        ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference lowKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(lowKeyTb, lowKey, 44444);

        // Build high key.
        ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(1);
        ArrayTupleReference highKey = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(highKeyTb, highKey, 44500);

        // Prefix-Range search in [44444, 44500]
        rangeSearch(cmpFactories, indexAccessor, fieldSerdes, lowKey, highKey, null, null);

        treeIndex.validate();
        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Bulk load failure example. Repeatedly loads a tree with 1,000 tuples, of
     * which one tuple at each possible position does not conform to the
     * expected order. We expect the bulk load to fail with an exception.
     */
    @Test
    public void bulkOrderVerificationExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Bulk load order verification example");
        }
        // Declare fields.
        int fieldCount = 2;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        Random rnd = new Random();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        // This is only used for the LSM-BTree.
        int[] bloomFilterKeyFields = new int[keyFieldCount];
        bloomFilterKeyFields[0] = 0;

        int ins = 1000;
        for (int i = 1; i < ins; i++) {

            ITreeIndex treeIndex =
                    createTreeIndex(typeTraits, cmpFactories, bloomFilterKeyFields, null, null, null, null);
            treeIndex.create();
            treeIndex.activate();

            // Load sorted records, and expect to fail at tuple i.
            IIndexBulkLoader bulkLoader =
                    treeIndex.createBulkLoader(0.7f, true, ins, true, NoOpPageWriteCallback.INSTANCE);
            for (int j = 0; j < ins; j++) {
                if (j > i) {
                    fail("Bulk load failure test unexpectedly succeeded past tuple: " + j);
                }
                int key = j;
                if (j == i) {
                    int swapElementCase = Math.abs(rnd.nextInt()) % 2;
                    if (swapElementCase == 0) {
                        // Element equal to previous element.
                        key--;
                    } else {
                        // Element smaller than previous element.
                        key -= Math.abs(Math.random() % (ins - 1)) + 1;
                    }
                }
                TupleUtils.createIntegerTuple(tb, tuple, key, 5);
                try {
                    bulkLoader.add(tuple);
                } catch (HyracksDataException e) {
                    if (e.getErrorCode() == ErrorCode.UNSORTED_LOAD_INPUT || e.getErrorCode() == ErrorCode.DUPLICATE_KEY
                            || e.getErrorCode() == ErrorCode.DUPLICATE_LOAD_INPUT) {
                        if (j != i) {
                            fail("Unexpected exception: " + e.getMessage());
                        }
                        // Success.
                        break;
                    } else {
                        throw e;
                    }
                }
            }
            treeIndex.deactivate();
            treeIndex.destroy();
        }
    }

    protected void orderedScan(IIndexAccessor indexAccessor, ISerializerDeserializer[] fieldSerdes) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Ordered Scan:");
        }
        IIndexCursor scanCursor = indexAccessor.createSearchCursor(false);
        try {
            RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);
            indexAccessor.search(scanCursor, nullPred);
            try {
                while (scanCursor.hasNext()) {
                    scanCursor.next();
                    ITupleReference frameTuple = scanCursor.getTuple();
                    String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(rec);
                    }
                }
            } finally {
                scanCursor.close();
            }
        } finally {
            scanCursor.destroy();
        }
    }

    protected void diskOrderScan(IIndexAccessor indexAccessor, ISerializerDeserializer[] fieldSerdes) throws Exception {
        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Disk-Order Scan:");
            }
            ITreeIndexAccessor treeIndexAccessor = (ITreeIndexAccessor) indexAccessor;
            TreeIndexDiskOrderScanCursor diskOrderCursor =
                    (TreeIndexDiskOrderScanCursor) treeIndexAccessor.createDiskOrderScanCursor();
            try {
                treeIndexAccessor.diskOrderScan(diskOrderCursor);
                try {
                    while (diskOrderCursor.hasNext()) {
                        diskOrderCursor.next();
                        ITupleReference frameTuple = diskOrderCursor.getTuple();
                        String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                        LOGGER.info(rec);
                    }
                } finally {
                    diskOrderCursor.close();
                }
            } finally {
                diskOrderCursor.destroy();
            }
        } catch (UnsupportedOperationException e) {
            // Ignore exception because some indexes, e.g. the LSMBTree, don't
            // support disk-order scan.
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        } catch (ClassCastException e) {
            // Ignore exception because IIndexAccessor sometimes isn't
            // an ITreeIndexAccessor, e.g., for the LSMBTree.
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        }
    }

    protected void rangeSearch(IBinaryComparatorFactory[] cmpFactories, IIndexAccessor indexAccessor,
            ISerializerDeserializer[] fieldSerdes, ITupleReference lowKey, ITupleReference highKey,
            ITupleReference minFilterTuple, ITupleReference maxFilterTuple) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            String lowKeyString = TupleUtils.printTuple(lowKey, fieldSerdes);
            String highKeyString = TupleUtils.printTuple(highKey, fieldSerdes);
            LOGGER.info("Range-Search in: [ " + lowKeyString + ", " + highKeyString + "]");
        }
        IIndexCursor rangeCursor = indexAccessor.createSearchCursor(false);
        try {
            MultiComparator lowKeySearchCmp = BTreeUtils.getSearchMultiComparator(cmpFactories, lowKey);
            MultiComparator highKeySearchCmp = BTreeUtils.getSearchMultiComparator(cmpFactories, highKey);
            RangePredicate rangePred;
            if (minFilterTuple != null && maxFilterTuple != null) {
                rangePred = new RangePredicate(lowKey, highKey, true, true, lowKeySearchCmp, highKeySearchCmp,
                        minFilterTuple, maxFilterTuple);
            } else {
                rangePred = new RangePredicate(lowKey, highKey, true, true, lowKeySearchCmp, highKeySearchCmp);
            }
            indexAccessor.search(rangeCursor, rangePred);
            try {
                while (rangeCursor.hasNext()) {
                    rangeCursor.next();
                    ITupleReference frameTuple = rangeCursor.getTuple();
                    String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(rec);
                    }
                }
            } finally {
                rangeCursor.close();
            }
        } finally {
            rangeCursor.destroy();
        }
    }

    public static String randomString(int length, Random random) {
        String s = Long.toHexString(Double.doubleToLongBits(random.nextDouble()));
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < s.length() && i < length; i++) {
            strBuilder.append(s.charAt(Math.abs(random.nextInt()) % s.length()));
        }
        return strBuilder.toString();
    }
}
