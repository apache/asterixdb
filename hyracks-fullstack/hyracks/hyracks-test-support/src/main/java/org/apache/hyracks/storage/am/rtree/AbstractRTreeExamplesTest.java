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

package org.apache.hyracks.storage.am.rtree;

import java.util.Random;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public abstract class AbstractRTreeExamplesTest {

    public static enum RTreeType {
        LSMRTREE,
        LSMRTREE_WITH_ANTIMATTER,
        RTREE
    }

    protected static final Logger LOGGER = LogManager.getLogger();
    protected final Random rnd = new Random(50);
    protected RTreeType rTreeType;

    protected abstract ITreeIndex createTreeIndex(ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType, int[] rtreeFields,
            int[] btreeFields, ITypeTraits[] filterTypeTraits, IBinaryComparatorFactory[] filterCmpFactories,
            int[] filterFields) throws HyracksDataException;

    /**
     * Two Dimensions Example. Create an RTree index of two dimensions, where
     * they keys are of type integer, and the payload is two integer values.
     * Fill index with random values using insertions (not bulk load). Perform
     * scans and range search.
     */
    @Test
    public void twoDimensionsExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Fixed-Length Key,Value Example.");
        }

        // Declare fields.
        int fieldCount = 6;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = IntegerPointable.TYPE_TRAITS;
        typeTraits[4] = IntegerPointable.TYPE_TRAITS;
        typeTraits[5] = IntegerPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
            btreeKeyFieldCount = 2;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeFields = new int[btreeKeyFieldCount];
            for (int i = 0; i < btreeKeyFieldCount; i++) {
                btreeFields[i] = rtreeKeyFieldCount + i;
            }

        } else {
            btreeKeyFieldCount = 6;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[4] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[5] = IntegerBinaryComparatorFactory.INSTANCE;
        }

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(rtreeCmpFactories.length, IntegerPointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                RTreePolicyType.RTREE, null, btreeFields, null, null, null);
        treeIndex.create();
        treeIndex.activate();

        long start = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = treeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        int numInserts = 10000;
        for (int i = 0; i < numInserts; i++) {
            int p1x = rnd.nextInt();
            int p1y = rnd.nextInt();
            int p2x = rnd.nextInt();
            int p2y = rnd.nextInt();

            int pk1 = 5;
            int pk2 = 10;

            TupleUtils.createIntegerTuple(tb, tuple, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                    Math.max(p1y, p2y), pk1, pk2);
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (!e.matches(ErrorCode.DUPLICATE_KEY)) {
                    throw e;
                }
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        scan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(keyTb, key, -1000, -1000, 1000, 1000);

        rangeSearch(rtreeCmpFactories, indexAccessor, fieldSerdes, key, null, null);

        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * This test the rtree page split. Originally this test didn't pass since
     * the rtree assumes always that there will be enough space for the new
     * tuple after split. Now it passes since if there is not space in the
     * designated page, then we will just insert it in the other split page.
     */
    @Test
    public void rTreePageSplitTestExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("RTree page split test.");
        }

        // Declare fields.
        int fieldCount = 5;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = IntegerPointable.TYPE_TRAITS;
        typeTraits[4] = UTF8StringPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
            btreeKeyFieldCount = 1;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
            btreeFields = new int[btreeKeyFieldCount];
            for (int i = 0; i < btreeKeyFieldCount; i++) {
                btreeFields[i] = rtreeKeyFieldCount + i;
            }

        } else {
            btreeKeyFieldCount = 5;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[4] = UTF8StringBinaryComparatorFactory.INSTANCE;
        }

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(rtreeCmpFactories.length, IntegerPointable.FACTORY);

        //2
        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                RTreePolicyType.RTREE, null, btreeFields, null, null, null);

        treeIndex.create();
        treeIndex.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        IIndexAccessor indexAccessor = treeIndex.createAccessor(actx);

        int p1x = rnd.nextInt();
        int p1y = rnd.nextInt();
        int p2x = rnd.nextInt();
        int p2y = rnd.nextInt();
        String data = "";
        for (int i = 0; i < 210; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "XXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "XXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "XXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "";
        for (int i = 0; i < 210; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "";
        for (int i = 0; i < 210; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * This test the r*tree page split. Originally this test didn't pass since
     * the r*tree assumes always that there will be enough space for the new
     * tuple after split. Now it passes since if there is not space in the
     * designated page, then we will just insert it in the other split page.
     */
    @Test
    public void rStarTreePageSplitTestExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("R*Tree page split test.");
        }

        // Declare fields.
        int fieldCount = 5;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = IntegerPointable.TYPE_TRAITS;
        typeTraits[4] = UTF8StringPointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
            btreeKeyFieldCount = 1;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
            btreeFields = new int[btreeKeyFieldCount];
            for (int i = 0; i < btreeKeyFieldCount; i++) {
                btreeFields[i] = rtreeKeyFieldCount + i;
            }

        } else {
            btreeKeyFieldCount = 5;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[4] = UTF8StringBinaryComparatorFactory.INSTANCE;
        }

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(rtreeCmpFactories.length, IntegerPointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                RTreePolicyType.RSTARTREE, null, btreeFields, null, null, null);

        treeIndex.create();
        treeIndex.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        IIndexAccessor indexAccessor = treeIndex.createAccessor(actx);

        int p1x = rnd.nextInt();
        int p1y = rnd.nextInt();
        int p2x = rnd.nextInt();
        int p2y = rnd.nextInt();
        String data = "";
        for (int i = 0; i < 210; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "";
        for (int i = 0; i < 210; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "XXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "XXX";
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "";
        for (int i = 0; i < 210; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        p1x = rnd.nextInt();
        p1y = rnd.nextInt();
        p2x = rnd.nextInt();
        p2y = rnd.nextInt();
        data = "";
        for (int i = 0; i < 210; i++) {
            data += "X";
        }
        TupleUtils.createTuple(tb, tuple, fieldSerdes, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                Math.max(p1y, p2y), data);
        indexAccessor.insert(tuple);

        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Two Dimensions Example. Create an RTree index of three dimensions, where
     * they keys are of type double, and the payload is one double value. Fill
     * index with random values using insertions (not bulk load). Perform scans
     * and range search.
     */
    @Test
    public void threeDimensionsExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Fixed-Length Key,Value Example.");
        }

        // Declare fields.
        int fieldCount = 7;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = DoublePointable.TYPE_TRAITS;
        typeTraits[1] = DoublePointable.TYPE_TRAITS;
        typeTraits[2] = DoublePointable.TYPE_TRAITS;
        typeTraits[3] = DoublePointable.TYPE_TRAITS;
        typeTraits[4] = DoublePointable.TYPE_TRAITS;
        typeTraits[5] = DoublePointable.TYPE_TRAITS;
        typeTraits[6] = DoublePointable.TYPE_TRAITS;
        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 6;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = DoubleBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[1] = DoubleBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[2] = DoubleBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[3] = DoubleBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[4] = DoubleBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[5] = DoubleBinaryComparatorFactory.INSTANCE;

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
            btreeKeyFieldCount = 1;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = DoubleBinaryComparatorFactory.INSTANCE;
            btreeFields = new int[btreeKeyFieldCount];
            for (int i = 0; i < btreeKeyFieldCount; i++) {
                btreeFields[i] = rtreeKeyFieldCount + i;
            }

        } else {
            btreeKeyFieldCount = 7;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = DoubleBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[1] = DoubleBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[2] = DoubleBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[3] = DoubleBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[4] = DoubleBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[5] = DoubleBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[6] = DoubleBinaryComparatorFactory.INSTANCE;
        }

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(rtreeCmpFactories.length, DoublePointable.FACTORY);

        //4
        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                RTreePolicyType.RTREE, null, btreeFields, null, null, null);
        treeIndex.create();
        treeIndex.activate();

        long start = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = treeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        int numInserts = 10000;
        for (int i = 0; i < numInserts; i++) {
            double p1x = rnd.nextDouble();
            double p1y = rnd.nextDouble();
            double p1z = rnd.nextDouble();
            double p2x = rnd.nextDouble();
            double p2y = rnd.nextDouble();
            double p2z = rnd.nextDouble();

            double pk = 5.0;

            TupleUtils.createDoubleTuple(tb, tuple, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.min(p1z, p2z),
                    Math.max(p1x, p2x), Math.max(p1y, p2y), Math.max(p1z, p2z), pk);
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (!e.matches(ErrorCode.DUPLICATE_KEY)) {
                    throw e;
                }
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        scan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createDoubleTuple(keyTb, key, -1000.0, -1000.0, -1000.0, 1000.0, 1000.0, 1000.0);

        rangeSearch(rtreeCmpFactories, indexAccessor, fieldSerdes, key, null, null);

        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Deletion Example. Create an RTree index of two dimensions, where they
     * keys are of type integer, and the payload is one integer value. Fill
     * index with random values using insertions, then delete entries
     * one-by-one. Repeat procedure a few times on same RTree.
     */
    @Test
    public void deleteExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Deletion Example");
        }

        // Declare fields.
        int fieldCount = 5;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = IntegerPointable.TYPE_TRAITS;
        typeTraits[4] = IntegerPointable.TYPE_TRAITS;

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
            btreeKeyFieldCount = 1;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeFields = new int[btreeKeyFieldCount];
            for (int i = 0; i < btreeKeyFieldCount; i++) {
                btreeFields[i] = rtreeKeyFieldCount + i;
            }

        } else {
            btreeKeyFieldCount = 5;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[4] = IntegerBinaryComparatorFactory.INSTANCE;
        }

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(rtreeCmpFactories.length, IntegerPointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                RTreePolicyType.RTREE, null, btreeFields, null, null, null);
        treeIndex.create();
        treeIndex.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = treeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);

        int runs = 3;
        for (int run = 0; run < runs; run++) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Deletion example run: " + (run + 1) + "/" + runs);
                LOGGER.info("Inserting into tree...");
            }

            int numInserts = 10000;
            int[] p1xs = new int[numInserts];
            int[] p1ys = new int[numInserts];
            int[] p2xs = new int[numInserts];
            int[] p2ys = new int[numInserts];
            int[] pks = new int[numInserts];
            int insDone = 0;

            int[] insDoneCmp = new int[numInserts];
            for (int i = 0; i < numInserts; i++) {
                int p1x = rnd.nextInt();
                int p1y = rnd.nextInt();
                int p2x = rnd.nextInt();
                int p2y = rnd.nextInt();
                int pk = 5;

                p1xs[i] = Math.min(p1x, p2x);
                p1ys[i] = Math.min(p1y, p2y);
                p2xs[i] = Math.max(p1x, p2x);
                p2ys[i] = Math.max(p1y, p2y);
                pks[i] = pk;

                TupleUtils.createIntegerTuple(tb, tuple, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                        Math.max(p1y, p2y), pk);
                try {
                    indexAccessor.insert(tuple);
                } catch (HyracksDataException e) {
                    if (!e.matches(ErrorCode.DUPLICATE_KEY)) {
                        throw e;
                    }
                }
                insDoneCmp[i] = insDone;
            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Deleting from tree...");
            }
            int delDone = 0;
            for (int i = 0; i < numInserts; i++) {
                TupleUtils.createIntegerTuple(tb, tuple, p1xs[i], p1ys[i], p2xs[i], p2ys[i], pks[i]);
                try {
                    indexAccessor.delete(tuple);
                    delDone++;
                } catch (HyracksDataException e) {
                    if (!e.matches(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY)) {
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
        treeIndex.deactivate();
        treeIndex.destroy();
    }

    /**
     * Bulk load example. Load a tree with 10,000 tuples.
     */
    @Test
    public void bulkLoadExample() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Bulk load example");
        }
        // Declare fields.
        int fieldCount = 5;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = IntegerPointable.TYPE_TRAITS;
        typeTraits[4] = IntegerPointable.TYPE_TRAITS;

        // Declare field serdes.
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;;
        rtreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
        rtreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
            btreeKeyFieldCount = 1;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeFields = new int[btreeKeyFieldCount];
            for (int i = 0; i < btreeKeyFieldCount; i++) {
                btreeFields[i] = rtreeKeyFieldCount + i;
            }

        } else {
            btreeKeyFieldCount = 5;
            btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[2] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[3] = IntegerBinaryComparatorFactory.INSTANCE;
            btreeCmpFactories[4] = IntegerBinaryComparatorFactory.INSTANCE;
        }

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(rtreeCmpFactories.length, IntegerPointable.FACTORY);

        //6
        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                RTreePolicyType.RTREE, null, btreeFields, null, null, null);
        treeIndex.create();
        treeIndex.activate();

        // Load records.
        int numInserts = 10000;
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Bulk loading " + numInserts + " tuples");
        }
        long start = System.currentTimeMillis();
        IIndexBulkLoader bulkLoader =
                treeIndex.createBulkLoader(0.7f, false, numInserts, true, NoOpPageWriteCallback.INSTANCE);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        for (int i = 0; i < numInserts; i++) {
            int p1x = rnd.nextInt();
            int p1y = rnd.nextInt();
            int p2x = rnd.nextInt();
            int p2y = rnd.nextInt();

            int pk = 5;

            TupleUtils.createIntegerTuple(tb, tuple, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                    Math.max(p1y, p2y), pk);
            bulkLoader.add(tuple);
        }

        bulkLoader.end();
        long end = System.currentTimeMillis();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(numInserts + " tuples loaded in " + (end - start) + "ms");
        }

        IIndexAccessor indexAccessor = treeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(keyTb, key, -1000, -1000, 1000, 1000);

        rangeSearch(rtreeCmpFactories, indexAccessor, fieldSerdes, key, null, null);

        treeIndex.deactivate();
        treeIndex.destroy();
    }

    protected void scan(IIndexAccessor indexAccessor, ISerializerDeserializer[] fieldSerdes) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Scan:");
        }
        IIndexCursor scanCursor = indexAccessor.createSearchCursor(false);
        try {
            SearchPredicate nullPred = new SearchPredicate(null, null);
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
            // Ignore exception because some indexes, e.g. the LSMRTree, don't
            // support disk-order scan.
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        } catch (ClassCastException e) {
            // Ignore exception because IIndexAccessor sometimes isn't
            // an ITreeIndexAccessor, e.g., for the LSMRTree.
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        }
    }

    protected void rangeSearch(IBinaryComparatorFactory[] cmpFactories, IIndexAccessor indexAccessor,
            ISerializerDeserializer[] fieldSerdes, ITupleReference key, ITupleReference minFilterTuple,
            ITupleReference maxFilterTuple) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            String kString = TupleUtils.printTuple(key, fieldSerdes);
            LOGGER.info("Range-Search using key: " + kString);
        }
        MultiComparator cmp = RTreeUtils.getSearchMultiComparator(cmpFactories, key);
        SearchPredicate rangePred;
        if (minFilterTuple != null && maxFilterTuple != null) {
            rangePred = new SearchPredicate(key, cmp, minFilterTuple, maxFilterTuple);
        } else {
            rangePred = new SearchPredicate(key, cmp);
        }
        IIndexCursor rangeCursor = indexAccessor.createSearchCursor(false);
        try {
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
}
