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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.TestOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoader;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SearchPredicate;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;

@SuppressWarnings("rawtypes")
public abstract class AbstractRTreeExamplesTest {
    protected static final Logger LOGGER = Logger.getLogger(AbstractRTreeExamplesTest.class.getName());
    protected final Random rnd = new Random(50);

    protected abstract ITreeIndex createTreeIndex(ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] rtreeCmpFactories, IBinaryComparatorFactory[] btreeCmpFactories,
            IPrimitiveValueProviderFactory[] valueProviderFactories, RTreePolicyType rtreePolicyType)
            throws TreeIndexException;

    /**
     * Two Dimensions Example. Create an RTree index of two dimensions, where
     * they keys are of type integer, and the payload is two integer values.
     * Fill index with random values using insertions (not bulk load). Perform
     * scans and range search.
     */
    @Test
    public void twoDimensionsExample() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
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
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount = 6;
        IBinaryComparatorFactory[] btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
        btreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[4] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[5] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmpFactories.length, IntegerPointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, RTreePolicyType.RTREE);
        treeIndex.create();
        treeIndex.activate();

        long start = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = (IIndexAccessor) treeIndex.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
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
            } catch (TreeIndexException e) {
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        scan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(keyTb, key, -1000, -1000, 1000, 1000);

        rangeSearch(rtreeCmpFactories, indexAccessor, fieldSerdes, key);

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
        if (LOGGER.isLoggable(Level.INFO)) {
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
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount = 5;
        IBinaryComparatorFactory[] btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
        btreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[4] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmpFactories.length, IntegerPointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, RTreePolicyType.RTREE);

        treeIndex.create();
        treeIndex.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = (IIndexAccessor) treeIndex.createAccessor(TestOperationCallback.INSTANCE,
                TestOperationCallback.INSTANCE);

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
        if (LOGGER.isLoggable(Level.INFO)) {
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
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };

        // Declare RTree keys.
        int rtreeKeyFieldCount = 4;
        IBinaryComparatorFactory[] rtreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rtreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // Declare BTree keys, this will only be used for LSMRTree
        int btreeKeyFieldCount = 5;
        IBinaryComparatorFactory[] btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
        btreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[4] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmpFactories.length, IntegerPointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, RTreePolicyType.RSTARTREE);

        treeIndex.create();
        treeIndex.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = (IIndexAccessor) treeIndex.createAccessor(TestOperationCallback.INSTANCE,
                TestOperationCallback.INSTANCE);

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
        if (LOGGER.isLoggable(Level.INFO)) {
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
        rtreeCmpFactories[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        rtreeCmpFactories[1] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        rtreeCmpFactories[2] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        rtreeCmpFactories[3] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        rtreeCmpFactories[4] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        rtreeCmpFactories[5] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);

        // Declare RTree keys.
        int btreeKeyFieldCount = 7;
        IBinaryComparatorFactory[] btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
        btreeCmpFactories[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        btreeCmpFactories[1] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        btreeCmpFactories[2] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        btreeCmpFactories[3] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        btreeCmpFactories[4] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        btreeCmpFactories[5] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        btreeCmpFactories[6] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmpFactories.length, DoublePointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, RTreePolicyType.RTREE);
        treeIndex.create();
        treeIndex.activate();

        long start = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Inserting into tree...");
        }
        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = (IIndexAccessor) treeIndex.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
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
            } catch (TreeIndexException e) {
            }
        }
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(numInserts + " inserts in " + (end - start) + "ms");
        }

        scan(indexAccessor, fieldSerdes);
        diskOrderScan(indexAccessor, fieldSerdes);

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createDoubleTuple(keyTb, key, -1000.0, -1000.0, -1000.0, 1000.0, 1000.0, 1000.0);

        rangeSearch(rtreeCmpFactories, indexAccessor, fieldSerdes, key);

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
        if (LOGGER.isLoggable(Level.INFO)) {
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
        rtreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // Declare BTree keys.
        int btreeKeyFieldCount = 5;
        IBinaryComparatorFactory[] btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
        btreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[4] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmpFactories.length, IntegerPointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, RTreePolicyType.RTREE);
        treeIndex.create();
        treeIndex.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor indexAccessor = (IIndexAccessor) treeIndex.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

        int runs = 3;
        for (int run = 0; run < runs; run++) {
            if (LOGGER.isLoggable(Level.INFO)) {
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
                } catch (TreeIndexException e) {
                }
                insDoneCmp[i] = insDone;
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deleting from tree...");
            }
            int delDone = 0;
            for (int i = 0; i < numInserts; i++) {
                TupleUtils.createIntegerTuple(tb, tuple, p1xs[i], p1ys[i], p2xs[i], p2ys[i], pks[i]);
                try {
                    indexAccessor.delete(tuple);
                    delDone++;
                } catch (TreeIndexException e) {
                }
                if (insDoneCmp[i] != delDone) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("INCONSISTENT STATE, ERROR IN DELETION EXAMPLE.");
                        LOGGER.info("INSDONECMP: " + insDoneCmp[i] + " " + delDone);
                    }
                    break;
                }
            }
            if (insDone != delDone) {
                if (LOGGER.isLoggable(Level.INFO)) {
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
        if (LOGGER.isLoggable(Level.INFO)) {
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
        rtreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        rtreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // Declare BTree keys.
        int btreeKeyFieldCount = 5;
        IBinaryComparatorFactory[] btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
        btreeCmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        btreeCmpFactories[4] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                rtreeCmpFactories.length, IntegerPointable.FACTORY);

        ITreeIndex treeIndex = createTreeIndex(typeTraits, rtreeCmpFactories, btreeCmpFactories,
                valueProviderFactories, RTreePolicyType.RTREE);
        treeIndex.create();
        treeIndex.activate();

        // Load records.
        int numInserts = 10000;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Bulk loading " + numInserts + " tuples");
        }
        long start = System.currentTimeMillis();
        IIndexBulkLoader bulkLoader = treeIndex.createBulkLoader(0.7f, false, numInserts, true);
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
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(numInserts + " tuples loaded in " + (end - start) + "ms");
        }

        IIndexAccessor indexAccessor = (IIndexAccessor) treeIndex.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(keyTb, key, -1000, -1000, 1000, 1000);

        rangeSearch(rtreeCmpFactories, indexAccessor, fieldSerdes, key);

        treeIndex.deactivate();
        treeIndex.destroy();
    }

    private void scan(IIndexAccessor indexAccessor, ISerializerDeserializer[] fieldSerdes) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Scan:");
        }
        ITreeIndexCursor scanCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor();
        SearchPredicate nullPred = new SearchPredicate(null, null);
        indexAccessor.search(scanCursor, nullPred);
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                ITupleReference frameTuple = scanCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(rec);
                }
            }
        } finally {
            scanCursor.close();
        }
    }

    private void diskOrderScan(IIndexAccessor indexAccessor, ISerializerDeserializer[] fieldSerdes) throws Exception {
        try {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Disk-Order Scan:");
            }
            ITreeIndexAccessor treeIndexAccessor = (ITreeIndexAccessor) indexAccessor;
            TreeIndexDiskOrderScanCursor diskOrderCursor = (TreeIndexDiskOrderScanCursor) treeIndexAccessor
                    .createDiskOrderScanCursor();
            treeIndexAccessor.diskOrderScan(diskOrderCursor);
            try {
                while (diskOrderCursor.hasNext()) {
                    diskOrderCursor.next();
                    ITupleReference frameTuple = diskOrderCursor.getTuple();
                    String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info(rec);
                    }
                }
            } finally {
                diskOrderCursor.close();
            }
        } catch (UnsupportedOperationException e) {
            // Ignore exception because some indexes, e.g. the LSMRTree, don't
            // support disk-order scan.
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        } catch (ClassCastException e) {
            // Ignore exception because IIndexAccessor sometimes isn't
            // an ITreeIndexAccessor, e.g., for the LSMRTree.
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring disk-order scan since it's not supported.");
            }
        }
    }

    private void rangeSearch(IBinaryComparatorFactory[] cmpFactories, IIndexAccessor indexAccessor,
            ISerializerDeserializer[] fieldSerdes, ITupleReference key) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            String kString = TupleUtils.printTuple(key, fieldSerdes);
            LOGGER.info("Range-Search using key: " + kString);
        }
        ITreeIndexCursor rangeCursor = (ITreeIndexCursor) indexAccessor.createSearchCursor();
        MultiComparator cmp = RTreeUtils.getSearchMultiComparator(cmpFactories, key);
        SearchPredicate rangePred = new SearchPredicate(key, cmp);
        indexAccessor.search(rangeCursor, rangePred);
        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ITupleReference frameTuple = rangeCursor.getTuple();
                String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(rec);
                }
            }
        } finally {
            rangeCursor.close();
        }
    }

}