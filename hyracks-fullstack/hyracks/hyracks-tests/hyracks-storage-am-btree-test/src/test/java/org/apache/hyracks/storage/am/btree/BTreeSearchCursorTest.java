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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.TreeSet;
import java.util.logging.Level;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.AbstractBTreeTest;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrameFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BTreeSearchCursorTest extends AbstractBTreeTest {
    private final int fieldCount = 2;
    private final ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
    private final TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
    private final ITreeIndexMetadataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
    private final Random rnd = new Random(50);

    @Override
    @Before
    public void setUp() throws HyracksDataException {
        super.setUp();
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = IntegerPointable.TYPE_TRAITS;
    }

    @Test
    public void uniqueIndexTest() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("TESTING RANGE SEARCH CURSOR ON UNIQUE INDEX");
        }

        IBufferCache bufferCache = harness.getBufferCache();

        // declare keys
        int keyFieldCount = 1;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();

        IMetadataPageManager freePageManager = new LinkedMetaDataPageManager(bufferCache, metaFrameFactory);

        BTree btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                fieldCount, harness.getFileReference());
        btree.create();
        btree.activate();

        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        ITreeIndexAccessor indexAccessor =
                btree.createAccessor(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);

        // generate keys
        int numKeys = 50;
        int maxKey = 1000;
        TreeSet<Integer> uniqueKeys = new TreeSet<>();
        ArrayList<Integer> keys = new ArrayList<>();
        while (uniqueKeys.size() < numKeys) {
            int key = rnd.nextInt() % maxKey;
            uniqueKeys.add(key);
        }
        for (Integer i : uniqueKeys) {
            keys.add(i);
        }

        // insert keys into btree
        for (int i = 0; i < keys.size(); i++) {

            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

            try {
                indexAccessor.insert(tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        int minSearchKey = -100;
        int maxSearchKey = 100;

        // forward searches
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, true, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, false, true, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, false, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, true, false));

        btree.deactivate();
        btree.destroy();
    }

    @Test
    public void nonUniqueIndexTest() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("TESTING RANGE SEARCH CURSOR ON NONUNIQUE INDEX");
        }

        IBufferCache bufferCache = harness.getBufferCache();

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        cmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();

        IMetadataPageManager freePageManager = new LinkedMetaDataPageManager(bufferCache, metaFrameFactory);

        BTree btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                fieldCount, harness.getFileReference());
        btree.create();
        btree.activate();

        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        ITreeIndexAccessor indexAccessor =
                btree.createAccessor(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);

        // generate keys
        int numKeys = 50;
        int maxKey = 10;
        ArrayList<Integer> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            int k = rnd.nextInt() % maxKey;
            keys.add(k);
        }
        Collections.sort(keys);

        // insert keys into btree
        for (int i = 0; i < keys.size(); i++) {

            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

            try {
                indexAccessor.insert(tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        int minSearchKey = -100;
        int maxSearchKey = 100;

        // forward searches
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, true, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, false, true, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, false, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, true, false));

        btree.deactivate();
        btree.destroy();
    }

    @Test
    public void nonUniqueFieldPrefixIndexTest() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("TESTING RANGE SEARCH CURSOR ON NONUNIQUE FIELD-PREFIX COMPRESSED INDEX");
        }

        IBufferCache bufferCache = harness.getBufferCache();

        // declare keys
        int keyFieldCount = 2;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        cmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(tupleWriterFactory);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(tupleWriterFactory);

        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) leafFrameFactory.createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) interiorFrameFactory.createFrame();

        IMetadataPageManager freePageManager = new LinkedMetaDataPageManager(bufferCache, metaFrameFactory);

        BTree btree = new BTree(bufferCache, freePageManager, interiorFrameFactory, leafFrameFactory, cmpFactories,
                fieldCount, harness.getFileReference());
        btree.create();
        btree.activate();

        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();

        ITreeIndexAccessor indexAccessor =
                btree.createAccessor(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);

        // generate keys
        int numKeys = 50;
        int maxKey = 10;
        ArrayList<Integer> keys = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            int k = rnd.nextInt() % maxKey;
            keys.add(k);
        }
        Collections.sort(keys);

        // insert keys into btree
        for (int i = 0; i < keys.size(); i++) {

            TupleUtils.createIntegerTuple(tupleBuilder, tuple, keys.get(i), i);
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());

            try {
                indexAccessor.insert(tuple);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        int minSearchKey = -100;
        int maxSearchKey = 100;

        // forward searches
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, true, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, false, true, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, false, false));
        Assert.assertTrue(
                performSearches(keys, btree, leafFrame, interiorFrame, minSearchKey, maxSearchKey, true, true, false));

        btree.deactivate();
        btree.destroy();
    }

    public RangePredicate createRangePredicate(int lk, int hk, boolean lowKeyInclusive, boolean highKeyInclusive)
            throws HyracksDataException {

        // create tuplereferences for search keys
        ITupleReference lowKey = TupleUtils.createIntegerTuple(false, lk);
        ITupleReference highKey = TupleUtils.createIntegerTuple(false, hk);

        IBinaryComparator[] searchCmps = new IBinaryComparator[1];
        searchCmps[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        MultiComparator searchCmp = new MultiComparator(searchCmps);

        RangePredicate rangePred =
                new RangePredicate(lowKey, highKey, lowKeyInclusive, highKeyInclusive, searchCmp, searchCmp);
        return rangePred;
    }

    public void getExpectedResults(ArrayList<Integer> expectedResults, ArrayList<Integer> keys, int lk, int hk,
            boolean lowKeyInclusive, boolean highKeyInclusive) {

        // special cases
        if (lk == hk && (!lowKeyInclusive || !highKeyInclusive)) {
            return;
        }
        if (lk > hk) {
            return;
        }

        for (int i = 0; i < keys.size(); i++) {
            if ((lk == keys.get(i) && lowKeyInclusive) || (hk == keys.get(i) && highKeyInclusive)) {
                expectedResults.add(keys.get(i));
                continue;
            }

            if (lk < keys.get(i) && hk > keys.get(i)) {
                expectedResults.add(keys.get(i));
                continue;
            }
        }
    }

    public boolean performSearches(ArrayList<Integer> keys, BTree btree, IBTreeLeafFrame leafFrame,
            IBTreeInteriorFrame interiorFrame, int minKey, int maxKey, boolean lowKeyInclusive,
            boolean highKeyInclusive, boolean printExpectedResults) throws Exception {

        ArrayList<Integer> results = new ArrayList<>();
        ArrayList<Integer> expectedResults = new ArrayList<>();

        for (int i = minKey; i < maxKey; i++) {
            for (int j = minKey; j < maxKey; j++) {

                results.clear();
                expectedResults.clear();

                int lowKey = i;
                int highKey = j;

                ITreeIndexCursor rangeCursor = new BTreeRangeSearchCursor(leafFrame, false);
                RangePredicate rangePred = createRangePredicate(lowKey, highKey, lowKeyInclusive, highKeyInclusive);
                ITreeIndexAccessor indexAccessor =
                        btree.createAccessor(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
                indexAccessor.search(rangeCursor, rangePred);

                try {
                    while (rangeCursor.hasNext()) {
                        rangeCursor.next();
                        ITupleReference frameTuple = rangeCursor.getTuple();
                        ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(0),
                                frameTuple.getFieldStart(0), frameTuple.getFieldLength(0));
                        DataInput dataIn = new DataInputStream(inStream);
                        Integer res = IntegerSerializerDeserializer.INSTANCE.deserialize(dataIn);
                        results.add(res);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    rangeCursor.close();
                }

                getExpectedResults(expectedResults, keys, lowKey, highKey, lowKeyInclusive, highKeyInclusive);

                if (printExpectedResults) {
                    if (expectedResults.size() > 0) {
                        char l, u;

                        if (lowKeyInclusive) {
                            l = '[';
                        } else {
                            l = '(';
                        }

                        if (highKeyInclusive) {
                            u = ']';
                        } else {
                            u = ')';
                        }

                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("RANGE: " + l + " " + lowKey + " , " + highKey + " " + u);
                        }
                        StringBuilder strBuilder = new StringBuilder();
                        for (Integer r : expectedResults) {
                            strBuilder.append(r + " ");
                        }
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info(strBuilder.toString());
                        }
                    }
                }

                if (results.size() == expectedResults.size()) {
                    for (int k = 0; k < results.size(); k++) {
                        if (!results.get(k).equals(expectedResults.get(k))) {
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("DIFFERENT RESULTS AT: i=" + i + " j=" + j + " k=" + k);
                                LOGGER.info(results.get(k) + " " + expectedResults.get(k));
                            }
                            return false;
                        }
                    }
                } else {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("UNEQUAL NUMBER OF RESULTS AT: i=" + i + " j=" + j);
                        LOGGER.info("RESULTS: " + results.size());
                        LOGGER.info("EXPECTED RESULTS: " + expectedResults.size());
                    }
                    return false;
                }
            }
        }

        return true;
    }
}
