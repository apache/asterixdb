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

import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Level;

import org.junit.Before;
import org.junit.Test;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.util.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IFreePageManager;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.util.HashMultiSet;
import org.apache.hyracks.storage.am.rtree.api.IRTreeInteriorFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSearchCursor;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;
import org.apache.hyracks.storage.am.rtree.utils.AbstractRTreeTest;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class RTreeSearchCursorTest extends AbstractRTreeTest {

    private final RTreeTestUtils rTreeTestUtils;
    private Random rnd = new Random(50);

    public RTreeSearchCursorTest() {
        this.rTreeTestUtils = new RTreeTestUtils();
    }

    @Before
    public void setUp() throws HyracksDataException {
        super.setUp();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void rangeSearchTest() throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("TESTING RANGE SEARCH CURSOR FOR RTREE");
        }

        IBufferCache bufferCache = harness.getBufferCache();

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

        // Declare keys.
        int keyFieldCount = 4;
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[keyFieldCount];
        cmpFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        cmpFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        cmpFactories[2] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);
        cmpFactories[3] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories = RTreeUtils.createPrimitiveValueProviderFactories(
                cmpFactories.length, IntegerPointable.FACTORY);

        RTreeTypeAwareTupleWriterFactory tupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(typeTraits);
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();

        ITreeIndexFrameFactory interiorFrameFactory = new RTreeNSMInteriorFrameFactory(tupleWriterFactory,
                valueProviderFactories, RTreePolicyType.RTREE);
        ITreeIndexFrameFactory leafFrameFactory = new RTreeNSMLeafFrameFactory(tupleWriterFactory,
                valueProviderFactories, RTreePolicyType.RTREE);

        IRTreeInteriorFrame interiorFrame = (IRTreeInteriorFrame) interiorFrameFactory.createFrame();
        IRTreeLeafFrame leafFrame = (IRTreeLeafFrame) leafFrameFactory.createFrame();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, 0, metaFrameFactory);

        RTree rtree = new RTree(bufferCache, harness.getFileMapProvider(), freePageManager, interiorFrameFactory,
                leafFrameFactory, cmpFactories, fieldCount, harness.getFileReference());
        rtree.create();
        rtree.activate();

        ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        ITreeIndexAccessor indexAccessor = rtree.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        int numInserts = 10000;
        ArrayList<RTreeCheckTuple> checkTuples = new ArrayList<RTreeCheckTuple>();
        for (int i = 0; i < numInserts; i++) {
            int p1x = rnd.nextInt();
            int p1y = rnd.nextInt();
            int p2x = rnd.nextInt();
            int p2y = rnd.nextInt();

            int pk = rnd.nextInt();;

            TupleUtils.createIntegerTuple(tb, tuple, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                    Math.max(p1y, p2y), pk);
            try {
                indexAccessor.insert(tuple);
            } catch (TreeIndexException e) {
            }
            RTreeCheckTuple checkTuple = new RTreeCheckTuple(fieldCount, keyFieldCount);
            checkTuple.appendField(Math.min(p1x, p2x));
            checkTuple.appendField(Math.min(p1y, p2y));
            checkTuple.appendField(Math.max(p1x, p2x));
            checkTuple.appendField(Math.max(p1y, p2y));
            checkTuple.appendField(pk);

            checkTuples.add(checkTuple);
        }

        // Build key.
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(keyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(keyTb, key, -1000, -1000, 1000, 1000);

        MultiComparator cmp = MultiComparator.create(cmpFactories);
        ITreeIndexCursor searchCursor = new RTreeSearchCursor(interiorFrame, leafFrame);
        SearchPredicate searchPredicate = new SearchPredicate(key, cmp);

        RTreeCheckTuple keyCheck = (RTreeCheckTuple) rTreeTestUtils.createCheckTupleFromTuple(key, fieldSerdes,
                keyFieldCount);
        HashMultiSet<RTreeCheckTuple> expectedResult = rTreeTestUtils.getRangeSearchExpectedResults(checkTuples,
                keyCheck);

        rTreeTestUtils.getRangeSearchExpectedResults(checkTuples, keyCheck);
        indexAccessor.search(searchCursor, searchPredicate);

        rTreeTestUtils.checkExpectedResults(searchCursor, expectedResult, fieldSerdes, keyFieldCount, null);

        rtree.deactivate();
        rtree.destroy();
    }

}
