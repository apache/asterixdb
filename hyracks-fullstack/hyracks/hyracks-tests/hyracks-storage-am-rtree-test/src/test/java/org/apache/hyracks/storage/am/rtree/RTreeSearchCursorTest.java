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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexMetadataFrameFactory;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetaDataPageManager;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
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
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.junit.Before;
import org.junit.Test;

public class RTreeSearchCursorTest extends AbstractRTreeTest {

    public static final int FIELD_COUNT = 5;
    public static final ITypeTraits[] TYPE_TRAITS = { IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS,
            IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS };
    // Declare field serdes.
    @SuppressWarnings("rawtypes")
    public static final ISerializerDeserializer[] FIELD_SERDES = { IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    public static final int KEY_FIELD_COUNT = 4;
    public static final IBinaryComparatorFactory[] CMP_FACTORIES =
            { IntegerBinaryComparatorFactory.INSTANCE, IntegerBinaryComparatorFactory.INSTANCE,
                    IntegerBinaryComparatorFactory.INSTANCE, IntegerBinaryComparatorFactory.INSTANCE };
    public static final IPrimitiveValueProviderFactory[] VALUE_PROVIDER_FACTORY =
            RTreeUtils.createPrimitiveValueProviderFactories(CMP_FACTORIES.length, IntegerPointable.FACTORY);
    public static final RTreeTypeAwareTupleWriterFactory TUPLE_WRITER_FACTORY =
            new RTreeTypeAwareTupleWriterFactory(TYPE_TRAITS);
    public static final ITreeIndexMetadataFrameFactory META_FRAME_FACTORY = new LIFOMetaDataFrameFactory();
    public static final ITreeIndexFrameFactory INTERIOR_FRAME_FACTORY = new RTreeNSMInteriorFrameFactory(
            TUPLE_WRITER_FACTORY, VALUE_PROVIDER_FACTORY, RTreePolicyType.RTREE, false);
    public static final ITreeIndexFrameFactory LEAF_FRAME_FACTORY =
            new RTreeNSMLeafFrameFactory(TUPLE_WRITER_FACTORY, VALUE_PROVIDER_FACTORY, RTreePolicyType.RTREE, false);
    private static final Random RND = new Random(50);
    private final RTreeTestUtils rTreeTestUtils;

    public RTreeSearchCursorTest() {
        this.rTreeTestUtils = new RTreeTestUtils();
    }

    @Override
    @Before
    public void setUp() throws HyracksDataException {
        super.setUp();
    }

    @SuppressWarnings({ "rawtypes" })
    @Test
    public void rangeSearchTest() throws Exception {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("TESTING RANGE SEARCH CURSOR FOR RTREE");
        }
        IBufferCache bufferCache = harness.getBufferCache();
        // create value providers
        IRTreeInteriorFrame interiorFrame = (IRTreeInteriorFrame) INTERIOR_FRAME_FACTORY.createFrame();
        IRTreeLeafFrame leafFrame = (IRTreeLeafFrame) LEAF_FRAME_FACTORY.createFrame();
        IMetadataPageManager freePageManager = new LinkedMetaDataPageManager(bufferCache, META_FRAME_FACTORY);
        RTree rtree = new RTree(bufferCache, freePageManager, INTERIOR_FRAME_FACTORY, LEAF_FRAME_FACTORY, CMP_FACTORIES,
                FIELD_COUNT, harness.getFileReference(), false);
        rtree.create();
        rtree.activate();
        ArrayList<RTreeCheckTuple> checkTuples = insert(rtree);
        ITreeIndexAccessor indexAccessor = rtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        try {
            // Build key.
            ArrayTupleReference key = new ArrayTupleReference();
            SearchPredicate searchPredicate = createSearchPredicate(key, -1000, -1000, 1000, 1000);
            ITreeIndexCursor searchCursor = new RTreeSearchCursor(interiorFrame, leafFrame);
            try {
                RTreeCheckTuple keyCheck =
                        (RTreeCheckTuple) rTreeTestUtils.createCheckTupleFromTuple(key, FIELD_SERDES, KEY_FIELD_COUNT);
                HashMultiSet<RTreeCheckTuple> expectedResult =
                        rTreeTestUtils.getRangeSearchExpectedResults(checkTuples, keyCheck);
                rTreeTestUtils.getRangeSearchExpectedResults(checkTuples, keyCheck);
                indexAccessor.search(searchCursor, searchPredicate);
                try {
                    rTreeTestUtils.checkExpectedResults(searchCursor, expectedResult, FIELD_SERDES, KEY_FIELD_COUNT,
                            null);
                } finally {
                    searchCursor.close();
                }
            } finally {
                searchCursor.destroy();
            }
        } finally {
            indexAccessor.destroy();
        }
        rtree.deactivate();
        rtree.destroy();
    }

    public static SearchPredicate createSearchPredicate(ArrayTupleReference key, int first, int second, int third,
            int fourth) throws HyracksDataException {
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(KEY_FIELD_COUNT);
        TupleUtils.createIntegerTuple(keyTb, key, first, second, third, fourth);
        MultiComparator cmp = MultiComparator.create(CMP_FACTORIES);
        return new SearchPredicate(key, cmp);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static ArrayList<RTreeCheckTuple> insert(RTree rtree) throws HyracksDataException {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(FIELD_COUNT);
        ArrayTupleReference tuple = new ArrayTupleReference();
        ITreeIndexAccessor indexAccessor = rtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        int numInserts = 10000;
        ArrayList<RTreeCheckTuple> checkTuples = new ArrayList<>();
        for (int i = 0; i < numInserts; i++) {
            int p1x = RND.nextInt();
            int p1y = RND.nextInt();
            int p2x = RND.nextInt();
            int p2y = RND.nextInt();

            int pk = RND.nextInt();

            TupleUtils.createIntegerTuple(tb, tuple, Math.min(p1x, p2x), Math.min(p1y, p2y), Math.max(p1x, p2x),
                    Math.max(p1y, p2y), pk);
            try {
                indexAccessor.insert(tuple);
            } catch (HyracksDataException e) {
                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
            RTreeCheckTuple checkTuple = new RTreeCheckTuple(FIELD_COUNT, KEY_FIELD_COUNT);
            checkTuple.appendField(Math.min(p1x, p2x));
            checkTuple.appendField(Math.min(p1y, p2y));
            checkTuple.appendField(Math.max(p1x, p2x));
            checkTuple.appendField(Math.max(p1y, p2y));
            checkTuple.appendField(pk);

            checkTuples.add(checkTuple);
        }
        return checkTuples;
    }
}
