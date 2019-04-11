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
package org.apache.hyracks.storage.am.lsm.btree.cursor;

import java.util.ArrayList;
import java.util.List;
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
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.test.IIndexCursorTest;
import org.apache.hyracks.storage.am.lsm.btree.LSMBTreeExamplesTest;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeOpContext;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreePointSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class LSMBTreePointSearchCursorTest extends IIndexCursorTest {
    public static final int FIELD_COUNT = 2;
    public static final ITypeTraits[] TYPE_TRAITS = { IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS };
    @SuppressWarnings("rawtypes")
    public static final ISerializerDeserializer[] FIELD_SERDES =
            { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    public static final int KEY_FIELD_COUNT = 1;
    public static final IBinaryComparatorFactory[] CMP_FACTORIES = { IntegerBinaryComparatorFactory.INSTANCE };
    public static final int[] BLOOM_FILTER_KEY_FIELDS = { 0 };
    public static final Random RND = new Random(50);

    private static final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private static LSMBTree lsmBtree;
    private static LSMBTreeOpContext opCtx;

    @BeforeClass
    public static void setup() throws HyracksDataException {
        harness.setUp();
        lsmBtree = LSMBTreeExamplesTest.createTreeIndex(harness, TYPE_TRAITS, CMP_FACTORIES, BLOOM_FILTER_KEY_FIELDS,
                null, null, null, null);
        lsmBtree.create();
        lsmBtree.activate();
        insertData(lsmBtree);
    }

    @AfterClass
    public static void teardown() throws HyracksDataException {
        try {
            lsmBtree.deactivate();
            lsmBtree.destroy();
        } finally {
            harness.tearDown();
        }
    }

    @Override
    protected List<ISearchPredicate> createSearchPredicates() throws Exception {
        List<ISearchPredicate> predicates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // Build low key.
            ArrayTupleBuilder lowKeyTb = new ArrayTupleBuilder(KEY_FIELD_COUNT);
            ArrayTupleReference lowKey = new ArrayTupleReference();
            TupleUtils.createIntegerTuple(lowKeyTb, lowKey, -100 + (i * 50));

            // Build high key.
            ArrayTupleBuilder highKeyTb = new ArrayTupleBuilder(KEY_FIELD_COUNT);
            ArrayTupleReference highKey = new ArrayTupleReference();
            TupleUtils.createIntegerTuple(highKeyTb, highKey, -100 + (i * 50));

            MultiComparator lowKeySearchCmp = BTreeUtils.getSearchMultiComparator(CMP_FACTORIES, lowKey);
            MultiComparator highKeySearchCmp = BTreeUtils.getSearchMultiComparator(CMP_FACTORIES, highKey);
            predicates.add(new RangePredicate(lowKey, highKey, true, true, lowKeySearchCmp, highKeySearchCmp));
        }
        return predicates;
    }

    @Override
    protected IIndexCursor createCursor(IIndexAccessor accessor) {
        opCtx = lsmBtree.createOpContext(NoOpIndexAccessParameters.INSTANCE);
        return new LSMBTreePointSearchCursor(opCtx);
    }

    @Override
    protected void open(IIndexAccessor accessor, IIndexCursor cursor, ISearchPredicate predicate)
            throws HyracksDataException {
        opCtx.reset();
        opCtx.setOperation(IndexOperation.SEARCH);
        lsmBtree.getOperationalComponents(opCtx);
        opCtx.getSearchInitialState().reset(predicate, opCtx.getComponentHolder());
        cursor.open(opCtx.getSearchInitialState(), predicate);
    }

    @Override
    protected IIndexAccessor createAccessor() throws Exception {
        return lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
    }

    public static void insertData(ITreeIndex lsmBtree) throws HyracksDataException {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(FIELD_COUNT);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        IIndexAccessor indexAccessor = lsmBtree.createAccessor(actx);
        try {
            int numInserts = 10000;
            for (int i = 0; i < numInserts; i++) {
                int f0 = RND.nextInt() % numInserts;
                int f1 = 5;
                TupleUtils.createIntegerTuple(tb, tuple, f0, f1);
                try {
                    indexAccessor.insert(tuple);
                } catch (HyracksDataException e) {
                    if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                        e.printStackTrace();
                        throw e;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        } finally {
            indexAccessor.destroy();
        }
    }
}
