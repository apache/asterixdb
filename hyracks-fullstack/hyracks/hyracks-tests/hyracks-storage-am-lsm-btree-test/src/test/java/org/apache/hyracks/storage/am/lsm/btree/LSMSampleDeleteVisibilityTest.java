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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.btree.impls.BatchPredicateWithKeys;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeOpContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the cross-component visibility predicate used by the LSM sample cursors.
 * <p>
 * When the sample cursor draws a live (matter) tuple from an older disk component, it must reject
 * the draw if the same key is shadowed by a <i>newer</i> component — whether the newer entry is a
 * fresh insert (matter) <b>or a delete (antimatter)</b>. In both cases the older tuple is not part
 * of the logical, fully-merged dataset and must not be counted in the sample.
 * <p>
 * The sampler implements this shadow check via
 * {@link LSMBTreeBatchPointSearchCursor#doHasNextWithPredicate(BitSet)} over the set of newer
 * components. A single disk component holding an antimatter entry for key K stands in exactly for
 * "the newer component the sampler searches" — the predicate must report K as present so the older
 * live tuple is rejected.
 */
@SuppressWarnings("rawtypes")
public class LSMSampleDeleteVisibilityTest {

    private static final int FIELD_COUNT = 2;
    private static final int KEY_FIELD_COUNT = 1;
    private static final ITypeTraits[] TYPE_TRAITS = { IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS };
    private static final ISerializerDeserializer[] FIELD_SERDES =
            { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    private static final IBinaryComparatorFactory[] CMP_FACTORIES = { IntegerBinaryComparatorFactory.INSTANCE };
    private static final int[] BLOOM_FILTER_KEY_FIELDS = { 0 };

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private LSMBTree lsmBtree;

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
        lsmBtree = LSMBTreeExamplesTest.createTreeIndex(harness, TYPE_TRAITS, CMP_FACTORIES, BLOOM_FILTER_KEY_FIELDS,
                null, null, null, null);
        lsmBtree.create();
        lsmBtree.activate();
    }

    @After
    public void tearDown() throws HyracksDataException {
        try {
            lsmBtree.deactivate();
            lsmBtree.destroy();
        } finally {
            harness.tearDown();
        }
    }

    @Test
    public void antimatterKeyMustBeReportedPresentForShadowCheck() throws HyracksDataException {
        // Build a single disk component that plays the role of a "newer" component the sampler
        // searches: it holds live keys {10, 20, 30} and a delete (antimatter) for key 25.
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) lsmBtree.createAccessor(actx);
        insert(accessor, 10, 5);
        insert(accessor, 20, 5);
        insert(accessor, 30, 5);
        delete(accessor, 25);
        accessor.scheduleFlush();
        Assert.assertEquals("expected exactly one disk component", 1, lsmBtree.getDiskComponents().size());

        // Drive the batch point-search cursor over the disk component exactly as the sample cursor does.
        LSMBTreeOpContext opCtx = lsmBtree.createOpContext(NoOpIndexAccessParameters.INSTANCE);
        opCtx.setOperation(IndexOperation.SEARCH);
        lsmBtree.getOperationalComponents(opCtx);
        RangePredicate searchPred = new RangePredicate(null, null, true, true, null, null);
        opCtx.getSearchInitialState().reset(searchPred, opCtx.getComponentHolder());

        LSMBTreeBatchPointSearchCursor cursor = new LSMBTreeBatchPointSearchCursor(opCtx);
        cursor.open(opCtx.getSearchInitialState(), searchPred);
        try {
            // Keys must be supplied in ascending order: matter, antimatter, absent.
            List<ITupleReference> keys = new ArrayList<>();
            keys.add(keyTuple(10)); // matter        -> present
            keys.add(keyTuple(25)); // antimatter     -> must be present (shadow)
            keys.add(keyTuple(99)); // never inserted -> absent
            BatchPredicateWithKeys batchPredicate = new BatchPredicateWithKeys();
            batchPredicate.reset(keys);
            cursor.setPredicate(batchPredicate);

            BitSet found = new BitSet();
            cursor.doHasNextWithPredicate(found);

            Assert.assertTrue("live (matter) key 10 must be reported present", found.get(0));
            Assert.assertTrue("antimatter key 25 must be reported present so the shadowed older "
                    + "live tuple is rejected by the sampler", found.get(1));
            Assert.assertFalse("absent key 99 must be reported not present", found.get(2));
        } finally {
            cursor.close();
            cursor.destroy();
        }
    }

    private ITupleReference keyTuple(int key) throws HyracksDataException {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(KEY_FIELD_COUNT);
        ArrayTupleReference tuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(tb, tuple, key);
        return tuple;
    }

    private void insert(ILSMIndexAccessor accessor, int key, int value) throws HyracksDataException {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(FIELD_COUNT);
        ArrayTupleReference tuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(tb, tuple, key, value);
        accessor.insert(tuple);
    }

    private void delete(ILSMIndexAccessor accessor, int key) throws HyracksDataException {
        ArrayTupleBuilder tb = new ArrayTupleBuilder(KEY_FIELD_COUNT);
        ArrayTupleReference tuple = new ArrayTupleReference();
        TupleUtils.createIntegerTuple(tb, tuple, key);
        accessor.delete(tuple);
    }
}
