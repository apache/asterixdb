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

import java.util.Collections;

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
import org.apache.hyracks.storage.am.btree.impls.DiskBTree;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.btree.impls.AntimatterAwareTupleAcceptor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeOpContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Fragmentation contamination experiment (in-repo proxy for the eval doc's 85%-delete Table 10),
 * re-run on the fixed build.
 * <p>
 * Builds a two-component LSM where the older component holds N live inserts and the newer component
 * deletes (antimatters) 85% of them — the exact "tombstone fragmentation" shape. It then runs the
 * real {@link org.apache.hyracks.storage.am.btree.impls.DiskBTreeSampleCursor} over the older
 * component with the cross-component visibility search pointed at the newer component, and measures
 * how many emitted sample tuples are <i>logically deleted</i> (contamination / leakage).
 * <p>
 * Post-fix expectation: zero deleted rows leak into the sample. (On the pre-fix build, where the
 * shadow check used the delete-resolving point search, ~85% of emitted tuples were deleted rows.)
 * The NDV/row-count path is independent (handled by {@code ThetaEstimator}) and is not exercised here.
 */
@SuppressWarnings("rawtypes")
public class LSMSampleFragmentationContaminationTest {

    private static final int FIELD_COUNT = 2;
    private static final int KEY_FIELD_COUNT = 1;
    private static final ITypeTraits[] TYPE_TRAITS = { IntegerPointable.TYPE_TRAITS, IntegerPointable.TYPE_TRAITS };
    private static final ISerializerDeserializer[] FIELD_SERDES =
            { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    private static final IBinaryComparatorFactory[] CMP_FACTORIES = { IntegerBinaryComparatorFactory.INSTANCE };
    private static final int[] BLOOM_FILTER_KEY_FIELDS = { 0 };

    // Experiment parameters: N keys, 85% deleted in a newer component. Kept small enough that each
    // phase fits in one in-memory component (≈12.5KB in the test harness) so the flush boundaries are
    // exactly two disk components (older inserts, newer tombstones) under NoMergePolicy.
    private static final int NUM_KEYS = 300;
    private static final int NUM_DELETED = 255; // 85%
    private static final int NUM_LIVE = NUM_KEYS - NUM_DELETED; // 45 (15%)
    private static final long SAMPLE_SEED = 12345L;

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
    public void deletedRowsMustNotLeakIntoSampleUnderHeavyFragmentation() throws HyracksDataException {
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) lsmBtree.createAccessor(actx);

        // Older component: N live inserts (keys 0..N-1).
        for (int k = 0; k < NUM_KEYS; k++) {
            insert(accessor, k, 1);
        }
        accessor.scheduleFlush();

        // Newer component: delete (antimatter) 85% of them (keys 0..NUM_DELETED-1). Live = [NUM_DELETED, N).
        for (int k = 0; k < NUM_DELETED; k++) {
            delete(accessor, k);
        }
        accessor.scheduleFlush();

        Assert.assertEquals("expected two disk components (older inserts + newer tombstones)", 2,
                lsmBtree.getDiskComponents().size());

        // Wire the sampler exactly as LSMIndexSampleCursor does when sampling the older component:
        // the cross-component visibility search covers the newer component only.
        LSMBTreeOpContext opCtx = lsmBtree.createOpContext(NoOpIndexAccessParameters.INSTANCE);
        opCtx.setOperation(IndexOperation.SEARCH);
        lsmBtree.getOperationalComponents(opCtx);
        ILSMComponent newer = opCtx.getComponentHolder().get(0); // newest first
        ILSMComponent older = opCtx.getComponentHolder().get(1);

        RangePredicate searchPred = new RangePredicate(null, null, true, true, null, null);
        opCtx.getSearchInitialState().reset(searchPred, Collections.singletonList(newer));
        LSMBTreeBatchPointSearchCursor searchCursor = new LSMBTreeBatchPointSearchCursor(opCtx);
        searchCursor.open(opCtx.getSearchInitialState(), searchPred);

        DiskBTree olderBtree = (DiskBTree) older.getIndex();
        DiskBTree.DiskBTreeAccessor sampleAccessor =
                (DiskBTree.DiskBTreeAccessor) olderBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);

        // Aim to collect NUM_LIVE samples; maxLeafTupleCount=0 disables fill-rejection (not under test here).
        ITreeIndexCursor sampleCursor = sampleAccessor.createSampleCursor(NUM_LIVE, SAMPLE_SEED, searchCursor,
                /*maxLeafAttempts*/ 50000, /*leafDrawBatchSize*/ NUM_KEYS, /*maxLeafTupleCount*/ 0,
                /*samplesPerPage*/ 0, AntimatterAwareTupleAcceptor.INSTANCE);

        int emitted = 0;
        int leakedDeleted = 0;
        int liveSampled = 0;
        try {
            sampleAccessor.diskSampleScan(sampleCursor);
            while (sampleCursor.hasNext()) {
                sampleCursor.next();
                int key = IntegerPointable.getInteger(sampleCursor.getTuple().getFieldData(0),
                        sampleCursor.getTuple().getFieldStart(0));
                emitted++;
                if (key < NUM_DELETED) {
                    leakedDeleted++; // this key was deleted in the newer component -> must not appear
                } else {
                    liveSampled++;
                }
            }
        } finally {
            sampleCursor.close();
            sampleCursor.destroy();
            searchCursor.close();
            searchCursor.destroy();
        }

        double leakPct = emitted == 0 ? 0.0 : (100.0 * leakedDeleted / emitted);
        System.out.println("=".repeat(72));
        System.out.println("LSM 85%-delete fragmentation contamination experiment (fixed build)");
        System.out.printf("  keys=%d, deleted(newer)=%d (85%%), live=%d (15%%)%n", NUM_KEYS, NUM_DELETED, NUM_LIVE);
        System.out.printf("  emitted sample tuples : %d%n", emitted);
        System.out.printf("  live (correct)        : %d%n", liveSampled);
        System.out.printf("  DELETED leaked        : %d  (%.2f%% of sample)%n", leakedDeleted, leakPct);
        System.out.println("=".repeat(72));

        Assert.assertEquals("deleted-in-newer rows must not leak into the sample (contamination)", 0, leakedDeleted);
        Assert.assertTrue("sampler should still surface live rows", liveSampled > 0);
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
