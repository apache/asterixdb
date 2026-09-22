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

package org.apache.hyracks.storage.am.lsm.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeTopKSearchCursor;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.QuantizedSearchTestDriver;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.BulkLoadRecordFormat;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.am.vector.utils.NoOpVectorQuantizer;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Asserts that the two LSM VTree search cursors agree on what a record's history makes visible.
 * <p>
 * The index has two: {@code LSMVTreeTopKSearchCursor} answers production ANN queries, and
 * {@code LSMVTreeSearchCursor} streams components during merges — so the first decides what a user sees
 * and the second decides what survives into durable bytes. They reconcile delete markers by different
 * routes: the streaming cursor drains every older same-key version against a held reference, the top-K
 * cursor groups the equal-distance run and takes the newest entry per key. Both rest on the same ordering
 * key (the index's {@code comparatorFields}), but nothing before this test checked that they reach the
 * same answer.
 * <p>
 * If they ever diverge, a record could be visible to queries and dropped at merge, or the reverse — the
 * latter resurrecting a deleted row. That is what makes agreement, rather than either cursor's own
 * output, the property worth pinning.
 * <p>
 * K is set well above the record count on purpose: the question is which records are <em>visible</em>,
 * and a K that truncates would make the two sets differ for reasons that have nothing to do with
 * reconciliation.
 */
public class LSMVTreeCursorAgreementTest {

    private static final VectorTestStructure STRUCT_3D = VectorTestStructure.threeDim1Centroid();
    /** Above the record count, so neither cursor truncates and the comparison is about visibility. */
    private static final int K = 10;

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /**
     * A delete whose distance-to-centroid ties with an unrelated record in the same component, so that
     * record lands between the delete marker and its live twin in the merged sequence.
     * <p>
     * The streaming cursor holds one antimatter element and cancels it against the next queue head, so it
     * rests on a marker and its twin being adjacent in the merged stream. They are, because every
     * component's chain is ordered on the same {@code <distance, PK>} key the cursor reconciles with.
     * Before the pages carried the primary key, an intervening tie released the hold and the deleted
     * record was emitted — by the cursor that merges components, i.e. into durable bytes.
     */
    @Test
    public void bothCursorsAgreeAfterADeleteAtACollidingDistance() throws Exception {
        AbstractVectorTreeTestContext ctx = newContext(collidingDistanceCluster());
        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();
            testUtils.buildStaticStructure(ctx);
            testUtils.bulkLoadRecords(ctx);

            // Delete A, whose distance ties with B. The marker lands in the memory component, so this is
            // cross-component reconciliation.
            testUtils.deleteRecordsFromIndex(ctx,
                    Arrays.asList(VectorTestStructure.createInsertTuple(new double[] { 5, 0, 0 }, "pk_collide_A")));

            assertCursorsAgree(ctx, new double[] { 5, 0, 0 }, "pk_collide_A");
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /**
     * Insert, delete, then re-insert the same primary key. Three versions of one key exist across
     * components, which is where reconciliation that only cancels a single pair leaks the third.
     */
    @Test
    public void bothCursorsAgreeAfterDeleteThenReinsert() throws Exception {
        AbstractVectorTreeTestContext ctx = newContext(collidingDistanceCluster());
        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();
            testUtils.buildStaticStructure(ctx);
            testUtils.bulkLoadRecords(ctx);

            ITupleReference recordA = VectorTestStructure.createInsertTuple(new double[] { 5, 0, 0 }, "pk_collide_A");
            testUtils.deleteRecordsFromIndex(ctx, Arrays.asList(recordA));
            // Re-insert the same key: the newest version is matter again, so it must be visible to BOTH.
            testUtils.insertRecordsIntoMemoryComponent(ctx, Arrays.asList(Arrays.asList(recordA)));

            assertCursorsAgree(ctx, new double[] { 5, 0, 0 });
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /** With no deletes at all the two cursors must still see the same records. */
    @Test
    public void bothCursorsAgreeWithNoDeletes() throws Exception {
        AbstractVectorTreeTestContext ctx = newContext(collidingDistanceCluster());
        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();
            testUtils.buildStaticStructure(ctx);
            testUtils.bulkLoadRecords(ctx);

            assertCursorsAgree(ctx, new double[] { 5, 0, 0 });
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /**
     * The durable half: force the streaming cursor to actually write the merged bytes, then query.
     * <p>
     * The other tests read the streaming cursor as a query cursor, which production never does — queries
     * always take the top-K path. This one puts the cursor where it really runs. A full merge over every
     * disk component drops delete markers rather than preserving them, so a marker that fails to cancel
     * its twin leaves the twin alone in the merged component with nothing left to suppress it: the record
     * is resurrected in durable bytes, and the query afterwards sees it.
     */
    @Test
    public void aFullMergeDoesNotResurrectADeletedRecordAtACollidingDistance() throws Exception {
        AbstractVectorTreeTestContext ctx = newContext(collidingDistanceCluster());
        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();
            testUtils.buildStaticStructure(ctx);
            testUtils.bulkLoadRecords(ctx);

            LSMVTree lsmVTree = (LSMVTree) ctx.getIndex();
            Assert.assertEquals("one disk component after bulk load", 1, lsmVTree.getDiskComponents().size());

            // A's matter is on disk, so the delete leaves a marker in the memory component; flush it down
            // so both live in disk components and the merge has to reconcile them.
            testUtils.deleteRecordsFromIndex(ctx,
                    Arrays.asList(VectorTestStructure.createInsertTuple(new double[] { 5, 0, 0 }, "pk_collide_A")));
            flush(ctx);
            Assert.assertEquals("marker flushed to its own component", 2, lsmVTree.getDiskComponents().size());

            Set<String> beforeMerge = visiblePrimaryKeys(ctx, new double[] { 5, 0, 0 }, true);
            Assert.assertFalse("precondition: the delete is already invisible to queries",
                    beforeMerge.contains("pk_collide_A"));

            // Full merge: every disk component, so markers are dropped instead of carried forward.
            List<ILSMDiskComponent> allDiskComponents = new ArrayList<>(lsmVTree.getDiskComponents());
            ILSMIndexAccessor lsmAccessor =
                    (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            ILSMIOOperation mergeOp = lsmAccessor.scheduleMerge(allDiskComponents);
            mergeOp.sync();
            if (mergeOp.getStatus() == LSMIOOperationStatus.FAILURE) {
                throw HyracksDataException.create(mergeOp.getFailure());
            }
            Assert.assertEquals("one component after a full merge", 1, lsmVTree.getDiskComponents().size());

            Set<String> afterMerge = visiblePrimaryKeys(ctx, new double[] { 5, 0, 0 }, true);
            Assert.assertFalse("the deleted record was written back into the merged component",
                    afterMerge.contains("pk_collide_A"));
            // The merge must not have taken the live records with it.
            Assert.assertEquals("merge changed what is visible beyond the deleted record", beforeMerge, afterMerge);
            Assert.assertTrue("the same-distance sibling must survive", afterMerge.contains("pk_collide_B"));
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    private void flush(AbstractVectorTreeTestContext ctx) throws HyracksDataException, InterruptedException {
        ILSMIndexAccessor accessor =
                (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ILSMIOOperation flushOp = accessor.scheduleFlush();
        flushOp.sync();
        if (flushOp.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(flushOp.getFailure());
        }
    }

    // ---- the agreement assertion --------------------------------------------------------------

    /**
     * Runs one query through both cursors and requires the same visible set, then requires every named
     * key to be absent from it. The absence check is separate because two cursors that both wrongly
     * showed a deleted record would agree with each other.
     */
    private void assertCursorsAgree(AbstractVectorTreeTestContext ctx, double[] queryVector, String... mustBeAbsent)
            throws Exception {
        Set<String> viaTopK = visiblePrimaryKeys(ctx, queryVector, true);
        Set<String> viaStreaming = visiblePrimaryKeys(ctx, queryVector, false);

        Assert.assertEquals("the two cursors disagree on what is visible", viaStreaming, viaTopK);
        for (String absent : mustBeAbsent) {
            Assert.assertFalse("deleted record " + absent + " is visible to the top-K cursor",
                    viaTopK.contains(absent));
            Assert.assertFalse("deleted record " + absent + " is visible to the streaming cursor",
                    viaStreaming.contains(absent));
        }
        Assert.assertFalse("the query returned nothing, so agreement is vacuous", viaTopK.isEmpty());
    }

    /** @param useTopK selects the cursor exactly the way the production search operator does */
    private Set<String> visiblePrimaryKeys(AbstractVectorTreeTestContext ctx, double[] queryVector, boolean useTopK)
            throws Exception {
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(K);

        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);
        iap.getParameters().put(IVTreeQuantizer.IAP_KEY, NoOpVectorQuantizer.INSTANCE);
        iap.getParameters().put(HyracksConstants.HYRACKS_TASK_CONTEXT, ctx.getHyracksTaskContext());
        if (useTopK) {
            iap.getParameters().put(LSMVTreeTopKSearchCursor.IAP_KEY, Boolean.TRUE);
        }

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);
        Set<String> primaryKeys = new LinkedHashSet<>();
        try {
            accessor.search(cursor, predicate);
            try {
                while (cursor.hasNext()) {
                    cursor.next();
                    // Extracted at emission time: getTuple() hands back a reference the cursor mutates.
                    primaryKeys.add(testUtils.extractPrimaryKeyFromOptimizedTuple(cursor.getTuple()));
                }
            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
        return primaryKeys;
    }

    // ---- fixture -----------------------------------------------------------------------------

    /**
     * One leaf cluster whose records are distance-ordered, with two records tied at distance 5, so an
     * unrelated record shares the delete marker's distance.
     * <p>
     * The tie is listed in primary-key order because the bulk loader takes its input as the stored order
     * and rejects anything else. Mis-ordering is covered where it can still arise: at the loader's own
     * boundary by {@code LSMVTreeBulkLoadOrderTest}, and through DML by
     * {@code LSMVTreeMultiClusterAgreementTest}.
     */
    private static List<List<ITupleReference>> collidingDistanceCluster() throws Exception {
        List<ITupleReference> cluster = new ArrayList<>();
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(3.0, 0, new double[] { 3, 0, 0 },
                "pk_filler_3"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(4.0, 0, new double[] { 4, 0, 0 },
                "pk_filler_4"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(5.0, 0, new double[] { 5, 0, 0 },
                "pk_collide_A"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(5.0, 0, new double[] { 0, 5, 0 },
                "pk_collide_B"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(6.0, 0, new double[] { 6, 0, 0 },
                "pk_filler_6"));
        List<List<ITupleReference>> leafRecords = new ArrayList<>();
        leafRecords.add(cluster);
        return leafRecords;
    }

    private AbstractVectorTreeTestContext newContext(List<List<ITupleReference>> leafRecords) throws Exception {
        ISerializerDeserializer[] dataRecordSerdes = STRUCT_3D.getDataRecordSerdes(BulkLoadRecordFormat.QUANTIZED);
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, STRUCT_3D.getVectorDimension(), harness.getMergePolicy(),
                harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(),
                harness.getDataTupleBuilderFactory());
        ctx.setHyracksTaskContext(harness.getHyracksTastContext());
        ctx.setStaticStructureCentroids(STRUCT_3D.buildCentroidTuples());
        ctx.setNumClustersPerLevel(STRUCT_3D.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(STRUCT_3D.getCentroidsPerCluster());
        ctx.setDataRecords(leafRecords);
        return ctx;
    }
}
