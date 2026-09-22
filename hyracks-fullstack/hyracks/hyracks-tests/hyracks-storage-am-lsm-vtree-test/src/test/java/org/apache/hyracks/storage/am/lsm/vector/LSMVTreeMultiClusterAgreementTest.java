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
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.BulkLoadRecordFormat;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * The multi-cluster half of {@link LSMVTreeCursorAgreementTest}, over a tie built by DML rather than by
 * bulk load. The streaming cursor cancels a delete marker against the next queue head only, so it rests
 * on the marker and its twin being adjacent in the merged stream. Two things have to hold for that:
 * every component's chain is ordered on the ordering key, and the components are all probing the same
 * cluster at the same time.
 * <p>
 * The tie is inserted in the order that used to defeat this. A data page positioned a tuple by distance
 * alone, so two records tied on distance were stored in arrival order; arriving after its tie partner
 * put the record that sorts <em>first</em> physically <em>second</em>, and the marker's twin was then no
 * longer the next head. Reverse the two inserts and the merged stream is accidentally correct, so the
 * order here is the point of the fixture rather than an accident of it.
 * <p>
 * Components are given unequal cluster occupancy on purpose — the memory component holds only the
 * marker's cluster, one disk component only cluster A, another only cluster B, the bulk-loaded one every
 * cluster — and cluster B's record sits closer to its centroid than the deleted one does to cluster A's.
 * If per-component cluster iteration were not positionally aligned, the merged stream would interleave
 * the two clusters and release the hold before the twin arrived.
 */
public class LSMVTreeMultiClusterAgreementTest {

    private static final VectorTestStructure STRUCT = VectorTestStructure.threeDim3Level();

    /** Above the record count, so neither cursor truncates and the comparison is about visibility. */
    private static final int K = 200;

    private static final int BULK_RECORDS_PER_CLUSTER = 4;

    /** Cluster A is leaf centroid c10 at {20,30,20}; both vectors sit at distance 3 from it. */
    private static final double[] TIE_FIRST_WRITTEN = { 23.0, 30.0, 20.0 };
    private static final double[] TIE_SECOND_WRITTEN = { 20.0, 33.0, 20.0 };

    /** The deleted key sorts before its tie partner, but is written after it. */
    private static final String PK_TIE_KEPT = "pk_tieA_b";
    private static final String PK_TIE_DELETED = "pk_tieA_a";

    /** Cluster B is leaf centroid c13 at {-20,30,20}, holding a record closer to it than the tie. */
    private static final double[] NEAR_CLUSTER_B = { -19.0, 30.0, 20.0 };
    private static final String PK_CLUSTER_B = "pk_clusterB";

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

    @Test
    public void bothCursorsAgreeAcrossClustersAfterADelete() throws Exception {
        AbstractVectorTreeTestContext ctx = newContext();
        try {
            buildFixture(ctx);
            assertCursorsAgree(ctx, TIE_FIRST_WRITTEN, PK_TIE_DELETED);
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /**
     * The durable half: a full merge drops delete markers rather than carrying them forward, so a marker
     * that fails to cancel leaves its twin alone in the merged component with nothing left to suppress
     * it.
     */
    @Test
    public void aFullMergeAcrossClustersDoesNotResurrectTheDeletedRecord() throws Exception {
        AbstractVectorTreeTestContext ctx = newContext();
        try {
            buildFixture(ctx);
            LSMVTree tree = (LSMVTree) ctx.getIndex();

            Set<String> beforeMerge = visiblePrimaryKeys(ctx, TIE_FIRST_WRITTEN, true);
            Assert.assertFalse("precondition: the delete is already invisible to queries",
                    beforeMerge.contains(PK_TIE_DELETED));

            List<ILSMDiskComponent> allDiskComponents = new ArrayList<>(tree.getDiskComponents());
            Assert.assertTrue("the fixture must leave several disk components to merge", allDiskComponents.size() >= 3);
            ILSMIndexAccessor lsmAccessor =
                    (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
            ILSMIOOperation mergeOp = lsmAccessor.scheduleMerge(allDiskComponents);
            mergeOp.sync();
            if (mergeOp.getStatus() == LSMIOOperationStatus.FAILURE) {
                throw HyracksDataException.create(mergeOp.getFailure());
            }

            Set<String> afterMerge = visiblePrimaryKeys(ctx, TIE_FIRST_WRITTEN, true);
            Assert.assertFalse("the deleted record was written back into the merged component",
                    afterMerge.contains(PK_TIE_DELETED));
            Assert.assertTrue("its tie partner must survive the merge", afterMerge.contains(PK_TIE_KEPT));
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /**
     * Bulk-load every cluster, then split the later writes across two clusters and two components so no
     * two components hold the same set, and leave the delete marker alone in the memory component.
     */
    private void buildFixture(AbstractVectorTreeTestContext ctx) throws Exception {
        ctx.getIndex().create();
        ctx.getIndex().activate();
        testUtils.buildStaticStructure(ctx);
        testUtils.bulkLoadRecords(ctx);

        testUtils.insertRecordsIntoMemoryComponent(ctx,
                List.of(List.of(VectorTestStructure.createInsertTuple(TIE_FIRST_WRITTEN, PK_TIE_KEPT),
                        VectorTestStructure.createInsertTuple(TIE_SECOND_WRITTEN, PK_TIE_DELETED))));
        flush(ctx);

        testUtils.insertRecordsIntoMemoryComponent(ctx,
                List.of(List.of(VectorTestStructure.createInsertTuple(NEAR_CLUSTER_B, PK_CLUSTER_B))));
        flush(ctx);

        // The matter is on disk by now, so the delete finds nothing local to remove and leaves a marker.
        testUtils.deleteRecordsFromIndex(ctx,
                Arrays.asList(VectorTestStructure.createInsertTuple(TIE_SECOND_WRITTEN, PK_TIE_DELETED)));
    }

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
        Assert.assertTrue("the tie partner must stay visible", viaTopK.contains(PK_TIE_KEPT));
        Assert.assertTrue("the other cluster's record must stay visible", viaTopK.contains(PK_CLUSTER_B));
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

    private void flush(AbstractVectorTreeTestContext ctx) throws HyracksDataException, InterruptedException {
        ILSMIndexAccessor accessor =
                (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        ILSMIOOperation flushOp = accessor.scheduleFlush();
        flushOp.sync();
        if (flushOp.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(flushOp.getFailure());
        }
    }

    private AbstractVectorTreeTestContext newContext() throws Exception {
        ISerializerDeserializer[] dataRecordSerdes = STRUCT.getDataRecordSerdes(BulkLoadRecordFormat.QUANTIZED);
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, STRUCT.getVectorDimension(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(),
                harness.getDataTupleBuilderFactory());
        ctx.setHyracksTaskContext(harness.getHyracksTastContext());
        ctx.setStaticStructureCentroids(STRUCT.buildCentroidTuples());
        ctx.setNumClustersPerLevel(STRUCT.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(STRUCT.getCentroidsPerCluster());
        ctx.setDataRecords(STRUCT.generateBulkLoadRecords(BulkLoadRecordFormat.QUANTIZED, BULK_RECORDS_PER_CLUSTER));
        return ctx;
    }
}
