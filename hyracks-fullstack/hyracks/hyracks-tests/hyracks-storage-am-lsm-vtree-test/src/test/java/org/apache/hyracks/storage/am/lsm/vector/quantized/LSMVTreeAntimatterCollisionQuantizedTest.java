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
package org.apache.hyracks.storage.am.lsm.vector.quantized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.QuantizedSearchTestDriver;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.BulkLoadRecordFormat;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression test for cross-component antimatter reconciliation when two DIFFERENT records share the
 * same distance-to-centroid (field 0, the priority-queue ordering key).
 * <p>
 * The top-K cursor merges per-component streams ordered by distance-to-centroid and reconciles a
 * delete marker against the <em>immediately following</em> queue element. That is only correct if a
 * marker and its live twin are adjacent — which holds when every record has a distinct distance, but
 * NOT when an unrelated record shares the twin's distance and is emitted between them.
 * <p>
 * Setup: one leaf cluster; records B=[0,5,0] and A=[5,0,0] both at distance-to-centroid 5, with B
 * bulk-loaded to the disk component BEFORE A (so the disk stream emits B then A at distance 5). A is
 * then deleted (antimatter in the memory component). A query at [5,0,0] makes A the single closest
 * record to the query (distance 0), so if reconciliation fails to cancel A it resurfaces as the top
 * result. The test asserts the deleted A is absent.
 */
public class LSMVTreeAntimatterCollisionQuantizedTest {

    private static final VectorTestStructure STRUCT_3D = VectorTestStructure.threeDim1Centroid();

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

    // Regression gate for the top-K antimatter reconciliation fix: a deleted record whose
    // distance-to-centroid collides with another record must NOT resurface as a search result.
    @Test
    public void deletedRecordAtCollidingDistanceMustNotResurface() throws Exception {
        ISerializerDeserializer[] dataRecordSerdes = STRUCT_3D.getDataRecordSerdes(BulkLoadRecordFormat.QUANTIZED);
        List<ITupleReference> centroids = STRUCT_3D.buildCentroidTuples();

        // Single leaf cluster. Records sorted by distance-to-centroid ascending; the two records at
        // distance 5 are B (loaded first) then A, so the disk stream emits an unrelated same-distance
        // record (B) between A's antimatter and A's matter during the merge.
        List<ITupleReference> cluster = new ArrayList<>();
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(3.0, 0, new double[] { 3, 0, 0 },
                "pk_filler_3"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(4.0, 0, new double[] { 4, 0, 0 },
                "pk_filler_4"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(5.0, 0, new double[] { 0, 5, 0 },
                "pk_collide_B"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(5.0, 0, new double[] { 5, 0, 0 },
                "pk_collide_A"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(6.0, 0, new double[] { 6, 0, 0 },
                "pk_filler_6"));
        cluster.add(QuantizedSearchTestDriver.createOptimizedSearchRecordTuple(7.0, 0, new double[] { 7, 0, 0 },
                "pk_filler_7"));
        List<List<ITupleReference>> leafRecords = new ArrayList<>();
        leafRecords.add(cluster);

        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, STRUCT_3D.getVectorDimension(), harness.getMergePolicy(),
                harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(),
                harness.getDataTupleBuilderFactory());

        ctx.setHyracksTaskContext(harness.getHyracksTastContext());
        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(STRUCT_3D.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(STRUCT_3D.getCentroidsPerCluster());
        ctx.setDataRecords(leafRecords);

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();
            testUtils.buildStaticStructure(ctx);
            // Bulk load -> first disk component holds the matter for A and B.
            testUtils.bulkLoadRecords(ctx);

            // Delete A -> antimatter marker in the memory component (cross-component reconciliation).
            List<ITupleReference> deleteTuples =
                    Arrays.asList(VectorTestStructure.createInsertTuple(new double[] { 5, 0, 0 }, "pk_collide_A"));
            testUtils.deleteRecordsFromIndex(ctx, deleteTuples);

            // Query at A's location: A is the closest record to the query (distance 0), so a
            // failure to cancel it would surface it as the top result.
            ctx.setQueryVector(new double[] { 5, 0, 0 });
            ctx.setQueryK(3);
            ctx.setExpectedPrimaryKeys(Collections.emptyList());
            ctx.setExcludedPrimaryKeys(Collections.singletonList("pk_collide_A"));
            testUtils.naiveBlockedSearch(ctx);
        } finally {
            ctx.getIndex().deactivate();
        }
    }
}
