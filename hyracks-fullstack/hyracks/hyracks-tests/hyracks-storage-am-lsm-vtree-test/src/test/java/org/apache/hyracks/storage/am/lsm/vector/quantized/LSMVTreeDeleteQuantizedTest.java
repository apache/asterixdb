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
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.QuantizedSearchTestDriver;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * LSMVTree delete test with quantized vector embedding.
 * Tests delete operations after bulk loading the first disk component.
 *
 * Inherits two test cases from QuantizedSearchTestDriver:
 * - optimizedSearchThreeDimension(): 3D single-centroid structure (1 level, 20 bulk-loaded records)
 * - twoDimensionTwoLevels(): 2D two-layer structure (2 levels, 16 leaf centroids, 800 bulk-loaded records)
 *
 * Each test case bulk loads the dataset, then deletes specific records (creating antimatter
 * tuples in the memory component), and verifies that deleted records are absent from
 * optimized search results while remaining top-K results shift correctly.
 *
 * Data tuple format (quantized): <distance, centroid_id, vector, primary_key>
 * Delete tuple format: <vector, primary_key> (same as insert)
 */
public class LSMVTreeDeleteQuantizedTest extends QuantizedSearchTestDriver {

    private static final Logger LOGGER = LogManager.getLogger();

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
     * Implementation of runTest from QuantizedSearchTestDriver.
     * Performs: build static structure → bulk load → delete → verify with delete-aware query cases.
     *
     * After deleting specific records, antimatter tuples in the memory component cancel
     * matching matter tuples from the disk component. The optimized search cursor
     * (LSMVTreePrunedTopKSearchCursor) handles this via the hold-and-check pattern.
     */
    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords, List<QueryCase> queryCases) throws Exception {

        LOGGER.info("LSMVTree Delete Quantized Test: {} levels, {} centroids, {} leaf clusters, {}D vectors",
                numClustersPerLevel.size(), centroids.size(), leafRecords.size(), vectorDimension);

        // Create test context with quantized data tuple creator factory
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(),
                harness.getDataTupleBuilderFactory());

        // Required by LSMVTreeTopKSearchCursor (naive blocked, SQ8-quantized auto-route):
        // SpillableTopKBuffer needs a real IHyracksTaskContext for frame allocation and spill.
        ctx.setHyracksTaskContext(harness.getHyracksTastContext());

        // Set test data in context (quantized format: PK starts at field 4)
        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);
        ctx.setDataRecords(leafRecords);

        try {
            // 1. Create and activate index
            ctx.getIndex().create();
            ctx.getIndex().activate();
            LOGGER.info("Index created and activated");

            // 2. Build static structure
            testUtils.buildStaticStructure(ctx);
            LOGGER.info("Static structure built with {} centroids", centroids.size());

            // 3. Bulk load data records (first disk component)
            testUtils.bulkLoadRecords(ctx);
            int bulkLoadedCount = leafRecords.stream().mapToInt(List::size).sum();
            LOGGER.info("Bulk loaded {} records across {} clusters", bulkLoadedCount, leafRecords.size());

            // 4. Generate and delete specific records
            List<ITupleReference> deleteTuples = getDeleteTuples(vectorDimension);
            int deletedCount = testUtils.deleteRecordsFromIndex(ctx, deleteTuples);
            LOGGER.info("Deleted {} records from index", deletedCount);

            // 5. Verify with optimized search (delete-aware query cases)
            for (int i = 0; i < queryCases.size(); i++) {
                QueryCase qc = queryCases.get(i);
                ctx.setQueryVector(qc.queryVector);
                ctx.setQueryK(qc.queryK);
                ctx.setExpectedPrimaryKeys(qc.expectedPrimaryKeys);
                ctx.setExcludedPrimaryKeys(qc.excludedPrimaryKeys);
                testUtils.naiveBlockedSearch(ctx);
                LOGGER.info("Query case {}/{} (naive blocked) succeeded: K={}", i + 1, queryCases.size(), qc.queryK);
            }

        } finally {
            // Cleanup
            ctx.getIndex().deactivate();
            LOGGER.info("Index deactivated");
        }
    }

    /**
     * Generate delete tuples based on vector dimension.
     */
    private List<ITupleReference> getDeleteTuples(int vectorDimension) throws Exception {
        if (vectorDimension == 3) {
            return get3DDeleteTuples();
        } else {
            return get2DDeleteTuples();
        }
    }

    /**
     * 3D delete tuples: pk_opt_5 at [5,0,0] and pk_opt_6 at [6,0,0].
     * These are the two closest records to query [5,0,0].
     */
    private List<ITupleReference> get3DDeleteTuples() throws Exception {
        return generateDeleteTuples(new double[][] { { 5.0, 0.0, 0.0 }, { 6.0, 0.0, 0.0 } },
                new String[] { "pk_opt_5", "pk_opt_6" });
    }

    /**
     * 2D delete tuples: pk_2d_c4_48 at [27.6,25] and pk_2d_c4_46 at [25,27.4].
     * These are the two closest records from c4 to query [30,30].
     *
     * From generate2DDataRecords():
     * - pk_2d_c4_48: Ring 13 (d=2.6), +x direction → [25+2.6, 25] = [27.6, 25]
     * - pk_2d_c4_46: Ring 12 (d=2.4), +y direction → [25, 25+2.4] = [25, 27.4]
     */
    private List<ITupleReference> get2DDeleteTuples() throws Exception {
        return generateDeleteTuples(new double[][] { { 27.6, 25.0 }, { 25.0, 27.4 } },
                new String[] { "pk_2d_c4_48", "pk_2d_c4_46" });
    }

    /**
     * Override 3D query cases for delete test.
     * Delete pk_opt_5 [5,0,0] (D(q,x)=0) and pk_opt_6 [6,0,0] (D(q,x)=1).
     * After deletion, top-5 shifts to: pk_opt_4, pk_opt_3, pk_opt_7, pk_opt_2, pk_opt_8.
     */
    @Override
    protected List<QueryCase> build3DQueryCases() {
        List<QueryCase> queryCases = new ArrayList<>();
        // Query at [5,0,0] K=5
        // After deleting pk_opt_5 (D=0) and pk_opt_6 (D=1):
        //   pk_opt_4: [4,0,0], D(q,x) = 1.0
        //   pk_opt_3: [3,0,0], D(q,x) = 2.0
        //   pk_opt_7: [7,0,0], D(q,x) = 2.0
        //   pk_opt_2: [2,0,0], D(q,x) = 3.0
        //   pk_opt_8: [8,0,0], D(q,x) = 3.0
        queryCases.add(new QueryCase(new double[] { 5.0, 0.0, 0.0 }, 5,
                Arrays.asList("pk_opt_4", "pk_opt_3", "pk_opt_7", "pk_opt_2", "pk_opt_8"),
                Arrays.asList("pk_opt_5", "pk_opt_6")));
        return queryCases;
    }

    /**
     * Override 2D query cases for delete test.
     * Delete pk_2d_c4_48 [27.6,25] and pk_2d_c4_46 [25,27.4].
     * After deletion, top-5 from c4 shifts to the next closest records.
     */
    @Override
    protected List<QueryCase> build2DQueryCases() {
        List<QueryCase> queryCases = new ArrayList<>();
        // Query at [30, 30] K=5
        // Before deletion top-5:
        //   pk_2d_c4_48: [27.6,25], D(q,x) = sqrt(30.76) ≈ 5.55
        //   pk_2d_c4_44: [27.4,25], D(q,x) = sqrt(31.76) ≈ 5.64
        //   pk_2d_c4_46: [25,27.4], D(q,x) = sqrt(31.76) ≈ 5.64
        //   pk_2d_c4_40: [27.2,25], D(q,x) = sqrt(32.84) ≈ 5.73
        //   pk_2d_c4_42: [25,27.2], D(q,x) = sqrt(32.84) ≈ 5.73
        //
        // After deleting pk_2d_c4_48 and pk_2d_c4_46, top-5 becomes:
        //   pk_2d_c4_44: [27.4,25], D(q,x) ≈ 5.64
        //   pk_2d_c4_40: [27.2,25], D(q,x) ≈ 5.73
        //   pk_2d_c4_42: [25,27.2], D(q,x) ≈ 5.73
        //   pk_2d_c4_36: [27.0,25], D(q,x) = sqrt(34) ≈ 5.83
        //   pk_2d_c4_38: [25,27.0], D(q,x) = sqrt(34) ≈ 5.83
        queryCases.add(new QueryCase(new double[] { 30.0, 30.0 }, 5,
                Arrays.asList("pk_2d_c4_44", "pk_2d_c4_40", "pk_2d_c4_42", "pk_2d_c4_36", "pk_2d_c4_38"),
                Arrays.asList("pk_2d_c4_48", "pk_2d_c4_46")));
        return queryCases;
    }
}
