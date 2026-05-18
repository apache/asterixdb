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
 * LSMVTree quantized search test.
 * Tests the LSMVTreeTopKSearchCursor with sequential cluster scanning and top-K window collection.
 *
 * Uses test data from QuantizedSearchTestDriver.quantizedSearchThreeDimension():
 * - Single centroid at origin [0, 0, 0]
 * - 20 records at integer distances 1-20 along x-axis
 * - Query at [5, 0, 0] gives D(q, C) = 5.0
 *
 * Tuple format: <distance_to_centroid, centroid_id, quantized_distance, quantized_embedding, primary_key>
 */
public class LSMVTreeQuantizedSearchTest extends QuantizedSearchTestDriver {

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

    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords, List<QueryCase> queryCases) throws Exception {

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("LSMVTree Optimized Search Test: {} levels, {} centroids, {} leaf clusters, {}D vectors",
                    numClustersPerLevel.size(), centroids.size(), leafRecords.size(), vectorDimension);
        }

        // Create test context with quantized tuple format (5 fields: distance, centroidId, quantized_distance, quantized_embedding, pk)
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

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Index created and activated");
            }

            // 2. Build static structure (single centroid at origin)
            testUtils.buildStaticStructure(ctx);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Static structure built with {} centroids", centroids.size());
            }

            // 3. Bulk load data records (20 records at distances 1-20)
            testUtils.bulkLoadRecords(ctx);

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Bulk loaded {} clusters with data records", leafRecords.size());
            }

            // 4. Validate: optimized search for each query case
            for (int i = 0; i < queryCases.size(); i++) {
                QueryCase qc = queryCases.get(i);
                ctx.setQueryVector(qc.queryVector);
                ctx.setQueryK(qc.queryK);
                ctx.setExpectedPrimaryKeys(qc.expectedPrimaryKeys);
                testUtils.naiveBlockedSearch(ctx);

                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Query case {}/{} (naive blocked) succeeded: K={}", i + 1, queryCases.size(),
                            qc.queryK);
                }
            }

        } finally {
            // Cleanup
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Index deactivated and destroyed");
            }
        }
    }
}
