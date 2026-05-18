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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorIndexTestDriver;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;

/**
 * LSMVTree insert test (standard, non-quantized).
 * Tests insert operations into memory component after bulk loading the first disk component.
 *
 * Inherits test case from VectorIndexTestDriver:
 * - threeDimensionThreeLevels(): 3D three-layer structure (3 levels, 24 leaf centroids, 2400 bulk-loaded records)
 *
 * The test bulk loads the dataset, then inserts additional records into the memory component,
 * and verifies that records from both components are retrievable via search.
 *
 * Data tuple format (standard): <distance, centroid_id, primary_key>
 * No vector field is stored in data tuples.
 */
public class LSMVTreeInsertTest extends VectorIndexTestDriver {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    private static final int INSERT_RECORDS_PER_CLUSTER = 30;

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    /**
     * Implementation of runTest from VectorIndexTestDriver.
     * Performs: build static structure → bulk load → insert → verify.
     */
    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords) throws Exception {

        LOGGER.info("LSMVTree Insert Test: {} levels, {} centroids, {} leaf clusters, {}D vectors",
                numClustersPerLevel.size(), centroids.size(), leafRecords.size(), vectorDimension);

        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory());

        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);
        ctx.setDataRecords(leafRecords);

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();

            testUtils.buildStaticStructure(ctx);
            LOGGER.info("Static structure built with {} centroids", centroids.size());

            testUtils.bulkLoadRecords(ctx);
            int bulkLoadedCount = leafRecords.stream().mapToInt(List::size).sum();
            LOGGER.info("Bulk loaded {} records across {} clusters", bulkLoadedCount, leafRecords.size());

            List<List<ITupleReference>> insertRecords =
                    generateInsertRecords(centroids, centroidSerdes, centroidsPerCluster, vectorDimension);
            int insertedCount = insertRecordsIntoMemoryComponent(ctx, insertRecords);
            LOGGER.info("Inserted {} records into memory component", insertedCount);

            // Query near first leaf centroid c10 at [20, 30, 20].
            double[] queryVector = { 20.0, 30.0, 20.0 };
            int queryK = 500;
            verifyRecordsWithSearch(ctx, queryVector, queryK);
            LOGGER.info("Verification: Found records from both bulk-loaded and inserted components");
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /**
     * Generate insert records for all leaf centroids.
     * Extracts leaf centroid vectors from the centroids list and generates
     * INSERT_RECORDS_PER_CLUSTER records per leaf centroid.
     *
     * Insert tuple format: <vector, primary_key>
     */
    private List<List<ITupleReference>> generateInsertRecords(List<ITupleReference> centroids,
            ISerializerDeserializer[] centroidSerdes, List<List<Integer>> centroidsPerCluster, int vectorDimension)
            throws Exception {

        // Determine leaf centroid count from last level of structure
        List<Integer> lastLevelClusters = centroidsPerCluster.get(centroidsPerCluster.size() - 1);
        int numLeafCentroids = lastLevelClusters.stream().mapToInt(Integer::intValue).sum();
        int firstLeafCentroidIndex = centroids.size() - numLeafCentroids;

        List<List<ITupleReference>> allRecords = new ArrayList<>();

        for (int i = 0; i < numLeafCentroids; i++) {
            // Deserialize centroid tuple to extract ID and vector
            ITupleReference centroidTuple = centroids.get(firstLeafCentroidIndex + i);
            Object[] values = TupleUtils.deserializeTuple(centroidTuple, centroidSerdes);
            int centroidId = (Integer) values[0];
            double[] centroidVector = (double[]) values[1];

            List<ITupleReference> clusterRecords = new ArrayList<>();
            double baseDistance = 0.15;
            int recordCount = 0;

            while (recordCount < INSERT_RECORDS_PER_CLUSTER) {
                double currentDistance = baseDistance;

                // 6 records per ring (±x, ±y, ±z directions) for 3D
                double[][] offsets = { { currentDistance, 0, 0 }, { -currentDistance, 0, 0 }, { 0, currentDistance, 0 },
                        { 0, -currentDistance, 0 }, { 0, 0, currentDistance }, { 0, 0, -currentDistance } };

                for (double[] offset : offsets) {
                    if (recordCount >= INSERT_RECORDS_PER_CLUSTER)
                        break;

                    double[] vector = new double[vectorDimension];
                    for (int d = 0; d < vectorDimension; d++) {
                        vector[d] = centroidVector[d] + offset[d];
                    }

                    String primaryKey = "pk_ins_c" + centroidId + "_" + recordCount;
                    ITupleReference tuple = createInsertTuple(vector, primaryKey);
                    clusterRecords.add(tuple);
                    recordCount++;
                }

                baseDistance += 0.15;
            }

            allRecords.add(clusterRecords);
        }

        return allRecords;
    }

    /**
     * Create an insert tuple.
     * Format: <vector, primary_key>
     */
    private ITupleReference createInsertTuple(double[] vector, String primaryKey) throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }

    /**
     * Insert records into the memory component using the index accessor.
     */
    private int insertRecordsIntoMemoryComponent(AbstractVectorTreeTestContext ctx,
            List<List<ITupleReference>> insertRecords) throws Exception {

        IIndexAccessor accessor = ctx.getIndex().createAccessor(
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));

        int insertedCount = 0;
        for (List<ITupleReference> clusterRecords : insertRecords) {
            for (ITupleReference tuple : clusterRecords) {
                accessor.insert(tuple);
                insertedCount++;
            }
        }

        LOGGER.info("Inserted {} records via accessor", insertedCount);
        return insertedCount;
    }

    /**
     * Verify records by scanning with LSMVTreeSearchCursor.
     * Checks that records from both disk (bulk-loaded) and memory (inserted) components are found.
     */
    private void verifyRecordsWithSearch(AbstractVectorTreeTestContext ctx, double[] queryVector, int k)
            throws Exception {

        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setMinProbeFraction(0.4);
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(k);

        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        try {
            accessor.search(cursor, predicate);

            List<String> foundPKs = new ArrayList<>();
            int bulkLoadCount = 0;
            int insertCount = 0;

            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                String pk = extractPrimaryKeyFromTuple(tuple);
                foundPKs.add(pk);

                if (pk.startsWith("pk_ins_")) {
                    insertCount++;
                } else {
                    bulkLoadCount++;
                }
            }

            LOGGER.info("Search returned {} total records: {} bulk-loaded, {} inserted", foundPKs.size(), bulkLoadCount,
                    insertCount);

            // Verify we got records from both components
            assertTrue("Should find bulk-loaded records", bulkLoadCount > 0);
            assertTrue("Should find inserted records", insertCount > 0);

            int sampleSize = Math.min(10, foundPKs.size());
            LOGGER.info("Sample of found PKs: {}", foundPKs.subList(0, sampleSize));

        } finally {
            cursor.close();
            cursor.destroy();
        }
    }

    /**
     * Extract primary key from a result tuple.
     * Standard result tuple format: <distance, centroid_id, primary_key>
     * PK is at field index 2.
     */
    private String extractPrimaryKeyFromTuple(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (String) values[2];
    }
}
