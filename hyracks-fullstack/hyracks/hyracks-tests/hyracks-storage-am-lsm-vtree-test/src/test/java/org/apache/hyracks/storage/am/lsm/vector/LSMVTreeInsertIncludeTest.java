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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.BulkLoadRecordFormat;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.IncludeFieldValueGenerator;
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
import org.junit.Test;

/**
 * LSMVTree insert test with integer include fields.
 * Tests insert operations with include fields in the data record format.
 *
 * Data tuple format: <distance, centroid_id, primary_key, include_value>
 * Insert tuple format: <vector, include_value, primary_key>
 *
 * Include values are generated deterministically using a formula based on centroidId and recordIndex.
 * After bulk loading and inserting, the test searches and verifies:
 * 1. Records from both disk (bulk-loaded) and memory (inserted) components are found
 * 2. Include field values match the expected deterministic values
 * 3. Pre-counted distribution of include values is correct
 */
@SuppressWarnings("rawtypes")
public class LSMVTreeInsertIncludeTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    private static final int RECORDS_PER_CENTROID = 100;
    private static final int INSERT_RECORDS_PER_CLUSTER = 30;
    private static final int INCLUDE_VALUE_RANGE = 10;

    /**
     * Deterministic include value generator.
     * Generates integer values in [0, INCLUDE_VALUE_RANGE) based on centroidId and recordIndex.
     */
    private static final IncludeFieldValueGenerator INCLUDE_GENERATOR = (centroidId,
            recordIndex) -> new Object[] { Math.abs((centroidId * 31 + recordIndex * 17) % INCLUDE_VALUE_RANGE) };

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Test
    public void threeDimensionThreeLevelsWithIncludes() throws Exception {
        ISerializerDeserializer[] includeSerdes = { IntegerSerializerDeserializer.INSTANCE };
        VectorTestStructure struct =
                VectorTestStructure.threeDim3Level().withIncludeFields(includeSerdes, INCLUDE_GENERATOR);

        ISerializerDeserializer[] centroidSerdes = struct.getCentroidSerdes();
        ISerializerDeserializer[] dataRecordSerdes =
                struct.getDataRecordSerdes(BulkLoadRecordFormat.NAIVE_WITH_INCLUDES);

        List<ITupleReference> centroids = struct.buildCentroidTuples();
        List<List<ITupleReference>> bulkLoadRecords =
                struct.generateBulkLoadRecords(BulkLoadRecordFormat.NAIVE_WITH_INCLUDES, RECORDS_PER_CENTROID);

        LOGGER.info("InsertInclude Test: {} centroids, {}D vectors, {} records/centroid, 1 integer include field",
                centroids.size(), struct.getVectorDimension(), RECORDS_PER_CENTROID);

        Map<Integer, Integer> expectedBulkLoadCounts = preCountIncludeValues(10, RECORDS_PER_CENTROID);
        LOGGER.info("Pre-counted include value distribution for c10 bulk load: {}", expectedBulkLoadCounts);

        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, struct.getVectorDimension(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(), 1);

        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(struct.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(struct.getCentroidsPerCluster());
        ctx.setDataRecords(bulkLoadRecords);

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();

            testUtils.buildStaticStructure(ctx);
            LOGGER.info("Static structure built with {} centroids", centroids.size());

            testUtils.bulkLoadRecords(ctx);
            int bulkLoadedCount = bulkLoadRecords.stream().mapToInt(List::size).sum();
            LOGGER.info("Bulk loaded {} records with include fields", bulkLoadedCount);

            List<List<ITupleReference>> insertRecords = struct.generateInsertRecords(INSERT_RECORDS_PER_CLUSTER);
            int insertedCount = testUtils.insertRecordsIntoMemoryComponent(ctx, insertRecords);
            LOGGER.info("Inserted {} records with include fields", insertedCount);

            double[] queryVector = { 20.0, 30.0, 20.0 };
            int queryK = 500;
            verifyIncludeFieldValues(ctx, queryVector, queryK, expectedBulkLoadCounts);
            LOGGER.info("Verification: Include field values correct in search results");
        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }

    /**
     * Pre-count include value distribution for a given centroid.
     *
     * @param centroidId Centroid ID (e.g., 10 for c10)
     * @param numRecords Number of records for this centroid
     * @return Map of include_value → count
     */
    private Map<Integer, Integer> preCountIncludeValues(int centroidId, int numRecords) {
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < numRecords; i++) {
            int includeValue = Math.abs((centroidId * 31 + i * 17) % INCLUDE_VALUE_RANGE);
            counts.merge(includeValue, 1, Integer::sum);
        }
        return counts;
    }

    /**
     * Verify include field values in search results.
     * Checks that:
     * 1. Results from both bulk-loaded and inserted components are found
     * 2. Include field values match the expected deterministic values
     * 3. Distribution of include values for c10 bulk load records matches pre-counted values
     */
    private void verifyIncludeFieldValues(AbstractVectorTreeTestContext ctx, double[] queryVector, int k,
            Map<Integer, Integer> expectedBulkLoadCounts) throws Exception {

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

            int bulkLoadCount = 0;
            int insertCount = 0;
            int includeFieldMismatch = 0;
            Map<Integer, Integer> actualBulkLoadCounts = new HashMap<>();

            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                Object[] values = extractFieldsWithInclude(tuple);
                String pk = (String) values[0];
                int includeValue = (Integer) values[1];

                if (pk.startsWith("pk_ins_")) {
                    insertCount++;
                    // Verify insert record include value
                    int expectedValue = computeExpectedInsertIncludeValue(pk);
                    if (includeValue != expectedValue) {
                        LOGGER.warn("Include mismatch for {}: expected={}, actual={}", pk, expectedValue, includeValue);
                        includeFieldMismatch++;
                    }
                } else if (pk.startsWith("pk_c_10_")) {
                    bulkLoadCount++;
                    // Verify bulk load record include value
                    int recordIndex = extractRecordIndex(pk);
                    int expectedValue = Math.abs((10 * 31 + recordIndex * 17) % INCLUDE_VALUE_RANGE);
                    if (includeValue != expectedValue) {
                        LOGGER.warn("Include mismatch for {}: expected={}, actual={}", pk, expectedValue, includeValue);
                        includeFieldMismatch++;
                    }
                    actualBulkLoadCounts.merge(includeValue, 1, Integer::sum);
                }
            }

            LOGGER.info("Search returned {} bulk-loaded c10 records, {} inserted records, {} include mismatches",
                    bulkLoadCount, insertCount, includeFieldMismatch);

            // Verify records from both components
            assertTrue("Should find bulk-loaded records", bulkLoadCount > 0);
            assertTrue("Should find inserted records", insertCount > 0);

            // Verify no include field mismatches
            assertEquals("All include field values should match expected", 0, includeFieldMismatch);

            // Verify include value distribution for c10 bulk load records
            assertEquals("Include value distribution should match pre-counted", expectedBulkLoadCounts,
                    actualBulkLoadCounts);
            LOGGER.info("Include value distribution verified: {}", actualBulkLoadCounts);

        } finally {
            cursor.close();
            cursor.destroy();
        }
    }

    /**
     * Extract PK and include field from result tuple.
     * Result tuple format (NAIVE_WITH_INCLUDES): <distance, centroid_id, primary_key, include_value>
     *
     * @return Object[]{pk (String), includeValue (Integer)}
     */
    private Object[] extractFieldsWithInclude(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes =
                { DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return new Object[] { values[2], values[3] }; // pk, includeValue
    }

    /**
     * Compute expected include value for an insert record based on its PK.
     * Insert PK format: "pk_ins_c{centroidId}_{recordIndex}"
     */
    private int computeExpectedInsertIncludeValue(String pk) {
        // Parse "pk_ins_c{centroidId}_{recordIndex}"
        String[] parts = pk.substring("pk_ins_c".length()).split("_");
        int centroidId = Integer.parseInt(parts[0]);
        int recordIndex = Integer.parseInt(parts[1]);
        return Math.abs((centroidId * 31 + recordIndex * 17) % INCLUDE_VALUE_RANGE);
    }

    /**
     * Extract record index from PK.
     * PK format: "pk_c_10_42" → recordIndex = 42
     */
    private int extractRecordIndex(String pk) {
        String suffix = pk.substring(pk.lastIndexOf('_') + 1);
        return Integer.parseInt(suffix);
    }
}
