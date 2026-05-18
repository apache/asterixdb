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
import java.util.Arrays;
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
 * LSMVTree delete test with integer include fields.
 * Tests delete operations (antimatter tuples) with include fields in the data record format.
 *
 * Data tuple format: <distance, centroid_id, primary_key, include_value>
 * Delete tuple format: <vector, include_value, primary_key>
 *
 * Flow:
 * 1. Bulk load records with include fields
 * 2. Delete specific records from c10 (antimatter tuples with include fields)
 * 3. Verify deleted records are absent while remaining records have correct include values
 */
@SuppressWarnings("rawtypes")
public class LSMVTreeDeleteIncludeTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    private static final int RECORDS_PER_CENTROID = 100;
    private static final int INCLUDE_VALUE_RANGE = 10;

    /**
     * Deterministic include value generator (same as InsertIncludeTest).
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
    public void threeDimensionThreeLevelsDeleteWithIncludes() throws Exception {
        ISerializerDeserializer[] includeSerdes = { IntegerSerializerDeserializer.INSTANCE };
        VectorTestStructure struct =
                VectorTestStructure.threeDim3Level().withIncludeFields(includeSerdes, INCLUDE_GENERATOR);

        ISerializerDeserializer[] dataRecordSerdes =
                struct.getDataRecordSerdes(BulkLoadRecordFormat.NAIVE_WITH_INCLUDES);

        List<ITupleReference> centroids = struct.buildCentroidTuples();
        List<List<ITupleReference>> bulkLoadRecords =
                struct.generateBulkLoadRecords(BulkLoadRecordFormat.NAIVE_WITH_INCLUDES, RECORDS_PER_CENTROID);

        LOGGER.info("DeleteInclude Test: {} centroids, {}D vectors, {} records/centroid, 1 integer include field",
                centroids.size(), struct.getVectorDimension(), RECORDS_PER_CENTROID);

        Map<Integer, Integer> fullCounts = preCountIncludeValues(10, RECORDS_PER_CENTROID);
        List<String> deletedPKs = Arrays.asList("pk_c_10_0", "pk_c_10_1", "pk_c_10_2", "pk_c_10_3", "pk_c_10_4");

        // Adjust expected counts after deleting the first 5 records from c10.
        Map<Integer, Integer> expectedCountsAfterDelete = new HashMap<>(fullCounts);
        for (int i = 0; i < 5; i++) {
            int includeValue = Math.abs((10 * 31 + i * 17) % INCLUDE_VALUE_RANGE);
            expectedCountsAfterDelete.merge(includeValue, -1, Integer::sum);
            if (expectedCountsAfterDelete.get(includeValue) == 0) {
                expectedCountsAfterDelete.remove(includeValue);
            }
        }
        LOGGER.info("Expected include value distribution after delete: {}", expectedCountsAfterDelete);

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

            List<ITupleReference> deleteTuples = generateDeleteTuplesWithInclude();
            int deletedCount = deleteRecords(ctx, deleteTuples);
            LOGGER.info("Deleted {} records with antimatter tuples", deletedCount);

            double[] queryVector = { 20.0, 30.0, 20.0 };
            int queryK = 500;
            verifyDeletedAndIncludeValues(ctx, queryVector, queryK, deletedPKs, expectedCountsAfterDelete);
            LOGGER.info("Verification: Deleted records absent, include values correct");
        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }

    /**
     * Pre-count include value distribution for a given centroid.
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
     * Generate delete tuples with include fields for the first 5 records from c10.
     * c10 centroid: [20.0, 30.0, 20.0]
     *
     * Delete tuple format: <vector, include_value, primary_key>
     *
     * Records at distance 0.2 in first ring:
     *   pk_c_10_0: [20.2, 30.0, 20.0], include = (10*31 + 0*17) % 10 = 0
     *   pk_c_10_1: [19.8, 30.0, 20.0], include = (10*31 + 1*17) % 10 = 7
     *   pk_c_10_2: [20.0, 30.2, 20.0], include = (10*31 + 2*17) % 10 = 4
     *   pk_c_10_3: [20.0, 29.8, 20.0], include = (10*31 + 3*17) % 10 = 1
     *   pk_c_10_4: [20.0, 30.0, 20.2], include = (10*31 + 4*17) % 10 = 8
     */
    @SuppressWarnings("unchecked")
    private List<ITupleReference> generateDeleteTuplesWithInclude() throws Exception {
        double[][] vectors = { { 20.2, 30.0, 20.0 }, { 19.8, 30.0, 20.0 }, { 20.0, 30.2, 20.0 }, { 20.0, 29.8, 20.0 },
                { 20.0, 30.0, 20.2 } };
        String[] primaryKeys = { "pk_c_10_0", "pk_c_10_1", "pk_c_10_2", "pk_c_10_3", "pk_c_10_4" };

        List<ITupleReference> deleteTuples = new ArrayList<>();
        for (int i = 0; i < vectors.length; i++) {
            int includeValue = Math.abs((10 * 31 + i * 17) % INCLUDE_VALUE_RANGE);
            deleteTuples.add(createDeleteTupleWithInclude(vectors[i], includeValue, primaryKeys[i]));
        }
        return deleteTuples;
    }

    /**
     * Create a delete tuple with include field.
     * Format: <vector, include_value, primary_key>
     */
    @SuppressWarnings("unchecked")
    private ITupleReference createDeleteTupleWithInclude(double[] vector, int includeValue, String primaryKey)
            throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        IntegerSerializerDeserializer.INSTANCE.serialize(includeValue, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }

    /**
     * Delete records using the index accessor.
     */
    private int deleteRecords(AbstractVectorTreeTestContext ctx, List<ITupleReference> deleteTuples) throws Exception {
        IIndexAccessor accessor = ctx.getIndex().createAccessor(
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));

        int deletedCount = 0;
        for (ITupleReference tuple : deleteTuples) {
            accessor.delete(tuple);
            deletedCount++;
        }
        return deletedCount;
    }

    /**
     * Verify that deleted records are absent and remaining records have correct include values.
     */
    private void verifyDeletedAndIncludeValues(AbstractVectorTreeTestContext ctx, double[] queryVector, int k,
            List<String> deletedPKs, Map<Integer, Integer> expectedCountsAfterDelete) throws Exception {

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

            int c10Count = 0;
            int includeFieldMismatch = 0;
            Map<Integer, Integer> actualCounts = new HashMap<>();

            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                Object[] values = extractFieldsWithInclude(tuple);
                String pk = (String) values[0];
                int includeValue = (Integer) values[1];

                // Verify deleted PKs are absent
                assertFalse("Deleted record " + pk + " should not be in results", deletedPKs.contains(pk));

                if (pk.startsWith("pk_c_10_")) {
                    c10Count++;

                    // Verify include field value
                    int recordIndex = extractRecordIndex(pk);
                    int expectedValue = Math.abs((10 * 31 + recordIndex * 17) % INCLUDE_VALUE_RANGE);
                    if (includeValue != expectedValue) {
                        LOGGER.warn("Include mismatch for {}: expected={}, actual={}", pk, expectedValue, includeValue);
                        includeFieldMismatch++;
                    }
                    actualCounts.merge(includeValue, 1, Integer::sum);
                }
            }

            LOGGER.info("Search returned {} c10 records (expected {}), {} include mismatches", c10Count,
                    RECORDS_PER_CENTROID - deletedPKs.size(), includeFieldMismatch);

            // Verify correct count (100 - 5 deleted = 95)
            assertEquals("Should find 95 non-deleted c10 records", RECORDS_PER_CENTROID - deletedPKs.size(), c10Count);

            // Verify no include field mismatches
            assertEquals("All include field values should match expected", 0, includeFieldMismatch);

            // Verify include value distribution after deletion
            assertEquals("Include value distribution should match expected after delete", expectedCountsAfterDelete,
                    actualCounts);
            LOGGER.info("Include value distribution after delete verified: {}", actualCounts);

        } finally {
            cursor.close();
            cursor.destroy();
        }
    }

    /**
     * Extract PK and include field from result tuple.
     * Result tuple format (NAIVE_WITH_INCLUDES): <distance, centroid_id, primary_key, include_value>
     */
    private Object[] extractFieldsWithInclude(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes =
                { DoubleSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                        new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return new Object[] { values[2], values[3] }; // pk, includeValue
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
