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
 * LSMVTree delete test (standard, non-quantized).
 * Tests delete operations (antimatter tuples) after bulk loading the first disk component.
 *
 * Inherits test case from VectorIndexTestDriver:
 * - threeDimensionThreeLevels(): 3D three-layer structure (3 levels, 24 leaf centroids, 2400 bulk-loaded records)
 *
 * The test bulk loads the dataset, then deletes specific records from centroid c10 by inserting
 * antimatter tuples into the memory component. Verification confirms that deleted records are
 * absent from search results while non-deleted records remain accessible.
 *
 * Data tuple format (standard): <distance, centroid_id, primary_key>
 * Delete tuple format: <vector, primary_key> (same as insert)
 */
public class LSMVTreeDeleteTest extends VectorIndexTestDriver {

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
     * Implementation of runTest from VectorIndexTestDriver.
     * Performs: build static structure → bulk load → delete → verify.
     */
    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords) throws Exception {

        LOGGER.info("LSMVTree Delete Test: {} levels, {} centroids, {} leaf clusters, {}D vectors",
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

            // Delete records at first ring around c10 ([20.0, 30.0, 20.0]).
            List<ITupleReference> deleteTuples = generateDeleteTuples();
            List<String> deletedPKs = getDeletedPKs();
            int deletedCount = deleteRecords(ctx, deleteTuples);
            LOGGER.info("Deleted {} records (antimatter tuples) from c10", deletedCount);

            double[] queryVector = { 20.0, 30.0, 20.0 };
            int queryK = 500;
            verifyDeletedRecordsAbsent(ctx, queryVector, queryK, deletedPKs);
            LOGGER.info("Verification: Deleted records confirmed absent from search results");
        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }

    /**
     * Generate delete tuples for known records from c10.
     * c10 centroid: [20.0, 30.0, 20.0]
     * Records at distance 0.2 in first ring:
     *   pk_c_10_0: [20.2, 30.0, 20.0] (offset [+0.2, 0, 0])
     *   pk_c_10_1: [19.8, 30.0, 20.0] (offset [-0.2, 0, 0])
     *   pk_c_10_2: [20.0, 30.2, 20.0] (offset [0, +0.2, 0])
     *   pk_c_10_3: [20.0, 29.8, 20.0] (offset [0, -0.2, 0])
     *   pk_c_10_4: [20.0, 30.0, 20.2] (offset [0, 0, +0.2])
     *
     * Delete tuple format: <vector, primary_key>
     */
    private List<ITupleReference> generateDeleteTuples() throws Exception {
        double[][] vectors = { { 20.2, 30.0, 20.0 }, { 19.8, 30.0, 20.0 }, { 20.0, 30.2, 20.0 }, { 20.0, 29.8, 20.0 },
                { 20.0, 30.0, 20.2 } };
        String[] primaryKeys = { "pk_c_10_0", "pk_c_10_1", "pk_c_10_2", "pk_c_10_3", "pk_c_10_4" };

        List<ITupleReference> deleteTuples = new ArrayList<>();
        for (int i = 0; i < vectors.length; i++) {
            deleteTuples.add(createDeleteTuple(vectors[i], primaryKeys[i]));
        }
        return deleteTuples;
    }

    /**
     * Get the list of PKs that were deleted, for verification.
     */
    private List<String> getDeletedPKs() {
        return Arrays.asList("pk_c_10_0", "pk_c_10_1", "pk_c_10_2", "pk_c_10_3", "pk_c_10_4");
    }

    /**
     * Create a delete tuple. Format: <vector, primary_key>
     */
    private ITupleReference createDeleteTuple(double[] vector, String primaryKey) throws Exception {
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
     * Verify that deleted records are absent from search results.
     * Also verify that non-deleted records from c10 are still found.
     */
    private void verifyDeletedRecordsAbsent(AbstractVectorTreeTestContext ctx, double[] queryVector, int k,
            List<String> deletedPKs) throws Exception {

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
            int c10Count = 0;

            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                String pk = extractPrimaryKeyFromTuple(tuple);
                foundPKs.add(pk);

                if (pk.startsWith("pk_c_10_")) {
                    c10Count++;
                }
            }

            LOGGER.info("Search returned {} total records, {} from c10", foundPKs.size(), c10Count);

            // Verify deleted PKs are NOT in results
            for (String deletedPK : deletedPKs) {
                assertFalse("Deleted record " + deletedPK + " should not be in search results",
                        foundPKs.contains(deletedPK));
            }

            // Verify some non-deleted c10 records ARE in results
            assertTrue("Should find non-deleted c10 records", c10Count > 0);

            // c10 originally has 100 records, we deleted 5, so expect 95 from c10
            assertEquals("Should find 95 non-deleted c10 records", 95, c10Count);

            LOGGER.info("Verification passed: {} deleted records absent, {} c10 records found", deletedPKs.size(),
                    c10Count);

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
