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

package org.apache.hyracks.storage.am.vector;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeDiskComponent;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeTopKSearchCursor;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.am.vector.utils.NoOpVectorQuantizer;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings({ "rawtypes", "deprecation" })
public class VectorTreeTestUtils extends TreeIndexTestUtils {
    private static final Logger LOGGER = LogManager.getLogger();

    public void buildStaticStructure(AbstractVectorTreeTestContext ctx) throws Exception {
        int numLevels = ctx.getNumCentroidsPerLevel().size();
        List<Integer> clustersPerLevel = ctx.getNumClustersPerLevel();
        List<List<Integer>> centroidsPerCluster = ctx.getNumCentroidsPerLevel();
        List<ITupleReference> centroids = ctx.getStaticStructureCentroids();

        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();

        // Create parameters map for static structure bulk load
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("numLevels", numLevels);
        parameters.put("clustersPerLevel", clustersPerLevel);
        parameters.put("centroidsPerCluster", centroidsPerCluster);
        parameters.put("maxEntriesPerPage", 100); // Default max entries per page

        // Create static structure bulk loader with parameters
        IIndexBulkLoader ssBuilder = lsmvTree.createBulkLoader(1.0f, false, centroids.size(), parameters);

        // Add centroids to the builder level by level (BFS order)
        for (ITupleReference tuple : centroids) {
            ssBuilder.add(tuple);
        }

        // Finalize the static structure
        ssBuilder.end();

        LOGGER.info("Static structure built successfully with {} centroids across {} levels", centroids.size(),
                numLevels);
    }

    public void bulkLoadRecords(AbstractVectorTreeTestContext ctx) throws Exception {
        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();
        List<List<ITupleReference>> dataRecords = ctx.getDataRecords();

        if (dataRecords == null || dataRecords.isEmpty()) {
            LOGGER.warn("No data records to bulk load");
            return;
        }

        // Create empty parameters map - createBulkLoader will add static_structure_component automatically
        // when it detects this is NOT a static structure load (no numLevels, clustersPerLevel, centroidsPerCluster)
        Map<String, Object> parameters = new HashMap<>();

        // Calculate total number of records for hint
        long totalRecords = 0;
        for (List<ITupleReference> clusterRecords : dataRecords) {
            totalRecords += clusterRecords.size();
        }

        // Create data bulk loader (not static structure)
        // The LSMVTree.createBulkLoader will automatically detect this is a data load
        // because parameters don't contain numLevels/clustersPerLevel/centroidsPerCluster
        IIndexBulkLoader bulkLoader = lsmvTree.createBulkLoader(1.0f, false, totalRecords, parameters);

        // Add all data records from all clusters
        for (List<ITupleReference> clusterRecords : dataRecords) {
            for (ITupleReference record : clusterRecords) {
                bulkLoader.add(record);
            }
        }

        // Finalize the data component
        bulkLoader.end();

        LOGGER.info("Bulk loaded {} records across {} clusters", totalRecords, dataRecords.size());
    }

    /**
     * Test cursor iteration and validate it returns expected records.
     * Validates that the cursor can find records with the correct tuple structure.
     *
     * Following RTree test pattern: use ctx.getIndexAccessor() for LSM-level searches
     * that coordinate across all components (memory + disk).
     *
     * Sets up the environment properly like the production VTreeSearchOperatorNodePushable:
     * 1. Creates a query tuple containing the vector
     * 2. Sets up the predicate with the query tuple and field index
     * 3. Creates an accessor with IVTreeBinaryAccessorFactory in its parameters
     */
    public void scanClosestLeafCluster(AbstractVectorTreeTestContext ctx) throws Exception {
        double[] queryVector = { 20.0d, 20.0d, 15.0d };

        // 1. Create query tuple containing the vector (like VTreeSearchOperatorNodePushable)
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // 2. Set up predicate with query tuple reference (following RTree pattern)
        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0); // Vector is at field 0

        // 3. Create accessor with IVTreeBinaryAccessorFactory in parameters
        // This is what VTreeSearchOperatorNodePushable does in addAdditionalIndexAccessorParams()
        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);
        assertNotNull("Cursor should be created", cursor);

        try {
            // Open cursor with predicate (positions cursor at first result)
            accessor.search(cursor, predicate);

            try {
                // Collect all results from cursor
                List<ITupleReference> results = new ArrayList<>();
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference tuple = cursor.getTuple();
                    assertNotNull("Tuple should not be null", tuple);
                    results.add(tuple);
                }

                // Validate we got at least some results
                assertFalse("Should find some records in the cluster", results.isEmpty());

                LOGGER.info("Found {} records in cluster for query vector [{}, {}, {}]", results.size(), queryVector[0],
                        queryVector[1], queryVector[2]);

                printTupleResults(results);

                // Validate cursor state after iteration
                assertFalse("Cursor should not have more results after iteration", cursor.hasNext());

            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
    }

    /**
     * Print tuple results from the data pages.
     * Data record format (from VectorIndexTestDriver.createBulkLoadRecordTuple):
     *   <distance_to_centroid: raw double (8 bytes),
     *    centroid_id: raw int (4 bytes),
     *    primary_key: UTF8String>
     *
     * Note: Fields 0 and 1 use raw types (no ADM type tags).
     */
    private void printTupleResults(List<ITupleReference> results) throws HyracksDataException {
        // Field serializers matching the raw tuple format
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE, // Field 0: distance (raw double)
                IntegerSerializerDeserializer.INSTANCE, // Field 1: centroidId (raw int)
                new UTF8StringSerializerDeserializer() // Field 2: primary_key
        };

        for (ITupleReference tuple : results) {
            try {
                Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
                double distance = (Double) values[0];
                int centroidId = (Integer) values[1];
                String primaryKey = (String) values[2];

                LOGGER.trace(" Record: pk='{}', centroidId={}, distance={}", primaryKey, centroidId, distance);
            } catch (Exception e) {
                LOGGER.error("Failed to deserialize tuple: {}", e.getMessage());
            }
        }
    }

    /**
     * Test top-K search with LSM-level accessor.
     * Following RTree test pattern for proper cursor lifecycle management.
     *
     * Sets up the environment properly like the production VTreeSearchOperatorNodePushable.
     *
     * Query vector {20.0, 30.0, 20.0} matches c10 from VectorIndexTestDriver.LEAF_CENTROIDS.
     * With K=100, we expect to get all 100 records from c10's cluster.
     */
    public void topKSearch(AbstractVectorTreeTestContext ctx) throws Exception {
        // Query vector matching c10: {20.0, 30.0, 20.0} from VectorIndexTestDriver
        double[] queryVector = { 20.0d, 30.0d, 20.0d };
        int k = 100; // Number of nearest neighbors to return

        // 1. Create query tuple containing the vector
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // 2. Set up predicate with query tuple reference and K value
        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(k); // Set K for top-K ANN search
        predicate.setEpsilon(0.0); // No level-wise cross-pollination

        // 3. Create accessor with IVTreeBinaryAccessorFactory in parameters
        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        try {
            // Open cursor with predicate
            accessor.search(cursor, predicate);

            try {
                // Deserialize tuples during iteration (ITupleReference is a shared mutable object
                // that gets repositioned on each next() call, so we must extract values immediately)
                ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };

                Set<String> seenPKs = new HashSet<>();
                int resultCount = 0;

                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference tuple = cursor.getTuple();

                    // Deserialize immediately before next() repositions the tuple reference
                    Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
                    double distance = (Double) values[0];
                    int centroidId = (Integer) values[1];
                    String primaryKey = (String) values[2];

                    LOGGER.trace(" Record: pk='{}', centroidId={}, distance={}", primaryKey, centroidId, distance);

                    assertEquals("All results should be from c10 (centroidId=10)", 10, centroidId);
                    assertTrue("Duplicate PK found: " + primaryKey, seenPKs.add(primaryKey));

                    resultCount++;
                }

                LOGGER.info("Top-K Search: Found {} unique records for query vector [{}, {}, {}] with K={}",
                        resultCount, queryVector[0], queryVector[1], queryVector[2], k);

                // Validate we got the expected number of results
                assertEquals("Top-K search should return K=" + k + " records from c10's cluster", k, resultCount);

                LOGGER.info("Top-K Search: All {} records verified to be from c10 cluster with unique PKs",
                        resultCount);

            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
    }

    /**
     * Test top-K search using LSMVTreeTopKSearchCursor with sequential cluster scanning
     * and top-K window collection.
     */
    public void naiveBlockedSearch(AbstractVectorTreeTestContext ctx) throws Exception {
        // Get query configuration from context
        double[] queryVector = ctx.getQueryVector();
        int k = ctx.getQueryK();
        List<String> expectedPKs = ctx.getExpectedPrimaryKeys();

        if (queryVector == null) {
            throw new IllegalStateException("Query vector must be set in context via ctx.setQueryVector()");
        }

        // 1. Create query tuple containing the vector
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // 2. Set up predicate with query tuple reference and K value
        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(k);

        // 3. Create accessor with IVTreeBinaryAccessorFactory in parameters.
        // Pass NoOpVectorQuantizer so the cursor can dequantize test embeddings
        // (test mode stores full-precision vectors as "quantized" embeddings).
        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);
        iap.getParameters().put(IVTreeQuantizer.IAP_KEY, NoOpVectorQuantizer.INSTANCE);
        // Opt in to LSMVTreeTopKSearchCursor — this test exercises the production top-K search
        // path. Other test verification paths (e.g. verifyInsertedRecords) leave the flag unset
        // and get the streaming LSMVTreeSearchCursor.
        iap.getParameters().put(LSMVTreeTopKSearchCursor.IAP_KEY, Boolean.TRUE);
        // LSMVTreeTopKSearchCursor's SpillableTopKBuffer requires a real IHyracksTaskContext for
        // frame allocation and disk spill. Tests must populate ctx.setHyracksTaskContext(...)
        // from the test harness; otherwise SpillableTopKBuffer's ctor will NPE on getInitialFrameSize().
        iap.getParameters().put(HyracksConstants.HYRACKS_TASK_CONTEXT, ctx.getHyracksTaskContext());

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        // Verify we got the naive blocked cursor
        LOGGER.info("Created cursor type: {}", cursor.getClass().getSimpleName());

        try {
            // Open cursor with predicate
            accessor.search(cursor, predicate);

            try {
                // Collect results from cursor. IMPORTANT: cursor.getTuple() returns a mutable
                // reference owned by the cursor (per-source ref in merge mode). Storing the
                // reference alone is unsafe — by the time we read it after the loop ends the
                // underlying state has been mutated to whichever entry the cursor last
                // emitted from that source. Extract every per-tuple value we will assert on
                // (PK, distance-to-centroid, vector) at emission time, while the cursor's
                // reference is still pointing at THIS tuple.
                List<String> actualPKs = new ArrayList<>();
                List<Double> actualDistancesToCentroid = new ArrayList<>();
                List<double[]> actualVectors = new ArrayList<>();
                int resultsCount = 0;
                while (cursor.hasNext()) {
                    cursor.next();
                    ITupleReference tuple = cursor.getTuple();
                    actualPKs.add(extractPrimaryKeyFromOptimizedTuple(tuple));
                    actualDistancesToCentroid.add(extractDistanceFromTuple(tuple));
                    actualVectors.add(extractVectorFromOptimizedTuple(tuple));
                    resultsCount++;
                }

                LOGGER.info("Naive Blocked Search: Found {} records for query vector {} with K={}", resultsCount,
                        Arrays.toString(queryVector), k);

                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("[naiveBlockedSearch] Results PKs:");
                    for (int i = 0; i < resultsCount; i++) {
                        double dqx = computeEuclideanDistance(queryVector, actualVectors.get(i));
                        LOGGER.trace("  [{}] pk={}, D(x,C)={}, D(q,x)={}, vec={}", i, actualPKs.get(i),
                                actualDistancesToCentroid.get(i), dqx, Arrays.toString(actualVectors.get(i)));
                    }
                }

                // Validate we got the expected number of results
                assertEquals("Naive blocked search should return K=" + k + " records", k, resultsCount);

                if (expectedPKs != null && !expectedPKs.isEmpty()) {
                    for (String expectedPK : expectedPKs) {
                        assertTrue("Expected " + expectedPK + " in results, but got: " + actualPKs,
                                actualPKs.contains(expectedPK));
                    }
                }

                // Validate excluded primary keys are NOT in results (for delete tests)
                List<String> excludedPKs = ctx.getExcludedPrimaryKeys();
                if (excludedPKs != null && !excludedPKs.isEmpty()) {
                    for (String excludedPK : excludedPKs) {
                        assertFalse("Deleted " + excludedPK + " should NOT be in results, but got: " + actualPKs,
                                actualPKs.contains(excludedPK));
                    }
                }

                LOGGER.info("Naive Blocked Search: All {} results verified correctly", resultsCount);

            } finally {
                cursor.close();
            }
        } finally {
            cursor.destroy();
        }
    }

    /**
     * Insert records into the memory component using the index accessor.
     *
     * @param ctx Test context with active index
     * @param insertRecords Records grouped by cluster, tuple format: <vector, primary_key>
     * @return Number of records inserted
     */
    public int insertRecordsIntoMemoryComponent(AbstractVectorTreeTestContext ctx,
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
     * Delete records from the index using the index accessor.
     * Delete tuple format: <vector, primary_key> (same as insert).
     *
     * @param ctx Test context with active index
     * @param deleteTuples Tuples to delete, format: <vector, primary_key>
     * @return Number of records deleted
     */
    public int deleteRecordsFromIndex(AbstractVectorTreeTestContext ctx, List<ITupleReference> deleteTuples)
            throws Exception {

        IIndexAccessor accessor = ctx.getIndex().createAccessor(
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));

        int deletedCount = 0;
        for (ITupleReference tuple : deleteTuples) {
            accessor.delete(tuple);
            deletedCount++;
        }

        LOGGER.info("Deleted {} records via accessor", deletedCount);
        return deletedCount;
    }

    /**
     * Verify records by scanning with LSMVTreeSearchCursor (regular, non-optimized).
     * Checks that records from both disk (bulk-loaded, "pk_2d_" or "pk_opt_" prefix)
     * and memory (inserted, "pk_ins_" prefix) components are found.
     *
     * @param ctx Test context with active index
     * @param queryVector Query vector for search
     * @param k Number of results to retrieve
     */
    public void verifyRecordsWithSearch(AbstractVectorTreeTestContext ctx, double[] queryVector, int k)
            throws Exception {

        // Create query tuple
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // Set up predicate
        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(k);

        // Create accessor with vector accessor factory
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
                String pk = extractPrimaryKeyFromOptimizedTuple(tuple);
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
     * Extract distance_to_centroid (field 0) from tuple.
     */
    private double extractDistanceFromTuple(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (Double) values[0];
    }

    /**
     * Extract vector (field 3: quantized_embedding) from optimized search tuple.
     * Tuple format: <distance, centroid_id, quantized_distance, quantized_embedding, primary_key>
     * Field 3 is stored as ByteArrayPointable: VarLen length prefix + raw big-endian doubles.
     */
    private double[] extractVectorFromOptimizedTuple(ITupleReference tuple) throws HyracksDataException {
        int fieldIndex = 3;
        byte[] data = tuple.getFieldData(fieldIndex);
        int offset = tuple.getFieldStart(fieldIndex);
        int contentLength = org.apache.hyracks.data.std.primitive.ByteArrayPointable.getContentLength(data, offset);
        int prefixSize =
                org.apache.hyracks.data.std.primitive.ByteArrayPointable.getNumberBytesToStoreMeta(contentLength);
        int contentOffset = offset + prefixSize;
        int count = contentLength / Double.BYTES;
        double[] vector = new double[count];
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(data, contentOffset, contentLength);
        for (int i = 0; i < count; i++) {
            vector[i] = buf.getDouble();
        }
        return vector;
    }

    /**
     * Extract primary_key (field 4) from optimized search tuple.
     * Tuple format: <distance, centroid_id, quantized_distance, quantized_embedding, primary_key>
     */
    public String extractPrimaryKeyFromOptimizedTuple(ITupleReference tuple) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = { DoubleSerializerDeserializer.INSTANCE, // Field 0: distance
                IntegerSerializerDeserializer.INSTANCE, // Field 1: centroidId
                DoubleSerializerDeserializer.INSTANCE, // Field 2: quantized_distance
                org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer.INSTANCE, // Field 3: quantized_embedding
                new UTF8StringSerializerDeserializer() // Field 4: primary_key
        };
        Object[] values = TupleUtils.deserializeTuple(tuple, fieldSerdes);
        return (String) values[4];
    }

    /**
     * Compute Euclidean distance between two vectors.
     */
    private double computeEuclideanDistance(double[] v1, double[] v2) {
        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            double diff = v1[i] - v2[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    public void clusterRecords(AbstractVectorTreeTestContext ctx) throws Exception {
        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();
        LSMVTreeDiskComponent staticStructure = lsmvTree.getStaticStructure();
        IIndexAccessor accessor = staticStructure.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        VTree.VTreeAccessor vcTreeAccessor = (VTree.VTreeAccessor) accessor;
        IVTreeDistanceFunction defaultDistanceFunction =
                TestVTreeDistanceFunctionFactory.INSTANCE.createDistanceFunction();
        vcTreeAccessor.findClosestLeafCentroid(new double[] { 21.0d, 31.0d, 21.0d }, defaultDistanceFunction);
    }

    @Override
    protected CheckTuple createCheckTuple(int numFields, int numKeyFields) {
        return new VectorCheckTuple(numFields, numKeyFields);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected CheckTuple createIntCheckTuple(int[] fieldValues, int numKeyFields) {
        VectorCheckTuple checkTuple = new VectorCheckTuple(fieldValues.length, numKeyFields);
        for (int v : fieldValues) {
            checkTuple.appendField((Comparable) Integer.valueOf(v));
        }
        return checkTuple;
    }

    @Override
    protected void setIntKeyFields(int[] fieldValues, int numKeyFields, int maxValue, Random rnd) {
        for (int j = 0; j < numKeyFields; j++) {
            fieldValues[j] = rnd.nextInt() % maxValue;
        }
    }

    @Override
    protected void setIntPayloadFields(int[] fieldValues, int numKeyFields, int numFields) {
        for (int j = numKeyFields; j < numFields; j++) {
            fieldValues[j] = j;
        }
    }

    @Override
    protected Collection createCheckTuplesCollection() {
        return new TreeSet<>();
    }

    @Override
    protected ArrayTupleBuilder createDeleteTupleBuilder(IIndexTestContext ctx) {
        return new ArrayTupleBuilder(ctx.getKeyFieldCount());
    }

    @Override
    protected ISearchPredicate createNullSearchPredicate() {
        return null;
    }

    @Override
    public void checkExpectedResults(IIndexCursor cursor, Collection checkTuples, ISerializerDeserializer[] fieldSerdes,
            int keyFieldCount, Iterator<CheckTuple> checkIter) throws Exception {
        // Implementation will be added based on vector-specific requirements
        throw new UnsupportedOperationException("Vector-specific implementation needed");
    }

    @Override
    protected boolean checkDiskOrderScanResult(ITupleReference tuple, CheckTuple checkTuple, IIndexTestContext ctx) {
        // Placeholder for vector-specific disk-order scan validation.
        return true;
    }
}
