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
package org.apache.hyracks.storage.am.lsm.vector.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.BulkLoadRecordFormat;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.junit.Test;

/**
 * Test driver for optimized search (LSMVTreePrunedTopKSearchCursor) tests.
 *
 * Generates test data with tuple format: <distance_to_centroid, centroid_id, quantized_distance, quantized_embedding, primary_key>
 * This format includes the vector field required for computing D(q, x) during optimized search.
 *
 * Separated from VectorIndexTestDriver to avoid tuple format conflicts with bulk load tests.
 */
@SuppressWarnings("rawtypes")
public abstract class QuantizedSearchTestDriver {

    /**
     * A single query case: query vector, K, expected primary keys, and optionally excluded primary keys.
     * Multiple query cases can be passed to runTest() to verify different search scenarios
     * (e.g., single-cluster vs multi-cluster, post-delete) within the same test dataset.
     */
    protected static class QueryCase {
        public final double[] queryVector;
        public final int queryK;
        public final List<String> expectedPrimaryKeys;
        public final List<String> excludedPrimaryKeys; // null = no exclusion check

        public QueryCase(double[] queryVector, int queryK, List<String> expectedPrimaryKeys) {
            this(queryVector, queryK, expectedPrimaryKeys, null);
        }

        public QueryCase(double[] queryVector, int queryK, List<String> expectedPrimaryKeys,
                List<String> excludedPrimaryKeys) {
            this.queryVector = queryVector;
            this.queryK = queryK;
            this.expectedPrimaryKeys = expectedPrimaryKeys;
            this.excludedPrimaryKeys = excludedPrimaryKeys;
        }
    }

    // ==================== Test Structures ====================

    protected static final VectorTestStructure STRUCT_3D = VectorTestStructure.threeDim1Centroid();
    protected static final VectorTestStructure STRUCT_2D = VectorTestStructure.twoDim2Level();

    // ==================== Insert Test Support ====================

    protected static final int INSERT_RECORDS_PER_CLUSTER = 40;

    protected abstract void runTest(ISerializerDeserializer[] centroidSerdes,
            ISerializerDeserializer[] dataRecordSerdes, List<ITupleReference> centroids,
            List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster, int vectorDimension,
            List<List<ITupleReference>> leafRecords, List<QueryCase> queryCases) throws Exception;

    /**
     * Build query cases for the 3D single-centroid test.
     * Default: query at [5,0,0], K=5, expect records at distances 3-7 from origin.
     * Subclasses override to provide insert-aware or alternative query cases.
     */
    protected List<QueryCase> build3DQueryCases() {
        List<QueryCase> queryCases = new ArrayList<>();
        queryCases.add(new QueryCase(new double[] { 5.0, 0.0, 0.0 }, 5,
                Arrays.asList("pk_opt_5", "pk_opt_4", "pk_opt_6", "pk_opt_3", "pk_opt_7")));
        return queryCases;
    }

    /**
     * Build query cases for the 2D two-layer test.
     * Default: query at [30,30], K=5, expect top-5 bulk-loaded records from c4.
     * Subclasses override to provide insert-aware or multi-cluster query cases.
     */
    protected List<QueryCase> build2DQueryCases() {
        List<QueryCase> queryCases = new ArrayList<>();
        // Query at [30, 30] closest to centroid c4 at [25, 25]
        // D(q, c4) = sqrt((30-25)^2 + (30-25)^2) = sqrt(50) ~ 7.07
        // Expected top-5: records from c4 farthest from centroid in +x/+y directions (closest to query)
        // pk_2d_c4_48: [27.6,25], d=2.6, D(q,x)=sqrt(30.76)~5.55
        // pk_2d_c4_44: [27.4,25], d=2.4, D(q,x)=sqrt(31.76)~5.64
        // pk_2d_c4_46: [25,27.4], d=2.4, D(q,x)=sqrt(31.76)~5.64
        // pk_2d_c4_40: [27.2,25], d=2.2, D(q,x)=sqrt(32.84)~5.73
        // pk_2d_c4_42: [25,27.2], d=2.2, D(q,x)=sqrt(32.84)~5.73
        queryCases.add(new QueryCase(new double[] { 30.0, 30.0 }, 5,
                Arrays.asList("pk_2d_c4_48", "pk_2d_c4_44", "pk_2d_c4_46", "pk_2d_c4_40", "pk_2d_c4_42")));
        return queryCases;
    }

    /**
     * Test data for optimized search (LSMVTreePrunedTopKSearchCursor).
     * Uses a simpler single-cluster structure for testing bidirectional traversal
     * and triangle inequality termination.
     *
     * Centroid at origin [0, 0, 0].
     * Records placed at integer distances 1-20 along x-axis.
     * Query at [5, 0, 0] gives D(q, C) = 5.0, pivot between records at D(x,C)=5 and D(x,C)=6.
     *
     * Tuple format: <distance_to_centroid, centroid_id, quantized_distance, quantized_embedding, primary_key>
     */
    @Test
    public void optimizedSearchThreeDimension() throws Exception {
        ISerializerDeserializer[] centroidSerdes = STRUCT_3D.getCentroidSerdes();
        ISerializerDeserializer[] dataRecordSerdes = STRUCT_3D.getDataRecordSerdes(BulkLoadRecordFormat.QUANTIZED);

        List<ITupleReference> centroids = STRUCT_3D.buildCentroidTuples();

        // Generate records with controlled distances (special linear layout, not concentric rings)
        List<List<ITupleReference>> dataRecords = generateOptimizedSearchRecords();

        List<QueryCase> queryCases = build3DQueryCases();

        runTest(centroidSerdes, dataRecordSerdes, centroids, STRUCT_3D.getNumClustersPerLevel(),
                STRUCT_3D.getCentroidsPerCluster(), STRUCT_3D.getVectorDimension(), dataRecords, queryCases);
    }

    /**
     * Test data for 2D two-layer optimized search.
     * Uses a grid structure with 4 quadrants, each subdivided into 4 sub-squares.
     *
     * Structure:
     * - Level 0: 1 cluster with 4 centroids (quadrant centers)
     * - Level 1: 4 clusters with 4 centroids each (16 leaf centroids total)
     *
     * Data records: 50 records per leaf centroid = 800 total records
     * Tuple format: <distance_to_centroid, centroid_id, quantized_distance, quantized_embedding, primary_key>
     */
    @Test
    public void twoDimensionTwoLevels() throws Exception {
        ISerializerDeserializer[] centroidSerdes = STRUCT_2D.getCentroidSerdes();
        ISerializerDeserializer[] dataRecordSerdes = STRUCT_2D.getDataRecordSerdes(BulkLoadRecordFormat.QUANTIZED);

        List<ITupleReference> centroids = STRUCT_2D.buildCentroidTuples();

        // Generate 50 records per leaf centroid using concentric ring pattern
        List<List<ITupleReference>> dataRecords = STRUCT_2D.generateBulkLoadRecords(BulkLoadRecordFormat.QUANTIZED, 50);

        List<QueryCase> queryCases = build2DQueryCases();

        runTest(centroidSerdes, dataRecordSerdes, centroids, STRUCT_2D.getNumClustersPerLevel(),
                STRUCT_2D.getCentroidsPerCluster(), STRUCT_2D.getVectorDimension(), dataRecords, queryCases);
    }

    // ==================== Delete Test Support ====================

    /**
     * Generate delete tuples from known vectors and primary keys.
     * Delete tuple format: <vector, primary_key> (same as insert).
     *
     * @param vectors Array of vector values for records to delete
     * @param primaryKeys Array of primary key strings for records to delete
     * @return List of delete tuples
     */
    protected List<ITupleReference> generateDeleteTuples(double[][] vectors, String[] primaryKeys) throws Exception {
        if (vectors.length != primaryKeys.length) {
            throw new IllegalArgumentException("vectors and primaryKeys must have same length");
        }

        List<ITupleReference> deleteTuples = new ArrayList<>();
        for (int i = 0; i < vectors.length; i++) {
            // Delete tuple format is same as insert: <vector, primary_key>
            deleteTuples.add(VectorTestStructure.createInsertTuple(vectors[i], primaryKeys[i]));
        }
        return deleteTuples;
    }

    // ==================== 3D Special Record Generation ====================

    /**
     * Generate records for optimized search testing.
     * 20 records at integer distances 1-20 along x-axis from centroid at origin.
     *
     * With query at [5, 0, 0] (D(q,C) = 5.0):
     * - Record at D(x,C) = 1: vector [1,0,0], D(q,x) = 4.0
     * - Record at D(x,C) = 2: vector [2,0,0], D(q,x) = 3.0
     * - Record at D(x,C) = 3: vector [3,0,0], D(q,x) = 2.0
     * - Record at D(x,C) = 4: vector [4,0,0], D(q,x) = 1.0
     * - Record at D(x,C) = 5: vector [5,0,0], D(q,x) = 0.0  <- PIVOT (closest to query)
     * - Record at D(x,C) = 6: vector [6,0,0], D(q,x) = 1.0
     * - Record at D(x,C) = 7: vector [7,0,0], D(q,x) = 2.0
     * - ... etc
     */
    private List<List<ITupleReference>> generateOptimizedSearchRecords() throws Exception {
        List<List<ITupleReference>> allRecords = new ArrayList<>();
        List<ITupleReference> clusterRecords = new ArrayList<>();

        int centroidId = 0; // Single centroid with ID 0

        // Generate 20 records at integer distances 1-20 along x-axis
        for (int i = 1; i <= 20; i++) {
            double distance = i;
            double[] vector = { distance, 0.0, 0.0 }; // Vector at [i, 0, 0]
            String primaryKey = "pk_opt_" + i;

            ITupleReference tuple = createOptimizedSearchRecordTuple(distance, centroidId, vector, primaryKey);
            clusterRecords.add(tuple);
        }

        allRecords.add(clusterRecords);
        return allRecords;
    }

    /**
     * Helper method to create a record tuple for optimized search testing.
     * Format: <distance_to_centroid, centroid_id, quantized_distance, quantized_embedding, primary_key>
     *
     * This format allows LSMVTreePrunedTopKSearchCursor to:
     * 1. Extract D(x,C) from field 0 for pivot finding and priority queue ordering
     * 2. Extract centroid_id from field 1 for cluster validation
     * 3. Extract quantized_distance from field 2 for pruning
     * 4. Extract quantized_embedding from field 3 for computing D(q,x) via IVTreeBinaryAccessor
     * 5. Extract primary key from field 4 for result identification
     */
    public static ITupleReference createOptimizedSearchRecordTuple(double distance, int centroidId, double[] vector,
            String primaryKey) throws Exception {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(5);
        ArrayTupleReference tupleRef = new ArrayTupleReference();

        // Field 0: distance_to_centroid (raw double - 8 bytes)
        tupleBuilder.getDataOutput().writeDouble(distance);
        tupleBuilder.addFieldEndOffset();

        // Field 1: centroid_id (raw int - 4 bytes)
        tupleBuilder.getDataOutput().writeInt(centroidId);
        tupleBuilder.addFieldEndOffset();

        // Field 2: quantized_distance (same as distance in test mode)
        tupleBuilder.getDataOutput().writeDouble(distance);
        tupleBuilder.addFieldEndOffset();

        // Field 3: quantized_embedding as ByteArrayPointable (VarLen prefix + raw big-endian doubles)
        ByteBuffer buf = ByteBuffer.allocate(vector.length * Double.BYTES);
        for (double d : vector) {
            buf.putDouble(d);
        }
        byte[] rawDoubles = buf.array();
        int metaLen = ByteArrayPointable.getNumberBytesToStoreMeta(rawDoubles.length);
        byte[] meta = new byte[metaLen];
        VarLenIntEncoderDecoder.encode(rawDoubles.length, meta, 0);
        tupleBuilder.getDataOutput().write(meta);
        tupleBuilder.getDataOutput().write(rawDoubles);
        tupleBuilder.addFieldEndOffset();

        // Field 4: primary_key (UTF8 string)
        new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tupleRef;
    }
}
