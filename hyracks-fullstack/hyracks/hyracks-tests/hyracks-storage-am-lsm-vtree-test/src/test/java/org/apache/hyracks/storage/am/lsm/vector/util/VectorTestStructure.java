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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.vector.utils.VTreeLeafNeighborList;

/**
 * Represents a vector index test dataset: tree structure + data generation.
 * Decoupled from test operations (bulkload/insert/search) and record format (naive/quantized).
 *
 * Usage:
 * <pre>
 * VectorTestStructure dataset = VectorTestStructure.threeDim3Level();
 * List&lt;ITupleReference&gt; centroids = dataset.buildCentroidTuples();
 * List&lt;List&lt;ITupleReference&gt;&gt; records = dataset.generateBulkLoadRecords(BulkLoadRecordFormat.NAIVE, 100);
 * </pre>
 */
@SuppressWarnings("rawtypes")
public class VectorTestStructure {

    /**
     * Determines bulk load record tuple layout.
     */
    public enum BulkLoadRecordFormat {
        /** Naive format: {@code <distance, centroid_id, pk>} */
        NAIVE,
        /** Naive format with include fields: {@code <distance, centroid_id, pk, include_fields...>} */
        NAIVE_WITH_INCLUDES,
        /** Quantized format: {@code <distance, centroid_id, quantized_distance, quantized_embedding, pk, include_fields...>} */
        QUANTIZED
    }

    /**
     * Generates include field values for a single record.
     * Called during bulk load record generation when format requires include fields.
     */
    @FunctionalInterface
    public interface IncludeFieldValueGenerator {
        /**
         * @param centroidId Global centroid ID (e.g., 10 for c10)
         * @param recordIndex Record index within this centroid's cluster (0-based)
         * @return Array of include field values matching the configured includeFieldSerdes
         */
        Object[] generate(int centroidId, int recordIndex);
    }

    private final int vectorDimension;
    private final double[][] leafCentroids;
    private final int firstLeafCentroidId;
    private final List<double[]> allCentroidVectors;
    private final List<Integer> allCentroidIds;
    private final List<Integer> numClustersPerLevel;
    private final List<List<Integer>> centroidsPerCluster;

    // Include field configuration (null = no include fields)
    private final ISerializerDeserializer[] includeFieldSerdes;
    private final IncludeFieldValueGenerator includeFieldValueGenerator;

    // PK prefix for bulk load records (default "pk_c_")
    private final String bulkLoadPkPrefix;

    private VectorTestStructure(int vectorDimension, double[][] leafCentroids, int firstLeafCentroidId,
            List<double[]> allCentroidVectors, List<Integer> allCentroidIds, List<Integer> numClustersPerLevel,
            List<List<Integer>> centroidsPerCluster, ISerializerDeserializer[] includeFieldSerdes,
            IncludeFieldValueGenerator includeFieldValueGenerator, String bulkLoadPkPrefix) {
        this.vectorDimension = vectorDimension;
        this.leafCentroids = leafCentroids;
        this.firstLeafCentroidId = firstLeafCentroidId;
        this.allCentroidVectors = allCentroidVectors;
        this.allCentroidIds = allCentroidIds;
        this.numClustersPerLevel = numClustersPerLevel;
        this.centroidsPerCluster = centroidsPerCluster;
        this.includeFieldSerdes = includeFieldSerdes;
        this.includeFieldValueGenerator = includeFieldValueGenerator;
        this.bulkLoadPkPrefix = bulkLoadPkPrefix;
    }

    // ===== Include Field Configuration =====

    /**
     * Return a new dataset with include field configuration.
     * The same structure is reused; only the include field config differs.
     *
     * @param serdes Serializers for the include fields (e.g., IntegerSerializerDeserializer, UTF8StringSerializerDeserializer)
     * @param generator Function that produces include field values for each record
     * @return A new VectorTestStructure with include fields configured
     */
    public VectorTestStructure withIncludeFields(ISerializerDeserializer[] serdes,
            IncludeFieldValueGenerator generator) {
        return new VectorTestStructure(vectorDimension, leafCentroids, firstLeafCentroidId, allCentroidVectors,
                allCentroidIds, numClustersPerLevel, centroidsPerCluster, serdes, generator, bulkLoadPkPrefix);
    }

    /**
     * Return a new dataset with a custom PK prefix for bulk load records.
     *
     * @param prefix PK prefix (e.g., "pk_2d_c" produces "pk_2d_c4_0", "pk_2d_c4_1", ...)
     * @return A new VectorTestStructure with the specified PK prefix
     */
    public VectorTestStructure withBulkLoadPkPrefix(String prefix) {
        return new VectorTestStructure(vectorDimension, leafCentroids, firstLeafCentroidId, allCentroidVectors,
                allCentroidIds, numClustersPerLevel, centroidsPerCluster, includeFieldSerdes,
                includeFieldValueGenerator, prefix);
    }

    /**
     * @return Number of configured include fields, or 0 if none
     */
    public int getNumIncludeFields() {
        return includeFieldSerdes != null ? includeFieldSerdes.length : 0;
    }

    // ===== Pre-built Factory Methods =====

    /**
     * 3D three-level dataset: 34 centroids (2+8+24), 24 leaf centroids in 8 octants.
     *
     * Level 0: 1 cluster with 2 centroids (split on z-axis)
     * Level 1: 2 clusters with 4 centroids each (8 octant centers)
     * Level 2: 8 clusters with 3 centroids each (24 leaf centroids)
     */
    public static VectorTestStructure threeDim3Level() {
        // Leaf centroids (c10 ~ c33)
        double[][] leafCentroids = {
                // Cluster 2.1: c10, c11, c12 (+x, +y, +z)
                { 20.0, 30.0, 20.0 }, { 20.0, 20.0, 30.0 }, { 35.0, 25.0, 25.0 },
                // Cluster 2.2: c13, c14, c15 (-x, +y, +z)
                { -20.0, 30.0, 20.0 }, { -20.0, 20.0, 30.0 }, { -35.0, 25.0, 25.0 },
                // Cluster 2.3: c16, c17, c18 (-x, -y, +z)
                { -20.0, -30.0, 20.0 }, { -20.0, -20.0, 30.0 }, { -35.0, -25.0, 25.0 },
                // Cluster 2.4: c19, c20, c21 (+x, -y, +z)
                { 20.0, -30.0, 20.0 }, { 20.0, -20.0, 30.0 }, { 35.0, -25.0, 25.0 },
                // Cluster 2.5: c22, c23, c24 (+x, +y, -z)
                { 20.0, 30.0, -20.0 }, { 20.0, 20.0, -30.0 }, { 35.0, 25.0, -25.0 },
                // Cluster 2.6: c25, c26, c27 (-x, +y, -z)
                { -20.0, 30.0, -20.0 }, { -20.0, 20.0, -30.0 }, { -35.0, 25.0, -25.0 },
                // Cluster 2.7: c28, c29, c30 (-x, -y, -z)
                { -20.0, -30.0, -20.0 }, { -20.0, -20.0, -30.0 }, { -35.0, -25.0, -25.0 },
                // Cluster 2.8: c31, c32, c33 (+x, -y, -z)
                { 20.0, -30.0, -20.0 }, { 20.0, -20.0, -30.0 }, { 35.0, -25.0, -25.0 } };

        // All centroids: root (2) + interior (8) + leaf (24)
        List<double[]> allVectors = new ArrayList<>();
        List<Integer> allIds = new ArrayList<>();

        // Level 0: 2 centroids splitting on z-axis
        allIds.add(0);
        allVectors.add(new double[] { 0.0, 0.0, 50.0 });
        allIds.add(1);
        allVectors.add(new double[] { 0.0, 0.0, -50.0 });

        // Level 1: 8 centroids in 2 clusters (4 each)
        allIds.add(2);
        allVectors.add(new double[] { 25.0, 25.0, 25.0 });
        allIds.add(3);
        allVectors.add(new double[] { -25.0, 25.0, 25.0 });
        allIds.add(4);
        allVectors.add(new double[] { -25.0, -25.0, 25.0 });
        allIds.add(5);
        allVectors.add(new double[] { 25.0, -25.0, 25.0 });
        allIds.add(6);
        allVectors.add(new double[] { 25.0, 25.0, -25.0 });
        allIds.add(7);
        allVectors.add(new double[] { -25.0, 25.0, -25.0 });
        allIds.add(8);
        allVectors.add(new double[] { -25.0, -25.0, -25.0 });
        allIds.add(9);
        allVectors.add(new double[] { 25.0, -25.0, -25.0 });

        // Level 2: 24 leaf centroids (c10 ~ c33)
        for (int i = 0; i < leafCentroids.length; i++) {
            allIds.add(i + 10);
            allVectors.add(leafCentroids[i]);
        }

        List<Integer> numClustersPerLevel = Arrays.asList(1, 2, 8);
        List<List<Integer>> centroidsPerCluster =
                Arrays.asList(Arrays.asList(2), Arrays.asList(4, 4), Arrays.asList(3, 3, 3, 3, 3, 3, 3, 3));

        return new VectorTestStructure(3, leafCentroids, 10, allVectors, allIds, numClustersPerLevel,
                centroidsPerCluster, null, null, "pk_c_");
    }

    /**
     * 2D two-level dataset: 20 centroids (4 root + 16 leaf), 16 leaf centroids in 4 quadrants.
     *
     * Level 0: 1 cluster with 4 centroids (quadrant centers at ±50)
     *   c0: [50,50], c1: [-50,50], c2: [-50,-50], c3: [50,-50]
     *
     * Level 1: 4 clusters with 4 centroids each (sub-grid at ±25, ±75)
     *   Q1 (under c0): c4[25,25], c5[75,25], c6[25,75], c7[75,75]
     *   Q2 (under c1): c8[-75,25], c9[-25,25], c10[-75,75], c11[-25,75]
     *   Q3 (under c2): c12[-75,-75], c13[-25,-75], c14[-75,-25], c15[-25,-25]
     *   Q4 (under c3): c16[25,-75], c17[75,-75], c18[25,-25], c19[75,-25]
     */
    public static VectorTestStructure twoDim2Level() {
        // 16 leaf centroids (c4 ~ c19)
        double[][] leafCentroids = {
                // Q1 (under c0): c4, c5, c6, c7
                { 25.0, 25.0 }, { 75.0, 25.0 }, { 25.0, 75.0 }, { 75.0, 75.0 },
                // Q2 (under c1): c8, c9, c10, c11
                { -75.0, 25.0 }, { -25.0, 25.0 }, { -75.0, 75.0 }, { -25.0, 75.0 },
                // Q3 (under c2): c12, c13, c14, c15
                { -75.0, -75.0 }, { -25.0, -75.0 }, { -75.0, -25.0 }, { -25.0, -25.0 },
                // Q4 (under c3): c16, c17, c18, c19
                { 25.0, -75.0 }, { 75.0, -75.0 }, { 25.0, -25.0 }, { 75.0, -25.0 } };

        List<double[]> allVectors = new ArrayList<>();
        List<Integer> allIds = new ArrayList<>();

        // Level 0: 4 quadrant centroids (c0-c3)
        allIds.add(0);
        allVectors.add(new double[] { 50.0, 50.0 });
        allIds.add(1);
        allVectors.add(new double[] { -50.0, 50.0 });
        allIds.add(2);
        allVectors.add(new double[] { -50.0, -50.0 });
        allIds.add(3);
        allVectors.add(new double[] { 50.0, -50.0 });

        // Level 1: 16 leaf centroids (c4 ~ c19)
        for (int i = 0; i < leafCentroids.length; i++) {
            allIds.add(i + 4);
            allVectors.add(leafCentroids[i]);
        }

        List<Integer> numClustersPerLevel = Arrays.asList(1, 4);
        List<List<Integer>> centroidsPerCluster = Arrays.asList(Arrays.asList(4), Arrays.asList(4, 4, 4, 4));

        return new VectorTestStructure(2, leafCentroids, 4, allVectors, allIds, numClustersPerLevel,
                centroidsPerCluster, null, null, "pk_2d_c");
    }

    /**
     * 3D single-centroid dataset: 1 centroid at origin [0,0,0].
     *
     * Level 0: 1 cluster with 1 centroid (c0 at origin)
     *
     * Note: This structure uses special linear record generation (not concentric rings).
     * Tests should use their own record generation and NOT {@link #generateBulkLoadRecords}.
     */
    public static VectorTestStructure threeDim1Centroid() {
        double[][] leafCentroids = { { 0.0, 0.0, 0.0 } };

        List<double[]> allVectors = new ArrayList<>();
        List<Integer> allIds = new ArrayList<>();

        allIds.add(0);
        allVectors.add(new double[] { 0.0, 0.0, 0.0 });

        List<Integer> numClustersPerLevel = Arrays.asList(1);
        List<List<Integer>> centroidsPerCluster = Arrays.asList(Arrays.asList(1));

        return new VectorTestStructure(3, leafCentroids, 0, allVectors, allIds, numClustersPerLevel,
                centroidsPerCluster, null, null, "pk_opt_");
    }

    // ===== Centroid Tuple Generation =====

    /**
     * Build centroid tuples for static structure building, in bottom-up order
     * (leaf level first, root last) as required by {@code VTreeStaticStructureBuilder}.
     * Format: {@code <centroid_id: int, vector: double[]>}.
     *
     * <p>The internal storage of {@code allCentroidIds}/{@code allCentroidVectors} is
     * BFS-from-root (root entries first, then interior, then leaf). We compute level
     * boundaries from {@code centroidsPerCluster} and emit levels in reverse — the
     * centroid IDs themselves are unchanged (root keeps ids 0..N_root-1, leaves keep
     * the highest ids), only the emission order differs.
     */
    public List<ITupleReference> buildCentroidTuples() throws HyracksDataException {
        int levels = numClustersPerLevel.size();
        int[] levelStart = new int[levels + 1];
        for (int L = 0; L < levels; L++) {
            int countInLevel = 0;
            for (int c : centroidsPerCluster.get(L)) {
                countInLevel += c;
            }
            levelStart[L + 1] = levelStart[L] + countInLevel;
        }

        List<ITupleReference> tuples = new ArrayList<>();
        for (int L = levels - 1; L >= 0; L--) {
            for (int i = levelStart[L]; i < levelStart[L + 1]; i++) {
                tuples.add(createCentroidTuple(allCentroidIds.get(i), allCentroidVectors.get(i)));
            }
        }
        return tuples;
    }

    /**
     * Build centroid tuples in bottom-up order like {@link #buildCentroidTuples()}, but emit leaf
     * centroids in the quantized + graph-neighbors layout {@code <cid, embedding, quantizedBytes,
     * neighborList>} so the static-structure builder stores a neighbor field on each leaf record.
     * Interior centroids keep the plain {@code <cid, embedding>} layout.
     *
     * <p>The neighbor list is encoded in provisional form (see {@link VTreeLeafNeighborList}): each
     * neighbor centroid id becomes a {@code [centroidId, SENTINEL]} entry. The quantized-bytes field
     * is a deterministic placeholder (not exercised by neighbor tests).
     *
     * @param neighborsByLeafCid maps a leaf centroid id to the centroid ids of its neighbors; a leaf
     *                           id absent from the map gets an empty neighbor list.
     */
    public List<ITupleReference> buildCentroidTuplesWithLeafNeighbors(Map<Integer, int[]> neighborsByLeafCid)
            throws HyracksDataException {
        int levels = numClustersPerLevel.size();
        int[] levelStart = new int[levels + 1];
        for (int L = 0; L < levels; L++) {
            int countInLevel = 0;
            for (int c : centroidsPerCluster.get(L)) {
                countInLevel += c;
            }
            levelStart[L + 1] = levelStart[L] + countInLevel;
        }

        List<ITupleReference> tuples = new ArrayList<>();
        for (int L = levels - 1; L >= 0; L--) {
            boolean isLeafLevel = (L == levels - 1);
            for (int i = levelStart[L]; i < levelStart[L + 1]; i++) {
                int cid = allCentroidIds.get(i);
                double[] vec = allCentroidVectors.get(i);
                if (isLeafLevel) {
                    int[] neighbors = neighborsByLeafCid.getOrDefault(cid, new int[0]);
                    byte[] neighborList = VTreeLeafNeighborList.encodeProvisional(neighbors);
                    tuples.add(createLeafCentroidTupleWithNeighbors(cid, vec, neighborList));
                } else {
                    tuples.add(createCentroidTuple(cid, vec));
                }
            }
        }
        return tuples;
    }

    // ===== Bulk Load Record Generation =====

    /**
     * Generate bulk load records for all leaf centroids.
     * Records arranged in concentric rings around each centroid.
     *
     * @param format NAIVE creates {@code <dist, cid, pk>}, QUANTIZED creates {@code <dist, cid, quantized_dist, quantized_embedding, pk>}
     * @param recordsPerCentroid Number of records per leaf centroid
     * @return Records grouped by leaf centroid
     */
    public List<List<ITupleReference>> generateBulkLoadRecords(BulkLoadRecordFormat format, int recordsPerCentroid)
            throws HyracksDataException {
        List<List<ITupleReference>> allRecords = new ArrayList<>();

        for (int centroidIndex = 0; centroidIndex < leafCentroids.length; centroidIndex++) {
            List<ITupleReference> clusterRecords = new ArrayList<>();
            int centroidId = firstLeafCentroidId + centroidIndex;
            double[] centroid = leafCentroids[centroidIndex];

            double baseDistance = 0.2;
            int recordCount = 0;

            while (recordCount < recordsPerCentroid) {
                double currentDistance = baseDistance;
                double[][] offsets = generateDirectionOffsets(vectorDimension, currentDistance);

                for (double[] offset : offsets) {
                    if (recordCount >= recordsPerCentroid)
                        break;

                    String primaryKey = bulkLoadPkPrefix + centroidId + "_" + recordCount;

                    // Compute actual vector and distance from it to avoid floating-point
                    // precision mismatch with deleteVector() which computes distance from
                    // the actual vector coordinates
                    double[] vector = addVectors(centroid, offset);
                    double actualDistance = computeEuclideanDistance(vector, centroid);

                    switch (format) {
                        case NAIVE:
                            clusterRecords.add(createNaiveBulkLoadRecord(actualDistance, centroidId, primaryKey));
                            break;
                        case NAIVE_WITH_INCLUDES:
                            if (includeFieldSerdes == null || includeFieldValueGenerator == null) {
                                throw new IllegalStateException(
                                        "NAIVE_WITH_INCLUDES requires include fields configured via withIncludeFields()");
                            }
                            Object[] includeValues = includeFieldValueGenerator.generate(centroidId, recordCount);
                            clusterRecords.add(createNaiveWithIncludesBulkLoadRecord(actualDistance, centroidId,
                                    primaryKey, includeFieldSerdes, includeValues));
                            break;
                        case QUANTIZED:
                            clusterRecords
                                    .add(createQuantizedBulkLoadRecord(actualDistance, centroidId, vector, primaryKey));
                            break;
                        default:
                            throw new UnsupportedOperationException("Format not yet supported: " + format);
                    }
                    recordCount++;
                }

                baseDistance += 0.2;
            }

            allRecords.add(clusterRecords);
        }

        return allRecords;
    }

    // ===== Insert Record Generation =====

    /**
     * Generate insert records for all leaf centroids.
     * Each record: {@code <vector, pk>} — always same format regardless of quantization.
     *
     * @param recordsPerCentroid Records to generate per centroid
     * @return Records grouped by leaf centroid
     */
    public List<List<ITupleReference>> generateInsertRecords(int recordsPerCentroid) throws HyracksDataException {
        List<List<ITupleReference>> allRecords = new ArrayList<>();

        for (int centroidIndex = 0; centroidIndex < leafCentroids.length; centroidIndex++) {
            int centroidId = firstLeafCentroidId + centroidIndex;
            double[] centroid = leafCentroids[centroidIndex];

            List<ITupleReference> clusterRecords = new ArrayList<>();
            double baseDistance = 0.30;
            int recordCount = 0;

            while (recordCount < recordsPerCentroid) {
                double currentDistance = baseDistance;
                double[][] offsets = generateDirectionOffsets(vectorDimension, currentDistance);

                for (double[] offset : offsets) {
                    if (recordCount >= recordsPerCentroid)
                        break;

                    double[] vector = addVectors(centroid, offset);
                    String primaryKey = "pk_ins_c" + centroidId + "_" + recordCount;
                    if (includeFieldSerdes != null && includeFieldValueGenerator != null) {
                        Object[] includeValues = includeFieldValueGenerator.generate(centroidId, recordCount);
                        clusterRecords.add(
                                createInsertTupleWithIncludes(vector, primaryKey, includeFieldSerdes, includeValues));
                    } else {
                        clusterRecords.add(createInsertTuple(vector, primaryKey));
                    }
                    recordCount++;
                }

                baseDistance += 0.30;
            }

            allRecords.add(clusterRecords);
        }

        return allRecords;
    }

    /**
     * Generate insert records near a specific leaf centroid.
     * For targeted testing (e.g., multi-thread test targeting one cluster).
     *
     * @param centroidIndex Index into leafCentroids array
     * @param count Number of records
     * @param pkPrefix PK prefix (e.g., "mt_t0_")
     * @param seed Random seed for reproducibility
     * @return List of insert tuples, format: {@code <vector, pk>}
     */
    public List<ITupleReference> generateInsertRecordsNearCentroid(int centroidIndex, int count, String pkPrefix,
            long seed) throws HyracksDataException {
        double[] centroid = leafCentroids[centroidIndex];
        Random rnd = new Random(seed);
        List<ITupleReference> tuples = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            double[] vector = new double[vectorDimension];
            for (int d = 0; d < vectorDimension; d++) {
                vector[d] = centroid[d] + (rnd.nextDouble() - 0.5);
            }
            String primaryKey = pkPrefix + i;
            int centroidId = firstLeafCentroidId + centroidIndex;
            if (includeFieldSerdes != null && includeFieldValueGenerator != null) {
                Object[] includeValues = includeFieldValueGenerator.generate(centroidId, i);
                tuples.add(createInsertTupleWithIncludes(vector, primaryKey, includeFieldSerdes, includeValues));
            } else {
                tuples.add(createInsertTuple(vector, primaryKey));
            }
        }

        return tuples;
    }

    // ===== Structure Config Access =====

    public int getVectorDimension() {
        return vectorDimension;
    }

    public double[][] getLeafCentroids() {
        return leafCentroids;
    }

    public int getFirstLeafCentroidId() {
        return firstLeafCentroidId;
    }

    public int getNumLeafCentroids() {
        return leafCentroids.length;
    }

    public List<Integer> getNumClustersPerLevel() {
        return numClustersPerLevel;
    }

    public List<List<Integer>> getCentroidsPerCluster() {
        return centroidsPerCluster;
    }

    // ===== Serdes =====

    /** Centroid tuple serdes: always {@code <int, double[]>} */
    public ISerializerDeserializer[] getCentroidSerdes() {
        return new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                DoubleArraySerializerDeserializer.INSTANCE };
    }

    /** Data record serdes: depends on format */
    public ISerializerDeserializer[] getDataRecordSerdes(BulkLoadRecordFormat format) {
        switch (format) {
            case NAIVE:
                return new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
            case NAIVE_WITH_INCLUDES: {
                if (includeFieldSerdes == null) {
                    throw new IllegalStateException(
                            "NAIVE_WITH_INCLUDES requires include fields configured via withIncludeFields()");
                }
                // Format: <distance, centroid_id, pk, include_field_1, include_field_2, ...>
                int numFields = 3 + includeFieldSerdes.length;
                ISerializerDeserializer[] serdes = new ISerializerDeserializer[numFields];
                serdes[0] = DoubleSerializerDeserializer.INSTANCE;
                serdes[1] = IntegerSerializerDeserializer.INSTANCE;
                serdes[2] = new UTF8StringSerializerDeserializer();
                System.arraycopy(includeFieldSerdes, 0, serdes, 3, includeFieldSerdes.length);
                return serdes;
            }
            case QUANTIZED:
                return new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                        ByteArraySerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer() };
            default:
                throw new UnsupportedOperationException("Format not yet supported: " + format);
        }
    }

    // ===== Private Tuple Creation Helpers =====

    /**
     * Create centroid tuple: {@code <centroid_id: int, vector: double[]>}
     */
    static ITupleReference createCentroidTuple(int centroidId, double[] vector) throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tupleRef = new ArrayTupleReference();
        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, DoubleArraySerializerDeserializer.INSTANCE };
        Object[] fieldValues = { centroidId, vector };
        TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);
        return tupleRef;
    }

    /**
     * Create a quantized leaf centroid tuple carrying a graph-neighbor list:
     * {@code <cid: int, embedding: double[], quantizedBytes: ByteArray, neighborList: ByteArray>}.
     * The quantized-bytes field is a deterministic placeholder; the neighbor list is passed through
     * verbatim (already encoded by {@link VTreeLeafNeighborList}).
     */
    static ITupleReference createLeafCentroidTupleWithNeighbors(int centroidId, double[] vector, byte[] neighborList)
            throws HyracksDataException {
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
        ArrayTupleReference tupleRef = new ArrayTupleReference();
        byte[] quantizedPlaceholder = new byte[] { (byte) centroidId };
        ISerializerDeserializer[] fieldSerdes =
                { IntegerSerializerDeserializer.INSTANCE, DoubleArraySerializerDeserializer.INSTANCE,
                        ByteArraySerializerDeserializer.INSTANCE, ByteArraySerializerDeserializer.INSTANCE };
        Object[] fieldValues = { centroidId, vector, quantizedPlaceholder, neighborList };
        TupleUtils.createTuple(tupleBuilder, tupleRef, fieldSerdes, fieldValues);
        return tupleRef;
    }

    /**
     * Create naive bulk load record: {@code <distance: raw double, centroid_id: raw int, pk: UTF8String>}
     */
    static ITupleReference createNaiveBulkLoadRecord(double distance, int centroidId, String primaryKey)
            throws HyracksDataException {
        try {
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(3);
            ArrayTupleReference tupleRef = new ArrayTupleReference();

            tupleBuilder.getDataOutput().writeDouble(distance);
            tupleBuilder.addFieldEndOffset();
            tupleBuilder.getDataOutput().writeInt(centroidId);
            tupleBuilder.addFieldEndOffset();
            new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();

            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tupleRef;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Create naive bulk load record with include fields:
     * {@code <distance: raw double, centroid_id: raw int, pk: UTF8String, include_field_1, include_field_2, ...>}
     */
    @SuppressWarnings("unchecked")
    static ITupleReference createNaiveWithIncludesBulkLoadRecord(double distance, int centroidId, String primaryKey,
            ISerializerDeserializer[] includeSerdes, Object[] includeValues) throws HyracksDataException {
        try {
            int numFields = 3 + includeSerdes.length;
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(numFields);
            ArrayTupleReference tupleRef = new ArrayTupleReference();

            tupleBuilder.getDataOutput().writeDouble(distance);
            tupleBuilder.addFieldEndOffset();
            tupleBuilder.getDataOutput().writeInt(centroidId);
            tupleBuilder.addFieldEndOffset();
            new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();

            for (int i = 0; i < includeSerdes.length; i++) {
                includeSerdes[i].serialize(includeValues[i], tupleBuilder.getDataOutput());
                tupleBuilder.addFieldEndOffset();
            }

            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tupleRef;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Create quantized bulk load record:
     * {@code <distance: raw double, centroid_id: raw int, quantized_distance: raw double, quantized_embedding: ByteArrayPointable(raw doubles), pk: UTF8String>}
     */
    static ITupleReference createQuantizedBulkLoadRecord(double distance, int centroidId, double[] vector,
            String primaryKey) throws HyracksDataException {
        try {
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(5);
            ArrayTupleReference tupleRef = new ArrayTupleReference();

            // Field 0: distance_to_centroid
            tupleBuilder.getDataOutput().writeDouble(distance);
            tupleBuilder.addFieldEndOffset();
            // Field 1: centroid_id
            tupleBuilder.getDataOutput().writeInt(centroidId);
            tupleBuilder.addFieldEndOffset();
            // Field 2: quantized_distance (same as distance in test mode)
            tupleBuilder.getDataOutput().writeDouble(distance);
            tupleBuilder.addFieldEndOffset();
            // Field 3: quantized_embedding as ByteArrayPointable (VarLen prefix + raw doubles)
            // Consistent with production format (ByteArraySerializerDeserializer)
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(vector.length * Double.BYTES);
            for (double d : vector) {
                buf.putDouble(d);
            }
            byte[] rawDoubles = buf.array();
            int metaLen = org.apache.hyracks.data.std.primitive.ByteArrayPointable
                    .getNumberBytesToStoreMeta(rawDoubles.length);
            byte[] meta = new byte[metaLen];
            org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder.encode(rawDoubles.length, meta, 0);
            tupleBuilder.getDataOutput().write(meta);
            tupleBuilder.getDataOutput().write(rawDoubles);
            tupleBuilder.addFieldEndOffset();
            // Field 4: primary_key
            new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();

            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tupleRef;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Create insert tuple: {@code <vector: double[], pk: UTF8String>}
     */
    public static ITupleReference createInsertTuple(double[] vector, String primaryKey) throws HyracksDataException {
        return createInsertTupleWithIncludes(vector, primaryKey, null, null);
    }

    /**
     * Create insert tuple with include fields: {@code <vector: double[], include_field_1, ..., pk: UTF8String>}
     */
    @SuppressWarnings("unchecked")
    private static ITupleReference createInsertTupleWithIncludes(double[] vector, String primaryKey,
            ISerializerDeserializer[] includeSerdes, Object[] includeValues) throws HyracksDataException {
        try {
            int numIncludes = includeSerdes != null ? includeSerdes.length : 0;
            int numFields = 2 + numIncludes; // vector + include_fields... + pk
            ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(numFields);
            ArrayTupleReference tupleRef = new ArrayTupleReference();

            DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();

            for (int i = 0; i < numIncludes; i++) {
                includeSerdes[i].serialize(includeValues[i], tupleBuilder.getDataOutput());
                tupleBuilder.addFieldEndOffset();
            }

            new UTF8StringSerializerDeserializer().serialize(primaryKey, tupleBuilder.getDataOutput());
            tupleBuilder.addFieldEndOffset();

            tupleRef.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            return tupleRef;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    // ===== Private Utility Methods =====

    /**
     * Generate direction offsets based on vector dimension.
     * 2D: 4 directions (±x, ±y), 3D: 6 directions (±x, ±y, ±z)
     */
    private double[][] generateDirectionOffsets(int dimension, double distance) {
        double[][] offsets = new double[dimension * 2][dimension];
        for (int d = 0; d < dimension; d++) {
            offsets[d * 2] = new double[dimension];
            offsets[d * 2][d] = distance;
            offsets[d * 2 + 1] = new double[dimension];
            offsets[d * 2 + 1][d] = -distance;
        }
        return offsets;
    }

    private static double[] addVectors(double[] a, double[] b) {
        double[] result = new double[a.length];
        for (int i = 0; i < a.length; i++) {
            result[i] = a[i] + b[i];
        }
        return result;
    }

    private static double computeEuclideanDistance(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }
}
