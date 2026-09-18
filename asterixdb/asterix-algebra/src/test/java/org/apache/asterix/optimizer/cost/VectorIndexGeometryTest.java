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

package org.apache.asterix.optimizer.cost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.vector.VectorQuantization;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.vector.VectorIndexParameters;
import org.junit.Test;

public class VectorIndexGeometryTest {

    private static final List<IAType> BIGINT_KEY = List.of(BuiltinType.AINT64);
    private static final List<IAType> NO_INCLUDES = List.of();

    private static final int DIMENSION = 384;
    private static final double CARDINALITY = 1_000_000;
    private static final int PARTITIONS = 4;
    private static final long PAGE_SIZE = 128 * 1024;
    private static final double PROBE_FRACTION = 0.1;
    private static final double CANDIDATES = 50;

    private static VectorIndexParameters params(int dimension) throws AsterixException {
        return VectorIndexParameters.builder().setDimension(dimension).setSimilarity(VectorSimilarityMetric.EUCLIDEAN)
                .build();
    }

    private static VectorIndexGeometry geometry(VectorIndexParameters params, List<IAType> includeTypes,
            double cardinality, double minProbeFraction, double candidateCount) {
        return new VectorIndexGeometry(params, includeTypes, BIGINT_KEY, cardinality, PARTITIONS, PAGE_SIZE,
                minProbeFraction, candidateCount);
    }

    /**
     * One million rows over four partitions, 384 dimensions, quantized, a tenth of the clusters probed.
     * 250,000 rows per partition cluster into floor(sqrt(250,000)) = 500 leaves of 500 entries each, of
     * which a tenth are probed.
     */
    @Test
    public void oneMillionRows() throws Exception {
        VectorIndexGeometry geometry =
                geometry(params(DIMENSION), NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES);

        assertEquals(50, geometry.getProbes(), 0);
        assertEquals(25_000, geometry.getScannedEntries(), 0);

        // 500 leaves, then floor(sqrt(500)) = 22, then 4, then 2; no level of one is ever built.
        assertEquals(528, geometry.getTreeCentroids(), 0);

        // 8 distance + 4 centroid id + 8 quantized distance + 1 its slot + 385 quantized embedding
        // + 1 its slot + 9 bigint key + 1 null flags + 4 page slot.
        assertEquals(423, geometry.getEntryBytes(), 0);

        // 500 entries of 423 bytes span two 128 KiB pages, counted per probed cluster since entries
        // chain from the cluster's own first page.
        assertEquals(100, geometry.getDataPages(), 0);
    }

    /** A cluster is priced by the rows it holds, whatever replication put them there. */
    @Test
    public void crossPollinationDoesNotWidenClusters() throws Exception {
        VectorIndexParameters replicated = VectorIndexParameters.builder().setDimension(DIMENSION)
                .setSimilarity(VectorSimilarityMetric.EUCLIDEAN).setCrossPollinationM(3).build();

        VectorIndexGeometry plain = geometry(params(DIMENSION), NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES);
        VectorIndexGeometry withReplicas = geometry(replicated, NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES);

        assertEquals(plain.getScannedEntries(), withReplicas.getScannedEntries(), 0);
        assertEquals(plain.getDataPages(), withReplicas.getDataPages(), 0);
    }

    /** A declared cluster count replaces the one the bulk load would derive from the row count. */
    @Test
    public void declaredClusterCountIsHonoured() throws Exception {
        VectorIndexParameters declared = VectorIndexParameters.builder().setDimension(DIMENSION)
                .setSimilarity(VectorSimilarityMetric.EUCLIDEAN).setNumClusters(100).build();

        VectorIndexGeometry geometry = geometry(declared, NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES);

        // 100 leaves, then 10, then 3, then 1 -- which is not built.
        assertEquals(113, geometry.getTreeCentroids(), 0);
        assertEquals(10, geometry.getProbes(), 0);
        // 250,000 rows per partition over 100 clusters, a tenth of them probed.
        assertEquals(25_000, geometry.getScannedEntries(), 0);
    }

    /** The probe fraction is a floor: the search keeps opening clusters until it has enough candidates. */
    @Test
    public void candidateCountRaisesProbesAboveTheFraction() throws Exception {
        // 100 rows per partition cluster into 10 leaves of 10 entries; a tenth would probe one.
        VectorIndexGeometry geometry = geometry(params(DIMENSION), NO_INCLUDES, 400, 0.1, 35);

        assertEquals(4, geometry.getProbes(), 0);
        assertEquals(40, geometry.getScannedEntries(), 0);
    }

    /** A zero fraction stands for the default, which is what the search substitutes for it too. */
    @Test
    public void zeroFractionProbesTheDefaultShare() throws Exception {
        VectorIndexGeometry byDefault = geometry(params(DIMENSION), NO_INCLUDES, CARDINALITY, 0, CANDIDATES);
        VectorIndexGeometry explicit = geometry(params(DIMENSION), NO_INCLUDES, CARDINALITY, 0.75, CANDIDATES);

        assertEquals(explicit.getProbes(), byDefault.getProbes(), 0);
    }

    /** However many candidates are asked for, no more clusters exist than the index has. */
    @Test
    public void probesNeverExceedTheClusterCount() throws Exception {
        VectorIndexGeometry geometry = geometry(params(DIMENSION), NO_INCLUDES, 400, 1.0, Double.MAX_VALUE);

        assertEquals(10, geometry.getProbes(), 0);
    }

    /** An empty dataset still describes one cluster rather than dividing by zero. */
    @Test
    public void emptyDatasetStaysFinite() throws Exception {
        VectorIndexGeometry geometry = geometry(params(DIMENSION), NO_INCLUDES, 0, PROBE_FRACTION, CANDIDATES);

        assertEquals(1, geometry.getProbes(), 0);
        assertEquals(1, geometry.getTreeCentroids(), 0);
        assertTrue(Double.isFinite(geometry.getScannedEntries()));
        assertTrue(Double.isFinite(geometry.getDataPages()));
    }

    /** Included fields widen an entry, which is what separates two otherwise identical indexes. */
    @Test
    public void includedFieldsWidenTheEntry() throws Exception {
        VectorIndexGeometry narrow = geometry(params(DIMENSION), NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES);
        VectorIndexGeometry wide = geometry(params(DIMENSION), List.of(BuiltinType.AINT64, BuiltinType.ASTRING),
                CARDINALITY, PROBE_FRACTION, CANDIDATES);

        assertTrue(wide.getEntryBytes() > narrow.getEntryBytes());
    }

    /** Both quantization widths round to a byte per dimension, so neither separates two indexes. */
    @Test
    public void quantizationWidthDoesNotSeparateIndexes() throws Exception {
        VectorIndexParameters sq4 = VectorIndexParameters.builder().setDimension(DIMENSION)
                .setSimilarity(VectorSimilarityMetric.EUCLIDEAN).setQuantization(VectorQuantization.SQ4).build();
        VectorIndexParameters sq8 = VectorIndexParameters.builder().setDimension(DIMENSION)
                .setSimilarity(VectorSimilarityMetric.EUCLIDEAN).setQuantization(VectorQuantization.SQ8).build();

        assertEquals(geometry(sq8, NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES).getEntryBytes(),
                geometry(sq4, NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES).getEntryBytes(), 0);
    }

    /** Wider vectors mean fewer entries per page, so more pages for the same number of entries. */
    @Test
    public void dimensionDrivesEntryWidthAndPageCount() throws Exception {
        VectorIndexGeometry small = geometry(params(64), NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES);
        VectorIndexGeometry large = geometry(params(1536), NO_INCLUDES, CARDINALITY, PROBE_FRACTION, CANDIDATES);

        assertTrue(large.getEntryBytes() > small.getEntryBytes());
        assertTrue(large.getDataPages() > small.getDataPages());
        // Neither the descent nor the entries scanned depend on the width.
        assertEquals(small.getScannedEntries(), large.getScannedEntries(), 0);
        assertEquals(small.getTreeCentroids(), large.getTreeCentroids(), 0);
    }

    /**
     * What separates a search priced below a scan of the same rows from one priced above it: a
     * selective probe fraction opens one cluster of two, while a fraction of one opens both and reads
     * every row the partition holds.
     */
    @Test
    public void probeFractionDecidesHowMuchOfThePartitionIsRead() throws Exception {
        VectorIndexParameters twoClusters = VectorIndexParameters.builder().setDimension(4)
                .setSimilarity(VectorSimilarityMetric.EUCLIDEAN).setNumClusters(2).build();

        // Forty rows over four partitions: ten per partition, five in each of the two clusters.
        VectorIndexGeometry selective = geometry(twoClusters, NO_INCLUDES, 40, 0.1, 3);
        VectorIndexGeometry exhaustive = geometry(twoClusters, NO_INCLUDES, 40, 1.0, 3);

        assertEquals(1, selective.getProbes(), 0);
        assertEquals(5, selective.getScannedEntries(), 0);

        assertEquals(2, exhaustive.getProbes(), 0);
        assertEquals(10, exhaustive.getScannedEntries(), 0);
    }

    /**
     * A candidate count no selective fraction can fill reads the whole partition anyway, so asking for
     * more candidates than the index holds prices the search as a scan however few clusters were asked
     * for.
     */
    @Test
    public void candidateCountAloneCanReadTheWholePartition() throws Exception {
        VectorIndexParameters twoClusters = VectorIndexParameters.builder().setDimension(4)
                .setSimilarity(VectorSimilarityMetric.EUCLIDEAN).setNumClusters(2).build();

        VectorIndexGeometry geometry = geometry(twoClusters, NO_INCLUDES, 40, 0.1, 90);

        assertEquals(2, geometry.getProbes(), 0);
        assertEquals(10, geometry.getScannedEntries(), 0);
    }
}
