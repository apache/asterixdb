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

import java.util.List;
import java.util.OptionalInt;

import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.vector.VectorIndexParameters;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;

/**
 * How much of one vector index a similarity search touches: the centroids it descends, the data
 * entries it scans, and the pages those entries occupy.
 * <p>
 * Every quantity here is what one storage partition does, since each partition searches its own
 * index over its own rows.
 */
public final class VectorIndexGeometry {

    // Data-page header: the tree frame's reserved header plus the cluster id, centroid id and
    // next-data-page pointers.
    private static final int DATA_PAGE_HEADER_BYTES = 34;

    private static final int TUPLE_SLOT_BYTES = 4;
    private static final int CENTROID_ID_BYTES = 4;
    private static final int DISTANCE_BYTES = Double.BYTES;

    // SQ4 and SQ8 both round up to a byte per dimension
    private static final int QUANTIZED_BYTES_PER_DIMENSION = 1;

    // Fixed fields ahead of the primary key in a data entry: distance and centroid id, plus the
    // quantized distance and quantized embedding when the index is quantized.
    private static final int UNQUANTIZED_FIXED_FIELDS = 2;
    private static final int QUANTIZED_FIXED_FIELDS = 4;

    // A data tuple's null flags carry one extra bit marking an antimatter (delete) entry.
    private static final int ANTIMATTER_BITS = 1;

    private static final int NULL_FLAG_BITS_PER_BYTE = 8;
    private static final int VAR_LEN_ONE_BYTE_LIMIT = 1 << 7;
    private static final int VAR_LEN_TWO_BYTE_LIMIT = 1 << 14;

    // Assumed serialized width of a variable-length key or included field, since no per-field
    // statistic is collected. The quantized embedding dominates a data entry at any realistic
    // dimension, so the estimate barely moves the result.
    private static final int VAR_LEN_FIELD_BYTES = 32;

    private static final double MIN_ROWS = 1.0;

    // The search substitutes this for a non-positive probe fraction, so the geometry has to as well.
    private static final double DEFAULT_MIN_PROBE_FRACTION = 0.75;

    private final double treeCentroids;
    private final double entriesPerCluster;
    private final double probes;
    private final double entryBytes;
    private final double dataPages;

    /**
     * @param params the index's WITH parameters
     * @param includeFieldTypes types of the index's INCLUDE fields
     * @param primaryKeyTypes the dataset's primary-key field types
     * @param cardinality dataset rows
     * @param numPartitions storage partitions
     * @param pageSize buffer-cache page size in bytes
     * @param minProbeFraction fraction of clusters the search probes; non-positive selects the default
     * @param candidateCount candidates the search has to collect before it can stop
     */
    public VectorIndexGeometry(VectorIndexParameters params, List<IAType> includeFieldTypes,
            List<IAType> primaryKeyTypes, double cardinality, int numPartitions, long pageSize, double minProbeFraction,
            double candidateCount) {
        double partitions = Math.max(numPartitions, 1);
        double rowsPerPartition = Math.max(cardinality / partitions, MIN_ROWS);
        double leafClusters = resolveLeafClusters(params.getNumClusters(), rowsPerPartition);
        treeCentroids = sumOfLevels(leafClusters);
        entriesPerCluster = rowsPerPartition / leafClusters;
        double probeFraction = minProbeFraction > 0 ? minProbeFraction : DEFAULT_MIN_PROBE_FRACTION;
        probes = Math.min(leafClusters, Math.max(Math.max(1.0, Math.floor(probeFraction * leafClusters)),
                Math.ceil(candidateCount / entriesPerCluster)));

        entryBytes = dataEntryBytes(params, includeFieldTypes, primaryKeyTypes);
        dataPages = probes * Math.ceil(entriesPerCluster * entryBytes / (pageSize - DATA_PAGE_HEADER_BYTES));
    }

    public double getTreeCentroids() {
        return treeCentroids;
    }

    public double getScannedEntries() {
        return probes * entriesPerCluster;
    }

    public double getProbes() {
        return probes;
    }

    public double getDataPages() {
        return dataPages;
    }

    public double getEntryBytes() {
        return entryBytes;
    }

    private static double resolveLeafClusters(OptionalInt declared, double rowsPerPartition) {
        if (declared.isPresent()) {
            return Math.max(declared.getAsInt(), 1);
        }
        return Math.max(Math.floor(Math.sqrt(rowsPerPartition)), 1);
    }

    private static double sumOfLevels(double leafClusters) {
        double total = 0;
        double level = leafClusters;
        while (level > 1) {
            total += level;
            level = Math.floor(Math.sqrt(level));
        }
        return Math.max(total, 1);
    }

    /**
     * @param params the index's WITH parameters
     * @param includeTypes types of the index's INCLUDE fields
     * @param primaryKeyTypes the dataset's primary-key field types
     */
    private static double dataEntryBytes(VectorIndexParameters params, List<IAType> includeTypes,
            List<IAType> primaryKeyTypes) {
        boolean quantized = params.isQuantized();
        double bytes = DISTANCE_BYTES + CENTROID_ID_BYTES;
        if (quantized) {
            // A second, quantized distance is kept alongside the raw one. It is written as a bare double
            // but declared variable length, so it is charged a field slot as well.
            bytes += DISTANCE_BYTES + varLenPrefixBytes(DISTANCE_BYTES);
            double quantizedBytes = params.getDimension() * QUANTIZED_BYTES_PER_DIMENSION;
            double quantizedField = varLenPrefixBytes(quantizedBytes) + quantizedBytes;
            bytes += quantizedField + varLenPrefixBytes(quantizedField);
        }
        bytes += serializedBytes(primaryKeyTypes) + serializedBytes(includeTypes);

        int fixedFields = quantized ? QUANTIZED_FIXED_FIELDS : UNQUANTIZED_FIXED_FIELDS;
        bytes += nullFlagBytes(fixedFields + primaryKeyTypes.size() + includeTypes.size() + ANTIMATTER_BITS);

        return bytes + TUPLE_SLOT_BYTES;
    }

    /**
     * Serialized, type-tagged width of a field list, falling back to an assumed width for the
     * variable-length ones. Those are charged a field slot on top, since only they need locating.
     */
    private static double serializedBytes(List<IAType> types) {
        double bytes = 0;
        for (IAType type : types) {
            ITypeTraits traits = TypeTraitProvider.INSTANCE.getTypeTrait(type);
            if (traits != null && traits.isFixedLength()) {
                bytes += traits.getFixedLength();
            } else {
                bytes += VAR_LEN_FIELD_BYTES + varLenPrefixBytes(VAR_LEN_FIELD_BYTES);
            }
        }
        return bytes;
    }

    private static int varLenPrefixBytes(double width) {
        if (width < VAR_LEN_ONE_BYTE_LIMIT) {
            return 1;
        }
        return width < VAR_LEN_TWO_BYTE_LIMIT ? 2 : 3;
    }

    private static double nullFlagBytes(int fieldCount) {
        return Math.ceil((double) fieldCount / NULL_FLAG_BITS_PER_BYTE);
    }
}
