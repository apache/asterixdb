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
package org.apache.asterix.metadata.utils;

import java.util.OptionalDouble;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.vector.VectorIndexParameters;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Infers the two sizes a vector index build needs when the user does not state them: the leaf cluster count
 * and the training list the k-means sees.
 * <p>
 * Everything here is expressed in <em>per storage partition</em> terms, because that is the unit a build
 * actually works in, since each partition trains its own tree over its own rows. {@code D} throughout is the
 * per-partition cardinality.
 * <p>
 * The two knobs are not independent. The training list is derived from the cluster count at
 * {@link #TRAIN_ROWS_PER_CLUSTER} rows per cluster, which is the rule of thumb for having enough data to pick
 * a representative centroid. That coupling is why the two cannot both be set by the user.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public final class VTreeParamsInference {

    /**
     * Below this, {@code sqrt(D)} would give clusters too small to train on, so the small-data rule applies
     * instead. At the boundary the two rules agree: {@code D = 10_000} gives 100 clusters either way.
     */
    private static final int SMALL_DATA_SQRT_THRESHOLD = 100;

    /** Target rows per cluster in the small-data branch. */
    private static final int SMALL_DATA_CLUSTER_ROWS = 100;

    /** Training rows per cluster: the rule of thumb for picking representative centroids. */
    public static final int TRAIN_ROWS_PER_CLUSTER = 50;

    private VTreeParamsInference() {
    }

    /** The per-partition cardinality {@code D}, the unit every rule below is expressed in. */
    public static long perPartitionCardinality(long sourceCardinality, int numPartitions) {
        return Math.max(1L, sourceCardinality / Math.max(1, numPartitions));
    }

    /**
     * The leaf cluster count when the user does not give one: {@code sqrt(D)}, or {@code ceil(D/100)} on small
     * data where {@code sqrt(D)} would leave too few rows per cluster to train on.
     */
    public static int defaultNumClusters(long d) {
        long k = isSmallData(d) ? (long) Math.ceil((double) d / SMALL_DATA_CLUSTER_ROWS) : (long) Math.sqrt((double) d);
        return (int) Math.max(1L, Math.min(k, Integer.MAX_VALUE));
    }

    /** True when {@code sqrt(D)} is too small to be a useful cluster count, so the build reads every row. */
    public static boolean isSmallData(long d) {
        return Math.sqrt((double) d) < SMALL_DATA_SQRT_THRESHOLD;
    }

    /**
     * The training list a given cluster count needs, bounded by the partition and by the backstop ceiling.
     * <p>
     * Small data trains on the whole partition rather than on {@code 50 * numClusters}. The build reads every
     * row there anyway, because there is no sampling operator and nothing truncates the scan, so training on a
     * subset would mean discarding rows already in hand to land exactly on a floor. The rule is a floor: the
     * spec accepts any list at or above {@code 50 * numClusters}, and the whole partition clears it by more.
     * Recording the whole partition is also what keeps the persisted fraction equal to what the build used.
     */
    public static long trainListSize(int numClusters, long d) {
        if (isSmallData(d)) {
            return d;
        }
        long wanted = (long) TRAIN_ROWS_PER_CLUSTER * numClusters;
        return Math.max(1L, Math.min(wanted, d));
    }

    /**
     * Whether the build should read every row instead of sampling: true exactly when the training list is the
     * whole partition. An integer comparison rather than a test on the fraction, which would be a float
     * landing on 1.0.
     */
    public static boolean useFullScan(long trainListSize, long d) {
        return trainListSize >= d;
    }

    /**
     * The training list an explicitly requested fraction asks for. Bounded only by the partition: an explicit
     * fraction is honoured as written, so that what is recorded is what the user asked for. Case 3 of the
     * model warns about a fraction that is too small; it puts no ceiling on one that is large.
     */
    public static long trainListSizeForFraction(double fraction, long d) {
        return Math.max(1L, Math.min((long) Math.ceil(fraction * d), d));
    }

    /**
     * The fraction to record for a training list of {@code size}. Small data reads every row, so it records
     * 1.0: what is persisted is what the build used, not what motivated the cluster count.
     */
    public static double effectiveFraction(long trainListSize, long d) {
        if (d <= 0L) {
            return 1.0;
        }
        return Math.min(1.0, (double) trainListSize / d);
    }

    /**
     * Infers the sizes a build will use and returns the parameters carrying them, so the index record states
     * what was used rather than leaving it to be re-derived later from a cardinality that has moved. Called
     * on the DDL path, before the record is written; the seed is fixed at the same point and for the same
     * reason.
     * <p>
     * Both inputs are compile-time facts, so both diagnostics below are compile-time errors, which is where
     * the model puts them: a missing ANALYZE and an empty collection.
     */
    public static VectorIndexParameters infer(MetadataProvider metadataProvider, Dataset dataset,
            VectorIndexParameters params, IWarningCollector warningCollector, SourceLocation sourceLoc)
            throws AlgebricksException {
        Index sampleIndex = metadataProvider.findSampleIndex(dataset.getDatabaseName(), dataset.getDataverseName(),
                dataset.getDatasetName());
        if (sampleIndex == null || !(sampleIndex.getIndexDetails() instanceof Index.SampleIndexDetails)) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Run ANALYZE on the dataset before creating a vector index.");
        }
        long cardinality = ((Index.SampleIndexDetails) sampleIndex.getIndexDetails()).getSourceCardinality();
        if (cardinality <= 0L) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, sourceLoc,
                    "Vector index requires a non-empty collection; load the data and run ANALYZE DATASET first.");
        }
        int numPartitions = metadataProvider.getPartitioningProperties(dataset).getNumberOfPartitions();
        final long d = perPartitionCardinality(cardinality, numPartitions);
        int maxClusters =
                metadataProvider.getApplicationContext().getCompilerProperties().getVectorIndexMaxNumClusters();
        // The ceiling binds the derived count too, so no path can size a build above it.
        int recommendedClusters = Math.min(defaultNumClusters(d), maxClusters);
        int numClusters = params.getNumClusters().orElse(recommendedClusters);
        if (params.getNumClusters().isPresent()) {
            validateNumClusters(numClusters, recommendedClusters, d, maxClusters, warningCollector, sourceLoc);
        }
        OptionalDouble requested = params.getTrainListFractionOpt();
        long size = requested.isPresent() ? trainListSizeForFraction(requested.getAsDouble(), d)
                : trainListSize(numClusters, d);
        if (requested.isPresent()) {
            validateTrainList(requested.getAsDouble(), size, numClusters, d, warningCollector, sourceLoc);
        }
        return params.withInferredSizes(numClusters, effectiveFraction(size, d));
    }

    /**
     * Case 2 of the model, under the configured ceiling. The ceiling is checked first because it bounds the
     * memory a build may take whatever the collection holds, so it is not for the data to overrule.
     * <p>
     * More clusters than rows cannot be built, because the padding path would invent the shortfall as
     * perturbed duplicates of real centroids, so that is an error. Otherwise a value far from what the
     * collection suggests is only a warning: it is legal, and the user may know something the cardinality
     * does not say.
     * <p>
     * The recommendation is whatever {@link #defaultNumClusters} would have chosen, not {@code sqrt(D)}
     * unconditionally. On a small collection those differ, and recommending {@code sqrt(D)} there would warn
     * about the value this same code picks when the user says nothing.
     */
    static void validateNumClusters(int numClusters, int recommended, long d, int maxNumClusters,
            IWarningCollector warningCollector, SourceLocation sourceLoc) throws CompilationException {
        if (numClusters > maxNumClusters) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, sourceLoc,
                    "num_clusters " + numClusters + " exceeds the configured maximum of " + maxNumClusters + ".");
        }
        if (numClusters > d) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, sourceLoc,
                    "num_clusters " + numClusters + " exceeds the " + d
                            + " records per storage partition available to train on. " + recommended
                            + " is recommended.");
        }
        if ((numClusters < 0.9 * recommended || numClusters > 1.1 * recommended) && warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(sourceLoc, ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "num_clusters " + numClusters + " is outside the recommended range for this collection. "
                            + recommended + " is recommended for " + d + " records per storage partition."));
        }
    }

    /**
     * Case 3 of the model. A training list smaller than the cluster count cannot produce a centroid per
     * cluster, so that is an error; below the rows-per-cluster rule of thumb it is a warning, since the
     * centroids will be built from thin evidence but can still be built.
     */
    static void validateTrainList(double fraction, long size, int numClusters, long d,
            IWarningCollector warningCollector, SourceLocation sourceLoc) throws CompilationException {
        if (size < numClusters) {
            throw new CompilationException(ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED, sourceLoc,
                    "train_list_fraction " + fraction + " trains on " + size + " records per storage partition, "
                            + "fewer than the " + numClusters + " clusters to be built. Raise it to at least "
                            + recommendedFraction(numClusters, d) + ".");
        }
        long recommendedSize = (long) TRAIN_ROWS_PER_CLUSTER * numClusters;
        if (size < recommendedSize && warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(sourceLoc, ErrorCode.COMPILATION_VECTOR_INDEX_CREATION_FAILED,
                    "train_list_fraction " + fraction + " trains on " + size + " records per storage partition, "
                            + "fewer than the " + recommendedSize + " recommended for " + numClusters
                            + " clusters. Raise it to at least " + recommendedFraction(numClusters, d) + "."));
        }
    }

    /** The smallest fraction that reaches the rows-per-cluster rule of thumb, for use in a recommendation. */
    private static double recommendedFraction(int numClusters, long d) {
        return Math.min(1.0, (double) TRAIN_ROWS_PER_CLUSTER * numClusters / d);
    }
}
