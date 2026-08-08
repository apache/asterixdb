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
package org.apache.asterix.runtime.operators;

import static org.apache.asterix.om.types.BuiltinType.ADOUBLE;
import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.utils.VectorDistanceFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.misc.PartitionedUUID;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Enhanced version of LocalKMeansPlusPlusCentroidsOperatorDescriptor that maintains
 * hierarchical cluster relationships with parent-child associations.
 * ALGORITHM OVERVIEW:
 * ===================
 * This operator implements a hierarchical K-means++ clustering algorithm that builds
 * a complete tree structure from bottom-up. The algorithm works as follows:
 * 1. MEMORY-EFFICIENT K-MEANS++ ON RAW DATA:
 *    - Uses probabilistic selection to avoid loading all data points into memory
 *    - Performs iterative candidate selection with weighted K-means++
 *    - Applies Lloyd's algorithm for centroid refinement
 *    - Output: Initial set of leaf centroids (Level 0)
 * 2. HIERARCHICAL TREE BUILDING:
 *    - Takes centroids from current level and clusters them into fewer centroids
 *    - Uses scalable K-means++ on centroids (not raw data) for efficiency
 *    - Establishes parent-child relationships using Lloyd's assignments
 *    - Continues until centroids fit in one frame or only one centroid remains
 * 3. TREE STRUCTURE ORGANIZATION:
 *    - Builds complete tree with nodes containing centroids and relationships
 *    - Assigns BFS-based cluster IDs for efficient traversal
 *    - Organizes parent-child relationships naturally in tree structure
 * 4. OUTPUT:
 *    - Emits all tree nodes in BFS order as tuples (treeLevel, centroidId, parentClusterId, embedding)
 *      on output frames to the downstream static-structure builder
 * MEMORY EFFICIENCY:
 * ==================
 * - Never loads all data points into memory simultaneously
 * - Uses streaming approach with probabilistic selection
 * - Only stores centroids and tree structure in memory
 * - Frame-based stopping criterion prevents memory overflow
 * TREE STRUCTURE:
 * ===============
 * The algorithm builds a tree where:
 * - Leaf nodes (Level 0): Clusters of raw data points
 * - Interior nodes (Level 1+): Clusters of centroids from previous level
 * - Root node: Single centroid representing entire dataset
 * Example tree structure:
 * ```
 *                    Root (Level 2)
 *                   /              \
 *              Parent1           Parent2
 *             (Level 1)         (Level 1)
 *            /    |    \        /    |    \
 *        Child1 Child2 Child3 Child4 Child5 Child6
 *       (Level 0) (Level 0) (Level 0) (Level 0) (Level 0) (Level 0)
 * ```
 * Each node contains:
 * - Centroid coordinates (double[])
 * - Cluster ID (within level)
 * - Global ID (unique across all levels)
 * - Parent reference (for children)
 * - Children list (for parents)
 */
public final class HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final long serialVersionUID = -6161592596382830106L;

    // Clipping constants for centroid values
    private static final double DEFAULT_CLIP_MIN = -1e3;

    private static final double DEFAULT_CLIP_MAX = 1e3;

    private final UUID sampleUUID;

    private final UUID tupleCountUUID;

    // Configuration parameters for hierarchical clustering
    private final IScalarEvaluatorFactory args; // Evaluator for extracting vector data from tuples

    private final int K; // Number of clusters for initial level (leaf nodes)

    private final int maxScalableKmeansIter; // Maximum iterations for scalable K-means++ candidate selection

    private final VectorSimilarityMetric similarityMetric; // resolved from distanceMetric; drives cosine normalization

    private final RecordDescriptor secondaryRecDesc; // Input record descriptor (2-field format)

    private final int vectorDimension;

    private final long trainSeed; // Base seed for the training RNG; per-partition offset keeps partitions decorrelated

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public HierarchicalKMeansPlusPlusCentroidsOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor outputRecDesc, RecordDescriptor secondaryRecDesc, UUID sampleUUID, UUID tupleCountUUID,
            IScalarEvaluatorFactory args, int K, int maxScalableKmeansIter, VectorSimilarityMetric similarityMetric,
            int vectorDimension, long trainSeed) {
        super(spec, 1, 1);
        // Output record descriptor defines the format of output tuples (treeLevel, centroidId, parentClusterId, embedding)
        // Input record descriptor is the 2-field format with vector embeddings
        outRecDescs[0] = outputRecDesc; // Output format (hierarchical structure with parent-child relationships)
        this.secondaryRecDesc = secondaryRecDesc; // Input format (2-field with vector embeddings)
        this.sampleUUID = sampleUUID;
        this.tupleCountUUID = tupleCountUUID;
        this.args = args;
        this.K = K;
        this.maxScalableKmeansIter = maxScalableKmeansIter;
        this.vectorDimension = vectorDimension;
        this.trainSeed = trainSeed;
        // Distance function from index DDL (WITH similarity "euclidean"|"cosine"|"cosine similarity"|etc.); default euclidean squared
        this.similarityMetric = similarityMetric;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        // Activity 1: Store centroids and materialize data
        StoreCentroidsActivity storeCentroidsActivity = new StoreCentroidsActivity(new ActivityId(odId, 0));
        // Activity 2: Find candidates and perform hierarchical clustering
        FindCandidatesActivity findCandidatesActivity = new FindCandidatesActivity(new ActivityId(odId, 1));

        builder.addActivity(this, storeCentroidsActivity);
        builder.addSourceEdge(0, storeCentroidsActivity, 0);

        builder.addActivity(this, findCandidatesActivity);
        builder.addTargetEdge(0, findCandidatesActivity, 0);

        // Add blocking edge to ensure data accumulation completes before clustering
        builder.addBlockingEdge(storeCentroidsActivity, findCandidatesActivity);
    }

    /**
     * Activity 1: Store Centroids and Materialize Data
     * This activity performs initial K-means++ on raw data and materializes all data for later processing.
     */
    private class StoreCentroidsActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private StoreCentroidsActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private final FrameTupleAccessor fta = new FrameTupleAccessor(secondaryRecDesc);
                private MaterializerTaskState materializedSample;
                private TupleCountState tupleCountState;

                @Override
                public void open() throws HyracksDataException {
                    // Initialize data persistence for multiple passes over the data
                    materializedSample = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(sampleUUID, partition));
                    materializedSample.open(ctx);

                    // Initialize tuple count state
                    tupleCountState = new TupleCountState(ctx.getJobletContext().getJobId(),
                            new PartitionedUUID(tupleCountUUID, partition));
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    // Count tuples in this frame
                    fta.reset(buffer);
                    tupleCountState.addTupleCount(fta.getTupleCount());

                    // Materialize all data to disk for subsequent processing passes
                    // This allows us to make multiple passes over the data without loading it all into memory
                    materializedSample.appendFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    if (materializedSample != null) {
                        materializedSample.close();
                        ctx.setStateObject(materializedSample);
                    }
                    if (tupleCountState != null) {
                        ctx.setStateObject(tupleCountState);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                }

            };
        }
    }

    /**
     * Activity 2: Find Candidates and Perform Hierarchical Clustering
     * This activity performs memory-efficient hierarchical clustering using the materialized data.
     */
    private class FindCandidatesActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private FindCandidatesActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                /**
                 * The single live reader over the materialized training sample. The k-means passes
                 * re-open the sample many times via {@link #resetRunFileReader}; tracking the live
                 * reader here lets each reset close its predecessor and the outer finally close the
                 * last one, so no open file handle outlives this operator.
                 */
                @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
                private GeneratedRunFileReader currentSampleReader;

                // Stateless distance function, built here on the NC from the serialized metric enum. Living on
                // the per-partition pushable (never Java-serialized) keeps the un-serializable function off the
                // wire and out of the shared descriptor.
                private final IVTreeDistanceFunction distanceFunction = distanceFunctionFor(similarityMetric);

                @Override
                public void initialize() throws HyracksDataException {
                    // Get file reader for written samples
                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader in = resetRunFileReader(ctx, sampleUUID, partition);
                    try {

                        FrameTupleAccessor fta;
                        FrameTupleReference tuple;
                        IScalarEvaluator eval = args.createScalarEvaluator(new EvaluatorContext(ctx));
                        IPointable inputVal = new VoidPointable();
                        IPointable tempVal = new VoidPointable();
                        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                        KMeansUtils KMeansUtils = new KMeansUtils(tempVal, storage);
                        fta = new FrameTupleAccessor(secondaryRecDesc);
                        tuple = new FrameTupleReference();
                        VSizeFrame vSizeFrame = new VSizeFrame(ctx);
                        FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                        ListAccessor listAccessorConstant = new ListAccessor();

                        writer.open();

                        // Get tuple count from first activity
                        TupleCountState tupleCountState =
                                (TupleCountState) ctx.getStateObject(new PartitionedUUID(tupleCountUUID, partition));
                        int totalTupleCount = tupleCountState != null ? tupleCountState.getTotalTupleCount() : 0;

                        // Perform memory-efficient hierarchical K-means clustering
                        HierarchicalClusterStructure clusterStructure =
                                performMemoryEfficientHierarchicalKMeans(ctx, in, fta, tuple, eval, inputVal,
                                        listAccessorConstant, KMeansUtils, vSizeFrame, partition, totalTupleCount);

                        if (clusterStructure.getNumLevels() == 0) {
                            return;
                        }

                        // Output hierarchical structure with parent-child relationships
                        // Manual buffer management handles flushing when needed
                        clusterStructure.outputHierarchicalStructure(appender, writer, ctx);

                        // Final flush
                        FrameUtils.flushFrame(appender.getBuffer(), writer);

                    } catch (Throwable e) {
                        writer.fail();
                        throw new RuntimeException(e);
                    } finally {
                        // Close the LAST reader opened by any nested reset, not just the first one.
                        closeCurrentSampleReader();
                        // Last consumer of the materialized sample: delete the run file or it leaks.
                        sampleState.deleteFile();
                        writer.close();
                    }
                }

                /**
                 * Performs   k-means|| (k-means parallel) on all data from run file to generate K centroids.
                 * Uses multiple rounds of probabilistic sampling to build candidate set, then reduces to k centroids.
                 */
                private ClusteringResult performInitialKMeansPlusPlus(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, int k, Random rand, int maxIterations, int totalTupleCount,
                        int partition) throws HyracksDataException, IOException {
                    //   k-means|| configuration
                    int numRounds = 5; // Number of sampling rounds (default 5-10)
                    double oversamplingFactor = 2.0 * k; // Oversampling factor l ≈ 2k

                    // k-means|| assumes the sample is far larger than the candidate set it draws. When
                    // rounds*l approaches the sample size, the per-point Bernoulli trial (p = l*D(x)/S)
                    // accepts essentially every point: the candidate set degenerates to the whole sample and
                    // the rounds — plus the weighting and dedup passes whose only job is to shrink that set —
                    // become pure overhead. Observed at k=300 on a 2500-point partition: 5 rounds x l=600
                    // targeted 3000 candidates from 2500 points and duly selected all 2499, costing ~16s per
                    // partition to accomplish nothing. Cap the expected candidate count at half the sample,
                    // trimming rounds first since each one costs two full passes over the sample.
                    // TODO: enable later
                    /*
                    long maxCandidates = Math.max(k, totalTupleCount / 2L);
                    if (numRounds * oversamplingFactor > maxCandidates) {
                        numRounds = (int) Math.max(1, Math.min(numRounds, maxCandidates / oversamplingFactor));
                        oversamplingFactor = Math.min(oversamplingFactor, maxCandidates / (double) numRounds);
                    }
                    */

                    return performKMeansParallel(ctx, in, fta, tuple, eval, inputVal, listAccessorConstant, kMeansUtils,
                            k, rand, maxIterations, totalTupleCount, partition, numRounds, oversamplingFactor);
                }

                /**
                 * Implements   k-means|| algorithm with configurable parameters.
                 */
                private ClusteringResult performKMeansParallel(IHyracksTaskContext ctx, GeneratedRunFileReader in,
                        FrameTupleAccessor fta, FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, int k, Random rand,
                        int maxIterations, int totalTupleCount, int partition, int numRounds, double oversamplingFactor)
                        throws HyracksDataException, IOException {

                    if (k <= 0 || totalTupleCount <= 0) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }

                    int[] assignments = new int[totalTupleCount];

                    // Step 1: Choose first centroid uniformly at random
                    int firstIdx = rand.nextInt(totalTupleCount);
                    double[] firstCentroid = getPointAtIndex(in, fta, tuple, eval, inputVal, listAccessorConstant,
                            kMeansUtils, firstIdx, ctx);
                    if (firstCentroid == null) {
                        return new ClusteringResult(new ArrayList<>(), assignments);
                    }

                    // Current centers for distance computation (starts with first centroid)
                    List<double[]> currentCenters = new ArrayList<>();
                    currentCenters.add(firstCentroid);

                    // Candidate set (will be oversampled)
                    List<double[]> candidates = new ArrayList<>();

                    // Step 2: Multiple rounds of probabilistic sampling (k-means||)
                    for (int round = 0; round < numRounds; round++) {
                        // PASS 1: Compute S = Σ_x D(x) by streaming (NO DISTANCE STORAGE)
                        double totalDistance = 0.0;

                        in = resetRunFileReader(ctx, sampleUUID, partition);
                        VSizeFrame frame = new VSizeFrame(ctx);
                        int tempIdx = 0;

                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    tempIdx++;
                                    continue;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);
                                    // Compute D(x) = min distance to current centers
                                    if (point.length != vectorDimension) {
                                        continue;
                                    }
                                    double minDist = Double.POSITIVE_INFINITY;
                                    for (double[] center : currentCenters) {
                                        double dist = distanceFunction.apply(point, center);
                                        minDist = Math.min(minDist, dist);
                                    }

                                    // Accumulate sum (NO STORAGE)
                                    totalDistance += minDist;
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                tempIdx++;
                            }
                        }

                        if (totalDistance <= 0) {
                            break;
                        }

                        // PASS 2: Stream again, recompute D(x), and sample probabilistically
                        in = resetRunFileReader(ctx, sampleUUID, partition);
                        frame = new VSizeFrame(ctx);
                        int currentIdx = 0;
                        int sampledCount = 0;

                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    currentIdx++;
                                    continue;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                    // RECOMPUTE D(x) (no storage from pass 1)
                                    double minDist = Double.POSITIVE_INFINITY;
                                    for (double[] center : currentCenters) {
                                        double dist = distanceFunction.apply(point, center);
                                        minDist = Math.min(minDist, dist);
                                    }

                                    //   probabilistic sampling: p(x) = l * D(x) / S
                                    double probability = oversamplingFactor * minDist / totalDistance;

                                    // Independent Bernoulli trial for each point
                                    if (rand.nextDouble() < probability) {
                                        // Add to candidates (copy to avoid mutation)
                                        candidates.add(Arrays.copyOf(point, point.length));
                                        sampledCount++;
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                currentIdx++;
                            }
                        }

                        // Update current centers: add all candidates from this round for next round's distance computation
                        if (sampledCount > 0) {
                            int startIdx = candidates.size() - sampledCount;
                            for (int idx = startIdx; idx < candidates.size(); idx++) {
                                currentCenters.add(Arrays.copyOf(candidates.get(idx), candidates.get(idx).length));
                            }
                        }
                    }

                    // Step 3: Weight candidates - count how many original points are nearest to each candidate
                    int[] candidateWeights = new int[candidates.size()];
                    Arrays.fill(candidateWeights, 0);

                    in = resetRunFileReader(ctx, sampleUUID, partition);
                    VSizeFrame weightFrame = new VSizeFrame(ctx);
                    int weightIdx = 0;

                    while (in.nextFrame(weightFrame)) {
                        ByteBuffer buffer = weightFrame.getBuffer();
                        fta.reset(buffer);
                        int tupleCount = fta.getTupleCount();

                        for (int j = 0; j < tupleCount; j++) {
                            tuple.reset(fta, j);
                            eval.evaluate(tuple, inputVal);
                            if (!ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                    .isListType()) {
                                weightIdx++;
                                continue;
                            }

                            listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                            try {
                                double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                // Find nearest candidate (recompute distance)
                                double minDist = Double.POSITIVE_INFINITY;
                                int nearestCandidate = -1;
                                for (int c = 0; c < candidates.size(); c++) {
                                    double dist = distanceFunction.apply(point, candidates.get(c));
                                    if (dist < minDist) {
                                        minDist = dist;
                                        nearestCandidate = c;
                                    }
                                }
                                if (nearestCandidate >= 0) {
                                    candidateWeights[nearestCandidate]++;
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            weightIdx++;
                        }
                    }

                    List<double[]> weightedCandidates = new ArrayList<>();
                    List<Integer> weightedCandidateWeights = new ArrayList<>();

                    // Reduce duplicates: combine identical/very close candidates and sum their weights
                    for (int i = 0; i < candidates.size(); i++) {
                        if (candidateWeights[i] == 0) {
                            continue; // Skip candidates with no assigned points (zero weight)
                        }

                        // Check if this candidate is a duplicate of an existing weighted candidate
                        boolean foundDuplicate = false;
                        for (int j = 0; j < weightedCandidates.size(); j++) {
                            double dist = distanceFunction.apply(candidates.get(i), weightedCandidates.get(j));
                            if (dist < 1e-10) { // Consider identical if very close
                                // Merge near-duplicate candidates by adding their weights
                                weightedCandidateWeights.set(j, weightedCandidateWeights.get(j) + candidateWeights[i]);
                                foundDuplicate = true;
                                break;
                            }
                        }

                        if (!foundDuplicate) {
                            weightedCandidates.add(Arrays.copyOf(candidates.get(i), candidates.get(i).length));
                            weightedCandidateWeights.add(candidateWeights[i]);
                        }
                    }

                    // Convert to arrays for easier use
                    int[] finalWeights = new int[weightedCandidateWeights.size()];
                    for (int i = 0; i < weightedCandidateWeights.size(); i++) {
                        finalWeights[i] = weightedCandidateWeights.get(i);
                    }

                    // Step 4: Select k initial centroids via weighted k-means++; pad from materialized sample if fewer than k
                    List<double[]> centroids;
                    if (weightedCandidates.isEmpty()) {
                        // Fallback: use first centroid only
                        centroids = new ArrayList<>();
                        centroids.add(firstCentroid);
                    } else if (weightedCandidates.size() <= k) {
                        // Fewer than k distinct candidates after dedup: keep them and pad from materialized sample
                        // Seed initial centroids from all distinct weighted candidates
                        centroids = new ArrayList<>();
                        for (double[] candidate : weightedCandidates) {
                            centroids.add(Arrays.copyOf(candidate, candidate.length));
                        }
                        // Pad to k centroids by reading additional points from the materialized sample
                        int needed = k - centroids.size();
                        if (needed > 0) {
                            List<Integer> additionalIndices = new ArrayList<>();
                            for (int i = 0; i < needed; i++) {
                                // Evenly spaced tuple indices over [0, totalTupleCount) for reproducible padding
                                int index = (i * totalTupleCount) / needed;
                                if (index >= totalTupleCount) {
                                    index = totalTupleCount - 1;
                                }
                                additionalIndices.add(index);
                            }

                            // Get the additional points in a single forward pass. additionalIndices is
                            // ascending, so we stream once instead of re-reading a reader that keeps
                            // advancing between reads.
                            in = resetRunFileReader(ctx, sampleUUID, partition);
                            List<double[]> additionalPoints = getPointsAtSortedIndices(in, fta, tuple, eval, inputVal,
                                    listAccessorConstant, kMeansUtils, additionalIndices, ctx);
                            for (double[] additionalPoint : additionalPoints) {
                                if (additionalPoint != null) {
                                    // Avoid duplicates - only add if not too close to existing centroids
                                    boolean isDuplicate = false;
                                    for (double[] existingCentroid : centroids) {
                                        double dist = distanceFunction.apply(additionalPoint, existingCentroid);
                                        if (dist < 1e-10) { // Consider it a duplicate if very close
                                            isDuplicate = true;
                                            break;
                                        }
                                    }
                                    if (!isDuplicate) {
                                        centroids.add(additionalPoint);
                                    }
                                }
                            }

                            // If we still don't have k (edge case: duplicates), pad with perturbed copies
                            while (centroids.size() < k && centroids.size() > 0) {
                                double[] base = centroids.get(centroids.size() - 1);
                                double[] perturbed = Arrays.copyOf(base, base.length);
                                // Add tiny random perturbation to make it distinct
                                for (int d = 0; d < perturbed.length; d++) {
                                    perturbed[d] += rand.nextGaussian() * 1e-6;
                                }
                                centroids.add(perturbed);
                            }
                        }

                    } else {
                        // Normal path - we have more than k weighted candidates
                        // Run weighted k-means++ on weightedCandidates to select exactly k
                        centroids = performWeightedKMeansPlusPlusOnCandidates(weightedCandidates, finalWeights, k, rand,
                                maxIterations);
                    }

                    // 3. Lloyd's algorithm for refinement using streaming approach
                    for (int iter = 0; iter < maxIterations; iter++) {
                        // Rewind before the assignment pass. Whatever ran last — the candidate weighting pass,
                        // the padding lookup, or the previous iteration's update pass — left the reader at (or
                        // part-way through) EOF. Without this the first iteration read nothing: every
                        // assignment stayed 0, so the update pass folded the entire sample into centroid 0 and
                        // overwrote it with the global mean, discarding one k-means++ seed and wasting a pass.
                        in = resetRunFileReader(ctx, sampleUUID, partition);
                        // Assignment phase: assign each point to closest centroid
                        VSizeFrame frame = new VSizeFrame(ctx);
                        int currentIdx = 0;
                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    currentIdx++;
                                    continue;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                    // Find closest centroid
                                    double minDist = Double.POSITIVE_INFINITY;
                                    int closestCentroid = 0;
                                    for (int c = 0; c < centroids.size(); c++) {
                                        double dist = distanceFunction.apply(point, centroids.get(c));
                                        if (dist < minDist) {
                                            minDist = dist;
                                            closestCentroid = c;
                                        }
                                    }
                                    assignments[currentIdx] = closestCentroid;

                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                currentIdx++;
                            }
                        }

                        // Reset reader for update phase
                        in = resetRunFileReader(ctx, sampleUUID, partition);

                        // Update phase: calculate new centroids
                        double[][] newCentroids = new double[centroids.size()][centroids.get(0).length];
                        int[] counts = new int[centroids.size()];

                        frame = new VSizeFrame(ctx);
                        currentIdx = 0;
                        while (in.nextFrame(frame)) {
                            ByteBuffer buffer = frame.getBuffer();
                            fta.reset(buffer);
                            int tupleCount = fta.getTupleCount();

                            for (int j = 0; j < tupleCount; j++) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    currentIdx++;
                                    continue;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                try {
                                    double[] point = kMeansUtils.createPrimitveList(listAccessorConstant);

                                    int centroidIdx = assignments[currentIdx];
                                    for (int d = 0; d < point.length; d++) {
                                        newCentroids[centroidIdx][d] += point[d];
                                    }
                                    counts[centroidIdx]++;

                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                currentIdx++;
                            }
                        }

                        // Check for convergence
                        boolean converged = true;
                        for (int i = 0; i < centroids.size(); i++) {
                            if (counts[i] > 0) {
                                for (int d = 0; d < newCentroids[i].length; d++) {
                                    newCentroids[i][d] /= counts[i];
                                }
                                // Check if centroid moved significantly
                                double dist = distanceFunction.apply(centroids.get(i), newCentroids[i]);
                                if (dist > 1e-4) {
                                    converged = false;
                                }
                                maybeNormalizeCentroid(newCentroids[i]);
                                centroids.set(i, newCentroids[i]);
                            }
                        }

                        if (converged) {
                            break;
                        }
                        // The next iteration rewinds the reader itself, so no reset here.
                    }

                    return new ClusteringResult(centroids, assignments);
                }

                /**
                 * Perform weighted K-means++ on candidates to select exactly k centroids.
                 * Uses weights when computing probabilities and weighted averages.
                 */
                private List<double[]> performWeightedKMeansPlusPlusOnCandidates(List<double[]> candidates,
                        int[] weights, int k, Random rand, int maxIterations) throws HyracksDataException {
                    if (candidates.isEmpty() || k <= 0) {
                        return new ArrayList<>();
                    }

                    // If we have <= k candidates, use them but ensure we have exactly k by duplicating if needed
                    // (always return k initial centers)
                    if (candidates.size() <= k) {
                        List<double[]> result = new ArrayList<>(candidates);
                        // If we have fewer than k, duplicate the most weighted candidates to fill the gap
                        if (result.size() < k) {
                            // Find candidates with highest weights for duplication
                            List<Integer> candidateIndices = new ArrayList<>();
                            for (int i = 0; i < candidates.size(); i++) {
                                candidateIndices.add(i);
                            }
                            // Sort by weight (descending)
                            candidateIndices.sort((a, b) -> Integer.compare(weights[b], weights[a]));

                            int remaining = k - result.size();
                            for (int i = 0; i < remaining && i < candidateIndices.size(); i++) {
                                int idx = candidateIndices.get(i);
                                // Add a slightly perturbed copy to ensure distinctness
                                double[] base = candidates.get(idx);
                                double[] copy = Arrays.copyOf(base, base.length);
                                for (int d = 0; d < copy.length; d++) {
                                    copy[d] += rand.nextGaussian() * 1e-6;
                                }
                                result.add(copy);
                            }
                        }
                        return result;
                    }

                    List<double[]> resultCentroids = new ArrayList<>();
                    int[] assignments = new int[candidates.size()];

                    // Weighted K-means++ initialization
                    // 1. Choose first centroid randomly (weighted by weights)
                    int firstIdx = selectWeightedRandomIndex(candidates, weights, rand);
                    resultCentroids.add(Arrays.copyOf(candidates.get(firstIdx), candidates.get(firstIdx).length));

                    // 2. Choose remaining centroids using weighted selection.
                    //
                    // D(c_j) — each candidate's distance to its nearest accepted centroid — is maintained
                    // incrementally: seeded against the first centroid, then folded against only the centroid
                    // added in the previous iteration. Since the nearest-centroid distance is a running
                    // minimum, this yields exactly the values a full rescan would, at O(C*k) distance
                    // computations instead of O(C*k^2). The rescan form made this the single longest phase of
                    // the static-structure job (measured: 66% of it, 112M of 161M distance computations at
                    // C=2499, k=300); the incremental form costs 750K for the same inputs.
                    double[] minDistToCentroid = new double[candidates.size()];
                    double[] weightedDistances = new double[candidates.size()];
                    double[] firstCentroid = resultCentroids.get(0);
                    for (int j = 0; j < candidates.size(); j++) {
                        minDistToCentroid[j] = distanceFunction.apply(candidates.get(j), firstCentroid);
                    }
                    for (int i = 1; i < k && i < candidates.size(); i++) {
                        double totalWeightedDistance = 0.0;

                        // Weighted distance: weight[j] * D(c_j), from the running nearest-centroid distance
                        for (int j = 0; j < candidates.size(); j++) {
                            weightedDistances[j] = weights[j] * minDistToCentroid[j];
                            totalWeightedDistance += weightedDistances[j];
                        }

                        if (totalWeightedDistance <= 0) {
                            break;
                        }

                        // Weighted random selection
                        double r = rand.nextDouble() * totalWeightedDistance;
                        double cumulativeDistance = 0.0;
                        int selectedIdx = 0;
                        for (int j = 0; j < candidates.size(); j++) {
                            cumulativeDistance += weightedDistances[j];
                            if (cumulativeDistance >= r) {
                                selectedIdx = j;
                                break;
                            }
                        }

                        double[] selected =
                                Arrays.copyOf(candidates.get(selectedIdx), candidates.get(selectedIdx).length);
                        resultCentroids.add(selected);

                        // Fold the newly accepted centroid into the running minimum for the next iteration
                        for (int j = 0; j < candidates.size(); j++) {
                            double dist = distanceFunction.apply(candidates.get(j), selected);
                            if (dist < minDistToCentroid[j]) {
                                minDistToCentroid[j] = dist;
                            }
                        }
                    }

                    // 3. Weighted Lloyd's algorithm for refinement
                    for (int iter = 0; iter < maxIterations; iter++) {
                        // Assign candidates to closest centroids
                        for (int i = 0; i < candidates.size(); i++) {
                            double minDist = Double.POSITIVE_INFINITY;
                            int closestCentroid = 0;
                            for (int j = 0; j < resultCentroids.size(); j++) {
                                double dist = distanceFunction.apply(candidates.get(i), resultCentroids.get(j));
                                if (dist < minDist) {
                                    minDist = dist;
                                    closestCentroid = j;
                                }
                            }
                            assignments[i] = closestCentroid;
                        }

                        // Update centroids using weighted averages
                        int numCentroids = resultCentroids.size();
                        double[][] newCentroids = new double[numCentroids][candidates.get(0).length];
                        double[] totalWeights = new double[numCentroids]; // Use double for weighted sums

                        for (int i = 0; i < candidates.size(); i++) {
                            int centroidIdx = assignments[i];
                            // Ensure centroidIdx is within bounds (safety check)
                            if (centroidIdx >= 0 && centroidIdx < numCentroids) {
                                double weight = weights[i];
                                for (int d = 0; d < candidates.get(i).length; d++) {
                                    // Weighted sum: Σ(weight[i] * candidate[i])
                                    newCentroids[centroidIdx][d] += weight * candidates.get(i)[d];
                                }
                                totalWeights[centroidIdx] += weight;
                            }
                        }

                        // Check for convergence - iterate up to actual number of centroids, not k
                        boolean converged = true;
                        for (int i = 0; i < numCentroids; i++) {
                            if (totalWeights[i] > 0) {
                                for (int d = 0; d < newCentroids[i].length; d++) {
                                    // Weighted average: Σ(weight[i] * candidate[i]) / Σ weight[i]
                                    newCentroids[i][d] /= totalWeights[i];
                                }
                                // Check if centroid moved significantly
                                double dist = distanceFunction.apply(resultCentroids.get(i), newCentroids[i]);
                                if (dist > 1e-4) {
                                    converged = false;
                                }
                                maybeNormalizeCentroid(newCentroids[i]);
                                resultCentroids.set(i, newCentroids[i]);
                            }
                        }

                        if (converged) {
                            break;
                        }
                    }

                    // Ensure exactly k centroids by filling gaps if initialization terminated early
                    if (resultCentroids.size() < k) {
                        // Fill gap by selecting additional candidates that are farthest from existing centroids
                        int remaining = k - resultCentroids.size();
                        for (int gap = 0; gap < remaining; gap++) {
                            double maxMinDist = -1.0;
                            int bestCandidateIdx = -1;

                            // Find candidate with maximum minimum distance to existing centroids
                            for (int i = 0; i < candidates.size(); i++) {
                                // Check if this candidate is already a centroid
                                boolean isAlreadyCentroid = false;
                                for (double[] centroid : resultCentroids) {
                                    double dist = distanceFunction.apply(candidates.get(i), centroid);
                                    if (dist < 1e-10) {
                                        isAlreadyCentroid = true;
                                        break;
                                    }
                                }
                                if (isAlreadyCentroid) {
                                    continue;
                                }

                                // Find minimum distance to existing centroids
                                double minDist = Double.POSITIVE_INFINITY;
                                for (double[] centroid : resultCentroids) {
                                    double dist = distanceFunction.apply(candidates.get(i), centroid);
                                    minDist = Math.min(minDist, dist);
                                }

                                // Weight by candidate weight and distance
                                double weightedScore = weights[i] * minDist;
                                if (weightedScore > maxMinDist) {
                                    maxMinDist = weightedScore;
                                    bestCandidateIdx = i;
                                }
                            }

                            if (bestCandidateIdx >= 0) {
                                resultCentroids.add(Arrays.copyOf(candidates.get(bestCandidateIdx),
                                        candidates.get(bestCandidateIdx).length));
                            } else {
                                // Fallback: if all candidates are duplicates, add a slightly perturbed copy of last centroid
                                if (resultCentroids.size() > 0) {
                                    double[] base = resultCentroids.get(resultCentroids.size() - 1);
                                    double[] perturbed = Arrays.copyOf(base, base.length);
                                    for (int d = 0; d < perturbed.length; d++) {
                                        perturbed[d] += rand.nextGaussian() * 1e-6;
                                    }
                                    resultCentroids.add(perturbed);
                                }
                            }
                        }
                    }

                    return resultCentroids;
                }

                /**
                 * Select a random index weighted by the weights array.
                 */
                private int selectWeightedRandomIndex(List<double[]> candidates, int[] weights, Random rand) {
                    int totalWeight = 0;
                    for (int w : weights) {
                        totalWeight += w;
                    }

                    if (totalWeight <= 0) {
                        // Fallback to uniform random if all weights are zero
                        return rand.nextInt(candidates.size());
                    }

                    int r = rand.nextInt(totalWeight);
                    int cumulativeWeight = 0;
                    for (int i = 0; i < candidates.size(); i++) {
                        cumulativeWeight += weights[i];
                        if (cumulativeWeight > r) {
                            return i;
                        }
                    }
                    return candidates.size() - 1; // Fallback
                }

                /**
                 * Get a specific point by index from the run file.
                 */
                private double[] getPointAtIndex(GeneratedRunFileReader in, FrameTupleAccessor fta,
                        FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, int targetIndex,
                        IHyracksTaskContext ctx) throws HyracksDataException, IOException {

                    VSizeFrame frame = new VSizeFrame(ctx);
                    int currentIndex = 0;

                    while (in.nextFrame(frame)) {
                        ByteBuffer buffer = frame.getBuffer();
                        fta.reset(buffer);
                        int tupleCount = fta.getTupleCount();

                        for (int j = 0; j < tupleCount; j++) {
                            if (currentIndex == targetIndex) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (!ATYPETAGDESERIALIZER
                                        .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]).isListType()) {
                                    return null;
                                }

                                listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                return kMeansUtils.createPrimitveList(listAccessorConstant);
                            }
                            currentIndex++;
                        }
                    }
                    return null;
                }

                /**
                 * Read the points at the given tuple indices in a single forward pass of the run file.
                 * {@code sortedIndices} must be non-decreasing (the caller builds evenly-spaced, ascending
                 * indices). Returns a list parallel to {@code sortedIndices}; an entry is {@code null} if its
                 * index is past the end of the file or the tuple is not a list. The reader is shared and its
                 * position carries across lookups (the caller resets it once), so the indices must be read in one
                 * forward pass rather than one scan per index.
                 */
                private List<double[]> getPointsAtSortedIndices(GeneratedRunFileReader in, FrameTupleAccessor fta,
                        FrameTupleReference tuple, IScalarEvaluator eval, IPointable inputVal,
                        ListAccessor listAccessorConstant, KMeansUtils kMeansUtils, List<Integer> sortedIndices,
                        IHyracksTaskContext ctx) throws HyracksDataException, IOException {
                    List<double[]> results = new ArrayList<>(Collections.nCopies(sortedIndices.size(), null));
                    int ti = 0; // pointer into sortedIndices
                    int currentIndex = 0;
                    VSizeFrame frame = new VSizeFrame(ctx);

                    while (ti < sortedIndices.size() && in.nextFrame(frame)) {
                        ByteBuffer buffer = frame.getBuffer();
                        fta.reset(buffer);
                        int tupleCount = fta.getTupleCount();

                        for (int j = 0; j < tupleCount && ti < sortedIndices.size(); j++) {
                            // Multiple requested indices may map to the same tuple (duplicates in the list).
                            while (ti < sortedIndices.size() && sortedIndices.get(ti) == currentIndex) {
                                tuple.reset(fta, j);
                                eval.evaluate(tuple, inputVal);
                                if (ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()])
                                        .isListType()) {
                                    listAccessorConstant.reset(inputVal.getByteArray(), inputVal.getStartOffset());
                                    results.set(ti, kMeansUtils.createPrimitveList(listAccessorConstant));
                                }
                                ti++;
                            }
                            currentIndex++;
                        }
                    }
                    return results;
                }

                /**
                 * Reset the run file reader to the beginning, closing the reader it replaces.
                 * Every {@link MaterializerTaskState#createReader()} opens a fresh file handle, so the
                 * previous reader must be closed here or its handle leaks for the JVM's lifetime.
                 * {@code currentSampleReader} tracks the single live reader across the nested k-means
                 * passes (which reassign their local variables); the outer finally closes the last one.
                 */
                @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
                private GeneratedRunFileReader resetRunFileReader(IHyracksTaskContext ctx, UUID sampleUUID,
                        int partition) throws HyracksDataException {
                    closeCurrentSampleReader();
                    MaterializerTaskState sampleState =
                            (MaterializerTaskState) ctx.getStateObject(new PartitionedUUID(sampleUUID, partition));
                    GeneratedRunFileReader reader = sampleState.createReader();
                    reader.open(); // Open the reader before returning it
                    currentSampleReader = reader;
                    return reader;
                }

                @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
                private void closeCurrentSampleReader() throws HyracksDataException {
                    if (currentSampleReader != null) {
                        currentSampleReader.close();
                        currentSampleReader = null;
                    }
                }

                /**
                 * Perform memory-efficient hierarchical K-means clustering using run files.
                 */
                private HierarchicalClusterStructure performMemoryEfficientHierarchicalKMeans(IHyracksTaskContext ctx,
                        GeneratedRunFileReader in, FrameTupleAccessor fta, FrameTupleReference tuple,
                        IScalarEvaluator eval, IPointable inputVal, ListAccessor listAccessorConstant,
                        KMeansUtils kMeansUtils, VSizeFrame vSizeFrame, int partition, int totalTupleCount)
                        throws HyracksDataException, IOException {

                    HierarchicalClusterStructure structure = new HierarchicalClusterStructure();

                    // Perform initial K-means++ on all data to generate initial centroids.
                    // Seeded from the descriptor's trainSeed with a per-partition offset: same seed + same
                    // input order => identical centroids (regression tests), while partitions stay decorrelated.
                    Random rand = new Random(trainSeed * 31 + partition);
                    int maxKMeansIterations = 20;
                    ClusteringResult initialResult =
                            performInitialKMeansPlusPlus(ctx, in, fta, tuple, eval, inputVal, listAccessorConstant,
                                    kMeansUtils, K, rand, maxKMeansIterations, totalTupleCount, partition);

                    if (initialResult.centroids.isEmpty()) {
                        return structure;
                    }

                    // Extract embedding dimension and frame size for frame fit calculations
                    int embeddingDimension = initialResult.centroids.get(0).length;
                    if (embeddingDimension <= 0) {
                        return structure;
                    }
                    int frameSize = ctx.getInitialFrameSize();

                    // Add Level 0 (initial centroids) - these are the leaf nodes
                    List<HierarchicalClusterStructure.CentroidInfo> level0Info = new ArrayList<>();
                    for (int i = 0; i < initialResult.centroids.size(); i++) {
                        level0Info.add(new HierarchicalClusterStructure.CentroidInfo(i, -1,
                                initialResult.centroids.get(i), 0));
                    }
                    structure.levelCentroids.put(0, level0Info);

                    // Build subsequent levels using scalable K-means++ on centroids
                    List<double[]> currentCentroids = initialResult.centroids;
                    // Initialize currentK using square root reduction for balanced hierarchical structure
                    int currentK =
                            Math.min(K, Math.max(1, (int) Math.floor(Math.sqrt(initialResult.centroids.size()))));
                    int maxIterations = 20;
                    int maxLevels = 100;
                    // The K leaf centroids were stored at map key 0 above. Parent levels must occupy keys
                    // 1..maxLevel so the leaves stay at key 0 and are emitted by outputHierarchicalStructure
                    // (which walks keys 0..maxLevel). Starting at 0 would overwrite key 0 and re-key the true
                    // leaves to -1, which the emission loop never writes, collapsing the tree to ~sqrt(K) leaves.
                    int currentLevel = 1;

                    // Build subsequent levels
                    while (currentCentroids.size() > 1 && currentK > 1 && currentLevel < maxLevels) {
                        // Initialize parent level with empty centroids
                        structure.initializeParentLevel(currentLevel, currentK);

                        // Perform K-means++ clustering on centroids from previous level
                        ClusteringResult levelResult = performScalableKMeansPlusPlusOnCentroids(currentCentroids,
                                currentK, rand, maxIterations);

                        if (levelResult.centroids.isEmpty()) {
                            break;
                        }

                        // Check if current level fits in one frame
                        if (HierarchicalClusterStructure.doesLevelFitInFrame(levelResult.centroids.size(),
                                embeddingDimension, frameSize)) {
                            // Build this level before breaking (so it's stored in structure)
                            structure.buildLevelFromAssignments(currentCentroids, levelResult.centroids,
                                    levelResult.assignments, currentLevel, currentLevel - 1);
                            break;
                        }

                        // Build level using assignments - currentCentroids are children, levelResult.centroids are parents
                        structure.buildLevelFromAssignments(currentCentroids, levelResult.centroids,
                                levelResult.assignments, currentLevel, currentLevel - 1);

                        // Prepare for next level
                        currentCentroids = levelResult.centroids;
                        // Update currentK using square root reduction (more gradual than division by 2)
                        currentK = Math.max(1, (int) Math.floor(Math.sqrt(currentK)));
                        currentLevel++;
                    }

                    return structure;
                }

                /**
                 * Perform scalable K-means++ on centroids (not raw data).
                 */
                private ClusteringResult performScalableKMeansPlusPlusOnCentroids(List<double[]> centroids, int k,
                        Random rand, int maxIterations) throws HyracksDataException {
                    if (centroids.isEmpty() || k <= 0) {
                        return new ClusteringResult(new ArrayList<>(), new int[0]);
                    }

                    List<double[]> resultCentroids = new ArrayList<>();
                    int[] assignments = new int[centroids.size()]; // Declare assignments outside the loop

                    // K-means++ initialization
                    // 1. Choose first centroid randomly
                    int firstIdx = rand.nextInt(centroids.size());
                    resultCentroids.add(Arrays.copyOf(centroids.get(firstIdx), centroids.get(firstIdx).length));

                    // 2. Choose remaining centroids using weighted selection.
                    // D(x) is maintained incrementally against only the newly accepted centroid — same
                    // running-minimum bookkeeping as performWeightedKMeansPlusPlusOnCandidates, giving
                    // identical results at O(n*k) instead of O(n*k^2). Cheap at this level (the input is just
                    // the previous level's centroids) but the same quadratic shape.
                    double[] distances = new double[centroids.size()];
                    double[] minDistToCentroid = new double[centroids.size()];
                    double[] firstResultCentroid = resultCentroids.get(0);
                    for (int j = 0; j < centroids.size(); j++) {
                        minDistToCentroid[j] = distanceFunction.apply(centroids.get(j), firstResultCentroid);
                    }
                    for (int i = 1; i < k && i < centroids.size(); i++) {
                        double totalDistance = 0.0;

                        for (int j = 0; j < centroids.size(); j++) {
                            distances[j] = minDistToCentroid[j];
                            totalDistance += distances[j];
                        }

                        // Weighted random selection
                        double r = rand.nextDouble() * totalDistance;
                        double cumulativeDistance = 0.0;
                        int selectedIdx = 0;
                        for (int j = 0; j < centroids.size(); j++) {
                            cumulativeDistance += distances[j];
                            if (cumulativeDistance >= r) {
                                selectedIdx = j;
                                break;
                            }
                        }

                        double[] selected =
                                Arrays.copyOf(centroids.get(selectedIdx), centroids.get(selectedIdx).length);
                        resultCentroids.add(selected);

                        // Fold the newly accepted centroid into the running minimum
                        for (int j = 0; j < centroids.size(); j++) {
                            double dist = distanceFunction.apply(centroids.get(j), selected);
                            if (dist < minDistToCentroid[j]) {
                                minDistToCentroid[j] = dist;
                            }
                        }
                    }

                    // Gap-filling: If we have fewer than k centroids, fill gaps
                    if (resultCentroids.size() < k && !centroids.isEmpty()) {
                        int remaining = k - resultCentroids.size();

                        for (int gap = 0; gap < remaining; gap++) {
                            double maxMinDist = -1.0;
                            int bestIdx = -1;

                            // Find centroid farthest from all existing centroids
                            for (int j = 0; j < centroids.size(); j++) {
                                // Check if this centroid is already selected
                                boolean alreadySelected = false;
                                for (double[] existing : resultCentroids) {
                                    double dist = distanceFunction.apply(centroids.get(j), existing);
                                    if (dist < 1e-10) {
                                        alreadySelected = true;
                                        break;
                                    }
                                }
                                if (alreadySelected) {
                                    continue;
                                }

                                // Find minimum distance to existing centroids
                                double minDist = Double.POSITIVE_INFINITY;
                                for (double[] existing : resultCentroids) {
                                    double dist = distanceFunction.apply(centroids.get(j), existing);
                                    minDist = Math.min(minDist, dist);
                                }

                                if (minDist > maxMinDist) {
                                    maxMinDist = minDist;
                                    bestIdx = j;
                                }
                            }

                            // Add best candidate or fallback to random
                            if (bestIdx >= 0) {
                                resultCentroids
                                        .add(Arrays.copyOf(centroids.get(bestIdx), centroids.get(bestIdx).length));
                            } else {
                                // Fallback: all candidates are duplicates, select random
                                int randomIdx = rand.nextInt(centroids.size());
                                resultCentroids
                                        .add(Arrays.copyOf(centroids.get(randomIdx), centroids.get(randomIdx).length));
                            }
                        }
                    }

                    // 3. Lloyd's algorithm for refinement
                    for (int iter = 0; iter < maxIterations; iter++) {
                        // Assign points to closest centroids
                        for (int i = 0; i < centroids.size(); i++) {
                            double minDist = Double.POSITIVE_INFINITY;
                            int closestCentroid = 0;
                            for (int j = 0; j < resultCentroids.size(); j++) {
                                double dist = distanceFunction.apply(centroids.get(i), resultCentroids.get(j));
                                if (dist < minDist) {
                                    minDist = dist;
                                    closestCentroid = j;
                                }
                            }
                            assignments[i] = closestCentroid;
                        }

                        // Update centroids
                        double[][] newCentroids = new double[k][centroids.get(0).length];
                        int[] counts = new int[k];

                        for (int i = 0; i < centroids.size(); i++) {
                            int centroidIdx = assignments[i];
                            for (int d = 0; d < centroids.get(i).length; d++) {
                                newCentroids[centroidIdx][d] += centroids.get(i)[d];
                            }
                            counts[centroidIdx]++;
                        }

                        // Check for convergence
                        boolean converged = true;
                        for (int i = 0; i < k; i++) {
                            if (counts[i] > 0) {
                                for (int d = 0; d < newCentroids[i].length; d++) {
                                    newCentroids[i][d] /= counts[i];
                                }
                                // Check if centroid moved significantly
                                double dist = distanceFunction.apply(resultCentroids.get(i), newCentroids[i]);
                                if (dist > 1e-4) {
                                    converged = false;
                                }
                                maybeNormalizeCentroid(newCentroids[i]);
                                resultCentroids.set(i, newCentroids[i]);
                            } else {
                                // Reinitialize empty cluster
                                // Select random centroid from input centroids list
                                if (!centroids.isEmpty()) {
                                    int randomIdx = rand.nextInt(centroids.size());
                                    double[] reinit =
                                            Arrays.copyOf(centroids.get(randomIdx), centroids.get(randomIdx).length);
                                    maybeNormalizeCentroid(reinit);
                                    resultCentroids.set(i, reinit);
                                    converged = false; // Force continuation since we changed a centroid
                                }
                            }
                        }

                        if (converged) {
                            break;
                        }
                    }

                    return new ClusteringResult(resultCentroids, assignments);
                }

                /**
                 * Normalizes centroid in place to unit L2 norm when using cosine similarity (spherical
                 * k-means), so that centroid semantics match FAISS/Spark. Dot product is not normalized.
                 * No-op for other metrics.
                 */
                private void maybeNormalizeCentroid(double[] centroid) {
                    if (centroid != null && requiresNormalizedCentroids()) {
                        KMeansUtils.normalizeL2(centroid);
                    }
                }

                /**
                 * Whether the current distance function requires centroids to be L2-normalized after each
                 * Lloyd update. Normalization is required only for cosine (spherical k-means); aligns with
                 * FAISS spherical k-means and Spark's CosineDistanceMeasure. Dot product (MIPS) uses raw
                 * centroids and does not require normalization.
                 */
                private boolean requiresNormalizedCentroids() {
                    return similarityMetric == VectorSimilarityMetric.COSINE;
                }

                private static IVTreeDistanceFunction distanceFunctionFor(VectorSimilarityMetric metric) {
                    try {
                        return new VectorDistanceFunctionFactory(metric).createDistanceFunction();
                    } catch (HyracksDataException e) {
                        throw new IllegalStateException("Failed to build vector distance function for " + metric, e);
                    }
                }
            };
        }
    }

    /**
     * Simple state class to pass tuple count between activities.
     */
    private static class TupleCountState extends AbstractStateObject {
        private int totalTupleCount;

        public TupleCountState(JobId jobId, PartitionedUUID objectId) {
            super(jobId, objectId);
            this.totalTupleCount = 0;
        }

        public int getTotalTupleCount() {
            return totalTupleCount;
        }

        public void addTupleCount(int count) {
            this.totalTupleCount += count;
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
            out.writeInt(totalTupleCount);
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
            totalTupleCount = in.readInt();
        }
    }

    /**
     * Result class for K-means++ clustering operations.
     */
    private record ClusteringResult(List<double[]> centroids, int[] assignments) {
    }

    /**
     * Data structure to hold hierarchical clustering results with parent-child relationships.
     */
    private static class HierarchicalClusterStructure {
        // Store centroids for each level (separate parent and child levels)
        private final Map<Integer, List<CentroidInfo>> levelCentroids;

        // Track parent-child relationships
        private final Map<Integer, Map<Integer, List<Integer>>> parentChildRelations;

        private HierarchicalClusterStructure() {
            this.levelCentroids = new HashMap<>();
            this.parentChildRelations = new HashMap<>();
        }

        private static class CentroidInfo {
            public final int centroidId;
            public final int parentClusterId;
            public final double[] embedding;
            public final int level;
            public final List<Integer> childrenIds;

            private CentroidInfo(int centroidId, int parentClusterId, double[] embedding, int level) {
                this.centroidId = centroidId;
                this.parentClusterId = parentClusterId;
                this.embedding = embedding;
                this.level = level;
                this.childrenIds = new ArrayList<>();
            }
        }

        private int getNumLevels() {
            return levelCentroids.size();
        }

        /**
         * Initialize a level with empty centroids (for parents)
         */
        private void initializeParentLevel(int level, int parentCount) {
            List<CentroidInfo> parentLevel = new ArrayList<>();
            Map<Integer, List<Integer>> parentChildMap = new HashMap<>();

            // Initialize empty parent centroids
            for (int i = 0; i < parentCount; i++) {
                parentLevel.add(new CentroidInfo(i, -1, null, level)); // -1 means no parent (root level)
                parentChildMap.put(i, new ArrayList<>());
            }

            this.levelCentroids.put(level, parentLevel);
            this.parentChildRelations.put(level, parentChildMap);
        }

        /**
         * Build parent-child relationships using assignments
         */
        private void buildLevelFromAssignments(List<double[]> childCentroids, List<double[]> parentCentroids,
                int[] assignments, int parentLevel, int childLevel) {

            // 1. Populate parent centroids
            List<CentroidInfo> parentLevelInfo = this.levelCentroids.get(parentLevel);
            for (int i = 0; i < parentCentroids.size() && i < parentLevelInfo.size(); i++) {
                CentroidInfo parentInfo = parentLevelInfo.get(i);
                // Update parent centroid with actual embedding
                parentLevelInfo.set(i,
                        new CentroidInfo(parentInfo.centroidId, -1, parentCentroids.get(i), parentLevel));
            }

            // 2. Create child level with proper parent assignments
            List<CentroidInfo> childLevelInfo = new ArrayList<>();
            Map<Integer, List<Integer>> parentChildMap = this.parentChildRelations.get(parentLevel);

            for (int i = 0; i < assignments.length; i++) {
                int parentClusterId = assignments[i]; // Which parent cluster this child belongs to
                int childId = i; // Child centroid index

                // Create child centroid info
                CentroidInfo childInfo = new CentroidInfo(childId, parentClusterId, childCentroids.get(i), childLevel);
                childLevelInfo.add(childInfo);

                // Add child to parent's children list
                if (parentChildMap.containsKey(parentClusterId)) {
                    parentChildMap.get(parentClusterId).add(childId);
                }
            }

            // Store child level information
            this.levelCentroids.put(childLevel, childLevelInfo);
        }

        /**
         * Output format: <treeLevel, centroidId, parentClusterId, embedding>
         * Uses BFS traversal starting from root level
         */
        private void outputHierarchicalStructure(FrameTupleAppender appender, IFrameWriter writer,
                IHyracksTaskContext ctx) throws HyracksDataException {
            // levelCentroids keys: 0 = leaf level in k-means terms, maxLevel = root.
            int maxLevel = -1;
            for (Integer level : levelCentroids.keySet()) {
                maxLevel = Math.max(maxLevel, level);
            }

            if (maxLevel == -1) {
                return;
            }

            // Emission order: bottom-up (leaves first, root last) so that
            // VTreeStaticStructureBuilder writes leaves at the lowest page ids and
            // the root last (at the highest page id).
            //
            // Centroid IDs preserve the BFS-from-root convention (root = 0..N_root-1,
            // leaves at the highest IDs), independent of emission order. To achieve
            // that with bottom-up emission, we pre-compute per-level ID offsets so the
            // root level starts at 0, the next level down starts at root_size, etc.
            int[] idOffset = new int[maxLevel + 1];
            idOffset[maxLevel] = 0;
            for (int L = maxLevel - 1; L >= 0; L--) {
                List<CentroidInfo> levelAbove = levelCentroids.get(L + 1);
                int sizeAbove = (levelAbove != null) ? levelAbove.size() : 0;
                idOffset[L] = idOffset[L + 1] + sizeAbove;
            }

            // Walk levels bottom-up: levelCentroids key 0 (leaves) → key maxLevel (root).
            // The tuple's treeLevel field keeps the existing convention: root = 0, leaf = maxLevel.
            for (int L = 0; L <= maxLevel; L++) {
                List<CentroidInfo> levelInfo = levelCentroids.get(L);
                if (levelInfo == null) {
                    continue;
                }
                int treeLevel = maxLevel - L;
                int globalCentroidId = idOffset[L];
                for (CentroidInfo centroid : levelInfo) {
                    createHierarchicalTuple(treeLevel, globalCentroidId, centroid.parentClusterId, centroid.embedding,
                            appender, writer, ctx);
                    globalCentroidId++;
                }
            }
        }

        private static void createHierarchicalTuple(int treeLevel, int centroidId, int parentClusterId,
                double[] embedding, FrameTupleAppender appender, IFrameWriter writer, IHyracksTaskContext ctx)
                throws HyracksDataException {
            try {
                // Apply clipping to embedding before creating tuple to prevent exorbitant values
                double[] clippedEmbedding = clipCentroid(embedding);

                // Create tuple: <treeLevel, centroidId, parentClusterId, embedding>
                ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(4);
                tupleBuilder.reset();

                // Field 0: Tree Level
                tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, treeLevel);

                // Field 1: Centroid ID
                tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, centroidId);

                // Field 2: Parent Cluster ID
                tupleBuilder.addField(IntegerSerializerDeserializer.INSTANCE, parentClusterId);

                // Field 3: Embedding - create AsterixDB AOrderedList format using clipped embedding
                OrderedListBuilder listBuilder = new OrderedListBuilder();
                listBuilder.reset(new AOrderedListType(ADOUBLE, "embedding"));

                ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                AMutableDouble aDouble = new AMutableDouble(0.0);

                for (int i = 0; i < clippedEmbedding.length; i++) {
                    aDouble.setValue(clippedEmbedding[i]);
                    storage.reset();
                    storage.getDataOutput().writeByte(ATypeTag.DOUBLE.serialize());
                    ADoubleSerializerDeserializer.INSTANCE.serialize(aDouble, storage.getDataOutput());
                    listBuilder.addItem(storage);
                }

                storage.reset();
                listBuilder.write(storage.getDataOutput(), true);
                tupleBuilder.addField(storage.getByteArray(), 0, storage.getLength());

                // Append tuple to frame, handle buffer overflow manually
                if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                        tupleBuilder.getSize())) {
                    // Frame is full, flush and reset
                    FrameUtils.flushFrame(appender.getBuffer(), writer);
                    appender.reset(new VSizeFrame(ctx), true);
                    appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize());
                }

            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        }

        /**
         * Calculate estimated tuple size for hierarchical output format (DOUBLE type).
         * Formula: 38 + 13 × dimension bytes
         * Breakdown:
         * - Tuple overhead: 20 bytes (4 bytes tuple offset + 4×4 bytes field offsets)
         * - Fixed fields: 12 bytes (3 integers: treeLevel, centroidId, parentClusterId)
         * - AOrderedList overhead: 6 bytes (tag + itemTag + list size)
         * - Item offsets: 4 bytes × dimension
         * - Item data: 9 bytes × dimension (1 byte type tag + 8 bytes double)
         * @param embeddingDimension The dimension of the embedding vector
         * @return Estimated tuple size in bytes
         */
        private static long calculateEstimatedTupleSize(int embeddingDimension) {
            return 38L + 13L * embeddingDimension;
        }

        /**
         * Check if a level with given number of centroids fits in one frame.
         * @param centroidCount Number of centroids at the level
         * @param embeddingDimension Dimension of embedding vectors
         * @param frameSize Frame size in bytes
         * @return true if the level fits in one frame, false otherwise
         */
        private static boolean doesLevelFitInFrame(int centroidCount, int embeddingDimension, int frameSize) {
            if (centroidCount <= 0 || embeddingDimension <= 0 || frameSize <= 0) {
                return false;
            }
            long tupleSize = calculateEstimatedTupleSize(embeddingDimension);
            long totalDataSize = (long) centroidCount * tupleSize;
            long frameOverhead = 9L + (4L * centroidCount); // META_DATA_LEN + tuple offsets
            long totalSize = totalDataSize + frameOverhead;
            return totalSize <= frameSize;
        }

        /**
         * Clips centroid values to reasonable bounds to prevent exorbitant values.
         * @param centroid The centroid array to clip
         * @return Clipped centroid array with values bounded between DEFAULT_CLIP_MIN and DEFAULT_CLIP_MAX
         */
        private static double[] clipCentroid(double[] centroid) {
            if (centroid == null) {
                return centroid;
            }

            double[] clipped = new double[centroid.length];

            for (int i = 0; i < centroid.length; i++) {
                double value = centroid[i];

                // Check for NaN or Infinity
                if (Double.isNaN(value) || Double.isInfinite(value)) {
                    clipped[i] = 0.0; // Replace with 0
                } else if (value < DEFAULT_CLIP_MIN) {
                    clipped[i] = DEFAULT_CLIP_MIN;
                } else if (value > DEFAULT_CLIP_MAX) {
                    clipped[i] = DEFAULT_CLIP_MAX;
                } else {
                    clipped[i] = value;
                }
            }

            return clipped;
        }
    }

}
