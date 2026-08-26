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
package org.apache.asterix.runtime.operators.kmeans;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The CLUSTER BY k-means|| RECLUSTER stage -- a single-input Score operator that consumes ONLY the broadcast
 * partials and emits plain centroid vectors. It merges the partials deterministically, then reduces the
 * weighted candidate pool to the initial centroids C0 with weighted k-means++ (see
 * {@link #weightedKMeansPlusPlus}), which weighs each candidate's mass against its distance from the centroids
 * already chosen. Fewer than {@code count} centroids come back when fewer than that many candidates attracted
 * points.
 * <p>
 * There is no vector input: the sole input is the broadcast partials envelope stream, so the stage is a pure
 * reduction over the partials. Two activities, and points never move between them. <b>StorePool</b> is a sink
 * that materializes the broadcast input as task state ({@link MaterializerTaskState}); <b>Score</b> is a SOURCE
 * activity behind a blocking edge, because an input connector across a blocking-edge stage boundary is never
 * delivered -- which is why the pool has to be materialized rather than streamed in. Score collects the pool
 * through {@link KMeansStageRuntime} and reduces it.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public final class KMeansReclusterOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int STORE_POOL_ACTIVITY_ID = 0;
    private static final int SCORE_ACTIVITY_ID = 1;

    // Seed for the weighted k-means++ draw, supplied by the plan. One seed covers the whole decision: this stage
    // runs on a single partition over one already-merged pool. It does not on its own make the pick
    // reproducible -- the draw walks a prefix sum in array order, so the pool's ORDER decides too.
    private final long reclusterSeed;

    // How many centroids to keep. Non-negative.
    private final int count;
    // Column of the pool variable in the input's tuples.
    private final int poolColumn;
    /** Frame budget for the partial sort; see KMeansStageRuntime#foldPartials. */
    private final int framesLimit;

    public KMeansReclusterOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor vectorRecDesc,
            int count, int poolColumn, int framesLimit, long reclusterSeed) {
        // One input: the broadcast partials, which are always envelope rows (the oversampling loop's output).
        super(spec, 1, 1);
        this.reclusterSeed = reclusterSeed;
        this.framesLimit = framesLimit;
        this.count = count;
        this.poolColumn = poolColumn;
        outRecDescs[0] = vectorRecDesc;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StorePoolActivityNode storePool = new StorePoolActivityNode(new ActivityId(odId, STORE_POOL_ACTIVITY_ID));
        ScoreActivityNode score = new ScoreActivityNode(new ActivityId(odId, SCORE_ACTIVITY_ID));

        builder.addActivity(this, storePool);
        builder.addSourceEdge(0, storePool, 0);

        builder.addActivity(this, score);
        builder.addTargetEdge(0, score, 0);

        builder.addBlockingEdge(storePool, score);
    }

    private void emitRecluster(IHyracksTaskContext ctx, TaskId taskId, KMeansStageRuntime rt,
            KMeansStageRuntime.Emitter emitter, int partition) throws Exception {
        if (partition != 0) {
            return; // the merged result is identical everywhere; one partition speaks
        }
        int poolSize = rt.poolSize();
        // Only the means outlive the fold, because weighted k-means++ rereads them once per centroid it
        // picks. Partials arrive ordered by pool position, so a position's total is complete when the next one
        // begins and its mean can be handed straight on -- no per-candidate count or sum vector is retained.
        long[] memberWeights = new long[poolSize];
        int[] meanCount = { 0 };
        try (KMeansLoopIO.VectorList means =
                new KMeansLoopIO.VectorList(ctx, ctx.getJobletContext().getJobId(), taskId, framesLimit)) {
            rt.foldPartials(poolSize, (position, weight, sum) -> {
                double[] mean = new double[sum.length];
                for (int d = 0; d < mean.length; d++) {
                    mean[d] = sum[d] / weight;
                }
                means.add(mean);
                memberWeights[meanCount[0]++] = weight;
            });
            means.seal();

            // Fewer means than requested says the input holds fewer distinct vectors: a candidate takes rows
            // unless an earlier one sits at the same point, and the pool stops growing only once every row
            // does. This stage alone can tell that apart from a cluster emptying during refinement, so it is
            // the one that names it. A warning, not a failure -- the groups that exist are a usable answer to
            // a k the data cannot supply, and every row still lands in exactly one of them.
            if (meanCount[0] < count && ctx.getWarningCollector().shouldWarn()) {
                ctx.getWarningCollector()
                        .warn(Warning.of(null, ErrorCode.CLUSTER_BY_INVALID_INPUT,
                                "NumClusters is " + count + " but only " + meanCount[0]
                                        + " distinct vector(s) were found in the input"
                                        + (meanCount[0] == 0 ? " -- no row matched the declared Dimension"
                                                : ", so only " + meanCount[0] + " cluster(s) are returned")));
            }
            // A shortfall is not topped up from the candidate pool: a candidate that took no rows is by
            // construction a duplicate of one that did, so it stands for a position already covered.
            weightedKMeansPlusPlus(means, memberWeights, meanCount[0], emitter);
        }
    }

    /**
     * Reduces the weighted candidates to at most {@code count} centroids with weighted k-means++, the closing
     * step of the k-means|| initialization. The first centre is drawn proportional to weight alone; each
     * subsequent one proportional to {@code w_x * d^2(x, chosen)}, so mass and distance both count.
     * <p>
     * Holds one weight, nearest-distance, score and taken flag per candidate -- scalars, not vectors. The
     * vectors are read and not retained: a round needs them twice, to fetch the member it just picked and to
     * stream the set past that member refreshing the nearest distances. Both reads are sequential, so
     * {@link KMeansLoopIO.VectorList} can serve them from disk once they outgrow the budget. Centroids are
     * emitted as they are picked, so the result does not accumulate either.
     *
     * @return how many centroids were emitted, at most {@code min(count, n)}.
     */
    private int weightedKMeansPlusPlus(KMeansLoopIO.VectorList means, long[] memberWeights, int n,
            KMeansStageRuntime.Emitter emitter) throws Exception {
        if (n == 0) {
            return 0;
        }
        final double[] nearest = new double[n]; // d^2 to the closest already-chosen centre
        Arrays.fill(nearest, Double.POSITIVE_INFINITY);
        final boolean[] taken = new boolean[n];
        final Random rng = new Random(reclusterSeed);
        final double[] score = new double[n];
        final int target = Math.min(count, n);
        int chosen = 0;
        while (chosen < target) {
            boolean first = chosen == 0;
            double total = 0.0;
            for (int i = 0; i < n; i++) {
                // Before the first pick there is nothing to measure against, so weight alone drives the draw.
                double s = taken[i] ? 0.0 : memberWeights[i] * (first ? 1.0 : nearest[i]);
                score[i] = s > 0 && !Double.isNaN(s) && !Double.isInfinite(s) ? s : 0.0;
                total += score[i];
            }
            int pick = -1;
            if (total > 0) {
                double r = rng.nextDouble() * total;
                double acc = 0.0;
                for (int i = 0; i < n; i++) {
                    if (score[i] > 0) {
                        acc += score[i];
                        if (acc >= r) {
                            pick = i;
                            break;
                        }
                    }
                }
            }
            if (pick < 0) {
                // Every remaining member coincides with one already chosen (all weighted distances vanish).
                // Fall back to pool order so the outcome stays deterministic rather than dropping a centroid.
                for (int i = 0; i < n && pick < 0; i++) {
                    if (!taken[i]) {
                        pick = i;
                    }
                }
            }
            if (pick < 0) {
                break;
            }
            taken[pick] = true;
            final double[] picked = means.get(pick);
            emitter.plainVector(picked);
            chosen++;
            final int[] at = { 0 };
            means.stream(candidate -> {
                int i = at[0]++;
                if (!taken[i]) {
                    double d = VectorDistanceCalculation.euclideanSquared(candidate, picked);
                    if (d < nearest[i]) {
                        nearest[i] = d;
                    }
                }
            });
        }
        return chosen;
    }

    /** Materializes the broadcast input as task state for the Score activity to read. */
    private final class StorePoolActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private StorePoolActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private MaterializerTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.open(ctx);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.appendFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.close();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
        }
    }

    private final class ScoreActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private ScoreActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final RecordDescriptor vecRecDesc = outRecDescs[0];
            return new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    writer.open();
                    try {
                        MaterializerTaskState poolState = (MaterializerTaskState) ctx.getStateObject(
                                new TaskId(new ActivityId(getOperatorId(), STORE_POOL_ACTIVITY_ID), partition));
                        KMeansStageRuntime rt =
                                new KMeansStageRuntime(ctx, writer, vecRecDesc, poolColumn, framesLimit);
                        rt.readInput(poolState);
                        KMeansStageRuntime.Emitter emitter = rt.newEmitter();
                        emitRecluster(ctx, new TaskId(getActivityId(), partition), rt, emitter, partition);
                        emitter.flush();
                    } catch (Exception e) {
                        writer.fail();
                        throw HyracksDataException.create(e);
                    } finally {
                        writer.close();
                    }
                }
            };
        }
    }
}
