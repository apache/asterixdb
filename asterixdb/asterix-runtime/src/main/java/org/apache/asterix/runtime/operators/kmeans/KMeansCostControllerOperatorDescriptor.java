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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
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
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — <b>Op1 Cost / Controller</b>: the loop head, the
 * registered descriptor for the {@code OVERSAMPLE_LOOP} logical operator (so the builder wires the vectors+seed
 * inputs here and the downstream RECLUSTER reads the weighed pool from here), and the fork of the systolic
 * sub-graph.
 * <p>
 * Three activities:
 * <ul>
 * <li><b>StoreVectors</b> (input 0, sink): decodes each resident vector once (ordered list -> {@code double[]})
 * and materializes the per-partition <b>vector run file</b> as raw doubles ({@link KMeansLoopIO#POOL_RD}) — no
 * REPLICATE, decode-once.</li>
 * <li><b>StoreSeed</b> (input 1, sink; the broadcast seed): decodes the seed into the per-partition <b>pool run
 * file</b> ({@code pool[0]}), and creates + registers this partition's {@link LoopControlState} (permit) and the
 * pool run file, so the co-located Sample/Release (Op3/Op5) can find them.</li>
 * <li><b>CostLoop</b> (a 0-input, 2-output source behind both blocking edges): runs the inline loop. Each round
 * reads {@code pool[r]} and streams the vector run file to a local potential {@code localSigma}, emits
 * {@code {round, localSigma}} on <b>output 1</b> (to PhiMerge), then {@code permit.acquire()} — waiting for
 * Release to append the round's global draws and release. After {@code loopRounds}, partition 0 reads the final
 * pool and emits it as {@link KMeansVectorCodec.PoolEnvelopeWriter KIND_POOL envelopes} on <b>output 0</b>.
 * Output 0 is idle during the loop, so the blocking consumer cannot back-pressure the iteration.</li>
 * </ul>
 * The loop is acyclic in the job graph — Release's feedback to CostLoop is the shared permit + pool run file, not
 * a data edge. The sampling itself is unchanged by this arrangement — the per-round/per-partition seed lives in
 * Sample, so the draws depend only on the data. Single-node vs multi-node is irrelevant here — this
 * sub-graph works on any topology (the co-located Op1/Op3/Op5 share an NC's joblet state; the merges
 * are single-node).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Declared dimension threaded to the vector/seed decoder")
public class KMeansCostControllerOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int STORE_VECTORS_ACTIVITY_ID = 0;
    private static final int STORE_SEED_ACTIVITY_ID = 1;
    private static final int COST_LOOP_ACTIVITY_ID = 2;

    private static final int OUT_POOL = 0; // weighed pool -> RECLUSTER (KIND_POOL envelopes)
    private static final int OUT_SIGMA = 1; // per-round local potential -> PhiMerge (SCALAR_RD)

    private final String loopKey;
    private final int vectorColumn; // vector column in input 0
    private final int seedColumn; // vector column in input 1 (the seed)
    private final int loopRounds; // N oversampling rounds
    private final int framesLimit; // block budget for the bounded scans
    // The declared Dimension, enforced by the decoder (see KMeansVectorCodec.ListVectorDecoder).
    private final int dimension;

    public KMeansCostControllerOperatorDescriptor(IOperatorDescriptorRegistry spec,
            RecordDescriptor poolEnvelopeRecDesc, RecordDescriptor sigmaRecDesc, String loopKey, int vectorColumn,
            int seedColumn, int loopRounds, int framesLimit, int dimension) {
        super(spec, 2, 2);
        this.loopKey = loopKey;
        this.vectorColumn = vectorColumn;
        this.seedColumn = seedColumn;
        this.loopRounds = loopRounds;
        this.framesLimit = framesLimit;
        this.dimension = dimension;
        outRecDescs[OUT_POOL] = poolEnvelopeRecDesc;
        outRecDescs[OUT_SIGMA] = sigmaRecDesc;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        StoreActivity storeVectors = new StoreActivity(new ActivityId(odId, STORE_VECTORS_ACTIVITY_ID), true);
        StoreActivity storeSeed = new StoreActivity(new ActivityId(odId, STORE_SEED_ACTIVITY_ID), false);
        CostLoopActivity costLoop = new CostLoopActivity(new ActivityId(odId, COST_LOOP_ACTIVITY_ID));

        builder.addActivity(this, storeVectors);
        builder.addSourceEdge(0, storeVectors, 0);
        builder.addActivity(this, storeSeed);
        builder.addSourceEdge(1, storeSeed, 0);
        builder.addActivity(this, costLoop);
        builder.addTargetEdge(OUT_POOL, costLoop, OUT_POOL);
        builder.addTargetEdge(OUT_SIGMA, costLoop, OUT_SIGMA);

        builder.addBlockingEdge(storeVectors, costLoop);
        builder.addBlockingEdge(storeSeed, costLoop);
    }

    /**
     * Materializes one input stream into a per-partition raw-double run file. {@code vectors=true} stores input 0
     * under the vectors id; {@code vectors=false} stores the seed as {@code pool[0]} under the pool id and also
     * creates + registers this partition's {@link LoopControlState}.
     */
    private final class StoreActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;
        private final boolean vectors;

        private StoreActivity(ActivityId id, boolean vectors) {
            super(id);
            this.vectors = vectors;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final int column = vectors ? vectorColumn : seedColumn;
            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
                private final FrameTupleReference tuple = new FrameTupleReference();
                private final KMeansVectorCodec.ListVectorDecoder decoder =
                        new KMeansVectorCodec.ListVectorDecoder(dimension);
                private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
                private MaterializerTaskState state;
                private VSizeFrame frame;
                private FrameTupleAppender appender;

                @Override
                public void open() throws HyracksDataException {
                    state = LoopControlState.sharedRunFile(ctx,
                            vectors ? LoopControlState.vectorsStateId(loopKey, partition)
                                    : LoopControlState.poolStateId(loopKey, partition));
                    frame = new VSizeFrame(ctx);
                    appender = new FrameTupleAppender(frame);
                    if (!vectors) {
                        // Register the loop control (permit) as soon as the pool file exists, so the co-located
                        // Sample/Release can rendezvous even before CostLoop starts.
                        LoopControlState control = new LoopControlState(ctx.getJobletContext().getJobId(),
                                LoopControlState.controlStateId(loopKey, partition),
                                new TaskId(getActivityId(), partition));
                        ctx.setStateObject(control);
                    }
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        tuple.reset(accessor, i);
                        double[] vec = decoder.decode(tuple, column);
                        if (vec == null) {
                            // Not a numeric array of the declared width: skip the row with a warning.
                            if (ctx.getWarningCollector().shouldWarn()) {
                                ctx.getWarningCollector()
                                        .warn(Warning.of(null, ErrorCode.CLUSTER_BY_INVALID_INPUT,
                                                "a row's clustering expression is not a numeric array of the declared "
                                                        + "dimension " + dimension + "; the row was excluded"));
                            }
                            continue;
                        }
                        tb.reset();
                        KMeansLoopIO.writeRawVector(tb, vec);
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            flushToState();
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new RuntimeDataException(ErrorCode.CLUSTER_BY_INVALID_INPUT,
                                        "a vector is too large to fit in a frame");
                            }
                        }
                    }
                }

                private void flushToState() throws HyracksDataException {
                    if (appender.getTupleCount() > 0) {
                        state.appendFrame(appender.getBuffer());
                        appender.reset(frame, true);
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    flushToState();
                    if (vectors) {
                        // The vector file is fully written; close the writer handle now (readers open their own
                        // independent handles via createReader). The POOL writer must stay open across the loop
                        // for Release's per-round appends -- CostLoop closes it after the final read.
                        state.close();
                    }
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    // The run file is created in open() and only registered in close(), so on this path nothing
                    // else holds a reference and nothing else will close it.
                    if (state != null) {
                        state.close();
                    }
                }
            };
        }
    }

    /** The inline loop: a 0-input, 2-output source behind both Store blocking edges. */
    private final class CostLoopActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private CostLoopActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
            return new AbstractOperatorNodePushable() {
                private final IFrameWriter[] writers = new IFrameWriter[2];

                @Override
                public int getInputArity() {
                    return 0;
                }

                @Override
                public IFrameWriter getInputFrameWriter(int index) {
                    // 0-input source: the framework never asks for an input writer. Unchecked by
                    // necessity -- IOperatorNodePushable.getInputFrameWriter declares no exception.
                    throw new IllegalStateException(
                            "kmeans loop source has no inputs; getInputFrameWriter(" + index + ") is unreachable");
                }

                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                    writers[index] = writer;
                }

                @Override
                public void initialize() throws HyracksDataException {
                    final IFrameWriter poolWriter = writers[OUT_POOL];
                    final IFrameWriter sigmaWriter = writers[OUT_SIGMA];
                    poolWriter.open();
                    sigmaWriter.open();
                    // Held outside the try so the finally can close the pool handle on every path, not just the
                    // successful one -- an aborted loop otherwise leaves this writer open until joblet cleanup.
                    MaterializerTaskState poolState = null;
                    try {
                        // Registered by StoreActivity, which addBlockingEdge (contributeActivities) joins ahead
                        // of this activity -- so these are already present; no wait is warranted.
                        LoopControlState control = (LoopControlState) LoopControlState.required(ctx,
                                LoopControlState.controlStateId(loopKey, partition));
                        poolState = (MaterializerTaskState) LoopControlState.required(ctx,
                                LoopControlState.poolStateId(loopKey, partition));
                        MaterializerTaskState vectorState = (MaterializerTaskState) LoopControlState.required(ctx,
                                LoopControlState.vectorsStateId(loopKey, partition));
                        runLoop(control, poolState, vectorState, sigmaWriter);
                        emitWeighPartials(poolState, vectorState, poolWriter);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        poolWriter.fail();
                        sigmaWriter.fail();
                        throw HyracksDataException.create(e);
                    } catch (Exception e) {
                        poolWriter.fail();
                        sigmaWriter.fail();
                        throw HyracksDataException.create(e);
                    } finally {
                        // Release has appended every round and Sample/Cost have read for the last time, so the
                        // pool writer handle can go (StoreVectors already closed the vector writer). The managed
                        // workspace files themselves are reclaimed at joblet cleanup.
                        if (poolState != null) {
                            poolState.close();
                        }
                        poolWriter.close();
                        sigmaWriter.close();
                    }
                }

                private void runLoop(LoopControlState control, MaterializerTaskState poolState,
                        MaterializerTaskState vectorState, IFrameWriter sigmaWriter)
                        throws HyracksDataException, InterruptedException {
                    FrameTupleAppender sigmaAppender = new FrameTupleAppender(new VSizeFrame(ctx));
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(3);
                    MaterializerTaskState scoreState = null;
                    try {
                        for (int r = 0; r < loopRounds; r++) {
                            // A fresh column per round; the previous one is dead once Op3 consumed it, which the
                            // loop's ordering guarantees has happened before this round begins.
                            if (scoreState != null) {
                                scoreState.close();
                                scoreState.deleteFile();
                            }
                            scoreState = LoopControlState.sharedRunFile(ctx,
                                    LoopControlState.scoreStateId(loopKey, partition));
                            ctx.setStateObject(scoreState);
                            final KMeansLoopIO.ScoreColumnWriter column =
                                    new KMeansLoopIO.ScoreColumnWriter(scoreState, ctx);
                            final double[] localSum = { 0.0d };
                            // Blocked against pool[r] rather than holding it: the pool is 2*k per round, so the
                            // resident form grew with the requested cluster count. Vectors still reach the sink in
                            // run-file order, so localSum adds its terms in the same order as before.
                            KMeansLoopIO.streamScoredAgainstPool(vectorState, poolState, ctx, framesLimit,
                                    (vecs, n, nearest, nearestIdx) -> {
                                        for (int i = 0; i < n; i++) {
                                            double best = nearest[i];
                                            if (!Double.isNaN(best) && best != Double.POSITIVE_INFINITY) {
                                                localSum[0] += best;
                                            }
                                        }
                                        // Recorded for Op3, which needs exactly these minima against this same pool.
                                        column.append(nearest, nearestIdx, n);
                                    });
                            column.finish();
                            tb.reset();
                            tb.addField(IntegerSerializerDeserializer.INSTANCE, r);
                            tb.addField(IntegerSerializerDeserializer.INSTANCE, partition);
                            tb.addField(DoubleSerializerDeserializer.INSTANCE, localSum[0]);
                            if (!sigmaAppender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "sigma tuple exceeds a frame");
                            }
                            sigmaAppender.write(sigmaWriter, true); // write + clear the frame for the next round
                            sigmaWriter.flush(); // push this round's localSigma so PhiMerge can proceed
                            // Wait for Release to append round r's global draws (pool[r] -> pool[r+1]) and release.
                            control.awaitTurn("kmeans systolic loop");
                        }
                    } finally {
                        if (scoreState != null) {
                            scoreState.close();
                            scoreState.deleteFile();
                        }
                    }
                }

                /**
                 * The terminal weighing pass: once the rounds are done, weigh every resident vector against the
                 * final candidate pool -- this partition's pool run file already holds the complete,
                 * byte-identical C -- and emit the pool echo (partition 0 only, so RECLUSTER learns how many
                 * candidates there are) plus this partition's non-empty (count, sum) partials, which are what
                 * RECLUSTER reduces.
                 * <p>
                 * The accumulators are the reason this is two passes. There is one (count, sum) slot per pool
                 * member, and the pool grows with k, so holding all of them was the last structure here whose
                 * size the user could choose: ~16 MB at k=512 and 384 dimensions, but ~3 GB at k=100,000. Unlike
                 * a scan there is nothing to stream -- a slot is written at whichever index the vector turned out
                 * to be nearest to, so the writes are random.
                 * <p>
                 * So the assignment is separated from the accumulation. Pass A scores every vector once and
                 * records the result; pass B holds only a window of slots and sweeps the vectors, taking those
                 * whose nearest member falls inside the window, until every slot has been covered. Reading the
                 * index back is what makes that affordable: recomputing it on each sweep would multiply the
                 * distance work by the number of windows, which is itself proportional to the pool.
                 * <p>
                 * The window is sized from the memory budget, so a pool that already fits is covered in a single
                 * sweep and does exactly what it did before; only a k large enough to overflow the budget pays
                 * for extra sweeps over the vectors. Large k becomes slow rather than fatal.
                 */
                private void emitWeighPartials(MaterializerTaskState poolState, MaterializerTaskState vectorState,
                        IFrameWriter poolWriter) throws HyracksDataException {
                    // How many slots there are, and how wide a vector is -- both read off the pool in one cheap
                    // sequential pass, rather than by keeping every candidate resident.
                    final int[] poolSize = { 0 };
                    final int[] dimension = { 0 };
                    KMeansLoopIO.streamRawVectors(poolState, ctx, member -> {
                        poolSize[0]++;
                        if (dimension[0] == 0) {
                            dimension[0] = member.length;
                        }
                    });

                    KMeansVectorCodec.PoolEnvelopeWriter envelope =
                            new KMeansVectorCodec.PoolEnvelopeWriter(ctx, poolWriter);
                    // Pool echo (partition 0 only — the pool is broadcast-complete on every partition), then this
                    // partition's non-empty partials.
                    if (partition == 0) {
                        final int[] echoIdx = { 0 };
                        KMeansLoopIO.streamRawVectors(poolState, ctx,
                                member -> envelope.poolMember(echoIdx[0]++, member));
                    }
                    if (poolSize[0] == 0) {
                        envelope.flush(); // no candidates: nothing to weigh against
                        return;
                    }

                    MaterializerTaskState weighColumn = new MaterializerTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    weighColumn.open(ctx);
                    try {
                        // Pass A: score every vector against the final pool once, keeping the assignment.
                        final KMeansLoopIO.ScoreColumnWriter column =
                                new KMeansLoopIO.ScoreColumnWriter(weighColumn, ctx);
                        KMeansLoopIO.streamScoredAgainstPool(vectorState, poolState, ctx, framesLimit,
                                (vecs, n, nearest, nearestIdx) -> column.append(nearest, nearestIdx, n));
                        column.finish();

                        // Pass B: sweep the slot range. The window is sized the same way the scan's block is --
                        // a slot costs a sum vector plus its header, reference and count -- so a pool small
                        // enough for the budget is covered in a single sweep.
                        int window = KMeansLoopIO.blockCapacity(ctx, framesLimit, dimension[0]);
                        KMeansLoopIO.accumulateInWindows(vectorState, weighColumn, ctx, poolSize[0], window,
                                (index, count, sum) -> envelope.envelope(KMeansVectorCodec.KIND_PARTIAL, partition,
                                        index, count, sum));
                        envelope.flush();
                    } finally {
                        weighColumn.close();
                        weighColumn.deleteFile();
                    }
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                }
            };
        }
    }
}
