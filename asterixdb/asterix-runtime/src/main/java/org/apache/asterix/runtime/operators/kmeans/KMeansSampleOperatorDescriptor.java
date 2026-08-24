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

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — <b>Op3 Sample</b>: the per-partition Bernoulli draw of
 * one oversampling round. Driven by the {@code {round, phi}} frames broadcast from PhiMerge (Op2). For each round
 * it streams the shared resident-vector run file alongside the score column Op1 wrote while computing this
 * round's local sigma, drawing each vector x independently with probability
 * {@code p_x = l * d^2(x, pool) / phi}. The random number of a draw comes from {@link KMeansLoopIO#uniformDraw}
 * and depends only on the vector contents, the query seed and the round. Which vectors are drawn therefore does
 * not depend on how the input was partitioned.
 * Survivors are emitted as {@link KMeansLoopIO#KIND_DRAW} frames {@code {round, part, seq, vec}}
 * to PoolMerge (Op4), followed by one {@link KMeansLoopIO#KIND_END} marker so Op4's per-round barrier
 * can fire.
 * <p>
 * This operator performs no distance computation and never opens the pool. Op1 scored every vector against
 * {@code pool[r]} in this same round -- that is what phi was summed from -- so re-deriving the distances here
 * would be the same arithmetic twice, and would need the pool resident to do it. Reading the column instead
 * removes a scan whose cost grew with the pool, which grows with k.
 * <p>
 * The vector run file and the column are looked up from joblet state (both created by the co-located Cost
 * operator, Op1) on the first frame rather than in {@code open()}, since only the arrival of data orders this
 * task behind Op1's registration. Points already covered by the pool (d^2 = 0) are never re-drawn, and a
 * non-positive phi (pool already covers everything) yields no draws, as in the paper.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class KMeansSampleOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final String loopKey;
    private final int oversamplingCount; // l = oversampling factor * k
    private final long seedBase;

    public KMeansSampleOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor drawRecDesc,
            String loopKey, int oversamplingCount, long seedBase) {
        super(spec, 1, 1);
        this.loopKey = loopKey;
        this.oversamplingCount = oversamplingCount;
        this.seedBase = seedBase;
        outRecDescs[0] = drawRecDesc; // DRAW_RD
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
            private final FrameTupleReference tuple = new FrameTupleReference();
            private final ArrayTupleBuilder tb = new ArrayTupleBuilder(5);
            private FrameTupleAppender appender;
            private MaterializerTaskState vectorState;

            @Override
            public void open() throws HyracksDataException {
                // State is resolved on the first frame, not here: Hyracks opens the whole pipeline before any
                // data flows, so at open() time the producing activity may not have registered yet. See
                // LoopControlState#required for the ordering that makes the first-frame lookup safe.
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
                writer.open();
            }

            /** Resolves the vector run file this partition shares with Op1. Safe from the first frame onward. */
            private void ensureState() throws HyracksDataException {
                if (vectorState == null) {
                    vectorState = (MaterializerTaskState) LoopControlState.required(ctx,
                            LoopControlState.vectorsStateId(loopKey, partition));
                }
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                ensureState();
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    int round = IntegerPointable.getInteger(tuple.getFieldData(0), tuple.getFieldStart(0));
                    double phi = DoublePointable.getDouble(tuple.getFieldData(1), tuple.getFieldStart(1));
                    sampleRound(round, phi);
                }
            }

            private void sampleRound(int round, double phi) throws HyracksDataException {
                final double l = oversamplingCount;
                if (phi > 0.0d) {
                    // Op1 already scored every vector against this exact pool in this exact round -- it is what
                    // phi was summed from -- so the distances are read rather than recomputed here, and the pool
                    // is never opened. The random number of a draw depends only on the vector. The order the
                    // vectors are visited in does not matter.
                    MaterializerTaskState scoreState = (MaterializerTaskState) LoopControlState.required(ctx,
                            LoopControlState.scoreStateId(loopKey, partition));
                    try (KMeansLoopIO.ScoreColumnReader scores = new KMeansLoopIO.ScoreColumnReader(scoreState, ctx)) {
                        KMeansLoopIO.streamRawVectors(vectorState, ctx, vec -> {
                            scores.advance();
                            double best = scores.nearest();
                            // p_x = 0 for points already in the pool (paper never re-draws them).
                            if (Double.isNaN(best) || best == Double.POSITIVE_INFINITY || best <= 0.0d) {
                                return;
                            }
                            if (KMeansLoopIO.uniformDraw(KMeansLoopIO.fingerprint(vec), seedBase, round) < l * best
                                    / phi) {
                                emitDraw(round, drawKey(vec), vec);
                            }
                        });
                    }
                }
                emitEnd(round);
                appender.write(writer, true);
                writer.flush();
            }

            /**
             * The draw's ordering key: a hash of the vector, NOT its position in this partition's scan.
             * <p>
             * PoolMerge orders each round's union by this key, and that order fixes every candidate's index in
             * the pool -- which RECLUSTER then picks from by index. A positional counter made those indices a
             * function of the order rows happened to be read in, so a pool holding exactly the same vectors
             * could still seat them differently and yield different initial centroids run to run. Hashing the
             * vector makes the pool's layout a property of the drawn set alone.
             * <p>
             * Truncating the 64-bit fingerprint to the int this column carries is fine: a collision only ties
             * two candidates, and the partition breaks that tie (see DRAW_SORT_FIELDS).
             */
            private int drawKey(double[] vec) {
                return (int) KMeansLoopIO.fingerprint(vec);
            }

            private void emitDraw(int round, int seq, double[] vec) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, partition);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, seq);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_DRAW);
                KMeansLoopIO.writeRawVector(tb, vec);
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }

            private void emitEnd(int round) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, partition);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, 0);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_END);
                KMeansLoopIO.writeRawVector(tb, new double[] { 0.0d });
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                writer.close();
            }
        };
    }
}
