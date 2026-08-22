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
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
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
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — <b>Op2 PhiMerge</b>: the single-node φ-reduce of one
 * oversampling round. It consumes the per-partition local potentials {@code {round, localSigma}} that the Cost
 * operators (Op1) emit — delivered here via a concurrent M-to-1 (broadcast into this 1-partition consumer) — and,
 * once it has seen all {@code nParticipants} partitions' contributions for a round, sums them into the global
 * potential {@code phi = Sigma_x d^2(x, pool)} and emits {@code {round, phi}} (broadcast onward to the Sample
 * operators, Op3).
 * <p>
 * "Have every partition's slot been filled for this round?" is the per-round barrier. Because the loop is
 * globally serialized (no partition starts round r+1 until round r's union has been formed), at most one round
 * is ever live — it is emitted and removed before the next round's frames arrive. The {@code round} field is
 * carried through (not merely counted) so the downstream Sample can seed its per-round RNG with the exact round.
 * <p>
 * The partials are held <b>indexed by partition</b> and summed in that order rather than accumulated on
 * arrival. Floating-point addition is not associative, so an arrival-order sum makes phi differ between runs of
 * the same query on the same data — and phi scales every draw probability, so the clustering would differ too.
 * Everything else in the loop is already deterministic on a fixed topology (fixed seeds; PoolMerge and
 * CentroidMerge sort before folding); this keeps the reduce from being the one place that is not.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class KMeansPhiMergeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    // Number of Cost (Op1) partitions whose local potentials must arrive before a round's phi is complete.
    private final int nParticipants;

    /** A round's slots, one per partition; NaN marks "has not reported yet". */
    static double[] newSigmaSlots(int nParticipants) {
        double[] slots = new double[nParticipants];
        Arrays.fill(slots, Double.NaN);
        return slots;
    }

    /** Whether every partition has filled its slot for the round. */
    static boolean allReported(double[] sigmas) {
        for (double v : sigmas) {
            if (Double.isNaN(v)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Sums by ascending partition. Package-private and separated from the pushable so the property that matters
     * -- the result does not depend on the order the frames arrived -- can be pinned by a test.
     */
    static double reduceInPartitionOrder(double[] sigmas) {
        double phi = 0.0d;
        for (double v : sigmas) {
            phi += v;
        }
        return phi;
    }

    /**
     * @param recDesc the {@code {round:int, value:double}} record descriptor, used for BOTH input (localSigma) and
     *                output (phi) — the two frames share the same shape.
     */
    public KMeansPhiMergeOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            int nParticipants) {
        super(spec, 1, 1);
        this.nParticipants = nParticipants;
        outRecDescs[0] = recDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
            private final FrameTupleReference tuple = new FrameTupleReference();
            // round -> this round's local potentials, indexed by PARTITION (NaN = not yet reported).
            //
            // Indexed rather than accumulated because floating-point addition is not associative: summing in
            // network arrival order makes phi -- and therefore every draw probability derived from it -- vary
            // between runs of the same query. Holding P doubles per round and summing by index makes the reduce
            // deterministic, which is what the rest of the loop already assumes (fixed seeds; PoolMerge and
            // CentroidMerge both sort before folding).
            private final Map<Integer, double[]> sigmasByRound = new HashMap<>();
            private FrameTupleAppender appender;
            private ArrayTupleBuilder tb;

            @Override
            public void open() throws HyracksDataException {
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
                tb = new ArrayTupleBuilder(2);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    int round = IntegerPointable.getInteger(tuple.getFieldData(0), tuple.getFieldStart(0));
                    int part = IntegerPointable.getInteger(tuple.getFieldData(1), tuple.getFieldStart(1));
                    double localSum = DoublePointable.getDouble(tuple.getFieldData(2), tuple.getFieldStart(2));
                    double[] sigmas = sigmasByRound.computeIfAbsent(round, k -> newSigmaSlots(nParticipants));
                    if (!Double.isNaN(sigmas[part])) {
                        throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                                "kmeans phi merge: partition " + part + " reported twice for round " + round);
                    }
                    sigmas[part] = localSum;
                    if (allReported(sigmas)) {
                        sigmasByRound.remove(round); // retired: what is left at close never completed
                        emitPhi(round, reduceInPartitionOrder(sigmas));
                        sigmasByRound.remove(round);
                    }
                }
            }

            private void emitPhi(int round, double phi) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(DoubleSerializerDeserializer.INSTANCE, phi);
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "phi tuple exceeds a frame");
                }
                appender.write(writer, true); // one tiny frame per round; push immediately
                writer.flush();
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                // A round only emits once every partition has reported, so a round still open here never
                // will. Left alone that stalls the loop head until its turn times out, minutes later and
                // with nothing naming the cause; say which round and how many arrived instead.
                try {
                    for (Map.Entry<Integer, double[]> pending : sigmasByRound.entrySet()) {
                        int reported = 0;
                        for (double sigma : pending.getValue()) {
                            if (!Double.isNaN(sigma)) {
                                reported++;
                            }
                        }
                        throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "kmeans phi round " + pending.getKey()
                                + ": only " + reported + " of " + nParticipants + " partitions reported");
                    }
                } finally {
                    writer.close();
                }
            }
        };
    }
}
