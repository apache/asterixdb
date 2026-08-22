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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunMerger;
import org.apache.hyracks.dataflow.std.sort.AbstractSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — <b>Op4 PoolMerge</b>: the single-node draw-union of one
 * oversampling round. It consumes each Sample (Op3) partition's drawn candidates plus a per-partition
 * {@link KMeansLoopIO#KIND_END} marker (delivered via a concurrent M-to-1), and once it has seen the end markers
 * from all {@code nParticipants} partitions for a round it emits that round's <b>global union</b> of draws in a
 * deterministic order (partition ASC, then per-partition draw sequence ASC), followed by one end marker
 * (broadcast to the Release operators, Op5).
 * <p>
 * The deterministic union order is what keeps every partition's pool run file byte-identical, so all partitions
 * agree on phi and the draws each subsequent round. Draw vectors are decoded and buffered as {@code double[]}
 * (the input frame buffers are transient), then re-emitted. Because the loop is globally serialized, at most one
 * round is live in the accumulator at a time (emitted and removed before the next round arrives).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class KMeansPoolMergeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    // Number of Sample (Op3) partitions whose end markers must arrive before a round's union is complete.
    private final int nParticipants;
    /** Frame budget for the per-round draw sort. */
    private final int framesLimit;

    public KMeansPoolMergeOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            int nParticipants, int framesLimit) {
        super(spec, 1, 1);
        this.nParticipants = nParticipants;
        this.framesLimit = framesLimit;
        outRecDescs[0] = recDesc; // DRAW_RD shape, in == out
    }

    /** One buffered draw awaiting the round's barrier: its origin partition, per-partition seq, and vector. */
    private static final class Draw {
        private final int part;
        private final int seq;
        private final double[] vec;

        private Draw(int part, int seq, double[] vec) {
            this.part = part;
            this.seq = seq;
            this.vec = vec;
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
            private final FrameTupleReference tuple = new FrameTupleReference();
            // Draws are ordered by (part, seq) before they are emitted: every downstream read of the pool is
            // positional -- ties in the nearest-candidate loops resolve to the first member, and RECLUSTER
            // numbers candidates by arrival -- so emitting in network-arrival order would make results depend
            // on timing. An external sort supplies that order without holding the round resident.
            private final Map<Integer, Integer> endsByRound = new HashMap<>();
            private AbstractSortRunGenerator drawSort;
            private VSizeFrame sortFrame;
            private FrameTupleAppender sortAppender;
            private final ArrayTupleBuilder sortTb = new ArrayTupleBuilder(5);
            private FrameTupleAppender appender;
            private ArrayTupleBuilder tb;

            @Override
            public void open() throws HyracksDataException {
                appender = new FrameTupleAppender(new VSizeFrame(ctx));
                tb = new ArrayTupleBuilder(5);
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
                    int seq = IntegerPointable.getInteger(tuple.getFieldData(2), tuple.getFieldStart(2));
                    int kind = IntegerPointable.getInteger(tuple.getFieldData(3), tuple.getFieldStart(3));
                    if (kind == KMeansLoopIO.KIND_END) {
                        int ends = endsByRound.merge(round, 1, Integer::sum);
                        if (ends == nParticipants) {
                            emitUnion(round);
                            endsByRound.remove(round);
                        }
                    } else {
                        double[] vec = KMeansLoopIO.readRawVector(tuple.getFieldData(4), tuple.getFieldStart(4),
                                tuple.getFieldLength(4));
                        ensureSort();
                        sortTb.reset();
                        for (int f = 0; f < 5; f++) {
                            sortTb.addField(tuple.getFieldData(f), tuple.getFieldStart(f), tuple.getFieldLength(f));
                        }
                        if (!sortAppender.append(sortTb.getFieldEndOffsets(), sortTb.getByteArray(), 0,
                                sortTb.getSize())) {
                            drawSort.nextFrame(sortFrame.getBuffer());
                            sortAppender.reset(sortFrame, true);
                            if (!sortAppender.append(sortTb.getFieldEndOffsets(), sortTb.getByteArray(), 0,
                                    sortTb.getSize())) {
                                throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                                        "a pool draw is too large to fit in a frame");
                            }
                        }
                    }
                }
            }

            private void emitUnion(int round) throws HyracksDataException {
                // A round may draw nothing (e.g. the pool already covers every point -> phi = 0): still emit the
                // end marker so Release wakes Cost for the next round. (getOrDefault(List.of()) would be immutable
                // -> sort throws; guard on null instead.)
                if (drawSort != null) {
                    if (sortAppender.getTupleCount() > 0) {
                        drawSort.nextFrame(sortFrame.getBuffer());
                    }
                    drawSort.close();
                    emitSorted(round);
                    drawSort = null; // next round gets a fresh sort
                }
                emitEnd(round);
                appender.write(writer, true);
                writer.flush();
            }

            private void ensureSort() throws HyracksDataException {
                if (drawSort == null) {
                    drawSort = new ExternalSortRunGenerator(ctx, KMeansLoopIO.DRAW_SORT_FIELDS, null,
                            KMeansLoopIO.DRAW_SORT_COMPARATORS, inRecDesc, Algorithm.MERGE_SORT, framesLimit);
                    drawSort.open();
                    sortFrame = new VSizeFrame(ctx);
                    sortAppender = new FrameTupleAppender(sortFrame);
                }
            }

            /**
             * Emits the round's draws in (part, seq) order. When they fit the budget the generator sorts in
             * place and produces no runs, so that case is flushed from the sorter -- merging an empty run list
             * would emit nothing and the round's draws would vanish from the pool.
             */
            private void emitSorted(int round) throws HyracksDataException {
                List<GeneratedRunFileReader> runs = drawSort.getRuns();
                IBinaryComparator[] cmps = new IBinaryComparator[KMeansLoopIO.DRAW_SORT_COMPARATORS.length];
                for (int i = 0; i < cmps.length; i++) {
                    cmps[i] = KMeansLoopIO.DRAW_SORT_COMPARATORS[i].createBinaryComparator();
                }
                AbstractExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, runs,
                        KMeansLoopIO.DRAW_SORT_FIELDS, cmps, null, inRecDesc, framesLimit, Integer.MAX_VALUE);
                final FrameTupleAccessor acc = new FrameTupleAccessor(inRecDesc);
                final FrameTupleReference t = new FrameTupleReference();
                IFrameWriter out = new IFrameWriter() {
                    @Override
                    public void open() {
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        acc.reset(buffer);
                        int n = acc.getTupleCount();
                        for (int i = 0; i < n; i++) {
                            t.reset(acc, i);
                            int part = IntegerPointable.getInteger(t.getFieldData(1), t.getFieldStart(1));
                            int seq = IntegerPointable.getInteger(t.getFieldData(2), t.getFieldStart(2));
                            double[] vec = KMeansLoopIO.readRawVector(t.getFieldData(4), t.getFieldStart(4),
                                    t.getFieldLength(4));
                            emitDraw(round, part, seq, vec);
                        }
                    }

                    @Override
                    public void fail() {
                    }

                    @Override
                    public void close() {
                    }
                };
                out.open();
                try {
                    if (runs.isEmpty()) {
                        drawSort.getSorter().flush(out);
                    } else {
                        merger.process(out);
                    }
                } finally {
                    out.close();
                }
            }

            private void emitDraw(int round, int part, int seq, double[] vec) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, part);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, seq);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_DRAW);
                KMeansLoopIO.writeRawVector(tb, vec);
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }

            private void emitEnd(int round) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, round);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, 0);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, 0);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_END);
                KMeansLoopIO.writeRawVector(tb, new double[] { 0.0d }); // ignored for end markers
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
