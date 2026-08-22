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
import java.util.Comparator;
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
import org.apache.hyracks.data.std.primitive.DoublePointable;
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
 * CLUSTER BY k-means‖ Lloyd loop — the single-node centroid reduce of one iteration: fold every partition's
 * {@code (count, sum)} partials into the next centroid set and broadcast it back.
 * <p>
 * Each centroid's new position is the mean of the points assigned to it. A centroid that attracted no points
 * anywhere is <b>dropped</b>, not carried forward or re-seeded, so the centroid count can shrink across
 * iterations — this mirrors a grouped aggregate, which produces no row for an empty group, and the labeling that
 * follows the loop simply never emits that cluster.
 * <p>
 * Determinism is the delicate part, because floating-point addition is not associative: the same partials summed
 * in two different orders can differ in the last bits and, once divided, move a centroid enough to flip a
 * borderline point's assignment. Partials are therefore accumulated in {@code (centroid index, partition)} order
 * rather than in frame-arrival order, which makes a run independent of how the network interleaved the
 * partitions.
 * <p>
 * "Have I heard from every partition?" is the iteration barrier: the reduce fires on the
 * {@code nParticipants}-th end marker. Because the loop is globally serialized — no partition begins iteration
 * i+1 until iteration i's centroids have been published — at most one iteration is ever in flight, so the
 * per-iteration accumulator is emitted and discarded before the next iteration's frames arrive.
 * <p>
 * Memory: the accumulator is O(partitions · k · dim) in this node's heap, held only for the iteration in flight.
 * That is the loop's dominant memory term and it is currently uncapped, which bounds usable k.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public class KMeansCentroidMergeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    /** Number of Controller partitions whose end markers must arrive before an iteration's reduce may fire. */
    private final int nParticipants;
    /** Frame budget for the per-iteration partial sort. */
    private final int framesLimit;

    public KMeansCentroidMergeOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            int nParticipants, int framesLimit) {
        super(spec, 1, 1);
        this.nParticipants = nParticipants;
        this.framesLimit = framesLimit;
        outRecDescs[0] = recDesc; // DRAW_RD: the new centroid set, one vector per tuple
    }

    /** One partition's contribution for one centroid: its local member count and component-wise sum. */
    private static final class Partial {
        private final int part;
        private final int seq;
        private final long count;
        private final double[] sum;

        private Partial(int part, int seq, long count, double[] sum) {
            this.part = part;
            this.seq = seq;
            this.count = count;
            this.sum = sum;
        }
    }

    /** Accumulate by centroid, then by partition — never by arrival. See the class comment on determinism. */
    private static final Comparator<Partial> MERGE_ORDER =
            Comparator.comparingInt((Partial p) -> p.seq).thenComparingInt(p -> p.part);

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
            private final FrameTupleReference tuple = new FrameTupleReference();
            // Partials are not accumulated in a list: every partition sends one per centroid, so the map was
            // O(P * k * dim) on this single node -- the only term in the loop that grows with cluster size.
            // They go through a sort keyed on (seq, part) instead, which is the order MERGE_ORDER imposed and
            // therefore folds to byte-identical centroids, spilled or not. The loop is globally serialized, so
            // at most one iteration is ever in flight and a single sort suffices.
            private final Map<Integer, Integer> endsByIter = new HashMap<>();
            private AbstractSortRunGenerator partialSort;
            private VSizeFrame sortFrame;
            private FrameTupleAppender sortAppender;
            private final ArrayTupleBuilder sortTb = new ArrayTupleBuilder(6);
            private int maxSeq = -1;
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
                    int iter = IntegerPointable.getInteger(tuple.getFieldData(0), tuple.getFieldStart(0));
                    int part = IntegerPointable.getInteger(tuple.getFieldData(1), tuple.getFieldStart(1));
                    int seq = IntegerPointable.getInteger(tuple.getFieldData(2), tuple.getFieldStart(2));
                    int kind = IntegerPointable.getInteger(tuple.getFieldData(3), tuple.getFieldStart(3));
                    if (kind == KMeansLoopIO.KIND_END) {
                        int ends = endsByIter.merge(iter, 1, Integer::sum);
                        if (ends == nParticipants) {
                            emitCentroids(iter);
                            endsByIter.remove(iter);
                        }
                    } else {
                        ensureSort();
                        if (seq > maxSeq) {
                            maxSeq = seq;
                        }
                        // Copied field for field: PARTIAL_RD is already flat, so nothing has to be decoded and
                        // re-encoded to make it sortable.
                        sortTb.reset();
                        for (int f = 0; f < 6; f++) {
                            sortTb.addField(tuple.getFieldData(f), tuple.getFieldStart(f), tuple.getFieldLength(f));
                        }
                        if (!sortAppender.append(sortTb.getFieldEndOffsets(), sortTb.getByteArray(), 0,
                                sortTb.getSize())) {
                            partialSort.nextFrame(sortFrame.getBuffer());
                            sortAppender.reset(sortFrame, true);
                            if (!sortAppender.append(sortTb.getFieldEndOffsets(), sortTb.getByteArray(), 0,
                                    sortTb.getSize())) {
                                throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                                        "a centroid partial is too large to fit in a frame");
                            }
                        }
                    }
                }
            }

            /**
             * Fold this iteration's partials into the next centroid set and broadcast it. The accumulation
             * reproduces the unrolled reduce exactly: sort by (centroid, partition), clone the first contribution
             * per centroid and add the rest component-wise, then divide by the total count. Centroids with no
             * members are skipped, so the emitted set may be shorter than the one that went in.
             */
            private void ensureSort() throws HyracksDataException {
                if (partialSort == null) {
                    partialSort = new ExternalSortRunGenerator(ctx, KMeansLoopIO.PARTIAL_SORT_FIELDS, null,
                            KMeansLoopIO.PARTIAL_SORT_COMPARATORS, inRecDesc, Algorithm.MERGE_SORT, framesLimit);
                    partialSort.open();
                    sortFrame = new VSizeFrame(ctx);
                    sortAppender = new FrameTupleAppender(sortFrame);
                }
            }

            private void emitCentroids(int iter) throws HyracksDataException {
                if (partialSort != null) {
                    if (sortAppender.getTupleCount() > 0) {
                        partialSort.nextFrame(sortFrame.getBuffer());
                    }
                    partialSort.close();
                    long[] weights = new long[maxSeq + 1];
                    double[][] sums = new double[maxSeq + 1][];
                    foldSorted(weights, sums);
                    int emittedSeq = 0;
                    for (int i = 0; i < weights.length; i++) {
                        if (weights[i] > 0) {
                            double[] mean = new double[sums[i].length];
                            for (int d = 0; d < mean.length; d++) {
                                mean[d] = sums[i][d] / weights[i];
                            }
                            emitCentroid(iter, emittedSeq++, mean);
                        }
                    }
                    partialSort = null; // next iteration gets a fresh sort
                    maxSeq = -1;
                }
                emitEnd(iter);
                appender.write(writer, true);
                writer.flush();
            }

            /**
             * Folds the sorted partials into per-centroid (weight, sum). Sorted by (seq, part), so each
             * centroid's contributions arrive in partition order -- the order MERGE_ORDER produced, and the
             * same whether or not the sort spilled. When everything fits, the generator sorts in place and
             * produces no runs, so that case must be flushed from the sorter rather than merged: merging an
             * empty run list would fold nothing and emit silently wrong centroids.
             */
            private void foldSorted(long[] weights, double[][] sums) throws HyracksDataException {
                List<GeneratedRunFileReader> runs = partialSort.getRuns();
                IBinaryComparator[] cmps = new IBinaryComparator[KMeansLoopIO.PARTIAL_SORT_COMPARATORS.length];
                for (int i = 0; i < cmps.length; i++) {
                    cmps[i] = KMeansLoopIO.PARTIAL_SORT_COMPARATORS[i].createBinaryComparator();
                }
                AbstractExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, runs,
                        KMeansLoopIO.PARTIAL_SORT_FIELDS, cmps, null, inRecDesc, framesLimit, Integer.MAX_VALUE);
                final FrameTupleAccessor acc = new FrameTupleAccessor(inRecDesc);
                final FrameTupleReference t = new FrameTupleReference();
                IFrameWriter fold = new IFrameWriter() {
                    @Override
                    public void open() {
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        acc.reset(buffer);
                        int n = acc.getTupleCount();
                        for (int i = 0; i < n; i++) {
                            t.reset(acc, i);
                            int seq = IntegerPointable.getInteger(t.getFieldData(2), t.getFieldStart(2));
                            if (seq < 0 || seq >= weights.length) {
                                continue;
                            }
                            double count = DoublePointable.getDouble(t.getFieldData(4), t.getFieldStart(4));
                            double[] vec = KMeansLoopIO.readRawVector(t.getFieldData(5), t.getFieldStart(5),
                                    t.getFieldLength(5));
                            weights[seq] += (long) count;
                            double[] sum = sums[seq];
                            if (sum == null) {
                                sums[seq] = vec;
                            } else {
                                for (int d = 0; d < Math.min(sum.length, vec.length); d++) {
                                    sum[d] += vec[d];
                                }
                            }
                        }
                    }

                    @Override
                    public void fail() {
                    }

                    @Override
                    public void close() {
                    }
                };
                fold.open();
                try {
                    if (runs.isEmpty()) {
                        partialSort.getSorter().flush(fold);
                    } else {
                        merger.process(fold);
                    }
                } finally {
                    fold.close();
                }
            }

            private void emitCentroid(int iter, int seq, double[] vec) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, iter);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, 0);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, seq);
                tb.addField(IntegerSerializerDeserializer.INSTANCE, KMeansLoopIO.KIND_DRAW);
                KMeansLoopIO.writeRawVector(tb, vec);
                FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                        tb.getSize());
            }

            private void emitEnd(int iter) throws HyracksDataException {
                tb.reset();
                tb.addField(IntegerSerializerDeserializer.INSTANCE, iter);
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
