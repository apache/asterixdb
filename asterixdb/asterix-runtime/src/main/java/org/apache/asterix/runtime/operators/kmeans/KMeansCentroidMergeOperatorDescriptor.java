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
import org.apache.asterix.common.vector.VectorSimilarityMetric;
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

/**
 * The Lloyd loop's single-node centroid reduce, which folds every partition's {@code (count, sum)} partials
 * into the next centroid set and broadcasts it back. A centroid's new position is the mean of its points,
 * and a centroid with no points anywhere is dropped, like an empty group in a grouped aggregate.
 * <p>
 * Partials are accumulated in {@code (centroid index, partition)} order; since floating-point addition is
 * not associative, the fixed order is what keeps runs byte-identical on any network interleaving. The
 * reduce fires on the {@code nParticipants}-th end marker, and the loop is globally serialized, so at most
 * one iteration is in flight. The accumulator is O(partitions * k * dim) heap, held for that iteration.
 */
public class KMeansCentroidMergeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    /** Number of Controller partitions whose end markers must arrive before an iteration's reduce may fire. */
    private final int nParticipants;
    /** Frame budget for the per-iteration partial sort. */
    private final int framesLimit;
    /** Which centroid update the iteration applies; see {@link KMeansLoopIO#centroidOf}. */
    private final VectorSimilarityMetric metric;

    public KMeansCentroidMergeOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            int nParticipants, int framesLimit, VectorSimilarityMetric metric) {
        super(spec, 1, 1);
        this.nParticipants = nParticipants;
        this.framesLimit = framesLimit;
        this.metric = metric;
        outRecDescs[0] = recDesc; // DRAW_RD: the new centroid set, one vector per tuple
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            private final FrameTupleAccessor accessor = new FrameTupleAccessor(inRecDesc);
            private final FrameTupleReference tuple = new FrameTupleReference();
            // Partials go through a sort keyed on (seq, part), which folds to byte-identical centroids
            // whether or not it spilled. The loop is globally serialized, so a single sort suffices.
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
                            emitCentroid(iter, emittedSeq++, KMeansLoopIO.centroidOf(sums[i], weights[i], metric));
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
             * Folds the sorted partials into per-centroid (weight, sum), where (seq, part) order makes each
             * centroid's contributions arrive in partition order whether or not the sort spilled. A fully
             * in-memory sort produces no runs and must be flushed from the sorter, since merging an empty
             * run list would fold nothing and emit silently wrong centroids.
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
                                if (vec.length != sum.length) {
                                    // Every vector was decoded at the declared dimension, so a mismatch here is
                                    // a corrupt partial; summing a prefix would be a silently wrong centroid.
                                    throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "a centroid partial is "
                                            + vec.length + " wide where its accumulator is " + sum.length);
                                }
                                for (int d = 0; d < sum.length; d++) {
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
