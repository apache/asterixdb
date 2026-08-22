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
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.evaluators.functions.vector.VectorListDecoder;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.dataflow.std.sort.AbstractExternalSortRunMerger;
import org.apache.hyracks.dataflow.std.sort.AbstractSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Per-task runtime for the CLUSTER BY k-means|| RECLUSTER stage ({@link KMeansReclusterOperatorDescriptor}): reads
 * the materialized (broadcast) input, folds the weigh partials into per-candidate totals, and serializes the
 * centroids it is handed. One instance per Score task.
 * <p>
 * An input row is an ENVELOPE -- an open list {@code [kind, partition, seq, score, vector]}. A pool member
 * ({@link #KIND_POOL}) carries a candidate; a partial ({@link #KIND_PARTIAL}) reports one partition's tally for
 * candidate {@code seq}, with {@code score} the row count and {@code vector} their sum. Every partition
 * receives the same rows, and the fold's order comes from an external sort on envelope fields, so the result
 * does not depend on arrival order.
 * <p>
 * Output vectors are re-serialized as OPEN lists (tagged items) because the output column is typed ANY.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
final class KMeansStageRuntime {

    static final double KIND_POOL = 0.0d;
    static final double KIND_PARTIAL = 2.0d;

    /** One row of the chained stream: pool member, scored candidate, or (count, sum) partial. */
    static final class Row {
        final double kind;
        final int partition;
        final long seq;
        final double score;
        final double[] vec;

        Row(double kind, int partition, long seq, double score, double[] vec) {
            this.kind = kind;
            this.partition = partition;
            this.seq = seq;
            this.score = score;
            this.vec = vec;
        }
    }

    private final IHyracksTaskContext ctx;
    private final IFrameWriter writer;
    private final RecordDescriptor vecRecDesc;
    private final int poolColumn;
    /** The frame budget the partial sort may use, from the operator's declared memory requirements. */
    private final int framesLimit;

    private final FrameTupleReference tupleRef = new FrameTupleReference();
    private final VoidPointable fieldPtr = new VoidPointable();
    private final ListAccessor listAccessor = new ListAccessor();
    private final ListAccessor nestedAccessor = new ListAccessor();
    private final VectorListDecoder decoder = new VectorListDecoder();

    /** Candidates carried by the pool echo. Only the count is kept: it bounds the positions the fold accepts. */
    private int poolSize;
    private AbstractSortRunGenerator partialSort;
    private FrameTupleAppender partialAppender;
    private VSizeFrame partialFrame;
    private final ArrayTupleBuilder partialTb = new ArrayTupleBuilder(4);

    KMeansStageRuntime(IHyracksTaskContext ctx, IFrameWriter writer, RecordDescriptor vecRecDesc, int poolColumn,
            int framesLimit) {
        this.ctx = ctx;
        this.writer = writer;
        this.vecRecDesc = vecRecDesc;
        this.poolColumn = poolColumn;
        this.framesLimit = framesLimit;
    }

    int poolSize() {
        return poolSize;
    }

    /** Re-emits one weigh partial into the sort, flat, keyed by (seq, part). */
    private void spillPartial(Row r) throws HyracksDataException {
        partialTb.reset();
        partialTb.addField(IntegerSerializerDeserializer.INSTANCE, (int) r.seq);
        partialTb.addField(IntegerSerializerDeserializer.INSTANCE, r.partition);
        partialTb.addField(DoubleSerializerDeserializer.INSTANCE, r.score);
        KMeansLoopIO.writeRawVector(partialTb, r.vec);
        if (!partialAppender.append(partialTb.getFieldEndOffsets(), partialTb.getByteArray(), 0, partialTb.getSize())) {
            partialSort.nextFrame(partialFrame.getBuffer());
            partialAppender.reset(partialFrame, true);
            if (!partialAppender.append(partialTb.getFieldEndOffsets(), partialTb.getByteArray(), 0,
                    partialTb.getSize())) {
                throw new RuntimeDataException(ErrorCode.CLUSTER_BY_INVALID_INPUT,
                        "a weigh partial is too large to fit in a frame");
            }
        }
    }

    /**
     * Cancellation responsiveness: Hyracks aborts tasks by interrupting their threads, but these loops are
     * pure CPU over materialized run files -- nothing blocks, so nothing throws. Poll the interrupt per frame
     * (a few dozen tuples) in every pass.
     */
    private void failIfInterrupted() throws HyracksDataException {
        if (Thread.currentThread().isInterrupted()) {
            throw HyracksDataException.create(new InterruptedException());
        }
    }

    /**
     * Reads the materialized (broadcast) input once, counting the candidate echo and routing the weigh
     * partials into the sort the fold consumes.
     */
    void readInput(MaterializerTaskState state) throws HyracksDataException {
        final FrameTupleAccessor poolAccessor = new FrameTupleAccessor(vecRecDesc);
        partialSort = new ExternalSortRunGenerator(ctx, KMeansLoopIO.PARTIAL_FLAT_SORT_FIELDS, null,
                KMeansLoopIO.PARTIAL_FLAT_COMPARATORS, KMeansLoopIO.PARTIAL_FLAT_RD, Algorithm.MERGE_SORT, framesLimit);
        partialSort.open();
        partialFrame = new VSizeFrame(ctx);
        partialAppender = new FrameTupleAppender(partialFrame);
        state.writeOut(new IFrameWriter() {
            @Override
            public void open() {
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                failIfInterrupted();
                poolAccessor.reset(buffer);
                int tupleCount = poolAccessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tupleRef.reset(poolAccessor, i);
                    Row r = decodeEnvelope(tupleRef, poolColumn);
                    if (r.kind == KIND_POOL) {
                        poolSize++;
                    } else {
                        spillPartial(r);
                    }
                }
            }

            @Override
            public void fail() {
            }

            @Override
            public void close() {
            }
        }, new VSizeFrame(ctx), false);
        if (partialAppender.getTupleCount() > 0) {
            partialSort.nextFrame(partialFrame.getBuffer());
        }
        partialSort.close();
    }

    /**
     * Folds the weigh partials and hands each pool member's total to {@code sink} as soon as it is complete.
     * <p>
     * The sort delivers partials in (pool position, origin partition) order, so a member is finished the
     * moment the position advances and nothing needs collecting -- the caller turns each total into a weighted
     * mean and passes it on, instead of holding a count and a sum vector per member. Members that attracted
     * nothing are not reported.
     * <p>
     * The partition order also fixes the summation. Floating-point addition is not associative, so folding a
     * member's contributions in a different order yields different centroids; an external sort gives the same
     * order whether or not it spilled, which a hash-partitioned spillable table would not.
     *
     * @param poolSize bounds the accepted positions; a partial outside it is dropped.
     */
    void foldPartials(int poolSize, KMeansLoopIO.WeighedSlotConsumer sink) throws HyracksDataException {
        List<GeneratedRunFileReader> runs = partialSort.getRuns();
        IBinaryComparator[] cmps = new IBinaryComparator[KMeansLoopIO.PARTIAL_FLAT_COMPARATORS.length];
        for (int i = 0; i < cmps.length; i++) {
            cmps[i] = KMeansLoopIO.PARTIAL_FLAT_COMPARATORS[i].createBinaryComparator();
        }
        AbstractExternalSortRunMerger merger =
                new ExternalSortRunMerger(ctx, runs, KMeansLoopIO.PARTIAL_FLAT_SORT_FIELDS, cmps, null,
                        KMeansLoopIO.PARTIAL_FLAT_RD, framesLimit, Integer.MAX_VALUE);
        final FrameTupleAccessor acc = new FrameTupleAccessor(KMeansLoopIO.PARTIAL_FLAT_RD);
        final FrameTupleReference t = new FrameTupleReference();

        /**
         * Folds one pool position at a time. The sort delivers positions in ascending order, so a position is
         * final the moment the next one appears -- there is never more than one open.
         */
        final class Fold implements IFrameWriter {
            private int position = -1;
            private long weight;
            private double[] sum;

            @Override
            public void open() {
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                acc.reset(buffer);
                int n = acc.getTupleCount();
                for (int i = 0; i < n; i++) {
                    t.reset(acc, i);
                    int idx = IntegerPointable.getInteger(t.getFieldData(0), t.getFieldStart(0));
                    double cnt = DoublePointable.getDouble(t.getFieldData(2), t.getFieldStart(2));
                    if (idx < 0 || idx >= poolSize) {
                        continue;
                    }
                    double[] vec =
                            KMeansLoopIO.readRawVector(t.getFieldData(3), t.getFieldStart(3), t.getFieldLength(3));
                    if (idx != position) {
                        flushPosition();
                        position = idx;
                        weight = 0L;
                        sum = vec; // the first contribution owns the buffer the rest accumulate into
                    } else {
                        for (int d = 0; d < Math.min(sum.length, vec.length); d++) {
                            sum[d] += vec[d];
                        }
                    }
                    weight += (long) cnt;
                }
            }

            /** Hands the finished position on. Positions nothing landed on are never reported. */
            void flushPosition() throws HyracksDataException {
                if (position >= 0 && weight > 0) {
                    sink.accept(position, weight, sum);
                }
                position = -1;
                sum = null;
                weight = 0L;
            }

            @Override
            public void fail() {
            }

            @Override
            public void close() {
            }
        }

        Fold fold = new Fold();
        // When the partials fit the budget the generator sorts IN MEMORY and produces no runs at all -- the
        // sorted tuples stay in the frame sorter. Merging an empty run list would fold nothing and hand back
        // silently wrong centroids, so the in-memory case has to be flushed from the sorter instead. This is
        // the same branch ExternalSortOperatorDescriptor's merge activity makes.
        fold.open();
        try {
            if (runs.isEmpty()) {
                partialSort.getSorter().flush(fold);
            } else {
                merger.process(fold);
            }
            fold.flushPosition(); // the last position has no successor to close it
        } finally {
            fold.close();
        }
    }

    /** Streams the materialized vector input, decoding each tuple's vector column and feeding it to sink. */
    void streamVectors(MaterializerTaskState state, int vectorColumn, VectorSink sink) throws HyracksDataException {
        final FrameTupleAccessor vecAccessor = new FrameTupleAccessor(vecRecDesc);
        state.writeOut(new IFrameWriter() {
            @Override
            public void open() {
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                failIfInterrupted();
                vecAccessor.reset(buffer);
                int tupleCount = vecAccessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tupleRef.reset(vecAccessor, i);
                    sink.accept(decodeVector(tupleRef, vectorColumn));
                }
            }

            @Override
            public void fail() {
            }

            @Override
            public void close() {
            }
        }, new VSizeFrame(ctx), false);
    }

    private double[] decodeVector(FrameTupleReference tuple, int col) throws HyracksDataException {
        try {
            fieldPtr.set(tuple.getFieldData(col), tuple.getFieldStart(col), tuple.getFieldLength(col));
            listAccessor.reset(fieldPtr.getByteArray(), fieldPtr.getStartOffset());
            double[] arr = new double[listAccessor.size()];
            return decoder.createArrayFromList(listAccessor, arr);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private Row decodeEnvelope(FrameTupleReference tuple, int col) throws HyracksDataException {
        try {
            fieldPtr.set(tuple.getFieldData(col), tuple.getFieldStart(col), tuple.getFieldLength(col));
            listAccessor.reset(fieldPtr.getByteArray(), fieldPtr.getStartOffset());
            byte[] bytes = listAccessor.getByteArray();
            double kind = envelopeDouble(bytes, 0);
            int origin = (int) envelopeDouble(bytes, 1);
            long seq = (long) envelopeDouble(bytes, 2);
            double score = envelopeDouble(bytes, 3);
            int vecOffset = listAccessor.getItemOffset(4);
            nestedAccessor.reset(bytes, vecOffset);
            double[] vec = decoder.createArrayFromList(nestedAccessor, new double[nestedAccessor.size()]);
            return new Row(kind, origin, seq, score, vec);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private double envelopeDouble(byte[] bytes, int itemIndex) throws HyracksDataException {
        // Envelope items are self-describing (open list): tag byte, then the payload.
        int offset = listAccessor.getItemOffset(itemIndex);
        return ADoubleSerializerDeserializer.getDouble(bytes, offset + 1);
    }

    Emitter newEmitter() throws HyracksDataException {
        return new Emitter();
    }

    @FunctionalInterface
    interface VectorSink {
        void accept(double[] vec) throws HyracksDataException;
    }

    /** Serialization state for one emit pass; every value is an OPEN list (tagged items). */
    final class Emitter {
        private final FrameTupleAppender appender;
        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        private final OrderedListBuilder listBuilder = new OrderedListBuilder();
        private final OrderedListBuilder vecBuilder = new OrderedListBuilder();
        private final ArrayBackedValueStorage itemStorage = new ArrayBackedValueStorage();
        private final ArrayBackedValueStorage vecStorage = new ArrayBackedValueStorage();
        private final AOrderedListType openList = new AOrderedListType(BuiltinType.ANY, null);

        private Emitter() throws HyracksDataException {
            appender = new FrameTupleAppender(new VSizeFrame(ctx));
        }

        void envelope(Row row) throws Exception {
            tb.reset();
            listBuilder.reset(openList);
            addDoubleItem(listBuilder, row.kind);
            addDoubleItem(listBuilder, row.partition);
            addDoubleItem(listBuilder, row.seq);
            addDoubleItem(listBuilder, row.score);
            buildVector(row.vec);
            vecStorage.reset();
            vecBuilder.write(vecStorage.getDataOutput(), true);
            listBuilder.addItem(vecStorage);
            listBuilder.write(tb.getDataOutput(), true);
            tb.addFieldEndOffset();
            appendToWriter();
        }

        void plainVector(double[] vec) throws Exception {
            tb.reset();
            buildVector(vec);
            vecBuilder.write(tb.getDataOutput(), true);
            tb.addFieldEndOffset();
            appendToWriter();
        }

        private void buildVector(double[] vec) throws Exception {
            vecBuilder.reset(openList);
            for (double d : vec) {
                addDoubleItem(vecBuilder, d);
            }
        }

        private void addDoubleItem(OrderedListBuilder builder, double value) throws Exception {
            itemStorage.reset();
            itemStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
            itemStorage.getDataOutput().writeDouble(value);
            builder.addItem(itemStorage);
        }

        private void appendToWriter() throws HyracksDataException {
            FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }

        void flush() throws HyracksDataException {
            appender.write(writer, true);
        }
    }
}
