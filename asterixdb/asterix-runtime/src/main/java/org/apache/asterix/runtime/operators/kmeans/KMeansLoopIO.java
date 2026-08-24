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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.runtime.utils.VectorDistanceCalculation;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.std.misc.MaterializerTaskState;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * CLUSTER BY k-means‖ initialization loop — shared wire formats and raw-vector (de)serialization
 * for the loop's internal edges and run files. These frames never leave the loop sub-graph -- the downstream
 * RECLUSTER is fed by the separate envelopes on Op1's pool output -- so they use a compact <b>raw double[]</b>
 * encoding
 * rather than the tagged ordered-list envelope.
 * <p>
 * A vector field is simply its {@code dim} components written back-to-back as raw doubles ({@code dim * 8} bytes);
 * it is read straight off the frame by byte offset ({@link #readRawVector}). The vector column's declared
 * {@link ISerializerDeserializer} in the record descriptors below is therefore a <b>placeholder</b>: every read
 * goes through {@link org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor} field offsets and every write
 * through {@link #writeRawVector}; the serde itself is never invoked, and the broadcast/M-to-1 connectors copy
 * frames byte-for-byte without deserializing.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
public final class KMeansLoopIO {

    private KMeansLoopIO() {
    }

    /** A draw frame carries a real drawn vector; an end-of-round marker closes a round's draw stream. */
    public static final int KIND_DRAW = 0;
    public static final int KIND_END = 1;

    /** PhiMerge -> Sample: {@code {round:int, value:double}} (the reduced phi). */
    public static final RecordDescriptor SCALAR_RD = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

    /**
     * Cost -> PhiMerge: {@code {round:int, part:int, localSigma:double}}.
     * <p>
     * The partition id is carried so PhiMerge can reduce in a fixed order. Without it the only order available
     * is network arrival, which varies run to run -- and floating-point addition is not associative, so the
     * same partials would produce different phi values, hence different draw probabilities, hence a different
     * clustering from the same query. The rest of the loop is deterministic on any topology: a draw depends
     * only on the vector (see {@link #uniformDraw}) and PoolMerge sorts its draws. This keeps the reduce from
     * being the one place that is not.
     */
    public static final RecordDescriptor SIGMA_RD =
            new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

    /**
     * Sample -> PoolMerge and PoolMerge -> Release: {@code {round:int, part:int, seq:int, kind:int, vec:rawDoubles}}.
     * For {@link #KIND_END} markers, {@code part} identifies the finishing partition and {@code vec}/{@code seq} are
     * ignored. The last column is a raw-double vector (see class comment).
     */
    public static final RecordDescriptor DRAW_RD = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE /* placeholder: raw double[] read by offset */ });

    /**
     * Lloyd loop, Controller -&gt; CentroidMerge:
     * {@code {iter:int, part:int, seq:int, kind:int, count:double, sum:rawDoubles}}, where {@code seq} is the
     * centroid index the partial belongs to and {@code count}/{@code sum} are that centroid's local member count
     * and component-wise sum. {@link #KIND_END} markers close a partition's contribution for one iteration and
     * carry no payload. Distinct from {@link #DRAW_RD} only by the extra {@code count} column.
     */
    public static final RecordDescriptor PARTIAL_RD =
            new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE /* placeholder: raw double[] read by offset */ });

    /** The pool run file: one raw-double vector per tuple, {@code {vec:rawDoubles}}. */
    public static final RecordDescriptor POOL_RD = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE /* placeholder: raw double[] read by offset */ });

    /**
     * RECLUSTER's internal weigh-partial layout: {@code {seq:int, part:int, count:double, sum:rawDoubles}}.
     * <p>
     * The partials cross the logical operator boundary packed into a single field, because
     * {@code KMeansStageOperator} declares one output variable and the record descriptor for that edge is
     * therefore single-field. Hyracks' external sort keys on <em>tuple fields</em>, so it cannot sort that
     * packed form. RECLUSTER re-emits each partial in this flat shape into a sort it owns; the layout never
     * leaves the operator, so widening it costs nothing elsewhere.
     */
    public static final RecordDescriptor PARTIAL_FLAT_RD =
            new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE /* placeholder: raw double[] read by offset */ });

    /** Sort keys for {@link #PARTIAL_RD}: (seq, part) — the order MERGE_ORDER imposes in CentroidMerge. */
    public static final int[] PARTIAL_SORT_FIELDS = { 2, 1 };

    public static final IBinaryComparatorFactory[] PARTIAL_SORT_COMPARATORS =
            { IntegerBinaryComparatorFactory.INSTANCE, IntegerBinaryComparatorFactory.INSTANCE };

    /**
     * Sort keys for {@link #DRAW_RD}: (seq, part) — the order PoolMerge emits a round in, where {@code seq} is
     * the drawing Sample's content hash of the vector rather than a positional counter. Ordering on content
     * first makes the pool's layout -- and so every candidate's index, which RECLUSTER picks by -- a property
     * of the drawn set alone, not of the order rows were read in or of which partition drew them. {@code part}
     * only breaks ties between equal hashes, which are duplicate vectors or (vanishingly) a truncation clash;
     * either way the candidates it separates are interchangeable.
     */
    public static final int[] DRAW_SORT_FIELDS = { 2, 1 };

    public static final IBinaryComparatorFactory[] DRAW_SORT_COMPARATORS =
            { IntegerBinaryComparatorFactory.INSTANCE, IntegerBinaryComparatorFactory.INSTANCE };

    /** Sort keys for {@link #PARTIAL_FLAT_RD}: (seq, part) — groups every partition's report per candidate. */
    public static final int[] PARTIAL_FLAT_SORT_FIELDS = { 0, 1 };

    public static final IBinaryComparatorFactory[] PARTIAL_FLAT_COMPARATORS =
            { IntegerBinaryComparatorFactory.INSTANCE, IntegerBinaryComparatorFactory.INSTANCE };

    /** Appends {@code v}'s components as one raw-double field (call after the tuple's earlier fields). */
    public static void writeRawVector(ArrayTupleBuilder tb, double[] v) throws HyracksDataException {
        try {
            DataOutput out = tb.getDataOutput();
            for (double d : v) {
                out.writeDouble(d);
            }
            tb.addFieldEndOffset();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /** Decodes a raw-double vector field ({@code length} bytes = {@code length/8} components) at {@code start}. */
    public static double[] readRawVector(byte[] data, int start, int length) {
        int dim = length / Double.BYTES;
        double[] v = new double[dim];
        for (int i = 0; i < dim; i++) {
            v[i] = DoublePointable.getDouble(data, start + i * Double.BYTES);
        }
        return v;
    }

    /** Sink for {@link #streamRawVectors}: receives each stored vector, may throw on cancellation/error. */
    @FunctionalInterface
    public interface RawVectorConsumer {
        void accept(double[] vec) throws HyracksDataException;
    }

    /**
     * Streams every raw-double vector out of a {@link MaterializerTaskState} run file (pool or resident vectors,
     * both {@link #POOL_RD}) via a fresh reader — repeatable, non-deleting, one frame buffered at a time. Polls
     * the task-thread interrupt per frame so a cancelled job's pure-CPU scan aborts promptly.
     */
    public static void streamRawVectors(MaterializerTaskState state, IHyracksTaskContext ctx, RawVectorConsumer sink)
            throws HyracksDataException {
        FrameTupleAccessor accessor = new FrameTupleAccessor(POOL_RD);
        FrameTupleReference tuple = new FrameTupleReference();
        VSizeFrame frame = new VSizeFrame(ctx);
        RunFileReader reader = state.createReader();
        reader.open();
        try {
            while (reader.nextFrame(frame)) {
                if (Thread.currentThread().isInterrupted()) {
                    throw HyracksDataException.create(new InterruptedException());
                }
                accessor.reset(frame.getBuffer());
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    tuple.reset(accessor, i);
                    sink.accept(readRawVector(tuple.getFieldData(0), tuple.getFieldStart(0), tuple.getFieldLength(0)));
                }
            }
        } finally {
            reader.close();
        }
    }

    /**
     * Hashes the contents of a vector into 64 bits. Two vectors with the same components get the same hash
     * no matter which partition or position they are stored at. {@link #uniformDraw} uses this hash as the
     * source of randomness so that a draw does not depend on the partition layout.
     * <p>
     * Hashes the bit pattern of each component ({@link Double#doubleToLongBits}) rather than its value.
     */
    public static long fingerprint(double[] v) {
        long h = mix64(0x9E3779B97F4A7C15L ^ v.length);
        for (double d : v) {
            h = mix64(h ^ Double.doubleToLongBits(d));
        }
        return h;
    }

    /** SplitMix64's finalizer: a 64-bit bijection with full avalanche. */
    public static long mix64(long z) {
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /**
     * Returns the random number in {@code [0, 1)} that SAMPLE compares against the draw probability
     * {@code l * d^2(x, pool) / phi} of one vector in one round.
     * <p>
     * The number is computed from the vector hash ({@link #fingerprint}), the query seed and the round. It does
     * not depend on the partition the vector is in or on its position in the run file. So the same vector is
     * drawn or not drawn on every topology. This replaces a per-partition {@code java.util.Random} stream whose
     * n-th value depended on the position n.
     * <p>
     * Duplicate vectors get the same number and are drawn together. RECLUSTER weighs duplicates so this is
     * harmless.
     */
    public static double uniformDraw(long fingerprint, long seedBase, int round) {
        long z = mix64(fingerprint ^ mix64(seedBase * 0x9E3779B97F4A7C15L + round));
        return (z >>> 11) * 0x1.0p-53;
    }

    /** Bytes per column entry: the nearest distance, then the index of the pool member it was nearest to. */
    public static final int SCORE_ENTRY_BYTES = Double.BYTES + Integer.BYTES;

    /** Entries packed into one tuple, so per-tuple framing does not dominate a {@link #SCORE_ENTRY_BYTES} payload. */
    private static final int SCORE_ENTRIES_PER_TUPLE = 1024;

    /** One packed field of {@code (nearest:double, index:int)} entries; read by offset, like the vector layouts. */
    public static final RecordDescriptor SCORE_RD = new RecordDescriptor(
            new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE /* placeholder: packed */ });

    /**
     * Writes the per-vector score column: for each resident vector, in run-file order, the distance to its
     * nearest pool member and that member's index.
     * <p>
     * COST computes these while scanning anyway, so recording them is nearly free -- and it lets SAMPLE, which
     * scores against the same pool in the same round, do no distance work and never open the pool at all.
     * Alignment needs no key because both sides are strictly sequential: entry {@code i} is vector {@code i}.
     */
    public static final class ScoreColumnWriter {
        private final MaterializerTaskState state;
        private final VSizeFrame frame;
        private final FrameTupleAppender appender;
        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);

        public ScoreColumnWriter(MaterializerTaskState state, IHyracksTaskContext ctx) throws HyracksDataException {
            this.state = state;
            this.frame = new VSizeFrame(ctx);
            this.appender = new FrameTupleAppender(frame);
        }

        /** Appends the first {@code count} entries of a finished block. */
        public void append(double[] nearest, int[] index, int count) throws HyracksDataException {
            for (int from = 0; from < count; from += SCORE_ENTRIES_PER_TUPLE) {
                int to = Math.min(count, from + SCORE_ENTRIES_PER_TUPLE);
                tb.reset();
                try {
                    DataOutput out = tb.getDataOutput();
                    for (int i = from; i < to; i++) {
                        out.writeDouble(nearest[i]);
                        out.writeInt(index[i]);
                    }
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
                tb.addFieldEndOffset();
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    flushFrame();
                    if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                        throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                                "a score column chunk is too large to fit in a frame");
                    }
                }
            }
        }

        /** Pushes the trailing partial frame; the column is only complete for readers after this. */
        public void finish() throws HyracksDataException {
            if (appender.getTupleCount() > 0) {
                flushFrame();
            }
        }

        private void flushFrame() throws HyracksDataException {
            ByteBuffer buffer = frame.getBuffer();
            buffer.position(0);
            buffer.limit(buffer.capacity());
            state.appendFrame(buffer);
            appender.reset(frame, true);
        }
    }

    /**
     * Sequential reader over a {@link ScoreColumnWriter} column, advanced one entry per vector. Running off the
     * end means the column and the vector run file disagree on length, which would silently mis-pair scores with
     * vectors, so it is raised rather than tolerated.
     */
    public static final class ScoreColumnReader implements AutoCloseable {
        private final FrameTupleAccessor accessor = new FrameTupleAccessor(SCORE_RD);
        private final FrameTupleReference tuple = new FrameTupleReference();
        private final VSizeFrame frame;
        private final RunFileReader reader;
        private int tupleIndex = -1;
        private int tupleCount;
        private byte[] data;
        private int entryStart;
        private int entryIndex;
        private int entryCount;
        private double nearest;
        private int index;

        public ScoreColumnReader(MaterializerTaskState state, IHyracksTaskContext ctx) throws HyracksDataException {
            this.frame = new VSizeFrame(ctx);
            this.reader = state.createReader();
            reader.open();
        }

        /** Moves to the next entry; {@link #nearest()} and {@link #index()} then describe it. */
        public void advance() throws HyracksDataException {
            while (entryIndex == entryCount) {
                if (++tupleIndex >= tupleCount) {
                    if (!reader.nextFrame(frame)) {
                        throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                                "the score column is shorter than the vectors it scores");
                    }
                    accessor.reset(frame.getBuffer());
                    tupleCount = accessor.getTupleCount();
                    tupleIndex = 0;
                    if (tupleCount == 0) {
                        tupleIndex = tupleCount; // an empty frame: keep pulling
                        continue;
                    }
                }
                tuple.reset(accessor, tupleIndex);
                data = tuple.getFieldData(0);
                entryStart = tuple.getFieldStart(0);
                entryCount = tuple.getFieldLength(0) / SCORE_ENTRY_BYTES;
                entryIndex = 0;
            }
            int at = entryStart + entryIndex * SCORE_ENTRY_BYTES;
            nearest = DoublePointable.getDouble(data, at);
            index = IntegerPointable.getInteger(data, at + Double.BYTES);
            entryIndex++;
        }

        public double nearest() {
            return nearest;
        }

        public int index() {
            return index;
        }

        @Override
        public void close() throws HyracksDataException {
            reader.close();
        }
    }

    /**
     * Anything that can replay a sequence of raw vectors in a fixed order: a run file, or the centroid store.
     * The scans below need only that, and the order they replay in is what makes their results reproducible.
     */
    @FunctionalInterface
    public interface RawVectorSource {
        void stream(RawVectorConsumer sink) throws HyracksDataException;
    }

    /** Adapts a materialized run file to {@link RawVectorSource}. */
    public static RawVectorSource source(MaterializerTaskState state, IHyracksTaskContext ctx) {
        return sink -> streamRawVectors(state, ctx, sink);
    }

    /**
     * An ordered sequence of vectors that stays in the heap while it fits the frame budget and moves to a run
     * file when it does not.
     * <p>
     * RECLUSTER's candidate set is {@code O(k * dim)} and weighted k-means++ rereads it once per centroid it
     * picks. Holding it grows the heap with k; always spilling it would charge a set that comfortably fits k
     * passes of I/O. The budget decides instead: under it nothing is written and reads are list indexing, over
     * it the heap holds one frame and reads stream.
     * <p>
     * Append-then-read: every vector is added before the first read. Reads are sequential
     * ({@link #stream}) except for fetching the one vector a round just picked ({@link #get}), which is a
     * scan with an early exit rather than random access.
     */
    public static final class VectorList implements RawVectorSource, AutoCloseable {
        private final IHyracksTaskContext ctx;
        private final JobId jobId;
        private final TaskId taskId;
        private final long byteBudget;
        private final ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        private List<double[]> resident = new ArrayList<>();
        private long residentBytes;
        private MaterializerTaskState spilled;
        private VSizeFrame frame;
        private FrameTupleAppender appender;
        private int size;

        public VectorList(IHyracksTaskContext ctx, JobId jobId, TaskId taskId, int framesLimit) {
            this.ctx = ctx;
            this.jobId = jobId;
            this.taskId = taskId;
            this.byteBudget = (long) Math.max(1, framesLimit) * ctx.getInitialFrameSize();
        }

        public void add(double[] vector) throws HyracksDataException {
            if (spilled == null) {
                resident.add(vector);
                residentBytes += (long) vector.length * Double.BYTES + PER_VECTOR_OVERHEAD;
                if (residentBytes > byteBudget) {
                    spillResident();
                }
            } else {
                append(vector);
            }
            size++;
        }

        public int size() {
            return size;
        }

        /** True while nothing has been written; the common case, and the one that costs no I/O. */
        public boolean isResident() {
            return spilled == null;
        }

        /** The vector at {@code index}. A scan with an early exit once spilled -- used once per round. */
        public double[] get(int index) throws HyracksDataException {
            if (spilled == null) {
                return resident.get(index);
            }
            final double[][] found = new double[1][];
            final int[] at = { 0 };
            streamRawVectors(spilled, ctx, v -> {
                if (at[0]++ == index) {
                    found[0] = v;
                }
            });
            if (found[0] == null) {
                throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE,
                        "vector " + index + " is past the end of a list of " + size);
            }
            return found[0];
        }

        @Override
        public void stream(RawVectorConsumer sink) throws HyracksDataException {
            if (spilled == null) {
                for (double[] v : resident) {
                    sink.accept(v);
                }
            } else {
                streamRawVectors(spilled, ctx, sink);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            resident = null;
            if (spilled != null) {
                spilled.close();
                spilled.deleteFile();
                spilled = null;
            }
        }

        /** Crossing the budget: write out what is held, then keep appending. */
        private void spillResident() throws HyracksDataException {
            spilled = new MaterializerTaskState(jobId, taskId);
            spilled.open(ctx);
            frame = new VSizeFrame(ctx);
            appender = new FrameTupleAppender(frame);
            for (double[] v : resident) {
                append(v);
            }
            resident = new ArrayList<>();
            residentBytes = 0;
        }

        private void append(double[] vector) throws HyracksDataException {
            tb.reset();
            writeRawVector(tb, vector);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                flushFrame();
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new RuntimeDataException(ErrorCode.ILLEGAL_STATE, "a vector is too large for a frame");
                }
            }
        }

        private void flushFrame() throws HyracksDataException {
            ByteBuffer buffer = frame.getBuffer();
            buffer.position(0);
            buffer.limit(buffer.capacity());
            spilled.appendFrame(buffer);
            appender.reset(frame, true);
        }

        /** Pushes the trailing partial frame. Must be called once adding is done, before any read. */
        public void seal() throws HyracksDataException {
            if (spilled != null && appender.getTupleCount() > 0) {
                flushFrame();
            }
        }
    }

    /** Receives one non-empty accumulator slot: its index, how many vectors landed on it, and their sum. */
    @FunctionalInterface
    public interface WeighedSlotConsumer {
        void accept(int index, long count, double[] sum) throws HyracksDataException;
    }

    /**
     * Reduces vectors onto {@code slotCount} accumulator slots while holding at most {@code window} of them.
     * <p>
     * A slot is written at whichever index a vector proved nearest to, so the writes are random and there is
     * no side to stream. Taking the assignment from a precomputed score column instead lets the slot range be
     * swept: each pass admits only the vectors whose nearest index falls in the current window, costing one
     * sequential read and holding {@code window} sums rather than {@code slotCount} of them.
     * <p>
     * The window is a memory bound, not a semantic one. Slots reach the sink in ascending index order and each
     * sums its vectors in run-file order, so the sums are identical bit for bit at any window; a window of at
     * least {@code slotCount} simply does it in one pass.
     */
    public static void accumulateInWindows(MaterializerTaskState vectorState, MaterializerTaskState scoreState,
            IHyracksTaskContext ctx, int slotCount, int window, WeighedSlotConsumer sink) throws HyracksDataException {
        accumulateInWindows(source(vectorState, ctx), scoreState, ctx, slotCount, window, sink);
    }

    /** As above, over any replayable vector source. */
    public static void accumulateInWindows(RawVectorSource vectors, MaterializerTaskState scoreState,
            IHyracksTaskContext ctx, int slotCount, int window, WeighedSlotConsumer sink) throws HyracksDataException {
        if (slotCount <= 0) {
            return;
        }
        int span = Math.max(1, window);
        for (int start = 0; start < slotCount; start += span) {
            final int from = start;
            final int to = Math.min(slotCount, start + span);
            final long[] counts = new long[to - from];
            final double[][] sums = new double[to - from][];
            try (ScoreColumnReader scores = new ScoreColumnReader(scoreState, ctx)) {
                vectors.stream(vec -> {
                    scores.advance();
                    int nearestIndex = scores.index();
                    // Outside this window, or unassigned (-1): another pass owns it, or no pass does.
                    if (nearestIndex < from || nearestIndex >= to || Double.isNaN(scores.nearest())) {
                        return;
                    }
                    int slot = nearestIndex - from;
                    counts[slot]++;
                    double[] sum = sums[slot];
                    if (sum == null) {
                        sum = new double[vec.length];
                        sums[slot] = sum;
                    }
                    for (int d = 0; d < Math.min(sum.length, vec.length); d++) {
                        sum[d] += vec[d];
                    }
                });
            }
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    sink.accept(from + i, counts[i], sums[i]);
                }
            }
        }
    }

    /** Receives one finished block of {@link #streamScoredAgainstPool}: the vectors and their nearest pool member. */
    @FunctionalInterface
    public interface ScoredBlockConsumer {
        void accept(double[][] vectors, int count, double[] nearestDistance, int[] nearestIndex)
                throws HyracksDataException;
    }

    /**
     * Scores every resident vector against the candidate pool without ever holding the pool in the heap.
     * <p>
     * The obvious shape -- pool resident, vectors streamed -- makes the heap grow with {@code k}, since the pool
     * is {@code 2 * k} members per round. This inverts it: a block of vectors is held with a running nearest
     * distance per slot, the whole pool is streamed past the block, and when the pool is exhausted the block's
     * minima are final. The heap is then bounded by the frame budget instead of by {@code k}, and the pool is
     * read one frame at a time. The price is {@code ceil(N / B)} sequential passes over the pool, which is the
     * small side; the number of distance computations is unchanged.
     * <p>
     * Order is preserved exactly, which is what lets this be swapped in under existing results: vectors reach the
     * sink in run-file order (blocks in order, and in order within a block), the pool is streamed in run-file
     * order so a strict {@code <} still resolves ties to the first pool member, and therefore any summation the
     * sink performs adds its terms in the same sequence as the resident form did. The output is bit-identical at
     * every block size.
     */
    public static void streamScoredAgainstPool(MaterializerTaskState vectorState, MaterializerTaskState poolState,
            IHyracksTaskContext ctx, int framesLimit, ScoredBlockConsumer sink) throws HyracksDataException {
        streamScoredAgainstPool(source(vectorState, ctx), source(poolState, ctx), ctx, framesLimit, sink);
    }

    /**
     * As above, over any replayable sources. The Lloyd loop scores against its centroid set, which is a store
     * rather than a run file, but the shape -- and the reason for inverting the residency -- is the same.
     */
    public static void streamScoredAgainstPool(RawVectorSource vectors, RawVectorSource pool, IHyracksTaskContext ctx,
            int framesLimit, ScoredBlockConsumer sink) throws HyracksDataException {
        BlockScan scan = new BlockScan(pool, ctx, framesLimit, sink);
        vectors.stream(scan::add);
        scan.flush(); // the final short block
    }

    /** Accumulates vectors into a block and scores the block against the pool once it is full. */
    private static final class BlockScan {
        private final RawVectorSource pool;
        private final IHyracksTaskContext ctx;
        private final int framesLimit;
        private final ScoredBlockConsumer sink;
        private double[][] block;
        private double[] nearest;
        private int[] nearestIndex;
        private int count;

        private BlockScan(RawVectorSource pool, IHyracksTaskContext ctx, int framesLimit, ScoredBlockConsumer sink) {
            this.pool = pool;
            this.ctx = ctx;
            this.framesLimit = framesLimit;
            this.sink = sink;
        }

        private void add(double[] vec) throws HyracksDataException {
            if (block == null) {
                // The width is only known once a vector has been read, and T6 guarantees every vector in the run
                // file shares it, so sizing off the first one sizes the whole scan.
                int capacity = blockCapacity(ctx, framesLimit, vec.length);
                block = new double[capacity][];
                nearest = new double[capacity];
                nearestIndex = new int[capacity];
            }
            block[count++] = vec;
            if (count == block.length) {
                flush();
            }
        }

        private void flush() throws HyracksDataException {
            if (count == 0) {
                return;
            }
            final int n = count;
            for (int i = 0; i < n; i++) {
                nearest[i] = Double.POSITIVE_INFINITY;
                nearestIndex[i] = -1;
            }
            final int[] poolIndex = { 0 };
            pool.stream(candidate -> {
                int c = poolIndex[0]++;
                for (int i = 0; i < n; i++) {
                    double d = VectorDistanceCalculation.euclideanSquared(block[i], candidate);
                    // Strict <: ties resolve to the first pool member, as in the resident form.
                    if (d < nearest[i]) {
                        nearest[i] = d;
                        nearestIndex[i] = c;
                    }
                }
            });
            count = 0;
            sink.accept(block, n, nearest, nearestIndex);
            Arrays.fill(block, 0, n, null); // drop the block's vectors before the next one is filled
        }
    }

    /** Per-slot cost besides the doubles: array header (16) + reference (8) + a {@code double} + an {@code int}. */
    private static final int PER_VECTOR_OVERHEAD = 16 + 8 + Double.BYTES + Integer.BYTES;

    /**
     * How many vectors fit the block budget. Counts what a slot actually costs on the heap -- the doubles, the
     * array header and reference, and the two per-slot scoring scalars -- so a low-dimension input cannot turn
     * the budget into millions of tiny arrays. At least one, so any width makes progress.
     */
    public static int blockCapacity(IHyracksTaskContext ctx, int framesLimit, int dim) {
        long perVector = (long) dim * Double.BYTES + PER_VECTOR_OVERHEAD;
        long capacity = (long) framesLimit * ctx.getInitialFrameSize() / perVector;
        return (int) Math.max(1L, Math.min(capacity, Integer.MAX_VALUE));
    }

    /** Appends one raw-double vector as a {@link #POOL_RD} tuple into {@code appender} (caller flushes frames). */
    public static void appendPoolVector(ArrayTupleBuilder tb, double[] vec) throws HyracksDataException {
        tb.reset();
        writeRawVector(tb, vec);
    }
}
