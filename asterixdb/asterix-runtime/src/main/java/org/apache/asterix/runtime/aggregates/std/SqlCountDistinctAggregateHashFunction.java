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
package org.apache.asterix.runtime.aggregates.std;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.formats.nontagged.NormalizedKeyComputerFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAppender;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunGenerator;
import org.apache.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import org.apache.hyracks.dataflow.std.sort.ISorter;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.annotations.AiProvenance.Agent;
import org.apache.hyracks.util.annotations.AiProvenance.ContributionKind;
import org.apache.hyracks.util.annotations.AiProvenance.Tool;

/**
 * HASH_DISTINCT_COUNT — counts distinct non-null values. Equivalent to {@code COUNT(DISTINCT x)}.
 *
 * <h3>Two operating modes (one operator, exact result, bounded memory)</h3>
 * <ol>
 *   <li><b>In-memory hash (fast path).</b> While a group's distinct values fit the memory budget, they
 *       are deduplicated in an open-addressing hash table and {@code finish()} returns the slot count
 *       directly. No sort, no disk. This is the common case for ordinary groups.</li>
 *   <li><b>Sort-spill (bounded path).</b> If a single group's table would exceed the frame budget, the
 *       group switches to spilling: every value (the ones already hashed <em>and</em> all later ones) is
 *       fed to an {@link ExternalSortRunGenerator}, which sorts within the budget and writes sorted runs
 *       to disk on overflow. {@code finish()} merges the runs with an {@link ExternalSortRunMerger}
 *       (a bounded, multi-pass merge) and counts <em>distinct adjacent</em> values — duplicates that
 *       landed in different runs collapse because equal values are adjacent in the merged order. Memory
 *       stays bounded and the count is exact regardless of group size.</li>
 * </ol>
 * The in-memory dedup is only an optimization; correctness in the spill case comes entirely from the
 * sorted merge, so the per-flush in-memory deduplication never needs to be summed.
 *
 * <h3>O(1) per-group reset</h3>
 * One aggregator instance is reused for every group of a GROUP BY / GROUPING SETS. {@link #init()} does
 * not clear the table; it advances {@code currentStamp}, which makes every slot read as empty. Capacity
 * reached by a large in-memory group is retained so later groups reuse the table. A group that spills
 * releases the table instead ({@link #releaseTable()}), and the next {@link #init()} re-creates it at
 * its initial size.
 *
 * <h3>Equality / ordering consistency</h3>
 * The hash function, comparator and normalized-key computer are built for the column's actual type via
 * the same matched providers hash-join, hash-group-by and external-sort use. The spill path relies on
 * the comparator's ordering being consistent with its equality — the same property hash-equality already
 * assumes — so the two modes always agree.
 */
@AiProvenance(agent = Agent.CLAUDE_SONNET_4_6, tool = Tool.CLAUDE_CODE_UI, contributionKind = ContributionKind.ASSISTED, notes = "Hash-optimized COUNT(DISTINCT): single-pass hash-based distinct counting with a bounded-memory ")
public class SqlCountDistinctAggregateHashFunction extends AbstractAggregateFunction {

    // ---- result serialization ----
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    private final AMutableInt64 resultValue = new AMutableInt64(-1);
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();

    private final IPointable inputVal = new VoidPointable();
    private final IScalarEvaluator eval;
    private final IBinaryHashFunction hashFn;
    private final IBinaryComparator comparator;

    // ---- in-memory hash table (fast path) ----
    private static final int INITIAL_CAPACITY = 1024; // must be power of two for bitwise slot lookup
    private static final double LOAD_FACTOR = 0.70; // rehash when 70% full to keep probes short
    private static final int INITIAL_ARENA_BYTES = 1 << 16; // 64 KB starting size for the value arena
    private static final int BYTES_PER_SLOT = 16; // four int[] entries per slot, used for the memory estimate
    private int[] slotValueOffset; // start of slot i's bytes in valueArena
    private int[] slotValueLength; // length of slot i's bytes in valueArena
    private int[] slotHash; // cached hash — skips comparator on most misses
    private int[] slotStamp; // slot is live only if slotStamp[i] == currentStamp
    private int currentStamp; // increment per group for O(1) reset without clearing arrays
    private byte[] valueArena; // all value bytes packed end-to-end
    private int arenaEnd; // next free byte in valueArena
    private int capacity; // slot count — always a power of two
    private int mask; // capacity - 1 — used for fast slot = hash & mask
    private int growAt; // precomputed capacity * LOAD_FACTOR — triggers rehash
    private int liveSlots;
    private long distinctCount;

    // ---- sort-spill (bounded path) ----
    private final IHyracksTaskContext taskCtx;
    private final int numFrames; // frame budget from compiler.aggregate.distinct.hash.memory (0 => unbounded)
    private final long memoryCapBytes; // in-memory table size at which a group switches to spilling
    private final IBinaryComparatorFactory comparatorFactory;
    private final INormalizedKeyComputerFactory nkComputerFactory; // may be null (e.g. ANY / complex types)
    private final RecordDescriptor valueRecordDesc; // single field: the tagged distinct value

    private boolean spilling; // current group has switched to the sort-spill path
    private ExternalSortRunGenerator runsGenerator; // lazily created on first overflow, reused across groups
    private ExternalSortRunMerger runsMerger; // bounded multi-pass merger, lazily created and reset per group
    private DistinctCountingFrameWriter distinctCounter;
    private IFrameTupleAppender appender;
    private ArrayTupleBuilder tupleBuilder;
    private IFrame sortFrame; // input frame feeding the run generator

    public SqlCountDistinctAggregateHashFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            SourceLocation sourceLoc, IAType itemType, int numFrames) throws HyracksDataException {
        super(sourceLoc);
        this.taskCtx = context.getTaskContext();
        this.numFrames = numFrames;
        eval = args[0].createScalarEvaluator(context);
        // Build the matched hash / comparator / normalized-key set for the argument's actual type.
        IAType type = itemType != null ? itemType : BuiltinType.ANY;
        hashFn = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(type)
                .createBinaryHashFunction();
        comparatorFactory = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(type, type, true);
        comparator = comparatorFactory.createBinaryComparator();
        nkComputerFactory = NormalizedKeyComputerFactoryProvider.INSTANCE.getNormalizedKeyComputerFactory(type, true);
        ISerializerDeserializer<?> valueSerde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(type);
        ITypeTraits valueTrait = TypeTraitProvider.INSTANCE.getTypeTrait(type);
        valueRecordDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { valueSerde }, new ITypeTraits[] { valueTrait });

        // A group switches to spilling once its in-memory table reaches the frame budget. The run
        // generator then uses up to numFrames more frames, so peak memory is ~2x the budget — a constant.
        memoryCapBytes = numFrames > 0 ? (long) numFrames * taskCtx.getInitialFrameSize() : Long.MAX_VALUE;

        allocateInitialTable();
        currentStamp = 0; // first init() advances to 1; default slot stamp 0 then reads as empty
    }

    /**
     * Allocates the hash table at its initial size and clears the byte/slot counters. Called by the
     * constructor (first-time setup) and by {@link #init()} when the previous group spilled and
     * {@link #releaseTable()} nulled the structures. It does not touch {@code currentStamp}: the freshly
     * zeroed {@code slotStamp} reads as empty.
     */
    private void allocateInitialTable() {
        capacity = INITIAL_CAPACITY;
        mask = capacity - 1;
        growAt = (int) (capacity * LOAD_FACTOR);
        slotValueOffset = new int[capacity];
        slotValueLength = new int[capacity];
        slotHash = new int[capacity];
        slotStamp = new int[capacity];
        valueArena = new byte[INITIAL_ARENA_BYTES];
        arenaEnd = 0;
        liveSlots = 0;
    }

    @Override
    public void init() throws HyracksDataException {
        if (spilling) {
            // The previous group spilled and released the in-memory table; re-create it for this group.
            allocateInitialTable();
        }
        // O(1) reset: a fresh generation makes every slot read as empty without touching memory.
        currentStamp++;
        if (currentStamp == 0) { // wrapped after 2^32 groups — clear stamps and restart
            Arrays.fill(slotStamp, 0);
            currentStamp = 1;
        }
        liveSlots = 0;
        arenaEnd = 0;
        distinctCount = 0;
        spilling = false;
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, inputVal);
        final byte[] bytes = inputVal.getByteArray();
        final int off = inputVal.getStartOffset();

        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[off]);
        if (typeTag == ATypeTag.NULL || typeTag == ATypeTag.MISSING || typeTag == ATypeTag.SYSTEM_NULL) {
            return;
        }

        if (spilling) {
            // Feed every value to the sorted-run generator. The
            // distinct count is computed by the merge in finish(); no in-memory dedup is done here.
            appendToRuns(inputVal);
            return;
        }

        if (insertIfAbsent(inputVal)) {
            distinctCount++;
            // Switch this group to the bounded sort-spill path once the table reaches the budget.
            if ((long) arenaEnd + (long) capacity * BYTES_PER_SLOT > memoryCapBytes) {
                startSpilling();
            }
        }
    }

    private boolean insertIfAbsent(IPointable item) throws HyracksDataException {
        final byte[] itemBytes = item.getByteArray();
        final int itemStart = item.getStartOffset();
        final int itemLen = item.getLength();
        final int hash = hashFn.hash(itemBytes, itemStart, itemLen);

        // Hoist arrays/fields into locals for the probe loop.
        final int stamp = currentStamp;
        final int m = mask;
        final int[] stamps = slotStamp;
        final int[] hashes = slotHash;
        final int[] lengths = slotValueLength;
        final int[] offsets = slotValueOffset;
        final byte[] arena = valueArena;

        int slot = hash & m;
        while (stamps[slot] == stamp) {
            if (hashes[slot] == hash
                    && comparator.compare(arena, offsets[slot], lengths[slot], itemBytes, itemStart, itemLen) == 0) {
                return false; // already present
            }
            slot = (slot + 1) & m;
        }

        // New value: copy its bytes into the arena and claim the slot.
        ensureArenaCapacity(itemLen);
        System.arraycopy(itemBytes, itemStart, valueArena, arenaEnd, itemLen);
        offsets[slot] = arenaEnd;
        lengths[slot] = itemLen;
        hashes[slot] = hash;
        stamps[slot] = stamp;
        arenaEnd += itemLen;
        liveSlots++;
        if (liveSlots > growAt) {
            rehash();
        }
        return true;
    }

    /**
     * Doubles the slot table and reinserts the live entries. The arena is untouched — only the slot
     * index is rebuilt. New stamp arrays default to 0, which reads as empty because the live stamp is
     * always &gt;= 1, so no clearing is needed.
     */
    private void rehash() {
        int newCapacity = capacity << 1;
        int newMask = newCapacity - 1;
        int[] newOffsets = new int[newCapacity];
        int[] newLengths = new int[newCapacity];
        int[] newHashes = new int[newCapacity];
        int[] newStamps = new int[newCapacity];
        final int stamp = currentStamp;
        for (int i = 0; i < capacity; i++) {
            if (slotStamp[i] == stamp) {
                int h = slotHash[i];
                int slot = h & newMask;
                while (newStamps[slot] == stamp) {
                    slot = (slot + 1) & newMask;
                }
                newOffsets[slot] = slotValueOffset[i];
                newLengths[slot] = slotValueLength[i];
                newHashes[slot] = h;
                newStamps[slot] = stamp;
            }
        }
        capacity = newCapacity;
        mask = newMask;
        growAt = (int) (capacity * LOAD_FACTOR);
        slotValueOffset = newOffsets;
        slotValueLength = newLengths;
        slotHash = newHashes;
        slotStamp = newStamps;
    }

    private void ensureArenaCapacity(int needed) {
        if (arenaEnd + needed > valueArena.length) {
            valueArena = Arrays.copyOf(valueArena, Math.max(valueArena.length << 1, arenaEnd + needed));
        }
    }

    /**
     * Transition the current group from in-memory hashing to sort-spilling. The sort structures are
     * created once and reused across groups; the live values already in the hash table are pushed into
     * the run generator first so the merge in finish() sees every value for this group.
     */
    private void startSpilling() throws HyracksDataException {
        ensureSpillStructures();
        runsGenerator.open();
        runsGenerator.getSorter().reset();
        appender.reset(sortFrame, true);
        // Push the values accumulated in the hash table into the sorted-run generator.
        for (int i = 0; i < capacity; i++) {
            if (slotStamp[i] == currentStamp) {
                appendBytesToRuns(valueArena, slotValueOffset[i], slotValueLength[i]);
            }
        }
        spilling = true;
        releaseTable();
    }

    /**
     * Releases the slot arrays and value arena that a big group grew — a spilling group no longer needs
     * them, so holding them alongside the sort frames would only add to peak memory. Allocation of the
     * fresh initial-size table is delayed until the next group's {@link #init()}: if this group is the
     * last one, the memory is simply never reallocated. Called only when a group switches to spilling —
     * a rare, already-expensive event — so the common in-memory groups keep their grown capacity for
     * fast reuse.
     */
    private void releaseTable() {
        slotValueOffset = null;
        slotValueLength = null;
        slotHash = null;
        slotStamp = null;
        valueArena = null;
        arenaEnd = 0;
        liveSlots = 0;
    }

    private void ensureSpillStructures() throws HyracksDataException {
        if (runsGenerator != null) {
            return;
        }
        sortFrame = new VSizeFrame(taskCtx);
        appender = new FrameTupleAppender();
        tupleBuilder = new ArrayTupleBuilder(1);
        distinctCounter = new DistinctCountingFrameWriter();
        runsGenerator = new ExternalSortRunGenerator(taskCtx, new int[] { 0 },
                new INormalizedKeyComputerFactory[] { nkComputerFactory },
                new IBinaryComparatorFactory[] { comparatorFactory }, valueRecordDesc, Algorithm.MERGE_SORT,
                EnumFreeSlotPolicy.LAST_FIT, numFrames);
    }

    private void appendToRuns(IPointable value) throws HyracksDataException {
        appendBytesToRuns(value.getByteArray(), value.getStartOffset(), value.getLength());
    }

    private void appendBytesToRuns(byte[] bytes, int start, int len) throws HyracksDataException {
        tupleBuilder.reset();
        tupleBuilder.addField(bytes, start, len);
        FrameUtils.appendToWriter(runsGenerator, appender, tupleBuilder.getFieldEndOffsets(),
                tupleBuilder.getByteArray(), 0, tupleBuilder.getSize());
    }

    @Override
    public void finish(IPointable resultPointable) throws HyracksDataException {
        long count = spilling ? finishSpilled() : distinctCount;
        resultStorage.reset();
        try {
            resultValue.setValue(count);
            int64Serde.serialize(resultValue, resultStorage.getDataOutput());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        resultPointable.set(resultStorage);
    }

    private long finishSpilled() throws HyracksDataException {
        if (appender.getTupleCount() > 0) {
            appender.write(runsGenerator, true);
        }
        runsGenerator.close();// sorts / flushes remaining in-memory data into runs
        distinctCounter.open(); // resets the reused instance for this group
        try {
            List<GeneratedRunFileReader> runs = runsGenerator.getRuns();
            if (runs.isEmpty()) {
                // Everything fit in sort memory: feed the sorter's sorted frames directly to the counter.
                ISorter sorter = runsGenerator.getSorter();
                if (sorter.hasRemaining()) {
                    sorter.flush(distinctCounter);
                }
            } else {
                if (runsMerger == null) {
                    // The merger gets its own comparator, kept separate from the probe comparator.
                    runsMerger = new ExternalSortRunMerger(taskCtx, runs, new int[] { 0 },
                            new IBinaryComparator[] { comparatorFactory.createBinaryComparator() },
                            nkComputerFactory != null ? nkComputerFactory.createNormalizedKeyComputer() : null,
                            valueRecordDesc, numFrames, Integer.MAX_VALUE);
                } else {
                    runsMerger.reset(runs);
                }
                runsMerger.process(distinctCounter);
            }
        } finally {
            distinctCounter.close();
        }
        return distinctCounter.count;
    }

    /**
     * Receives the merger's globally sorted output frame by frame and counts distinct values. Because the
     * stream is sorted, equal values are adjacent, so counting each value that differs from the previous
     * one yields the exact distinct count. {@code prevValue} is copied (not referenced) because the merger
     * reuses its output buffer between frames.
     */
    private final class DistinctCountingFrameWriter implements IFrameWriter {
        private final FrameTupleAccessor accessor = new FrameTupleAccessor(valueRecordDesc);
        private byte[] prevValue; // last distinct value seen (copied, because producers reuse their buffers)
        private int prevValueLen;
        private long count;
        private boolean hasPrev;

        /** Clears the per-group state; the instance is created once and reused like {@code runsMerger}. */
        private void reset() {
            count = 0;
            hasPrev = false;
        }

        @Override
        public void open() {
            reset();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            accessor.reset(buffer);
            int tupleCount = accessor.getTupleCount();
            byte[] buf = buffer.array();
            for (int i = 0; i < tupleCount; i++) {
                int fieldStart = accessor.getAbsoluteFieldStartOffset(i, 0);
                int fieldLen = accessor.getFieldLength(i, 0);
                if (!hasPrev || comparator.compare(prevValue, 0, prevValueLen, buf, fieldStart, fieldLen) != 0) {
                    count++;
                    rememberPrev(buf, fieldStart, fieldLen);
                    hasPrev = true;
                }
            }
        }

        private void rememberPrev(byte[] buf, int start, int len) {
            if (prevValue == null || prevValue.length < len) {
                prevValue = new byte[len];
            }
            System.arraycopy(buf, start, prevValue, 0, len);
            prevValueLen = len;
        }

        @Override
        public void fail() {
        }

        @Override
        public void close() {
        }
    }

    @Override
    public void finishPartial(IPointable resultPointable) throws HyracksDataException {
        // HASH_DISTINCT_COUNT is single-step (not combinable): the partial result is the final result.
        finish(resultPointable);
    }
}
