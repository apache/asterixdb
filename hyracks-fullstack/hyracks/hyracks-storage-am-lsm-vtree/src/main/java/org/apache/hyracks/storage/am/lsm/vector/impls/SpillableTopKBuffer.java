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
package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.hyracks.dataflow.std.structures.IResetableComparable;
import org.apache.hyracks.dataflow.std.structures.MaxHeap;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A spillable top-K buffer for vector ANN search.
 *
 * In-memory: MaxHeap of VectorSearchHeapEntry + VariableDeletableTupleMemoryManager (frame-backed).
 * On budget exhaustion: sorts current entries by dqx, writes worst half to a RunFileWriter,
 * resets buffer with best half, and continues. At drain time, merges in-memory entries with
 * spilled run files.
 *
 * Follows the inverted index precedent (InvertedIndexSearchResult) for dual-mode
 * in-memory/disk operation with managed workspace files.
 */
public class SpillableTopKBuffer {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int DEFAULT_FRAMES_LIMIT = 4;
    private static final String SPILL_FILE_PREFIX = "VectorTopKSpill";

    // Configuration
    private final int candidateLimit;
    private final int frameSize;
    private final int budgetBytes;
    private final IHyracksTaskContext ctx;

    // In-memory top-K
    private MaxHeap topKHeap;
    private IDeletableTupleBufferManager bufferManager;
    private ITuplePointerAccessor bufferAccessor;
    private final VectorSearchHeapEntry peekEntry;
    private final VectorSearchHeapEntry insertEntry;

    // Adapter: serialize ITupleReference into frame format for buffer manager
    private IFrame singleTupleFrame;
    private FrameTupleAppender singleTupleAppender;
    private FrameTupleAccessor singleTupleAccessor;
    private RecordDescriptor tupleRecordDescriptor;
    private boolean initialized;

    // Spill state
    private final List<GeneratedRunFileReader> spilledRuns;
    private int spilledTupleCount;
    /** Worst (largest) dqx among any spilled entry. -Inf when nothing has been spilled yet. */
    private double maxSpilledDqx = Double.NEGATIVE_INFINITY;

    // Run file serialization (dqx-prefixed tuples)
    private RecordDescriptor spillRecordDescriptor;
    private IFrame spillOutputFrame;
    private FrameTupleAppender spillOutputAppender;

    public SpillableTopKBuffer(int candidateLimit, IHyracksTaskContext ctx) {
        this.candidateLimit = candidateLimit;
        this.ctx = Objects.requireNonNull(ctx, "SpillableTopKBuffer requires a non-null IHyracksTaskContext "
                + "(set HyracksConstants.HYRACKS_TASK_CONTEXT in the IndexAccessParameters before opening the cursor)");
        this.frameSize = ctx.getInitialFrameSize();
        this.budgetBytes = DEFAULT_FRAMES_LIMIT * frameSize;
        this.peekEntry = new VectorSearchHeapEntry();
        this.insertEntry = new VectorSearchHeapEntry();
        this.spilledRuns = new ArrayList<>();
        this.spilledTupleCount = 0;
        this.initialized = false;
        this.singleTupleAppender = new FrameTupleAppender();
    }

    @SuppressWarnings("rawtypes")
    public void ensureInitialized(ITupleReference tuple) throws HyracksDataException {
        if (initialized) {
            return;
        }
        int fieldCount = tuple.getFieldCount();
        this.tupleRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[fieldCount]);
        IFramePool framePool = new VariableFramePool(ctx, budgetBytes);
        this.bufferManager = new VariableDeletableTupleMemoryManager(framePool, tupleRecordDescriptor);
        this.bufferAccessor = bufferManager.createTuplePointerAccessor();
        this.topKHeap = new MaxHeap(new VectorSearchHeapEntryFactory(), candidateLimit);
        this.singleTupleFrame = new FixedSizeFrame(ctx.allocateFrame(frameSize));
        this.singleTupleAccessor = new FrameTupleAccessor(tupleRecordDescriptor);

        // Spill record descriptor: [dqx (double), original fields...]
        ISerializerDeserializer[] spillSerdes = new ISerializerDeserializer[fieldCount + 1];
        spillSerdes[0] = DoubleSerializerDeserializer.INSTANCE;
        this.spillRecordDescriptor = new RecordDescriptor(spillSerdes);

        initialized = true;
    }

    /**
     * Insert a tuple with its approximate distance. Spills to disk if the memory budget is exceeded.
     * A candidate strictly worse than the current top-K worst is rejected up front; a tuple too large for a
     * single frame throws {@link ErrorCode#RECORD_IS_TOO_LARGE} rather than being silently dropped.
     */
    public void insert(ITupleReference tuple, double dqx) throws HyracksDataException {
        ensureInitialized(tuple);

        // Early reject: skip the new tuple only if it is strictly worse than the worst kept
        // candidate across BOTH the in-memory heap and any spilled run.
        //
        // Spilled tuples are immutable on disk — once a tuple is in a run file we cannot
        // evict it. So the floor for "is this new tuple worth keeping?" is the *larger* of
        // (in-memory peekMax, maxSpilledDqx). Comparing only against the in-memory peekMax
        // is unsafe after a spill, because the in-memory side holds the *best* half and the
        // disk side holds the *worst* half; an incoming dqx that beats the spilled worst is
        // a valid top-K candidate even when it is worse than every in-memory entry.
        //
        // Use strict '>' so ties at the K-boundary are admitted (both are equally valid).
        if (getNumEntries() >= candidateLimit && topKHeap.getNumEntries() > 0) {
            topKHeap.peekMax(peekEntry);
            double globalWorst = Math.max(peekEntry.dqx, maxSpilledDqx);
            if (dqx > globalWorst) {
                return;
            }
        }

        // Serialize tuple into frame format
        singleTupleAppender.reset(singleTupleFrame, true);
        if (!singleTupleAppender.append(tuple)) {
            // A tuple that does not fit a fresh single-frame staging buffer is oversized. Fail loudly with
            // RECORD_IS_TOO_LARGE (as the buffer-manager path below does), rather than silently dropping it:
            // the contract is "never drops tuples", and a silent drop would quietly miss a valid top-K
            // candidate.
            int tupleBytes = 0;
            for (int f = 0; f < tuple.getFieldCount(); f++) {
                tupleBytes += tuple.getFieldLength(f);
            }
            throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE, tupleBytes,
                    singleTupleFrame.getBuffer().capacity());
        }
        singleTupleAccessor.reset(singleTupleFrame.getBuffer());

        // Decide whether to grow the heap or replace its current max.
        // replaceMax is only safe when the new tuple is no worse than the in-memory worst
        // (otherwise we would evict a better candidate for a worse one). For the in-between
        // case — new is worse than in-memory peekMax but still better than maxSpilledDqx —
        // we must grow the heap instead, so the better in-memory candidates survive.
        boolean heapFull = topKHeap.getNumEntries() >= candidateLimit;
        boolean useReplaceMax = false;
        if (heapFull) {
            topKHeap.peekMax(peekEntry);
            if (dqx <= peekEntry.dqx) {
                useReplaceMax = true;
            }
        }

        TuplePointer outPointer = new TuplePointer();
        if (useReplaceMax) {
            // Heap full and the new tuple is at least as good as the worst in-memory entry. Insert the
            // newcomer FIRST and evict the victim only once it is safely stored — deleting first and then
            // failing the insert would lose BOTH tuples and leave a heap entry dangling at a freed pointer.
            if (bufferManager.insertTuple(singleTupleAccessor, 0, outPointer)) {
                bufferManager.deleteTuple(peekEntry.tuplePointer);
                insertEntry.reset(outPointer, dqx);
                topKHeap.replaceMax(insertEntry);
            } else {
                // Buffer full: spill the worst half of the heap to disk to free space. After spilling the
                // heap is no longer full (and the worst entry may itself have been spilled), so fall back
                // to a plain grow-insert instead of replacing.
                spillWorstHalf();
                if (!bufferManager.insertTuple(singleTupleAccessor, 0, outPointer)) {
                    throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE,
                            singleTupleAccessor.getTupleLength(0), singleTupleFrame.getBuffer().capacity());
                }
                insertEntry.reset(outPointer, dqx);
                topKHeap.insert(insertEntry);
            }
        } else {
            // Heap has room, or new tuple is worse than in-memory peekMax but better than
            // maxSpilledDqx — grow the heap (spill on memory pressure).
            if (!bufferManager.insertTuple(singleTupleAccessor, 0, outPointer)) {
                spillWorstHalf();
                if (!bufferManager.insertTuple(singleTupleAccessor, 0, outPointer)) {
                    throw HyracksDataException.create(ErrorCode.RECORD_IS_TOO_LARGE,
                            singleTupleAccessor.getTupleLength(0), singleTupleFrame.getBuffer().capacity());
                }
            }
            insertEntry.reset(outPointer, dqx);
            topKHeap.insert(insertEntry);
        }
    }

    /**
     * Peek the worst (maximum) dqx in the in-memory heap, or {@link Double#MAX_VALUE} when the heap is
     * empty. The sentinel is chosen so an empty heap always accepts the next candidate (nothing is worse
     * than {@code MAX_VALUE}); callers treat it as "no worst yet", not a real distance.
     */
    public double peekWorstDqx() {
        if (topKHeap == null || topKHeap.getNumEntries() == 0) {
            return Double.MAX_VALUE;
        }
        topKHeap.peekMax(peekEntry);
        return peekEntry.dqx;
    }

    /**
     * Total entries tracked (in-memory + spilled).
     */
    public int getNumEntries() {
        int inMemory = (topKHeap != null) ? topKHeap.getNumEntries() : 0;
        return inMemory + spilledTupleCount;
    }

    /**
     * Spill the worst half of the in-memory heap to a sorted run file.
     * Keeps the best half in memory for continued searching.
     */
    @SuppressWarnings("deprecation")
    private void spillWorstHalf() throws HyracksDataException {
        int n = topKHeap.getNumEntries();
        if (n == 0) {
            return;
        }

        LOGGER.info("[SpillableTopKBuffer] Spilling: {} entries in heap, spilling {}", n, n - n / 2);

        // Sort entries ascending by dqx
        IResetableComparable[] entries = topKHeap.getEntries();
        Arrays.sort(entries, 0, n, Comparator.comparingDouble(e -> ((VectorSearchHeapEntry) e).dqx));

        int keepCount = n / 2;
        int spillCount = n - keepCount;

        // Create run file
        FileReference spillFile = ctx.getJobletContext().createManagedWorkspaceFile(SPILL_FILE_PREFIX);
        RunFileWriter writer = new RunFileWriter(spillFile, ctx.getIoManager());
        writer.open();

        try {
            // Initialize spill output frame lazily
            if (spillOutputFrame == null) {
                spillOutputFrame = new FixedSizeFrame(ByteBuffer.allocate(frameSize));
                spillOutputAppender = new FrameTupleAppender();
            }
            spillOutputAppender.reset(spillOutputFrame, true);

            // Write worst half (indices keepCount..n-1) as dqx-prefixed tuples
            for (int i = keepCount; i < n; i++) {
                VectorSearchHeapEntry entry = (VectorSearchHeapEntry) entries[i];
                bufferAccessor.reset(entry.tuplePointer);

                // Build dqx-prefixed tuple: [dqx_bytes][original_tuple_bytes]
                if (!appendDqxPrefixedTuple(spillOutputAppender, entry.dqx, bufferAccessor)) {
                    // Frame full — flush and retry
                    spillOutputAppender.write(writer, true);
                    spillOutputAppender.reset(spillOutputFrame, true);
                    if (!appendDqxPrefixedTuple(spillOutputAppender, entry.dqx, bufferAccessor)) {
                        throw new HyracksDataException("Single dqx-prefixed tuple too large for spill frame");
                    }
                }
            }
            // Flush remaining
            if (spillOutputAppender.getTupleCount() > 0) {
                spillOutputAppender.write(writer, true);
            }
        } finally {
            writer.close();
        }

        spilledRuns.add(writer.createDeleteOnCloseReader());

        // Track the worst dqx that has ever been spilled. Spilled tuples are immutable
        // (we cannot evict them from the run file), so they form the floor for the
        // early-reject decision in insert().
        double thisSpillMax = ((VectorSearchHeapEntry) entries[n - 1]).dqx;
        if (thisSpillMax > maxSpilledDqx) {
            maxSpilledDqx = thisSpillMax;
        }

        // Delete spilled entries from buffer manager
        for (int i = keepCount; i < n; i++) {
            VectorSearchHeapEntry entry = (VectorSearchHeapEntry) entries[i];
            bufferManager.deleteTuple(entry.tuplePointer);
        }
        spilledTupleCount += spillCount;

        // Rebuild heap with kept entries only
        // Save kept entries' data before resetting
        TuplePointer[] keptPointers = new TuplePointer[keepCount];
        double[] keptDqx = new double[keepCount];
        for (int i = 0; i < keepCount; i++) {
            VectorSearchHeapEntry entry = (VectorSearchHeapEntry) entries[i];
            keptPointers[i] = new TuplePointer(entry.tuplePointer.getFrameIndex(), entry.tuplePointer.getTupleIndex());
            keptDqx[i] = entry.dqx;
        }

        topKHeap.reset();
        for (int i = 0; i < keepCount; i++) {
            insertEntry.reset(keptPointers[i], keptDqx[i]);
            topKHeap.insert(insertEntry);
        }

        LOGGER.info("[SpillableTopKBuffer] Spill complete: kept={}, spilled={} (total spilled: {}), runs={}", keepCount,
                spillCount, spilledTupleCount, spilledRuns.size());
    }

    /**
     * Append a dqx-prefixed tuple to the appender.
     * Format: [8-byte dqx as serialized double][original tuple field bytes]
     */
    private boolean appendDqxPrefixedTuple(FrameTupleAppender appender, double dqx, ITuplePointerAccessor tupleAccessor)
            throws HyracksDataException {
        // Compute total size: dqx field (serialized double) + original fields
        int numOriginalFields = tupleAccessor.getFieldCount();
        int tupleDataSize = 8; // dqx double
        for (int f = 0; f < numOriginalFields; f++) {
            tupleDataSize += tupleAccessor.getFieldLength(f);
        }

        // Build the tuple bytes manually into a temp buffer
        // Frame tuple format: [field end offsets (4 bytes each)][field data bytes]
        int numFields = numOriginalFields + 1;
        int headerSize = numFields * 4; // field end offsets
        int totalTupleSize = headerSize + tupleDataSize;

        byte[] tupleBytes = new byte[totalTupleSize];
        ByteBuffer tb = ByteBuffer.wrap(tupleBytes);

        // Field end offsets (relative to start of field data area)
        int fieldDataOffset = 0;
        // Field 0: dqx (8 bytes)
        fieldDataOffset += 8;
        tb.putInt(fieldDataOffset);
        // Fields 1..N: original fields
        for (int f = 0; f < numOriginalFields; f++) {
            fieldDataOffset += tupleAccessor.getFieldLength(f);
            tb.putInt(fieldDataOffset);
        }

        // Field data: dqx
        tb.putDouble(dqx);
        // Field data: original fields
        byte[] srcData = tupleAccessor.getBuffer().array();
        for (int f = 0; f < numOriginalFields; f++) {
            int fStart = tupleAccessor.getAbsFieldStartOffset(f);
            int fLen = tupleAccessor.getFieldLength(f);
            tb.put(srcData, fStart, fLen);
        }

        // Use a temp single-tuple frame to append via FrameTupleAccessor
        // Actually, we can use the raw append method
        return appender.append(tupleBytes, 0, totalTupleSize);
    }

    /**
     * Drain all results sorted by dqx ascending.
     */
    @SuppressWarnings("deprecation")
    public SpillableTopKDrainIterator drain() throws HyracksDataException {
        if (!initialized || topKHeap == null) {
            return SpillableTopKDrainIterator.empty();
        }

        // Sort in-memory entries ascending by dqx
        int n = topKHeap.getNumEntries();
        IResetableComparable[] entries = topKHeap.getEntries();
        Arrays.sort(entries, 0, n, Comparator.comparingDouble(e -> ((VectorSearchHeapEntry) e).dqx));

        VectorSearchHeapEntry[] sortedInMemory = new VectorSearchHeapEntry[n];
        for (int i = 0; i < n; i++) {
            sortedInMemory[i] = (VectorSearchHeapEntry) entries[i];
        }

        if (spilledRuns.isEmpty()) {
            // Fast path: no spill, just iterate sorted in-memory entries
            return SpillableTopKDrainIterator.inMemoryOnly(sortedInMemory, bufferAccessor);
        }

        // Merge path: open run readers and merge with in-memory entries
        return SpillableTopKDrainIterator.withMerge(sortedInMemory, bufferAccessor, spilledRuns, spillRecordDescriptor,
                ctx, candidateLimit);
    }

    public void close() throws HyracksDataException {
        if (bufferManager != null) {
            bufferManager.close();
            bufferManager = null;
        }
        for (GeneratedRunFileReader reader : spilledRuns) {
            reader.close();
        }
        spilledRuns.clear();
        topKHeap = null;
    }

}
