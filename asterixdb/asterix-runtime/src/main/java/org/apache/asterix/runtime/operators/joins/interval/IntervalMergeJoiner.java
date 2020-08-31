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
package org.apache.asterix.runtime.operators.joins.interval;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.FrameTupleCursor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.IntervalSideTuple;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFilePointer;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.TuplePointerCursor;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The right stream spills to memory and pause when memory is full.
 */
public class IntervalMergeJoiner {

    private final IDeallocatableFramePool framePool;
    private final IDeletableTupleBufferManager bufferManager;
    private final TuplePointerCursor memoryCursor;
    private final LinkedList<TuplePointer> memoryBuffer = new LinkedList<>();

    private final RunFileStream runFileStream;
    private final RunFilePointer runFilePointer;

    private IntervalSideTuple memoryTuple;
    private IntervalSideTuple[] inputTuple;

    private final IIntervalJoinUtil mjc;

    protected static final int JOIN_PARTITIONS = 2;
    protected static final int BUILD_PARTITION = 0;
    protected static final int PROBE_PARTITION = 1;

    protected final IFrame[] inputBuffer;
    protected final FrameTupleAppender resultAppender;
    protected final FrameTupleCursor[] inputCursor;

    public IntervalMergeJoiner(IHyracksTaskContext ctx, int memorySize, IIntervalJoinUtil mjc, int buildKeys,
            int probeKeys, RecordDescriptor buildRd, RecordDescriptor probeRd) throws HyracksDataException {
        this.mjc = mjc;

        // Memory (probe buffer)
        if (memorySize < 5) {
            throw new RuntimeException(
                    "IntervalMergeJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }

        inputCursor = new FrameTupleCursor[JOIN_PARTITIONS];
        inputCursor[BUILD_PARTITION] = new FrameTupleCursor(buildRd);
        inputCursor[PROBE_PARTITION] = new FrameTupleCursor(probeRd);

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[BUILD_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[PROBE_PARTITION] = new VSizeFrame(ctx);

        //Two frames are used for the runfile stream, and one frame for each input (2 outputs).
        framePool = new DeallocatableFramePool(ctx, (memorySize - 4) * ctx.getInitialFrameSize());
        bufferManager = new VariableDeletableTupleMemoryManager(framePool, probeRd);
        memoryCursor = new TuplePointerCursor(bufferManager.createTuplePointerAccessor());

        // Run File and frame cache (build buffer)
        runFileStream = new RunFileStream(ctx, "imj-build");
        runFilePointer = new RunFilePointer();
        runFileStream.createRunFileWriting();
        runFileStream.startRunFileWriting();

        memoryTuple = new IntervalSideTuple(mjc, memoryCursor, probeKeys);

        inputTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        inputTuple[PROBE_PARTITION] = new IntervalSideTuple(mjc, inputCursor[PROBE_PARTITION], probeKeys);
        inputTuple[BUILD_PARTITION] = new IntervalSideTuple(mjc, inputCursor[BUILD_PARTITION], buildKeys);

        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

    public void processBuildFrame(ByteBuffer buffer) throws HyracksDataException {
        inputCursor[BUILD_PARTITION].reset(buffer);
        for (int x = 0; x < inputCursor[BUILD_PARTITION].getAccessor().getTupleCount(); x++) {
            runFileStream.addToRunFile(inputCursor[BUILD_PARTITION].getAccessor(), x);
        }
    }

    public void processBuildClose() throws HyracksDataException {
        runFileStream.flushRunFile();
        runFileStream.startReadingRunFile(inputCursor[BUILD_PARTITION]);
    }

    public void processProbeFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        inputCursor[PROBE_PARTITION].reset(buffer);
        while (buildHasNext() && inputCursor[PROBE_PARTITION].hasNext()) {
            if (inputCursor[PROBE_PARTITION].hasNext() && mjc.checkToLoadNextProbeTuple(
                    inputCursor[BUILD_PARTITION].getAccessor(), inputCursor[BUILD_PARTITION].getTupleId() + 1,
                    inputCursor[PROBE_PARTITION].getAccessor(), inputCursor[PROBE_PARTITION].getTupleId() + 1)) {
                // Process probe side from stream
                inputCursor[PROBE_PARTITION].next();
                processProbeTuple(writer);
            } else {
                // Process build side from runfile
                inputCursor[BUILD_PARTITION].next();
                processBuildTuple(writer);
            }
        }
    }

    public void processProbeClose(IFrameWriter writer) throws HyracksDataException {
        while (buildHasNext() && memoryHasTuples()) {
            // Process build side from runfile
            inputCursor[BUILD_PARTITION].next();
            processBuildTuple(writer);
        }
        resultAppender.write(writer, true);
        runFileStream.close();
        runFileStream.removeRunFile();
    }

    private boolean buildHasNext() throws HyracksDataException {
        if (!inputCursor[BUILD_PARTITION].hasNext()) {
            // Must keep condition in a separate `if` due to actions applied in loadNextBuffer.
            return runFileStream.loadNextBuffer(inputCursor[BUILD_PARTITION]);
        } else {
            return true;
        }
    }

    private void processBuildTuple(IFrameWriter writer) throws HyracksDataException {
        // Check against memory
        if (memoryHasTuples()) {
            inputTuple[BUILD_PARTITION].loadTuple();
            memoryCursor.reset(memoryBuffer.iterator());
            while (memoryCursor.hasNext()) {
                memoryCursor.next();
                memoryTuple.loadTuple();
                if (inputTuple[BUILD_PARTITION].removeFromMemory(memoryTuple)) {
                    // remove from memory
                    bufferManager.deleteTuple(memoryCursor.getTuplePointer());
                    memoryCursor.remove();
                    continue;
                } else if (inputTuple[BUILD_PARTITION].checkForEarlyExit(memoryTuple)) {
                    // No more possible comparisons
                    break;
                } else if (inputTuple[BUILD_PARTITION].compareJoin(memoryTuple)) {
                    // add to result
                    addToResult(inputCursor[BUILD_PARTITION].getAccessor(), inputCursor[BUILD_PARTITION].getTupleId(),
                            memoryCursor.getAccessor(), memoryCursor.getTupleId(), writer);
                }
            }
        }
    }

    private void processProbeTuple(IFrameWriter writer) throws HyracksDataException {
        // append to memory
        // BUILD Cursor is guaranteed to have next
        if (mjc.checkToSaveInMemory(inputCursor[BUILD_PARTITION].getAccessor(),
                inputCursor[BUILD_PARTITION].getTupleId() + 1, inputCursor[PROBE_PARTITION].getAccessor(),
                inputCursor[PROBE_PARTITION].getTupleId())) {
            if (!addToMemory(inputCursor[PROBE_PARTITION].getAccessor(), inputCursor[PROBE_PARTITION].getTupleId())) {
                unfreezeAndClearMemory(writer);
                if (!addToMemory(inputCursor[PROBE_PARTITION].getAccessor(),
                        inputCursor[PROBE_PARTITION].getTupleId())) {
                    throw new RuntimeException("Should Never get called.");
                }
            }
        }
    }

    private void unfreezeAndClearMemory(IFrameWriter writer) throws HyracksDataException {
        runFilePointer.reset(runFileStream.getReadPointer(), inputCursor[BUILD_PARTITION].getTupleId());
        while (buildHasNext() && memoryHasTuples()) {
            // Process build side from runfile
            inputCursor[BUILD_PARTITION].next();
            processBuildTuple(writer);
        }
        // Clear memory
        memoryBuffer.clear();
        bufferManager.reset();
        // Start reading
        runFileStream.startReadingRunFile(inputCursor[BUILD_PARTITION], runFilePointer.getFileOffset());
        inputCursor[BUILD_PARTITION].resetPosition(runFilePointer.getTupleIndex());
    }

    private boolean addToMemory(IFrameTupleAccessor accessor, int tupleId) throws HyracksDataException {
        TuplePointer tp = new TuplePointer();
        if (bufferManager.insertTuple(accessor, tupleId, tp)) {
            memoryBuffer.add(tp);
            return true;
        }
        return false;
    }

    private void addToResult(IFrameTupleAccessor buildAccessor, int buildTupleId, IFrameTupleAccessor probeAccessor,
            int probeTupleId, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, buildAccessor, buildTupleId, probeAccessor,
                probeTupleId);
    }

    private boolean memoryHasTuples() {
        return bufferManager.getNumTuples() > 0;
    }
}
