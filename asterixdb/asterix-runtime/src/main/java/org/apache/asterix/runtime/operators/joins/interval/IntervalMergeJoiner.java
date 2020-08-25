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
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.ITupleAccessor;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.IntervalSideTuple;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.IntervalVariableDeletableTupleMemoryManager;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFilePointer;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.RunFileStream;
import org.apache.asterix.runtime.operators.joins.interval.utils.memory.TupleAccessor;
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
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The right stream spills to memory and pause when memory is full.
 */
public class IntervalMergeJoiner {

    public enum TupleStatus {
        LOADED,
        EMPTY;

        public boolean isLoaded() {
            return this.equals(LOADED);
        }

        public boolean isEmpty() {
            return this.equals(EMPTY);
        }
    }

    private final IDeallocatableFramePool framePool;
    private final IDeletableTupleBufferManager bufferManager;
    private final ITupleAccessor memoryAccessor;
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
    protected final ITupleAccessor[] inputAccessor;

    public IntervalMergeJoiner(IHyracksTaskContext ctx, int memorySize, IIntervalJoinUtil mjc, int buildKeys,
            int probeKeys, RecordDescriptor buildRd, RecordDescriptor probeRd) throws HyracksDataException {
        this.mjc = mjc;

        // Memory (probe buffer)
        if (memorySize < 5) {
            throw new HyracksDataException(
                    "MergeJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }

        inputAccessor = new TupleAccessor[JOIN_PARTITIONS];
        inputAccessor[BUILD_PARTITION] = new TupleAccessor(buildRd);
        inputAccessor[PROBE_PARTITION] = new TupleAccessor(probeRd);

        inputBuffer = new IFrame[JOIN_PARTITIONS];
        inputBuffer[BUILD_PARTITION] = new VSizeFrame(ctx);
        inputBuffer[PROBE_PARTITION] = new VSizeFrame(ctx);

        //Two frames are used for the runfile stream, and one frame for each input (2 outputs).
        framePool = new DeallocatableFramePool(ctx, (memorySize - 4) * ctx.getInitialFrameSize());
        bufferManager = new IntervalVariableDeletableTupleMemoryManager(framePool, probeRd);
        memoryAccessor = ((IntervalVariableDeletableTupleMemoryManager) bufferManager).createTupleAccessor();

        // Run File and frame cache (build buffer)
        runFileStream = new RunFileStream(ctx, "ismj-left");
        runFilePointer = new RunFilePointer();
        runFileStream.createRunFileWriting();
        runFileStream.startRunFileWriting();

        memoryTuple = new IntervalSideTuple(mjc, memoryAccessor, probeKeys);

        inputTuple = new IntervalSideTuple[JOIN_PARTITIONS];
        inputTuple[PROBE_PARTITION] = new IntervalSideTuple(mjc, inputAccessor[PROBE_PARTITION], probeKeys);
        inputTuple[BUILD_PARTITION] = new IntervalSideTuple(mjc, inputAccessor[BUILD_PARTITION], buildKeys);

        // Result
        this.resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

    public void processBuildFrame(ByteBuffer buffer) throws HyracksDataException {
        inputAccessor[BUILD_PARTITION].reset(buffer);
        for (int x = 0; x < inputAccessor[BUILD_PARTITION].getTupleCount(); x++) {
            runFileStream.addToRunFile(inputAccessor[BUILD_PARTITION], x);
        }
    }

    public void processBuildClose() throws HyracksDataException {
        runFileStream.flushRunFile();
        runFileStream.startReadingRunFile(inputAccessor[BUILD_PARTITION]);
    }

    public void processProbeFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        inputAccessor[PROBE_PARTITION].reset(buffer);
        inputAccessor[PROBE_PARTITION].next();

        TupleStatus buildTs = loadBuildTuple();
        TupleStatus probeTs = loadProbeTuple();
        while (buildTs.isLoaded() && probeTs.isLoaded()) {
            if (probeTs.isLoaded() && mjc.checkToLoadNextProbeTuple(inputAccessor[BUILD_PARTITION],
                    inputAccessor[BUILD_PARTITION].getTupleId(), inputAccessor[PROBE_PARTITION],
                    inputAccessor[PROBE_PARTITION].getTupleId())) {
                // Right side from stream
                processProbeTuple(writer);
                probeTs = loadProbeTuple();
            } else {
                // Left side from stream
                processBuildTuple(writer);
                buildTs = loadBuildTuple();
            }
        }
    }

    public void processProbeClose(IFrameWriter writer) throws HyracksDataException {

        TupleStatus buildTs = loadBuildTuple();
        while (buildTs.isLoaded() && memoryHasTuples()) {
            // Left side from stream
            processBuildTuple(writer);
            buildTs = loadBuildTuple();
        }

        resultAppender.write(writer, true);
        runFileStream.close();
        runFileStream.removeRunFile();
    }

    private TupleStatus loadProbeTuple() {
        TupleStatus loaded;
        if (inputAccessor[PROBE_PARTITION] != null && inputAccessor[PROBE_PARTITION].exists()) {
            // Still processing frame.
            loaded = TupleStatus.LOADED;
        } else {
            // No more frames or tuples to process.
            loaded = TupleStatus.EMPTY;
        }
        return loaded;
    }

    private TupleStatus loadBuildTuple() throws HyracksDataException {
        if (!inputAccessor[BUILD_PARTITION].exists()) {
            // Must keep condition in a separate if due to actions applied in loadNextBuffer.
            if (!runFileStream.loadNextBuffer(inputAccessor[BUILD_PARTITION])) {
                return TupleStatus.EMPTY;
            }
        }
        return TupleStatus.LOADED;
    }

    private void processBuildTuple(IFrameWriter writer) throws HyracksDataException {
        // Check against memory
        if (memoryHasTuples()) {
            inputTuple[BUILD_PARTITION].loadTuple();
            Iterator<TuplePointer> memoryIterator = memoryBuffer.iterator();
            while (memoryIterator.hasNext()) {
                TuplePointer tp = memoryIterator.next();
                memoryTuple.setTuple(tp);
                if (inputTuple[BUILD_PARTITION].removeFromMemory(memoryTuple)) {
                    // remove from memory
                    bufferManager.deleteTuple(tp);
                    memoryIterator.remove();
                    continue;
                } else if (inputTuple[BUILD_PARTITION].checkForEarlyExit(memoryTuple)) {
                    // No more possible comparisons
                    break;
                } else if (inputTuple[BUILD_PARTITION].compareJoin(memoryTuple)) {
                    // add to result
                    addToResult(inputAccessor[BUILD_PARTITION], inputAccessor[BUILD_PARTITION].getTupleId(),
                            memoryAccessor, tp.getTupleIndex(), writer);
                }
            }
        }
        inputAccessor[BUILD_PARTITION].next();
    }

    private void processProbeTuple(IFrameWriter writer) throws HyracksDataException {
        // append to memory
        if (mjc.checkToSaveInMemory(inputAccessor[BUILD_PARTITION], inputAccessor[BUILD_PARTITION].getTupleId(),
                inputAccessor[PROBE_PARTITION], inputAccessor[PROBE_PARTITION].getTupleId())) {
            if (!addToMemory(inputAccessor[PROBE_PARTITION])) {
                unfreezeAndClearMemory(writer, inputAccessor[BUILD_PARTITION]);
                return;
            }
        }
        inputAccessor[PROBE_PARTITION].next();
    }

    private void unfreezeAndClearMemory(IFrameWriter writer, ITupleAccessor accessor) throws HyracksDataException {
        runFilePointer.reset(runFileStream.getReadPointer(), inputAccessor[BUILD_PARTITION].getTupleId());
        TupleStatus buildTs = loadBuildTuple();
        while (buildTs.isLoaded() && memoryHasTuples()) {
            // Left side from stream
            processBuildTuple(writer);
            buildTs = loadBuildTuple();
        }
        // Finish writing
        runFileStream.flushRunFile();
        // Clear memory
        memoryBuffer.clear();
        bufferManager.reset();
        // Start reading
        runFileStream.startReadingRunFile(accessor, runFilePointer.getFileOffset());
        accessor.setTupleId(runFilePointer.getTupleIndex());
        runFilePointer.reset(-1, -1);
    }

    private boolean addToMemory(ITupleAccessor accessor) throws HyracksDataException {
        TuplePointer tp = new TuplePointer();
        if (bufferManager.insertTuple(accessor, accessor.getTupleId(), tp)) {
            memoryBuffer.add(tp);
            return true;
        }
        return false;
    }

    private void addToResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessorLeft, leftTupleIndex, accessorRight,
                rightTupleIndex);
    }

    private boolean memoryHasTuples() {
        return bufferManager.getNumTuples() > 0;
    }
}
