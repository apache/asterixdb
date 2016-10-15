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
package org.apache.hyracks.dataflow.std.join;

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.structures.RunFilePointer;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The right stream spills to memory and pause when memory is full.
 */
public class MergeJoiner extends AbstractMergeJoiner {

    private static final Logger LOGGER = Logger.getLogger(MergeJoiner.class.getName());

    private final IDeallocatableFramePool framePool;
    private final IDeletableTupleBufferManager bufferManager;
    private final ITuplePointerAccessor memoryAccessor;
    private final LinkedList<TuplePointer> memoryBuffer = new LinkedList<>();

    private int leftStreamIndex;
    private final RunFileStream runFileStreamOld;
    private final RunFileStream runFileStream;
    private ITupleAccessor tmpAccessor;
    private final RunFilePointer runFilePointer;

    private final IMergeJoinChecker mjc;

    private long joinComparisonCount = 0;
    private long joinResultCount = 0;
    private long spillFileCount = 0;
    private long spillWriteCount = 0;
    private long spillReadCount = 0;
    private long spillCount = 0;

    public MergeJoiner(IHyracksTaskContext ctx, int memorySize, int partition, MergeStatus status, MergeJoinLocks locks,
            IMergeJoinChecker mjc, RecordDescriptor leftRd, RecordDescriptor rightRd) throws HyracksDataException {
        super(ctx, partition, status, locks, leftRd, rightRd);
        this.mjc = mjc;
        tmpAccessor = new TupleAccessor(leftRd);
        runFilePointer = new RunFilePointer();

        // Memory (right buffer)
        if (memorySize < 1) {
            throw new HyracksDataException(
                    "MergeJoiner does not have enough memory (needs > 0, got " + memorySize + ").");
        }
        framePool = new DeallocatableFramePool(ctx, (memorySize) * ctx.getInitialFrameSize());
        bufferManager = new VariableDeletableTupleMemoryManager(framePool, rightRd);
        memoryAccessor = bufferManager.createTuplePointerAccessor();

        // Run File and frame cache (left buffer)
        leftStreamIndex = TupleAccessor.UNSET;
        runFileStreamOld = new RunFileStream(ctx, "left", status.branch[LEFT_PARTITION]);
        runFileStream = new RunFileStream(ctx, "left", status.branch[LEFT_PARTITION]);

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "MergeJoiner has started partition " + partition + " with " + memorySize + " frames of memory.");
        }
    }

    private boolean addToMemory(ITupleAccessor accessor) throws HyracksDataException {
        TuplePointer tp = new TuplePointer();
        if (bufferManager.insertTuple(accessor, accessor.getTupleId(), tp)) {
            memoryBuffer.add(tp);
            return true;
        }
        return false;
    }

    private void removeFromMemory(TuplePointer tp) throws HyracksDataException {
        memoryBuffer.remove(tp);
        bufferManager.deleteTuple(tp);
    }

    private void addToResult(IFrameTupleAccessor accessorLeft, int leftTupleIndex, IFrameTupleAccessor accessorRight,
            int rightTupleIndex, IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessorLeft, leftTupleIndex, accessorRight,
                rightTupleIndex);
        joinResultCount++;
    }

    private void flushMemory() throws HyracksDataException {
        memoryBuffer.clear();
        bufferManager.reset();
    }

    // memory management
    private boolean memoryHasTuples() {
        return bufferManager.getNumTuples() > 0;
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private TupleStatus loadRightTuple() throws HyracksDataException {
        TupleStatus loaded = loadMemoryTuple(RIGHT_PARTITION);
        if (loaded == TupleStatus.UNKNOWN) {
            loaded = pauseAndLoadRightTuple();
        }
        return loaded;
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private TupleStatus loadLeftTuple() throws HyracksDataException {
        TupleStatus loaded;
        if (status.branch[LEFT_PARTITION].isRunFileReading()) {
            loaded = loadSpilledTuple(LEFT_PARTITION);
            if (loaded.isEmpty()) {
                if (status.branch[LEFT_PARTITION].isRunFileWriting() && !status.branch[LEFT_PARTITION].hasMore()) {
                    unfreezeAndContinue(inputAccessor[LEFT_PARTITION]);
                } else {
                    continueStream(inputAccessor[LEFT_PARTITION]);
                }
                loaded = loadLeftTuple();
            }
        } else {
            loaded = loadMemoryTuple(LEFT_PARTITION);
        }
        return loaded;
    }

    private TupleStatus loadSpilledTuple(int partition) throws HyracksDataException {
        if (!inputAccessor[partition].exists()) {
            runFileStream.loadNextBuffer(tmpAccessor);
            if (!runFileStreamOld.loadNextBuffer(inputAccessor[partition])) {
                return TupleStatus.EMPTY;
            }
        }
        return TupleStatus.LOADED;
    }

    /**
     * Left
     *
     * @throws HyracksDataException
     */
    @Override
    public void processLeftFrame(IFrameWriter writer) throws HyracksDataException {
        TupleStatus leftTs = loadLeftTuple();
        TupleStatus rightTs = loadRightTuple();
        while (leftTs.isLoaded() && (status.branch[RIGHT_PARTITION].hasMore() || memoryHasTuples())) {
            if (status.branch[LEFT_PARTITION].isRunFileWriting()) {
                // Left side from disk
                leftTs = processLeftTupleSpill(writer);
            } else if (rightTs.isLoaded()
                    && mjc.checkToLoadNextRightTuple(inputAccessor[LEFT_PARTITION], inputAccessor[RIGHT_PARTITION])) {
                // Right side from stream
                processRightTuple();
                rightTs = loadRightTuple();
            } else {
                // Left side from stream
                processLeftTuple(writer);
                leftTs = loadLeftTuple();
            }
        }
    }

    @Override
    public void processLeftClose(IFrameWriter writer) throws HyracksDataException {
        if (status.branch[LEFT_PARTITION].isRunFileWriting()) {
            unfreezeAndContinue(inputAccessor[LEFT_PARTITION]);
        }
        processLeftFrame(writer);
        resultAppender.write(writer, true);
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("MergeJoiner statitics: " + joinComparisonCount + " comparisons, " + joinResultCount
                    + " results, " + spillCount + " spills, " + runFileStreamOld.getFileCount() + " files, "
                    + runFileStreamOld.getWriteCount() + " spill frames written, " + runFileStreamOld.getReadCount()
                    + " spill frames read.");
        }
    }

    private TupleStatus processLeftTupleSpill(IFrameWriter writer) throws HyracksDataException {
        //        System.err.print("Spill ");

        runFileStreamOld.addToRunFile(inputAccessor[LEFT_PARTITION]);
        if (true) {
            runFileStream.addToRunFile(inputAccessor[LEFT_PARTITION]);
        }

        processLeftTuple(writer);

        // Memory is empty and we can start processing the run file.
        if (!memoryHasTuples() && status.branch[LEFT_PARTITION].isRunFileWriting()) {
            unfreezeAndContinue(inputAccessor[LEFT_PARTITION]);
        }
        return loadLeftTuple();
    }

    private void processLeftTuple(IFrameWriter writer) throws HyracksDataException {
        //        TuplePrinterUtil.printTuple("Left", inputAccessor[LEFT]);
        // Check against memory (right)
        if (memoryHasTuples()) {
            for (int i = memoryBuffer.size() - 1; i > -1; --i) {
                memoryAccessor.reset(memoryBuffer.get(i));
                if (mjc.checkToSaveInResult(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                        memoryAccessor, memoryBuffer.get(i).getTupleIndex(), false)) {
                    // add to result
                    addToResult(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                            memoryAccessor, memoryBuffer.get(i).getTupleIndex(), writer);
                }
                joinComparisonCount++;
                if (mjc.checkToRemoveInMemory(inputAccessor[LEFT_PARTITION], inputAccessor[LEFT_PARTITION].getTupleId(),
                        memoryAccessor, memoryBuffer.get(i).getTupleIndex())) {
                    // remove from memory
                    //                    TuplePrinterUtil.printTuple("Remove Memory", memoryAccessor, memoryBuffer.get(i).getTupleIndex());
                    removeFromMemory(memoryBuffer.get(i));
                }
            }
        }
        inputAccessor[LEFT_PARTITION].next();
    }

    private void processRightTuple() throws HyracksDataException {
        // append to memory
        if (mjc.checkToSaveInMemory(inputAccessor[LEFT_PARTITION], inputAccessor[RIGHT_PARTITION])) {
            if (!addToMemory(inputAccessor[RIGHT_PARTITION])) {
                // go to log saving state
                freezeAndSpill();
                return;
            }
        }
        //        TuplePrinterUtil.printTuple("Memory", inputAccessor[RIGHT]);
        inputAccessor[RIGHT_PARTITION].next();
    }

    private void freezeAndSpill() throws HyracksDataException {
        //        System.err.println("freezeAndSpill");
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("freeze snapshot: " + frameCounts[RIGHT_PARTITION] + " right, " + frameCounts[LEFT_PARTITION]
                    + " left, " + joinComparisonCount + " comparisons, " + joinResultCount + " results, ["
                    + bufferManager.getNumTuples() + " tuples memory].");
        }

        if (runFilePointer.getFileOffset() > 0) {

        } else {
            runFilePointer.reset(0, 0);
            runFileStream.startRunFileWriting();
        }
        runFileStreamOld.startRunFileWriting();
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "Memory is full. Freezing the right branch. (memory tuples: " + bufferManager.getNumTuples() + ")");
        }
        spillCount++;
    }

    private void continueStream(ITupleAccessor accessor) throws HyracksDataException {
        //        System.err.println("continueStream");

        runFileStreamOld.closeRunFileReading();
        accessor.reset(inputBuffer[LEFT_PARTITION]);
        accessor.setTupleId(leftStreamIndex);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Continue with left stream.");
        }
    }

    private void unfreezeAndContinue(ITupleAccessor accessor) throws HyracksDataException {
        //        System.err.println("unfreezeAndContinue");
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("snapshot: " + frameCounts[RIGHT_PARTITION] + " right, " + frameCounts[LEFT_PARTITION]
                    + " left, " + joinComparisonCount + " comparisons, " + joinResultCount + " results, ["
                    + bufferManager.getNumTuples() + " tuples memory, " + spillCount + " spills, "
                    + (runFileStreamOld.getFileCount() - spillFileCount) + " files, "
                    + (runFileStreamOld.getWriteCount() - spillWriteCount) + " written, "
                    + (runFileStreamOld.getReadCount() - spillReadCount) + " read].");
            spillFileCount = runFileStreamOld.getFileCount();
            spillReadCount = runFileStreamOld.getReadCount();
            spillWriteCount = runFileStreamOld.getWriteCount();
        }

        runFileStreamOld.flushAndStopRunFile(accessor);
        runFileStream.flushAndStopRunFile(accessor);
        flushMemory();
        if (!status.branch[LEFT_PARTITION].isRunFileReading()) {
            leftStreamIndex = accessor.getTupleId();
        }
        runFileStreamOld.startReadingRunFile(accessor);

        runFileStream.resetReadPointer(runFilePointer.getFileOffset());
        accessor.setTupleId(runFilePointer.getTupleIndex());
        runFileStream.startReadingRunFile(accessor);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Unfreezing right partition.");
        }

    }

}
