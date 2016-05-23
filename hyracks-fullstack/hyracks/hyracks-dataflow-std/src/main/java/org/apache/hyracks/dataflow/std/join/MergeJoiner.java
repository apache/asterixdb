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

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.join.MergeStatus.BranchStatus;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The right stream spills to memory and pause when memory is full.
 */
public class MergeJoiner implements IMergeJoiner {

    private final ITupleAccessor accessorLeft;
    private final ITupleAccessor accessorRight;

    private MergeJoinLocks locks;
    private MergeStatus status;

    private ByteBuffer leftBuffer;
    private ByteBuffer rightBuffer;

    private final TuplePointer tp;
    private final IDeallocatableFramePool framePool;
    private IDeletableTupleBufferManager bufferManager;
    private ITupleAccessor memoryAccessor;

    private int leftStreamIndex;
    private RunFileStream runFileStream;

    private final FrameTupleAppender resultAppender;

    private final IMergeJoinChecker mjc;

    private final int partition;

    private static final Logger LOGGER = Logger.getLogger(MergeJoiner.class.getName());

    public MergeJoiner(IHyracksTaskContext ctx, int memorySize, int partition, MergeStatus status, MergeJoinLocks locks,
            IMergeJoinChecker mjc, RecordDescriptor leftRd, RecordDescriptor rightRd) throws HyracksDataException {
        this.partition = partition;
        this.status = status;
        this.locks = locks;
        this.mjc = mjc;

        accessorLeft = new TupleAccessor(leftRd);
        leftBuffer = ctx.allocateFrame();

        accessorRight = new TupleAccessor(rightRd);
        rightBuffer = ctx.allocateFrame();

        // Memory (right buffer)
        if (memorySize < 1) {
            throw new HyracksDataException(
                    "MergeJoiner does not have enough memory (needs > 0, got " + memorySize + ").");
        }
        framePool = new DeallocatableFramePool(ctx, (memorySize) * ctx.getInitialFrameSize());
        tp = new TuplePointer();
        bufferManager = new VariableDeletableTupleMemoryManager(framePool, rightRd);
        memoryAccessor = bufferManager.createTupleAccessor();

        // Run File and frame cache (left buffer)
        leftStreamIndex = TupleAccessor.UNSET;
        runFileStream = new RunFileStream(ctx, "left", status);

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(
                    "MergeJoiner has started partition " + partition + " with " + memorySize + " frames of memory.");
        }
    }

    private boolean addToMemory(ITupleAccessor accessor) throws HyracksDataException {
        if (bufferManager.insertTuple(accessor, accessor.getTupleId(), tp)) {
            return true;
        }
        return false;
    }

    private void removeFromMemory() throws HyracksDataException {
        memoryAccessor.getTuplePointer(tp);
        bufferManager.deleteTuple(tp);
    }

    private void addToResult(ITupleAccessor accessor1, ITupleAccessor accessor2, IFrameWriter writer)
            throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, accessor1.getTupleId(), accessor2,
                accessor2.getTupleId());
    }

    @Override
    public void closeResult(IFrameWriter writer) throws HyracksDataException {
        resultAppender.write(writer, true);
    }

    private void flushMemory() throws HyracksDataException {
        bufferManager.reset();
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private boolean loadRightTuple() throws HyracksDataException {
        boolean loaded = true;
        if (accessorRight != null && accessorRight.exists()) {
            // Still processing frame.
        } else if (status.rightHasMore) {
            status.continueRightLoad = true;
            locks.getRight(partition).signal();
            try {
                while (status.continueRightLoad
                        && status.getRightStatus().isEqualOrBefore(BranchStatus.DATA_PROCESSING)) {
                    locks.getLeft(partition).await();
                }
            } catch (InterruptedException e) {
                throw new HyracksDataException(
                        "SortMergeIntervalJoin interrupted exception while attempting to load right tuple", e);
            }
            if (!accessorRight.exists() && status.getRightStatus() == BranchStatus.CLOSED) {
                status.rightHasMore = false;
                loaded = false;
            }
        } else {
            // No more frames or tuples to process.
            loaded = false;
        }
        return loaded;
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private boolean loadLeftTuple() throws HyracksDataException {
        boolean loaded = true;
        if (status.isRunFileReading()) {
            if (!accessorLeft.exists()) {
                if (!runFileStream.loadNextBuffer(accessorLeft)) {
                    if (memoryHasTuples()) {
                        // More tuples from the right need to be processed. Clear memory and replay the run file.
                        runFileStream.flushAndStopRunFile(accessorLeft);
                        flushMemory();
                        runFileStream.resetReader(accessorLeft);
                    } else {
                        // Memory is empty and replay is complete.
                        runFileStream.closeRunFile();
                        accessorLeft.reset(leftBuffer);
                        accessorLeft.setTupleId(leftStreamIndex);
                    }
                    return loadLeftTuple();
                }
            }
        } else {
            if (!status.leftHasMore && status.isRunFileWriting()) {
                // Finished left stream. Start the replay.
                runFileStream.flushAndStopRunFile(accessorLeft);
                flushMemory();
                leftStreamIndex = accessorLeft.getTupleId();
                runFileStream.openRunFile(accessorLeft);
            } else if (!status.leftHasMore || !accessorLeft.exists()) {
                loaded = false;
            }
        }
        return loaded;
    }

    // memory management
    private boolean memoryHasTuples() {
        return bufferManager.getNumTuples() > 0;
    }

    /**
     * Left
     *
     * @throws HyracksDataException
     */
    @Override
    public void processMergeUsingLeftTuple(IFrameWriter writer) throws HyracksDataException {
        while (loadLeftTuple() && (status.rightHasMore || memoryHasTuples())) {
            if (loadRightTuple() && !status.isRunFileWriting()
                    && mjc.checkToLoadNextRightTuple(accessorLeft, accessorRight)) {
                // *********************
                // Right side from stream
                // *********************
                // append to memory
                if (mjc.checkToSaveInMemory(accessorLeft, accessorRight)) {
                    if (!addToMemory(accessorRight)) {
                        // go to log saving state
                        runFileStream.startRunFile();
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.fine("MergeJoiner memory is full with " + bufferManager.getNumTuples() + " tuples.");
                        }
                        continue;
                    }
                }
                accessorRight.next();
            } else {
                // *********************
                // Left side from stream or disk
                // *********************
                // Write left tuple to run file
                if (status.isRunFileWriting()) {
                    // TODO Add to the run file only it can match with future right tuples.
                    runFileStream.addToRunFile(accessorLeft);
                }

                // Check against memory (right)
                if (memoryHasTuples()) {
                    memoryAccessor.reset();
                    memoryAccessor.next();
                    while (memoryAccessor.exists()) {
                        if (mjc.checkToSaveInResult(accessorLeft, memoryAccessor)) {
                            // add to result
                            addToResult(accessorLeft, memoryAccessor, writer);
                        }
                        if (mjc.checkToRemoveInMemory(accessorLeft, memoryAccessor)) {
                            // remove from memory
                            removeFromMemory();
                        }
                        memoryAccessor.next();
                    }
                }
                accessorLeft.next();

                // Memory is empty and we can start processing the run file.
                if (!memoryHasTuples() && status.isRunFileWriting()) {
                    runFileStream.flushAndStopRunFile(accessorLeft);
                    flushMemory();
                    leftStreamIndex = accessorLeft.getTupleId();
                    runFileStream.openRunFile(accessorLeft);
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine(
                                "MergeJoiner memory is new empty. Replaying left branch while continuing with the right branch.");
                    }
                }
            }
        }
    }

    @Override
    public void setLeftFrame(ByteBuffer buffer) {
        leftBuffer.clear();
        if (leftBuffer.capacity() < buffer.capacity()) {
            leftBuffer.limit(buffer.capacity());
        }
        leftBuffer.put(buffer.array(), 0, buffer.capacity());
        accessorLeft.reset(leftBuffer);
        accessorLeft.next();
    }

    @Override
    public void setRightFrame(ByteBuffer buffer) {
        rightBuffer.clear();
        if (rightBuffer.capacity() < buffer.capacity()) {
            rightBuffer.limit(buffer.capacity());
        }
        rightBuffer.put(buffer.array(), 0, buffer.capacity());
        accessorRight.reset(rightBuffer);
        accessorRight.next();
        status.continueRightLoad = false;
    }

}
