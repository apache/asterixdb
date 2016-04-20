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
package org.apache.asterix.runtime.operators.joins.intervalindex;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.asterix.runtime.operators.joins.intervalindex.MergeBranchStatus.Stage;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.ITuplePointerAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.join.IMergeJoiner;
import org.apache.hyracks.dataflow.std.join.MergeJoinLocks;
import org.apache.hyracks.dataflow.std.join.RunFileStream;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Interval Index Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The both right and left use memory to maintain active intervals for the join.
 */
public class IntervalIndexJoiner implements IMergeJoiner {

    private static final Logger LOGGER = Logger.getLogger(IntervalIndexJoiner.class.getName());

    private static final int JOIN_PARTITIONS = 2;
    private static final int LEFT_PARTITION = 0;
    private static final int RIGHT_PARTITION = 1;

    FrameTupleAppender resultAppender;

    private IPartitionedDeletableTupleBufferManager bufferManager;

    ActiveSweepManager activeManager[];

    LinkedList<TuplePointer> buffer = new LinkedList<TuplePointer>();

    ITuplePointerAccessor leftMemoryAccessor;
    ITuplePointerAccessor rightMemoryAccessor;

    Comparator<EndPointIndexItem> endPointComparator;
    IIntervalMergeJoinChecker imjc;

    protected byte point;

    private final ITupleAccessor leftInputAccessor;
    private final ITupleAccessor rightInputAccessor;

    private MergeJoinLocks locks;
    private MergeStatus status;

    private ByteBuffer leftBuffer;
    private ByteBuffer rightBuffer;

    private final TuplePointer tp;

    private RunFileStream runFileStream[];

    private final int partition;

    private int leftKey;
    private int rightKey;

    public IntervalIndexJoiner(IHyracksTaskContext ctx, int memorySize, int partition, MergeStatus status,
            MergeJoinLocks locks, Comparator<EndPointIndexItem> endPointComparator,
            IIntervalMergeJoinCheckerFactory imjcf, int[] leftKeys, int[] rightKeys, RecordDescriptor leftRd,
            RecordDescriptor rightRd) throws HyracksDataException {
        this.point = imjcf.isOrderAsc() ? EndPointIndexItem.START_POINT : EndPointIndexItem.END_POINT;

        this.imjc = imjcf.createMergeJoinChecker(leftKeys, rightKeys, partition);

        this.leftKey = leftKeys[0];
        this.rightKey = rightKeys[0];

        this.partition = partition;
        this.status = status;
        this.locks = locks;

        leftInputAccessor = new TupleAccessor(leftRd);
        leftBuffer = ctx.allocateFrame();

        rightInputAccessor = new TupleAccessor(rightRd);
        rightBuffer = ctx.allocateFrame();

        RecordDescriptor[] recordDescriptors = new RecordDescriptor[JOIN_PARTITIONS];
        recordDescriptors[LEFT_PARTITION] = leftRd;
        recordDescriptors[RIGHT_PARTITION] = rightRd;

        if (memorySize < 5) {
            throw new HyracksDataException(
                    "IntervalIndexJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }
        bufferManager = new VPartitionDeletableTupleBufferManager(ctx,
                VPartitionDeletableTupleBufferManager.NO_CONSTRAIN, JOIN_PARTITIONS,
                (memorySize - 4) * ctx.getInitialFrameSize(), recordDescriptors);
        this.leftMemoryAccessor = bufferManager.getTuplePointerAccessor(leftRd);
        this.rightMemoryAccessor = bufferManager.getTuplePointerAccessor(rightRd);

        tp = new TuplePointer();

        activeManager = new ActiveSweepManager[JOIN_PARTITIONS];
        activeManager[LEFT_PARTITION] = new ActiveSweepManager(bufferManager, leftKey, LEFT_PARTITION,
                endPointComparator);
        activeManager[RIGHT_PARTITION] = new ActiveSweepManager(bufferManager, rightKey, RIGHT_PARTITION,
                endPointComparator);

        // Run files for both branches
        runFileStream = new RunFileStream[JOIN_PARTITIONS];
        runFileStream[LEFT_PARTITION] = new RunFileStream(ctx, "left", status.left);
        runFileStream[RIGHT_PARTITION] = new RunFileStream(ctx, "right", status.right);

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("IntervalIndexJoiner has started partition " + partition + " with " + memorySize
                    + " frames of memory.");
        }
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
    }

    public void closeResult(IFrameWriter writer) throws HyracksDataException {
        resultAppender.write(writer, true);
    }

    private void flushMemory(int partition) throws HyracksDataException {
        activeManager[partition].clear();
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private boolean loadRightTuple() throws HyracksDataException {
        boolean loaded = true;
        if (status.right.isRunFileReading()) {
            if (!rightInputAccessor.exists()) {
                if (!runFileStream[RIGHT_PARTITION].loadNextBuffer(rightInputAccessor)) {
                    if (memoryHasTuples()) {
                        // More tuples from the right need to be processed. Clear memory and replay the run file.
                        runFileStream[RIGHT_PARTITION].flushAndStopRunFile();
                        flushMemory(LEFT_PARTITION);
                        runFileStream[RIGHT_PARTITION].resetReader();
                    } else {
                        // Memory is empty and replay is complete.
                        runFileStream[RIGHT_PARTITION].closeRunFile();
                        rightInputAccessor.reset(rightBuffer);
                    }
                    return loadRightTuple();
                }
            }
        } else {
            if (!status.right.hasMore() && status.right.isRunFileWriting()) {
                // Finished left stream. Start the replay.
                runFileStream[RIGHT_PARTITION].flushAndStopRunFile();
                flushMemory(LEFT_PARTITION);
                runFileStream[RIGHT_PARTITION].openRunFile(rightInputAccessor);;
            } else if (rightInputAccessor != null && rightInputAccessor.exists()) {
                // Still processing frame.
            } else if (status.right.hasMore()) {
                status.continueRightLoad = true;
                locks.getRight(partition).signal();
                try {
                    while (status.continueRightLoad
                            && status.right.getStatus().isEqualOrBefore(Stage.DATA_PROCESSING)) {
                        locks.getLeft(partition).await();
                    }
                } catch (InterruptedException e) {
                    throw new HyracksDataException(
                            "SortMergeIntervalJoin interrupted exception while attempting to load right tuple", e);
                }
                if (!rightInputAccessor.exists() && status.right.getStatus() == Stage.CLOSED) {
                    status.right.noMore();
                    loaded = false;
                }
            } else {
                // No more frames or tuples to process.
                loaded = false;
            }
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
        if (status.left.isRunFileReading()) {
            if (!leftInputAccessor.exists()) {
                if (!runFileStream[LEFT_PARTITION].loadNextBuffer(leftInputAccessor)) {
                    if (memoryHasTuples()) {
                        // More tuples from the right need to be processed. Clear memory and replay the run file.
                        runFileStream[LEFT_PARTITION].flushAndStopRunFile();
                        flushMemory(RIGHT_PARTITION);
                        runFileStream[LEFT_PARTITION].resetReader();
                    } else {
                        // Memory is empty and replay is complete.
                        runFileStream[LEFT_PARTITION].closeRunFile();
                        leftInputAccessor.reset(leftBuffer);
                    }
                    return loadLeftTuple();
                }
            }
        } else {
            if (!status.left.hasMore() && status.left.isRunFileWriting()) {
                // Finished left stream. Start the replay.
                runFileStream[LEFT_PARTITION].flushAndStopRunFile();
                flushMemory(RIGHT_PARTITION);
                runFileStream[LEFT_PARTITION].openRunFile(leftInputAccessor);
            } else if (!status.left.hasMore() || !leftInputAccessor.exists()) {
                loaded = false;
            }
        }
        return loaded;
    }

    // memory management
    private boolean memoryHasTuples() {
        return !activeManager[RIGHT_PARTITION].isEmpty();
    }

    public void processMergeUsingLeftTuple(IFrameWriter writer) throws HyracksDataException {
        while (loadLeftTuple() && (status.right.hasMore() || memoryHasTuples())) {
            if (loadRightTuple() && !status.left.isRunFileWriting()
                    && (status.right.isRunFileWriting() || checkToProcessRightTuple())) {
                // *********************
                // Right side from stream or disk
                // *********************
                if (status.right.isRunFileWriting()) {
                    processRightTupleSpill(writer);
                } else {
                    processRightTuple(writer);
                }
            } else {
                // *********************
                // Left side from stream or disk
                // *********************
                if (status.left.isRunFileWriting()) {
                    processLeftTupleSpill(writer);
                } else {
                    processLeftTuple(writer);
                }
            }
        }
    }

    private boolean checkToProcessRightTuple() {
        long leftStart = IntervalJoinUtil.getIntervalStart(leftInputAccessor, leftKey);
        long rightStart = IntervalJoinUtil.getIntervalStart(rightInputAccessor, rightKey);
        if (leftStart < rightStart) {
            if (activeManager[RIGHT_PARTITION].hasRecords()
                    && activeManager[RIGHT_PARTITION].getTopPoint() < leftStart) {
                return true;
            } else {
                return false;
            }
        } else {
            if (activeManager[LEFT_PARTITION].hasRecords()
                    && activeManager[LEFT_PARTITION].getTopPoint() < rightStart) {
                return false;
            } else {
                return true;
            }
        }
    }

    private void processLeftTupleSpill(IFrameWriter writer) throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        while (loadLeftTuple() && activeManager[RIGHT_PARTITION].hasRecords()) {
            if (IntervalJoinUtil.getIntervalStart(leftInputAccessor, leftKey) < activeManager[RIGHT_PARTITION]
                    .getTopPoint()) {
                // Add individual tuples.
                for (TuplePointer rightTp : activeManager[RIGHT_PARTITION].getActiveList()) {
                    rightMemoryAccessor.reset(rightTp);
                    if (imjc.checkToSaveInResult(leftInputAccessor, leftInputAccessor.getTupleId(), rightMemoryAccessor,
                            rightTp.tupleIndex)) {
                        addToResult(leftInputAccessor, leftInputAccessor.getTupleId(), rightMemoryAccessor,
                                rightTp.tupleIndex, writer);
                    }
                }
                runFileStream[LEFT_PARTITION].addToRunFile(leftInputAccessor);
                leftInputAccessor.next();
            } else {
                // Remove from active.
                activeManager[RIGHT_PARTITION].removeTop();
            }
        }

        // Memory is empty and we can start processing the run file.
        if (activeManager[RIGHT_PARTITION].isEmpty()) {
            runFileStream[LEFT_PARTITION].flushAndStopRunFile();
            flushMemory(RIGHT_PARTITION);
            runFileStream[LEFT_PARTITION].openRunFile(leftInputAccessor);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Memory is now empty. Replaying left branch while continuing with the right branch.");
            }
        }
    }

    private void processLeftTuple(IFrameWriter writer) throws HyracksDataException {
        // Process endpoints
        do {
            if (!activeManager[LEFT_PARTITION].hasRecords() || IntervalJoinUtil.getIntervalStart(leftInputAccessor,
                    leftKey) < activeManager[LEFT_PARTITION].getTopPoint()) {
                // Add to active, end point index and buffer.
                TuplePointer tp = new TuplePointer();
                if (activeManager[LEFT_PARTITION].addTuple(leftInputAccessor, tp)) {
                    buffer.add(tp);
                } else {
                    // Spill case
                    freezeAndSpill();
                    break;
                }
                leftInputAccessor.next();
            } else {
                // Remove from active.
                activeManager[LEFT_PARTITION].removeTop();
            }
        } while (loadLeftTuple() && !checkToProcessRightTuple());

        // Add Results
        if (!buffer.isEmpty()) {
            for (TuplePointer rightTp : activeManager[RIGHT_PARTITION].getActiveList()) {
                rightMemoryAccessor.reset(rightTp);
                for (TuplePointer leftTp : buffer) {
                    leftMemoryAccessor.reset(leftTp);
                    if (imjc.checkToSaveInResult(leftMemoryAccessor, leftTp.tupleIndex, rightMemoryAccessor,
                            rightTp.tupleIndex)) {
                        addToResult(leftMemoryAccessor, leftTp.tupleIndex, rightMemoryAccessor, rightTp.tupleIndex,
                                writer);
                    }
                }
            }
            buffer.clear();
        }
    }

    private void processRightTupleSpill(IFrameWriter writer) throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        while (loadRightTuple() && activeManager[LEFT_PARTITION].hasRecords()) {
            if (IntervalJoinUtil.getIntervalStart(rightInputAccessor, rightKey) < activeManager[LEFT_PARTITION]
                    .getTopPoint()) {
                // Add individual tuples.
                for (TuplePointer leftTp : activeManager[LEFT_PARTITION].getActiveList()) {
                    leftMemoryAccessor.reset(leftTp);
                    if (imjc.checkToSaveInResult(leftMemoryAccessor, leftTp.tupleIndex, rightInputAccessor,
                            rightInputAccessor.getTupleId())) {
                        addToResult(leftMemoryAccessor, leftTp.tupleIndex, rightInputAccessor,
                                rightInputAccessor.getTupleId(), writer);
                    }
                }
                runFileStream[RIGHT_PARTITION].addToRunFile(rightInputAccessor);
                rightInputAccessor.next();
            } else {
                // Remove from active.
                activeManager[LEFT_PARTITION].removeTop();
            }
        }

        // Memory is empty and we can start processing the run file.
        if (!activeManager[LEFT_PARTITION].hasRecords()) {
            runFileStream[RIGHT_PARTITION].flushAndStopRunFile();
            flushMemory(LEFT_PARTITION);
            runFileStream[RIGHT_PARTITION].openRunFile(rightInputAccessor);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Memory is now empty. Replaying left branch while continuing with the left branch.");
            }
        }
    }

    private void processRightTuple(IFrameWriter writer) throws HyracksDataException {
        // Process endpoints
        do {
            if (!activeManager[RIGHT_PARTITION].hasRecords() || IntervalJoinUtil.getIntervalStart(rightInputAccessor,
                    rightKey) < activeManager[RIGHT_PARTITION].getTopPoint()) {
                // Add to active, end point index and buffer.
                TuplePointer tp = new TuplePointer();
                if (activeManager[RIGHT_PARTITION].addTuple(rightInputAccessor, tp)) {
                    buffer.add(tp);
                } else {
                    // Spill case
                    freezeAndSpill();
                    break;
                }
                rightInputAccessor.next();
            } else {
                // Remove from active.
                activeManager[RIGHT_PARTITION].removeTop();
            }
        } while (loadRightTuple() && checkToProcessRightTuple());

        // Add Results
        if (!buffer.isEmpty()) {
            for (TuplePointer leftTp : activeManager[LEFT_PARTITION].getActiveList()) {
                leftMemoryAccessor.reset(leftTp);
                for (TuplePointer rightTp : buffer) {
                    rightMemoryAccessor.reset(rightTp);
                    if (imjc.checkToSaveInResult(leftMemoryAccessor, leftTp.tupleIndex, rightMemoryAccessor,
                            rightTp.tupleIndex)) {
                        addToResult(leftMemoryAccessor, leftTp.tupleIndex, rightMemoryAccessor, rightTp.tupleIndex,
                                writer);
                    }
                }
            }
            buffer.clear();
        }
    }

    public void freezeAndSpill() throws HyracksDataException {
        if (bufferManager.getNumTuples(LEFT_PARTITION) > bufferManager.getNumTuples(RIGHT_PARTITION)) {
            runFileStream[RIGHT_PARTITION].startRunFile();
        } else {
            runFileStream[LEFT_PARTITION].startRunFile();
        }
    }

    public void setLeftFrame(ByteBuffer buffer) {
        leftBuffer.clear();
        if (leftBuffer.capacity() < buffer.capacity()) {
            leftBuffer.limit(buffer.capacity());
        }
        leftBuffer.put(buffer.array(), 0, buffer.capacity());
        leftInputAccessor.reset(leftBuffer);
        leftInputAccessor.next();
    }

    public void setRightFrame(ByteBuffer buffer) {
        rightBuffer.clear();
        if (rightBuffer.capacity() < buffer.capacity()) {
            rightBuffer.limit(buffer.capacity());
        }
        rightBuffer.put(buffer.array(), 0, buffer.capacity());
        rightInputAccessor.reset(rightBuffer);
        rightInputAccessor.next();
        status.continueRightLoad = false;
    }

}
