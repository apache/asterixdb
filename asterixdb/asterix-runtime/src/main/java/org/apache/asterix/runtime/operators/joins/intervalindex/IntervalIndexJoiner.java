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

import java.util.Comparator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.dataflow.data.nontagged.printers.adm.AObjectPrinterFactory;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.asterix.runtime.operators.joins.IntervalJoinUtil;
import org.apache.hyracks.algebricks.data.IPrinter;
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
import org.apache.hyracks.dataflow.std.join.AbstractMergeJoiner;
import org.apache.hyracks.dataflow.std.join.MergeJoinLocks;
import org.apache.hyracks.dataflow.std.join.MergeStatus;
import org.apache.hyracks.dataflow.std.join.RunFileStream;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Interval Index Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The both right and left use memory to maintain active intervals for the join.
 */
public class IntervalIndexJoiner extends AbstractMergeJoiner {

    private static final Logger LOGGER = Logger.getLogger(IntervalIndexJoiner.class.getName());

    private IPartitionedDeletableTupleBufferManager bufferManager;

    private ActiveSweepManager[] activeManager;

    private LinkedList<TuplePointer> buffer = new LinkedList<>();

    private ITuplePointerAccessor leftMemoryAccessor;
    private ITuplePointerAccessor rightMemoryAccessor;

    private IIntervalMergeJoinChecker imjc;

    protected byte point;

    private MergeStatus status;

    private int[] streamIndex;

    private RunFileStream[] runFileStream;

    private int leftKey;
    private int rightKey;

    private IPrinter printer;

    public IntervalIndexJoiner(IHyracksTaskContext ctx, int memorySize, int partition, MergeStatus status,
            MergeJoinLocks locks, Comparator<EndPointIndexItem> endPointComparator,
            IIntervalMergeJoinCheckerFactory imjcf, int[] leftKeys, int[] rightKeys, RecordDescriptor leftRd,
            RecordDescriptor rightRd) throws HyracksDataException {
        super(ctx, partition, status, locks, leftRd, rightRd);
        this.point = imjcf.isOrderAsc() ? EndPointIndexItem.START_POINT : EndPointIndexItem.END_POINT;

        this.imjc = imjcf.createMergeJoinChecker(leftKeys, rightKeys, partition);

        this.leftKey = leftKeys[0];
        this.rightKey = rightKeys[0];

        this.status = status;

        RecordDescriptor[] recordDescriptors = new RecordDescriptor[JOIN_PARTITIONS];
        recordDescriptors[LEFT_PARTITION] = leftRd;
        recordDescriptors[RIGHT_PARTITION] = rightRd;

        streamIndex = new int[JOIN_PARTITIONS];
        streamIndex[LEFT_PARTITION] = TupleAccessor.UNSET;
        streamIndex[RIGHT_PARTITION] = TupleAccessor.UNSET;

        if (memorySize < 5) {
            throw new HyracksDataException(
                    "IntervalIndexJoiner does not have enough memory (needs > 4, got " + memorySize + ").");
        }
        bufferManager = new VPartitionDeletableTupleBufferManager(ctx,
                VPartitionDeletableTupleBufferManager.NO_CONSTRAIN, JOIN_PARTITIONS,
                (memorySize - 4) * ctx.getInitialFrameSize(), recordDescriptors);
        this.leftMemoryAccessor = bufferManager.getTuplePointerAccessor(leftRd);
        this.rightMemoryAccessor = bufferManager.getTuplePointerAccessor(rightRd);

        activeManager = new ActiveSweepManager[JOIN_PARTITIONS];
        activeManager[LEFT_PARTITION] = new ActiveSweepManager(bufferManager, leftKey, LEFT_PARTITION,
                endPointComparator);
        activeManager[RIGHT_PARTITION] = new ActiveSweepManager(bufferManager, rightKey, RIGHT_PARTITION,
                endPointComparator);

        // Run files for both branches
        runFileStream = new RunFileStream[JOIN_PARTITIONS];
        runFileStream[LEFT_PARTITION] = new RunFileStream(ctx, "left", status.branch[LEFT_PARTITION]);
        runFileStream[RIGHT_PARTITION] = new RunFileStream(ctx, "right", status.branch[RIGHT_PARTITION]);

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("IntervalIndexJoiner has started partition " + partition + " with " + memorySize
                    + " frames of memory.");
        }

        printer = AObjectPrinterFactory.INSTANCE.createPrinter();
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
    }

    @Override
    public void closeResult(IFrameWriter writer) throws HyracksDataException {
        resultAppender.write(writer, true);
        activeManager[LEFT_PARTITION].clear();
        activeManager[RIGHT_PARTITION].clear();
        runFileStream[LEFT_PARTITION].close();
        runFileStream[RIGHT_PARTITION].close();
    }

    private void flushMemory(int partition) throws HyracksDataException {
        activeManager[partition].clear();
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private TupleStatus loadRightTuple() throws HyracksDataException {
        TupleStatus loaded;
        if (status.branch[RIGHT_PARTITION].isRunFileReading()) {
            loaded = loadRightSpilledTuple();
            if (loaded.isEmpty()) {
                continueStream(RIGHT_PARTITION, rightInputAccessor);
            }
        } else {
            if (rightInputAccessor != null && rightInputAccessor.exists()) {
                // Still processing frame.
                loaded = TupleStatus.LOADED;
            } else if (status.branch[RIGHT_PARTITION].hasMore()) {
                loaded = pauseAndLoadRightTuple();
            } else {
                // No more frames or tuples to process.
                loaded = TupleStatus.EMPTY;;
            }
        }
        printTuple("Right Pointer", rightInputAccessor);
        return loaded;
    }

    private TupleStatus loadRightSpilledTuple() throws HyracksDataException {
        if (!rightInputAccessor.exists()) {
            if (!runFileStream[RIGHT_PARTITION].loadNextBuffer(rightInputAccessor)) {
                return TupleStatus.EMPTY;
            }
        }
        return TupleStatus.LOADED;
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private TupleStatus loadLeftTuple() throws HyracksDataException {
        TupleStatus loaded;
        if (status.branch[LEFT_PARTITION].isRunFileReading()) {
            loaded = loadLeftSpilledTuple();
            if (loaded.isEmpty()) {
                continueStream(LEFT_PARTITION, leftInputAccessor);
                loaded = loadLeftTuple();
            }
        } else {
            if (leftInputAccessor != null && leftInputAccessor.exists()) {
                // Still processing frame.
                loaded = TupleStatus.LOADED;
            } else if (status.branch[LEFT_PARTITION].hasMore()) {
                loaded = TupleStatus.UNKNOWN;
            } else {
                // No more frames or tuples to process.
                loaded = TupleStatus.EMPTY;
            }

        }
        printTuple("Left Pointer", leftInputAccessor);
        return loaded;
    }

    private TupleStatus loadLeftSpilledTuple() throws HyracksDataException {
        if (!leftInputAccessor.exists()) {
            if (!runFileStream[LEFT_PARTITION].loadNextBuffer(leftInputAccessor)) {
                return TupleStatus.EMPTY;
            }
        }
        return TupleStatus.LOADED;
    }

    @Override
    public void processMergeUsingLeftTuple(IFrameWriter writer) throws HyracksDataException {
        TupleStatus ts = loadLeftTuple();
        while (ts.isLoaded() || (ts.isEmpty()
                && (activeManager[LEFT_PARTITION].hasRecords() || status.branch[LEFT_PARTITION].isRunFileWriting()))) {
            if (status.branch[RIGHT_PARTITION].isRunFileWriting()) {
                // Right side from disk
                processRightTupleSpill(writer);
            } else if (status.branch[LEFT_PARTITION].isRunFileWriting()) {
                // Left side from disk
                ts = processLeftTupleSpill(writer);
            } else {
                if (ts.isEmpty() || (loadRightTuple().isLoaded() && checkToProcessRightTuple())) {
                    // Right side from stream
                    processRightTuple(writer);
                } else {
                    // Left side from stream
                    processLeftTuple(writer);
                    ts = loadLeftTuple();
                }
            }
        }
    }

    private boolean checkToProcessRightTuple() {
        long leftStart = IntervalJoinUtil.getIntervalStart(leftInputAccessor, leftKey);
        long rightStart = IntervalJoinUtil.getIntervalStart(rightInputAccessor, rightKey);
        if (leftStart < rightStart) {
            return activeManager[RIGHT_PARTITION].hasRecords()
                    && activeManager[RIGHT_PARTITION].getTopPoint() < leftStart;
        } else {
            return !(activeManager[LEFT_PARTITION].hasRecords()
                    && activeManager[LEFT_PARTITION].getTopPoint() < rightStart);
        }
    }

    private TupleStatus processLeftTupleSpill(IFrameWriter writer) throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        System.err.println("---------------Start left spill");
        int count = 0;
        TupleStatus ts = loadLeftTuple();
        while (ts.isLoaded() && activeManager[RIGHT_PARTITION].hasRecords()) {
            long sweep = activeManager[RIGHT_PARTITION].getTopPoint();
            printTuple("Left Spill", leftInputAccessor);
            if (IntervalJoinUtil.getIntervalStart(leftInputAccessor, leftKey) < sweep) {
                // Add individual tuples.
                for (TuplePointer rightTp : activeManager[RIGHT_PARTITION].getActiveList()) {
                    rightMemoryAccessor.reset(rightTp);
                    printTuple("Right Memory", rightMemoryAccessor, rightTp.tupleIndex);
                    if (imjc.checkToSaveInResult(leftInputAccessor, leftInputAccessor.getTupleId(), rightMemoryAccessor,
                            rightTp.tupleIndex)) {
                        addToResult(leftInputAccessor, leftInputAccessor.getTupleId(), rightMemoryAccessor,
                                rightTp.tupleIndex, writer);
                    }
                }
                runFileStream[LEFT_PARTITION].addToRunFile(leftInputAccessor);
                leftInputAccessor.next();
                ts = loadLeftTuple();
                ++count;
            } else {
                // Remove from active.
                activeManager[RIGHT_PARTITION].removeTop();
            }
        }

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Spill for " + count + " left tuples");
        }

        // Memory is empty and we can start processing the run file.
        if (activeManager[RIGHT_PARTITION].isEmpty() || ts.isEmpty()) {
            unfreezeAndContinue(LEFT_PARTITION, leftInputAccessor, RIGHT_PARTITION);
            ts = loadLeftTuple();
        }
        System.err.println("------------------End left spill");
        return ts;
    }

    private void processRightTupleSpill(IFrameWriter writer) throws HyracksDataException {
        // Process left tuples one by one, check them with active memory from the right branch.
        System.err.println("---------------Start right spill");
        int count = 0;
        TupleStatus ts = loadRightTuple();
        while (ts.isLoaded() && activeManager[LEFT_PARTITION].hasRecords()) {
            long sweep = activeManager[LEFT_PARTITION].getTopPoint();
            printTuple("Right Spill", rightInputAccessor);
            if (IntervalJoinUtil.getIntervalStart(rightInputAccessor, rightKey) < sweep) {
                // Add individual tuples.
                for (TuplePointer leftTp : activeManager[LEFT_PARTITION].getActiveList()) {
                    leftMemoryAccessor.reset(leftTp);
                    printTuple("Left Memory", leftMemoryAccessor, leftTp.tupleIndex);
                    if (imjc.checkToSaveInResult(leftMemoryAccessor, leftTp.tupleIndex, rightInputAccessor,
                            rightInputAccessor.getTupleId())) {
                        addToResult(leftMemoryAccessor, leftTp.tupleIndex, rightInputAccessor,
                                rightInputAccessor.getTupleId(), writer);
                    }
                }
                runFileStream[RIGHT_PARTITION].addToRunFile(rightInputAccessor);
                rightInputAccessor.next();
                ts = loadRightTuple();
                ++count;
            } else {
                // Remove from active.
                printTuple("Right Forced Memory Delete", rightInputAccessor);
                activeManager[LEFT_PARTITION].removeTop();
            }
        }

        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Spill for " + count + " right tuples");
        }

        // Memory is empty and we can start processing the run file.
        if (!activeManager[LEFT_PARTITION].hasRecords() || ts.isEmpty()) {
            unfreezeAndContinue(RIGHT_PARTITION, rightInputAccessor, LEFT_PARTITION);
        }
        System.err.println("------------------End right spill");
    }

    private void processLeftTuple(IFrameWriter writer) throws HyracksDataException {
        // Process endpoints
        do {
            if (!activeManager[LEFT_PARTITION].hasRecords() || IntervalJoinUtil.getIntervalStart(leftInputAccessor,
                    leftKey) < activeManager[LEFT_PARTITION].getTopPoint()) {
                // Add to active, end point index and buffer.
                TuplePointer tp = new TuplePointer();
                if (activeManager[LEFT_PARTITION].addTuple(leftInputAccessor, tp)) {
                    printTuple("Left To Memory", leftInputAccessor);
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
        } while (loadLeftTuple().isLoaded() && !checkToProcessRightTuple());

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
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Sweep for " + buffer.size() + " left tuples");
            }
            buffer.clear();
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
                    printTuple("Right To Memory", rightInputAccessor);
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
        } while (loadRightTuple().isLoaded() && checkToProcessRightTuple());

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
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Sweep for " + buffer.size() + " right tuples");
            }

            buffer.clear();
        }
    }

    private void freezeAndSpill() throws HyracksDataException {
        if (bufferManager.getNumTuples(LEFT_PARTITION) > bufferManager.getNumTuples(RIGHT_PARTITION)) {
            runFileStream[RIGHT_PARTITION].startRunFile();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Memory is full. Freezing the left branch. (Left memory tuples: "
                        + bufferManager.getNumTuples(LEFT_PARTITION) + ", Right memory tuples: "
                        + bufferManager.getNumTuples(RIGHT_PARTITION) + ")");
            }
        } else {
            runFileStream[LEFT_PARTITION].startRunFile();
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("Memory is full. Freezing the right branch. (Left memory tuples: "
                        + bufferManager.getNumTuples(LEFT_PARTITION) + ", Right memory tuples: "
                        + bufferManager.getNumTuples(RIGHT_PARTITION) + ")");
            }
        }
    }

    private void continueStream(int diskPartition, ITupleAccessor accessor) throws HyracksDataException {
        runFileStream[diskPartition].closeRunFile();
        accessor.reset(leftBuffer);
        accessor.setTupleId(streamIndex[diskPartition]);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Continue with stream (" + diskPartition + ").");
        }
    }

    private void unfreezeAndContinue(int frozenPartition, ITupleAccessor accessor, int flushPartition)
            throws HyracksDataException {
        runFileStream[frozenPartition].flushAndStopRunFile(accessor);
        flushMemory(flushPartition);
        if ((LEFT_PARTITION == frozenPartition && !status.branch[LEFT_PARTITION].isRunFileReading())
                || (RIGHT_PARTITION == frozenPartition && !status.branch[RIGHT_PARTITION].isRunFileReading())) {
            streamIndex[frozenPartition] = accessor.getTupleId();
        }
        runFileStream[frozenPartition].openRunFile(accessor);
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Unfreezing (" + frozenPartition + ").");
        }
    }

    private void printTuple(String message, ITupleAccessor accessor) throws HyracksDataException {
        if (accessor.exists()) {
            printTuple(message, accessor, accessor.getTupleId());
        } else {
            System.err.print(String.format("%1$-" + 15 + "s", message) + " --");
            System.err.print("no tuple");
            System.err.println();
        }
    }

    private void printTuple(String message, IFrameTupleAccessor accessor, int tupleId) throws HyracksDataException {
        System.err.print(String.format("%1$-" + 15 + "s", message) + " --");
        int fields = accessor.getFieldCount();
        for (int i = 0; i < fields; ++i) {
            System.err.print(" " + i + ": ");
            int fieldStartOffset = accessor.getFieldStartOffset(tupleId, i);
            int fieldSlotsLength = accessor.getFieldSlotsLength();
            int tupleStartOffset = accessor.getTupleStartOffset(tupleId);
            printer.print(accessor.getBuffer().array(), fieldStartOffset + fieldSlotsLength + tupleStartOffset,
                    accessor.getFieldLength(tupleId, i), System.err);
        }
        System.err.println();
    }

}
