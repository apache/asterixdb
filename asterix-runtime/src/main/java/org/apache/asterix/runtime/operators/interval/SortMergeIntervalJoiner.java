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
package org.apache.asterix.runtime.operators.interval;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.runtime.operators.interval.SortMergeIntervalStatus.BranchStatus;
import org.apache.asterix.runtime.operators.interval.SortMergeIntervalStatus.RunFileStatus;
import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleBufferAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.buffermanager.VariableFramePool;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;

public class SortMergeIntervalJoiner {

    private static final int MEMORY_INDEX = -1;

    private final FrameTupleAccessor accessorLeft;
    private FrameTupleAccessor accessorRight;

    private SortMergeIntervalJoinLocks locks;
    private SortMergeIntervalStatus status;

    private final IFrameWriter writer;

    private ByteBuffer leftBuffer;
    private ByteBuffer rightBuffer;
    private int leftTupleIndex;
    private int rightRunFileTupleIndex;
    private int rightBufferTupleIndex;

    private final IDeletableTupleBufferManager bufferManager;
    private final List<TuplePointer> memoryTuples;
    private final ITupleBufferAccessor memoryAccessor;

    private final IFrame runFileBuffer;
    private final FrameTupleAppender runFileAppender;
    private final RunFileWriter runFileWriter;
    private RunFileReader runFileReader;
    private IFrameTupleAccessor runFileAccessor;

    private final FrameTupleAppender resultAppender;

    private final FrameTuplePairComparator comparator;

    private final int partition;

    public SortMergeIntervalJoiner(IHyracksTaskContext ctx, int memorySize, int partition,
            SortMergeIntervalStatus status, SortMergeIntervalJoinLocks locks, FrameTuplePairComparator comparator,
            IFrameWriter writer, RecordDescriptor leftRd) throws HyracksDataException {
        this.partition = partition;
        this.status = status;
        this.locks = locks;
        this.writer = writer;
        this.comparator = comparator;

        accessorLeft = new FrameTupleAccessor(leftRd);

        // Memory
        IFramePool framePool = new VariableFramePool(ctx, (memorySize - 1) * ctx.getInitialFrameSize());
        bufferManager =  new VariableDeletableTupleMemoryManager(framePool, leftRd);
        memoryTuples = new ArrayList<TuplePointer>();
        memoryAccessor = bufferManager.getTupleAccessor();

        // Run File and frame cache
        FileReference file = ctx.getJobletContext()
                .createManagedWorkspaceFile(this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, ctx.getIOManager());
        runFileWriter.open();
        runFileBuffer = new FixedSizeFrame(ctx.allocateFrame(ctx.getInitialFrameSize()));
        runFileAppender = new FrameTupleAppender(new VSizeFrame(ctx));

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

    private boolean addToMemory(IFrameTupleAccessor accessor, int idx) throws HyracksDataException {
        TuplePointer tuplePointer = new TuplePointer();
        if (bufferManager.insertTuple(accessor, idx, tuplePointer)) {
            memoryTuples.add(tuplePointer);
            return true;
        }
        return false;
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2)
            throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
    }

    private void addToRunFile(IFrameTupleAccessor accessor, int idx) throws HyracksDataException {
        if (!runFileAppender.append(accessor, idx)) {
            runFileAppender.write(runFileWriter, true);
            runFileAppender.append(accessor, idx);
        }
    }

    private void openFromRunFile() throws HyracksDataException {
        status.runFileStatus = RunFileStatus.READING;

        // Create reader
        runFileReader = runFileWriter.createReader();
        runFileReader.open();
        rightRunFileTupleIndex = 0;

        // Load first frame
        runFileReader.nextFrame(runFileBuffer);
        accessorRight.reset(runFileBuffer.getBuffer());
    }

    private void closeFromRunFile() throws HyracksDataException {
        status.runFileStatus = RunFileStatus.NOT_USED;
        runFileReader.close();
    }

    private void flushMemory() throws HyracksDataException {
        bufferManager.reset();
        memoryTuples.clear();
    }

    private void incrementLeftTuple() {
        leftTupleIndex++;
    }

    private int getRightTupleIndex() throws HyracksDataException {
        if (status.runFileStatus == RunFileStatus.READING) {
            return rightRunFileTupleIndex;
        } else {
            return rightBufferTupleIndex;
        }
    }

    private void incrementRightTuple() throws HyracksDataException {
        if (status.runFileStatus == RunFileStatus.READING) {
            ++rightRunFileTupleIndex;
        } else {
            rightBufferTupleIndex++;
        }
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private boolean loadRightTuple() throws HyracksDataException {
        boolean loaded = true;
        if (status.runFileStatus == RunFileStatus.READING) {
            if (rightRunFileTupleIndex >= accessorRight.getTupleCount()) {
                if (runFileReader.nextFrame(runFileBuffer)) {
                    accessorRight.reset(runFileBuffer.getBuffer());
                    rightRunFileTupleIndex = 0;
                } else {
                    closeFromRunFile();
                    return loadRightTuple();
                }
            }
        } else {
            if (rightBufferTupleIndex >= accessorRight.getTupleCount()) {
                status.loadRightFrame = true;
                locks.getRight(partition).signal();
                try {
                    while (status.loadRightFrame && status.getRightStatus() == BranchStatus.DATA_PROCESSING) {
                        locks.getLeft(partition).await();
                    }
                } catch (InterruptedException e) {
                    throw new HyracksDataException(
                            "SortMergeIntervalJoin interrupted exception while attempting to load right tuple", e);
                }
                status.loadRightFrame = false;
                loaded = (rightBufferTupleIndex == 0);
                if (!loaded) {
                    status.rightHasMore = false;
                }
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
        if (status.getLeftStatus() == BranchStatus.DATA_PROCESSING && leftTupleIndex >= accessorLeft.getTupleCount()) {
            return false;
        }
        return true;
    }

    // memory management
    private boolean memoryHasTuples() {
        return bufferManager.getNumTuples() > 0;
    }

    public void processMerge() throws HyracksDataException {
        // Ensure right tuple loaded into accessorRight
        while (loadRightTuple() && status.rightHasMore) {
            // *********************
            // Left side from memory
            // *********************
            if (status.reloadingLeftFrame) {
                // Skip the right frame memory processing.
                status.reloadingLeftFrame = false;
            } else {
                if (status.runFileStatus == RunFileStatus.WRITING) {
                    // Write right tuple to run file
                    addToRunFile(accessorRight, getRightTupleIndex());
                }

                for (Iterator<TuplePointer> memoryIterator = memoryTuples.iterator(); memoryIterator.hasNext();) {
                    TuplePointer tp = memoryIterator.next();
                    memoryAccessor.reset(tp);
                    int c = comparator.compare(memoryAccessor, MEMORY_INDEX, accessorRight, getRightTupleIndex());
                    if (c < 0) {
                        // remove from memory
                        bufferManager.deleteTuple(tp);
                        memoryIterator.remove();
                    }
                    if (c == 0) {
                        // add to result
                        addToResult(memoryAccessor, MEMORY_INDEX, accessorRight, getRightTupleIndex());
                    }
                }

                if (!memoryHasTuples() && status.runFileStatus == RunFileStatus.WRITING) {
                    // Memory is empty and we can start processing the run file.
                    openFromRunFile();
                    flushMemory();
                }
            }

            // *********************
            // Left side from stream
            // *********************
            if (status.runFileStatus == RunFileStatus.NOT_USED && status.leftHasMore) {
                int c = comparator.compare(accessorLeft, leftTupleIndex, accessorRight, getRightTupleIndex());
                while (c <= 0) {
                    if (c == 0) {
                        // add to result
                        addToResult(accessorLeft, leftTupleIndex, accessorRight, getRightTupleIndex());
                        // append to memory
                        if (!addToMemory(accessorLeft, leftTupleIndex)) {
                            // go to log saving state
                            status.runFileStatus = RunFileStatus.WRITING;
                            // write right tuple to run file
                            addToRunFile(accessorRight, getRightTupleIndex());
                            // break (do not increment left tuple)
                            break;
                        }
                    }
                    incrementLeftTuple();
                    if (!loadLeftTuple()) {
                        return;
                    }
                    c = comparator.compare(accessorLeft, leftTupleIndex, accessorRight, getRightTupleIndex());
                }
            }
            incrementRightTuple();
        }
    }

    public void setLeftFrame(ByteBuffer buffer) {
        leftBuffer = buffer;
        accessorLeft.reset(leftBuffer);
        leftTupleIndex = 0;
    }

    public void setRightFrame(ByteBuffer buffer) {
        rightBuffer = buffer;
        accessorRight.reset(rightBuffer);
        rightBufferTupleIndex = 0;
    }

    public void setRightRecordDescriptor(RecordDescriptor rightRd) {
        accessorRight = new FrameTupleAccessor(rightRd);
    }
}
