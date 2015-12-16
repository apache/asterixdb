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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hyracks.dataflow.std.buffermanager.DeletableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleBufferAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VariableDeletableTupleMemoryManager;
import org.apache.hyracks.dataflow.std.join.MergeStatus.BranchStatus;
import org.apache.hyracks.dataflow.std.join.MergeStatus.RunFileStatus;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

/**
 * Merge Joiner takes two sorted streams of input and joins.
 * The two sorted streams must be in a logical order and the comparator must
 * support keeping that order so the join will work.
 * The left stream will spill to disk when memory is full.
 * The right stream spills to memory and pause when memory is full.
 *
 * @author prestonc
 */
public class MergeJoiner {

    private final FrameTupleAccessor accessorLeft;
    private FrameTupleAccessor accessorRight;

    private MergeJoinLocks locks;
    private MergeStatus status;

    private ByteBuffer leftBuffer;
    private ByteBuffer rightBuffer;
    private int leftBufferTupleIndex;
    private int leftRunFileTupleIndex;
    private int rightBufferTupleIndex;

    private final TuplePointer tp;
    private final List<TuplePointer> memoryTuples;
    private final IDeletableFramePool framePool;
    private IDeletableTupleBufferManager bufferManager;
    private ITupleBufferAccessor memoryAccessor;

    private final IFrame runFileBuffer;
    private final FrameTupleAppender runFileAppender;
    private final RunFileWriter runFileWriter;
    private RunFileReader runFileReader;

    private final FrameTupleAppender resultAppender;

    private final IMergeJoinChecker mjc;

    private final int partition;

    public MergeJoiner(IHyracksTaskContext ctx, int memorySize, int partition, MergeStatus status, MergeJoinLocks locks,
            IMergeJoinChecker mjc, RecordDescriptor leftRd) throws HyracksDataException {
        this.partition = partition;
        this.status = status;
        this.locks = locks;
        this.mjc = mjc;

        accessorLeft = new FrameTupleAccessor(leftRd);
        leftBuffer = ctx.allocateFrame();
        rightBuffer = ctx.allocateFrame();
        leftBufferTupleIndex = -1;
        rightBufferTupleIndex = -1;

        // Memory (right buffer)
        framePool = new DeletableFramePool(ctx, (memorySize - 1) * ctx.getInitialFrameSize());
        memoryTuples = new ArrayList<TuplePointer>();
        tp = new TuplePointer();

        // Run File and frame cache (left buffer)
        FileReference file = ctx.getJobletContext()
                .createManagedWorkspaceFile(this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, ctx.getIOManager());
        runFileWriter.open();
        runFileBuffer = new FixedSizeFrame(ctx.allocateFrame(ctx.getInitialFrameSize()));
        runFileAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        leftRunFileTupleIndex = -1;

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

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            IFrameWriter writer) throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
    }

    public void closeResult(IFrameWriter writer) throws HyracksDataException {
        resultAppender.flush(writer, true);
    }

    private void addToRunFile(IFrameTupleAccessor accessor, int idx) throws HyracksDataException {
        if (!runFileAppender.append(accessor, idx)) {
            runFileAppender.flush(runFileWriter, true);
            runFileAppender.append(accessor, idx);
        }
    }

    private void openRunFile() throws HyracksDataException {
        status.runFileStatus = RunFileStatus.READING;

        // Create reader
        runFileReader = runFileWriter.createReader();
        runFileReader.open();
        leftRunFileTupleIndex = 0;

        // Load first frame
        runFileReader.nextFrame(runFileBuffer);
        accessorLeft.reset(runFileBuffer.getBuffer());
    }

    private void closeRunFile() throws HyracksDataException {
        status.runFileStatus = RunFileStatus.NOT_USED;
        runFileReader.close();
        accessorLeft.reset(leftBuffer);
    }

    private void flushMemory() throws HyracksDataException {
        bufferManager.reset();
        memoryTuples.clear();
    }

    private int getRightTupleIndex() throws HyracksDataException {
        return rightBufferTupleIndex;
    }

    private void incrementRightTuple() throws HyracksDataException {
        ++rightBufferTupleIndex;
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private boolean loadRightTuple() throws HyracksDataException {
        boolean loaded = true;
        if ((rightBufferTupleIndex == -1 || rightBufferTupleIndex >= accessorRight.getTupleCount())
                && status.rightHasMore == true) {
            status.loadRightFrame = true;
            locks.getRight(partition).signal();
            try {
                while (status.loadRightFrame && status.getRightStatus().isEqualOrBefore(BranchStatus.DATA_PROCESSING)) {
                    locks.getLeft(partition).await();
                }
            } catch (InterruptedException e) {
                throw new HyracksDataException(
                        "SortMergeIntervalJoin interrupted exception while attempting to load right tuple", e);
            }
            loaded = (rightBufferTupleIndex == 0);
            if (!loaded) {
                status.rightHasMore = false;
            }
        }
        return loaded;
    }

    private int getLeftTupleIndex() throws HyracksDataException {
        if (status.runFileStatus == RunFileStatus.READING) {
            return leftRunFileTupleIndex;
        } else {
            return leftBufferTupleIndex;
        }
    }

    private void incrementLeftTuple() throws HyracksDataException {
        if (status.runFileStatus == RunFileStatus.READING) {
            ++leftRunFileTupleIndex;
        } else {
            ++leftBufferTupleIndex;
        }
    }

    /**
     * Ensures a frame exists for the right branch, either from memory or the run file.
     *
     * @throws HyracksDataException
     */
    private boolean loadLeftTuple() throws HyracksDataException {
        boolean loaded = true;
        if (status.runFileStatus == RunFileStatus.READING) {
            if (leftRunFileTupleIndex >= accessorLeft.getTupleCount()) {
                if (runFileReader.nextFrame(runFileBuffer)) {
                    accessorLeft.reset(runFileBuffer.getBuffer());
                    leftRunFileTupleIndex = 0;
                } else {
                    closeRunFile();
                    return loadLeftTuple();
                }
            }
        } else {
            if (leftBufferTupleIndex == -1 || leftBufferTupleIndex >= accessorLeft.getTupleCount()) {
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
    private boolean needNewLeftTuple() throws HyracksDataException {
        if (leftBufferTupleIndex == -1 || leftBufferTupleIndex >= accessorLeft.getTupleCount()) {
            return true;
        }
        return false;
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
    public void processMergeUsingLeftTuple(IFrameWriter writer) throws HyracksDataException {
        // *********************
        // Left side from tuple
        // *********************
        while (!needNewLeftTuple() && loadLeftTuple()) {
            // Write left tuple to run file
            if (status.runFileStatus == RunFileStatus.WRITING) {
                addToRunFile(accessorLeft, getLeftTupleIndex());
            }

            // *********************
            // Right side from memory
            // *********************
            if (rightBufferTupleIndex != -1) {
                Iterator<TuplePointer> memoryIterator = memoryTuples.iterator();
                while (memoryIterator.hasNext()) {
                    tp.reset(memoryIterator.next());
                    memoryAccessor.reset(tp);
                    if (mjc.checkToSaveInResult(accessorLeft, getLeftTupleIndex(), memoryAccessor, tp.tupleIndex)) {
                        // add to result
                        addToResult(accessorLeft, getLeftTupleIndex(), memoryAccessor, tp.tupleIndex, writer);
                    }
                    if (mjc.checkToRemoveInMemory(accessorLeft, getLeftTupleIndex(), memoryAccessor, tp.tupleIndex)) {
                        // remove from memory
                        bufferManager.deleteTuple(tp);
                        memoryIterator.remove();
                    }
                }

                // Memory is empty and we can start processing the run file.
                if (!memoryHasTuples() && status.runFileStatus == RunFileStatus.WRITING) {
                    openRunFile();
                    flushMemory();
                }
            }

            // *********************
            // Right side from stream
            // *********************
            if (status.runFileStatus == RunFileStatus.NOT_USED && loadRightTuple() && status.rightHasMore) {
                while (mjc.checkToLoadNextRightTuple(accessorLeft, getLeftTupleIndex(), accessorRight,
                        getRightTupleIndex())) {
                    if (mjc.checkToSaveInResult(accessorLeft, getLeftTupleIndex(), accessorRight,
                            getRightTupleIndex())) {
                        // add to result
                        addToResult(accessorLeft, getLeftTupleIndex(), accessorRight, getRightTupleIndex(), writer);
                    }
                    // append to memory
                    if (mjc.checkToSaveInMemory(accessorLeft, getLeftTupleIndex(), accessorRight,
                            getRightTupleIndex())) {
                        if (!addToMemory(accessorRight, getRightTupleIndex())) {
                            // go to log saving state
                            status.runFileStatus = RunFileStatus.WRITING;
                            // write right tuple to run file
                            addToRunFile(accessorLeft, getLeftTupleIndex());
                            break;
                        }
                    }
                    incrementRightTuple();
                    if (!loadRightTuple()) {
                        break;
                    }
                }
            }
            incrementLeftTuple();
        }
    }

    public void setLeftFrame(ByteBuffer buffer) {
        if (leftBuffer.capacity() < buffer.capacity()) {
            leftBuffer.limit(buffer.capacity());
        }
        leftBuffer.put(buffer.array(), 0, buffer.capacity());
        accessorLeft.reset(leftBuffer);
        leftBufferTupleIndex = 0;
    }

    public void setRightFrame(ByteBuffer buffer) {
        if (rightBuffer.capacity() < buffer.capacity()) {
            rightBuffer.limit(buffer.capacity());
        }
        rightBuffer.put(buffer.array(), 0, buffer.capacity());
        accessorRight.reset(rightBuffer);
        rightBufferTupleIndex = 0;
        status.loadRightFrame = false;
    }

    public void setRightRecordDescriptor(RecordDescriptor rightRd) {
        accessorRight = new FrameTupleAccessor(rightRd);
        bufferManager = new VariableDeletableTupleMemoryManager(framePool, rightRd);
        memoryAccessor = bufferManager.getTupleAccessor();
    }
}
