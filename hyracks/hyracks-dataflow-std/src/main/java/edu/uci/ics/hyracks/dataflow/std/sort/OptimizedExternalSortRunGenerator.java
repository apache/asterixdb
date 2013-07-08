/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;

/**
 * @author pouria This class implements the run generator for sorting with
 *         replacement selection, where there is no limit on the output, i.e.
 *         the whole data should be sorted. A SortMinHeap is used as the
 *         selectionTree to decide the order of writing tuples into the runs,
 *         while memory manager is based on a binary search tree to allocate
 *         tuples in the memory. The overall process is as follows: - Read the
 *         input data frame by frame. For each tuple T in the current frame: -
 *         Try to allocate a memory slot for writing T along with the attached
 *         header/footer (for memory management purpose) - If T can not be
 *         allocated, try to output as many tuples, currently resident in
 *         memory, as needed so that a free slot, large enough to hold T, gets
 *         created. MinHeap decides about which tuple should be sent to the
 *         output at each step. - Write T into the memory - Calculate the runID
 *         of T (based on the last output tuple for the current run). It is
 *         either the current run or the next run. Also calculate Poorman's
 *         Normalized Key (PNK) for T, to make comparisons faster later. -
 *         Create a heap element for T, containing: its runID, the slot pointer
 *         to its memory location, and its PNK. - Insert the created heap
 *         element into the heap - Upon closing, write all the tuples, currently
 *         resident in memory, into their corresponding run(s). Again min heap
 *         decides about which tuple is the next for output.
 *         OptimizedSortOperatorDescriptor will merge the generated runs, to
 *         generate the final sorted output of the data.
 */
public class OptimizedExternalSortRunGenerator implements IRunGenerator {
    private final IHyracksTaskContext ctx;
    private final int[] sortFields;
    private final INormalizedKeyComputer nkc;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDescriptor;
    private final List<IFrameReader> runs;

    private ISelectionTree sTree;
    private IMemoryManager memMgr;

    private final int memSize;
    private FrameTupleAccessor inputAccessor; // Used to read tuples in
                                              // nextFrame()
    private FrameTupleAppender outputAppender; // Used to write tuple to the
                                               // dedicated output buffer
    private ByteBuffer outputBuffer; // Dedicated output buffer to write tuples
                                     // into run(s)
    private FrameTupleAccessor lastRecordAccessor; // Used to read last output
                                                   // record from the output
                                                   // buffer
    private int lastTupleIx; // Holds index of last output tuple in the
                             // dedicated output buffer
    private Slot allocationPtr; // Contains the ptr to the allocated memory slot
                                // by the memory manager for the new tuple
    private Slot outputedTuple; // Contains the ptr to the next tuple chosen by
                                // the selectionTree to output
    private int[] sTreeTop;

    private RunFileWriter writer;

    private boolean newRun;
    private int curRunId;

    public OptimizedExternalSortRunGenerator(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, int memSize) {
        this.ctx = ctx;
        this.sortFields = sortFields;
        nkc = firstKeyNormalizerFactory == null ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();
        this.comparatorFactories = comparatorFactories;
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.recordDescriptor = recordDesc;
        this.runs = new LinkedList<IFrameReader>();
        this.memSize = memSize;
    }

    @Override
    public void open() throws HyracksDataException {
        runs.clear();
        inputAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputBuffer = ctx.allocateFrame();
        outputAppender.reset(outputBuffer, true);
        lastRecordAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);

        this.memMgr = new BSTMemMgr(ctx, memSize);
        this.sTree = new SortMinHeap(ctx, sortFields, comparatorFactories, recordDescriptor, memMgr);
        this.allocationPtr = new Slot();
        this.outputedTuple = new Slot();
        this.sTreeTop = new int[] { -1, -1, -1, -1 };
        curRunId = -1;
        openNewRun();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inputAccessor.reset(buffer);
        byte[] bufferArray = buffer.array();
        int tupleCount = inputAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            allocationPtr.clear();
            int tLength = inputAccessor.getTupleEndOffset(i) - inputAccessor.getTupleStartOffset(i);
            memMgr.allocate(tLength, allocationPtr);
            while (allocationPtr.isNull()) {
                int unAllocSize = -1;
                while (unAllocSize < tLength) {
                    unAllocSize = outputRecord();
                    if (unAllocSize < 1) {
                        throw new HyracksDataException(
                                "Unable to allocate space for the new tuple, while there is no more tuple to output");
                    }
                }
                memMgr.allocate(tLength, allocationPtr);
            }
            memMgr.writeTuple(allocationPtr.getFrameIx(), allocationPtr.getOffset(), inputAccessor, i);
            int runId = getRunId(inputAccessor, i);
            int pnk = getPNK(inputAccessor, i, bufferArray);
            int[] entry = new int[] { runId, allocationPtr.getFrameIx(), allocationPtr.getOffset(), pnk };
            sTree.insert(entry);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
    }

    @Override
    public void close() throws HyracksDataException {
        while (!sTree.isEmpty()) { // Outputting remaining elements in the
                                   // selectionTree
            outputRecord();
        }
        if (outputAppender.getTupleCount() > 0) { // Writing out very last
                                                  // resident records to file
            FrameUtils.flushFrame(outputBuffer, writer);
        }
        outputAppender.reset(outputBuffer, true);
        writer.close();
        runs.add(writer.createReader());
        memMgr.close();
    }

    public List<IFrameReader> getRuns() {
        return runs;
    }

    private int outputRecord() throws HyracksDataException {
        outputedTuple.clear();
        sTree.getMin(sTreeTop);
        if (!isEntryValid(sTreeTop)) {
            throw new HyracksDataException("Invalid outputed tuple (Top of the selection tree is invalid)");
        }

        if (sTreeTop[SortMinHeap.RUN_ID_IX] != curRunId) { // We need to switch
                                                           // runs
            openNewRun();
        }

        int tFrameIx = sTreeTop[SortMinHeap.FRAME_IX];
        int tOffset = sTreeTop[SortMinHeap.OFFSET_IX];
        if (!memMgr.readTuple(tFrameIx, tOffset, outputAppender)) { // Can not
                                                                    // append to
                                                                    // the
                                                                    // tupleAppender
            FrameUtils.flushFrame(outputBuffer, writer);
            outputAppender.reset(outputBuffer, true);
            if (!memMgr.readTuple(tFrameIx, tOffset, outputAppender)) {
                throw new HyracksDataException("Can not append to the ouput buffer in sort");
            }
            lastTupleIx = 0;
        } else {
            lastTupleIx++;
        }
        outputedTuple.set(tFrameIx, tOffset);
        newRun = false;
        return memMgr.unallocate(outputedTuple);

    }

    private int getPNK(FrameTupleAccessor fta, int tIx, byte[] buffInArray) { // Moved
                                                                              // buffInArray
                                                                              // out
                                                                              // for
                                                                              // better
                                                                              // performance
                                                                              // (not
                                                                              // converting
                                                                              // for
                                                                              // each
                                                                              // and
                                                                              // every
                                                                              // tuple)
        int sfIdx = sortFields[0];
        int tStart = fta.getTupleStartOffset(tIx);
        int f0StartRel = fta.getFieldStartOffset(tIx, sfIdx);
        int f0EndRel = fta.getFieldEndOffset(tIx, sfIdx);
        int f0Start = f0StartRel + tStart + fta.getFieldSlotsLength();
        return (nkc == null ? 0 : nkc.normalize(buffInArray, f0Start, f0EndRel - f0StartRel));
    }

    private int getRunId(FrameTupleAccessor fta, int tupIx) { // Comparing
                                                              // current
                                                              // record to
                                                              // last output
                                                              // record, it
                                                              // decides about
                                                              // current
                                                              // record's
                                                              // runId
        if (newRun) { // Very first record for a new run
            return curRunId;
        }

        byte[] lastRecBuff = outputBuffer.array();
        lastRecordAccessor.reset(outputBuffer);
        int lastStartOffset = lastRecordAccessor.getTupleStartOffset(lastTupleIx);

        ByteBuffer fr2 = fta.getBuffer();
        byte[] curRecBuff = fr2.array();
        int r2StartOffset = fta.getTupleStartOffset(tupIx);

        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : outputBuffer.getInt(lastStartOffset + (fIdx - 1) * 4);
            int f1End = outputBuffer.getInt(lastStartOffset + fIdx * 4);
            int s1 = lastStartOffset + lastRecordAccessor.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : fr2.getInt(r2StartOffset + (fIdx - 1) * 4);
            int f2End = fr2.getInt(r2StartOffset + fIdx * 4);
            int s2 = r2StartOffset + fta.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(lastRecBuff, s1, l1, curRecBuff, s2, l2);
            if (c != 0) {
                if (c <= 0) {
                    return curRunId;
                } else {
                    return (curRunId + 1);
                }
            }
        }
        return curRunId;
    }

    private void openNewRun() throws HyracksDataException {
        if (writer != null) { // There is a prev run, so flush its tuples and
                              // close it first
            if (outputAppender.getTupleCount() > 0) {
                FrameUtils.flushFrame(outputBuffer, writer);
            }
            outputAppender.reset(outputBuffer, true);
            writer.close();
            runs.add(writer.createReader());
        }

        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                ExternalSortRunGenerator.class.getSimpleName());
        writer = new RunFileWriter(file, ctx.getIOManager());
        writer.open();
        curRunId++;
        newRun = true;
        lastTupleIx = -1;
    }

    private boolean isEntryValid(int[] entry) {
        return ((entry[SortMinHeap.RUN_ID_IX] > -1) && (entry[SortMinHeap.FRAME_IX] > -1) && (entry[SortMinHeap.OFFSET_IX] > -1));
    }
}