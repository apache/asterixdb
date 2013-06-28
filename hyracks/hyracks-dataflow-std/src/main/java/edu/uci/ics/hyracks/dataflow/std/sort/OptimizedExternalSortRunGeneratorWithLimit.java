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
 * @author pouria
 *         This class implements the run generator for sorting with replacement
 *         selection, where there is a limit on the output, i.e. we are looking
 *         for top-k tuples (first k smallest tuples w.r.t sorting keys).
 *         A SortMinMaxHeap is used as the selectionTree to decide the order of
 *         writing tuples into the runs, and also to prune tuples (if possible).
 *         Memory manager is based on a binary search tree and is used to
 *         allocate memory slots for tuples.
 *         The overall process is as follows (Assuming that the limit is K):
 *         - Read the input data frame by frame. For each tuple T in the current
 *         frame:
 *         - If currentRun R has reached the limit of K on the size, and (T >
 *         maximum tuple of R), then ignore T.
 *         - Otherwise, try to allocate a memory slot for writing T along with
 *         the attached header/footer (for memory management purpose)
 *         - If T can not be allocated, try to output as many tuples, currently
 *         resident in memory, as needed so that a free slot, large enough to
 *         hold T, gets created. MinMaxHeap decides about which tuple should be
 *         sent to the output at each step.
 *         - Write T into memory.
 *         - Calculate the runID of T (based on the last output tuple for the
 *         current run). It is either the current run or the next run. Also
 *         calculate Poorman's Normalized Key (PNK) for T, to make comparisons
 *         faster later.
 *         - Create an heap element for T, containing its runID, the slot ptr to
 *         its memory location, and its PNK.
 *         - If runID is the nextRun, insert the heap element into the heap, and
 *         increment the size of nextRun.
 *         - If runID is the currentRun, then:
 *         - If currentRun has not hit the limit of k, insert the element into
 *         the heap, and increase currentRun size. - Otherwise, currentRun has
 *         hit the limit of K, while T is less than the max. So discard the
 *         current max for the current run (by poping it from the heap and
 *         unallocating its memory location) and insert the heap element into
 *         the heap. No need to change the currentRun size as we are replacing
 *         an old element (the old max) with T.
 *         - Upon closing, write all the tuples, currently resident in memory,
 *         into their corresponding run(s).
 *         - Note that upon opening a new Run R, if size of R (based on stats)
 *         is S and (S > K), then (S-K) current maximum tuples of R (which are
 *         resident in memory) get discarded at the beginning. MinMax heap can
 *         be used to find these tuples.
 */
public class OptimizedExternalSortRunGeneratorWithLimit implements IRunGenerator {

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
    private FrameTupleAccessor fta2; // Used to read max record
    private final int outputLimit;
    private int curRunSize;
    private int nextRunSize;
    private int lastTupleIx; // Holds index of last output tuple in the
                             // dedicated output buffer
    private Slot allocationPtr; // Contains the ptr to the allocated memory slot
                                // by the memory manager for the new tuple
    private Slot outputedTuple; // Contains the ptr to the next tuple chosen by
                                // the selectionTree to output
    private Slot discard;
    private int[] sTreeTop;
    private int[] peek;
    private RunFileWriter writer;
    private boolean newRun;
    private int curRunId;

    public OptimizedExternalSortRunGeneratorWithLimit(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDesc, int memSize, int limit) {

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

        this.outputLimit = limit;
    }

    @Override
    public void open() throws HyracksDataException {
        runs.clear();
        inputAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        outputAppender = new FrameTupleAppender(ctx.getFrameSize());
        outputBuffer = ctx.allocateFrame();
        outputAppender.reset(outputBuffer, true);
        lastRecordAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        fta2 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        this.memMgr = new BSTMemMgr(ctx, memSize);
        this.sTree = new SortMinMaxHeap(ctx, sortFields, comparatorFactories, recordDescriptor, memMgr);
        this.allocationPtr = new Slot();
        this.outputedTuple = new Slot();
        this.sTreeTop = new int[] { -1, -1, -1, -1 };
        this.peek = new int[] { -1, -1, -1, -1 };
        this.discard = new Slot();

        curRunId = -1;
        curRunSize = 0;
        nextRunSize = 0;
        openNewRun();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inputAccessor.reset(buffer);
        byte[] bufferArray = buffer.array();
        int tupleCount = inputAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            if (curRunSize >= outputLimit) {
                sTree.peekMax(peek);
                if (isEntryValid(peek)
                        && compareRecords(inputAccessor, i, peek[SortMinMaxHeap.FRAME_IX],
                                peek[SortMinMaxHeap.OFFSET_IX]) >= 0) {
                    continue;
                }
            }

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

            int pnk = getPNK(inputAccessor, i, bufferArray);
            int runId = getRunId(inputAccessor, i);
            if (runId != curRunId) { // tuple belongs to the next run
                memMgr.writeTuple(allocationPtr.getFrameIx(), allocationPtr.getOffset(), inputAccessor, i);
                int[] entry = new int[] { runId, allocationPtr.getFrameIx(), allocationPtr.getOffset(), pnk };
                sTree.insert(entry);
                nextRunSize++;
                continue;
            }
            // belongs to the current run
            if (curRunSize < outputLimit) {
                memMgr.writeTuple(allocationPtr.getFrameIx(), allocationPtr.getOffset(), inputAccessor, i);
                int[] entry = new int[] { runId, allocationPtr.getFrameIx(), allocationPtr.getOffset(), pnk };
                sTree.insert(entry);
                curRunSize++;
                continue;
            }

            sTree.peekMax(peek);
            if (compareRecords(inputAccessor, i, peek[SortMinMaxHeap.FRAME_IX], peek[SortMinMaxHeap.OFFSET_IX]) > 0) {
                continue;
            }
            // replacing the max
            sTree.getMax(peek);
            discard.set(peek[SortMinMaxHeap.FRAME_IX], peek[SortMinMaxHeap.OFFSET_IX]);
            memMgr.unallocate(discard);
            memMgr.writeTuple(allocationPtr.getFrameIx(), allocationPtr.getOffset(), inputAccessor, i);
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
            outputRecordForClose();
        }

        if (outputAppender.getTupleCount() > 0) { // Writing out very last
                                                  // resident records to file
            FrameUtils.flushFrame(outputBuffer, writer);
        }

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
        int tFrameIx = sTreeTop[SortMinHeap.FRAME_IX];
        int tOffset = sTreeTop[SortMinHeap.OFFSET_IX];
        if (sTreeTop[SortMinMaxHeap.RUN_ID_IX] == curRunId) {
            if (!memMgr.readTuple(tFrameIx, tOffset, outputAppender)) { // Can
                                                                        // not
                                                                        // append
                                                                        // to
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
        // Minimum belongs to the next Run
        openNewRun();
        int popCount = curRunSize - outputLimit;
        int l = 0;
        int maxFreedSpace = 0;
        for (int p = 0; p < popCount; p++) {
            sTree.getMax(peek);
            if (!isEntryValid(peek)) {
                throw new HyracksDataException("Invalid Maximum extracted from MinMaxHeap");
            }
            discard.set(peek[SortMinMaxHeap.FRAME_IX], peek[SortMinMaxHeap.OFFSET_IX]);
            l = memMgr.unallocate(discard);
            if (l > maxFreedSpace) {
                maxFreedSpace = l;
            }
            curRunSize--;
        }

        if (maxFreedSpace != 0) {
            return maxFreedSpace;
        }
        // No max discarded (We just flushed out the prev run, so the output
        // buffer should be clear)
        if (!memMgr.readTuple(tFrameIx, tOffset, outputAppender)) { // Can not
                                                                    // append to
                                                                    // the
                                                                    // tupleAppender
            throw new HyracksDataException("Can not append to the ouput buffer in sort");
        }
        lastTupleIx = 0;
        outputedTuple.set(tFrameIx, tOffset);
        newRun = false;
        return memMgr.unallocate(outputedTuple);
    }

    private void outputRecordForClose() throws HyracksDataException {
        sTree.getMin(sTreeTop);
        if (!isEntryValid(sTreeTop)) {
            throw new HyracksDataException("Invalid outputed tuple (Top of the selection tree is invalid)");
        }
        int tFrameIx = sTreeTop[SortMinHeap.FRAME_IX];
        int tOffset = sTreeTop[SortMinHeap.OFFSET_IX];
        if (sTreeTop[SortMinMaxHeap.RUN_ID_IX] != curRunId) {
            openNewRun();
        }

        if (!memMgr.readTuple(tFrameIx, tOffset, outputAppender)) { // Can not
                                                                    // append to
                                                                    // the
                                                                    // tupleAppender
            FrameUtils.flushFrame(outputBuffer, writer);
            outputAppender.reset(outputBuffer, true);
            if (!memMgr.readTuple(tFrameIx, tOffset, outputAppender)) {
                throw new HyracksDataException("Can not append to the ouput buffer in sort");
            }
        }
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

    // first<sec : -1
    private int compareRecords(FrameTupleAccessor fta1, int ix1, int fix2, int offset2) {
        ByteBuffer buff1 = fta1.getBuffer();
        byte[] recBuff1 = buff1.array();
        int offset1 = fta1.getTupleStartOffset(ix1);

        offset2 += BSTNodeUtil.HEADER_SIZE;
        ByteBuffer buff2 = memMgr.getFrame(fix2);
        fta2.reset(buff2);
        byte[] recBuff2 = buff2.array();

        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : buff1.getInt(offset1 + (fIdx - 1) * 4);
            int f1End = buff1.getInt(offset1 + fIdx * 4);
            int s1 = offset1 + fta1.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : buff2.getInt(offset2 + (fIdx - 1) * 4);
            int f2End = buff2.getInt(offset2 + fIdx * 4);
            int s2 = offset2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(recBuff1, s1, l1, recBuff2, s2, l2);

            if (c != 0) {
                return c;
            }
        }
        return 0;

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
        curRunSize = nextRunSize;
        nextRunSize = 0;
        lastTupleIx = -1;
    }

    private boolean isEntryValid(int[] entry) {
        return ((entry[SortMinHeap.RUN_ID_IX] > -1) && (entry[SortMinHeap.FRAME_IX] > -1) && (entry[SortMinHeap.OFFSET_IX] > -1));
    }
}