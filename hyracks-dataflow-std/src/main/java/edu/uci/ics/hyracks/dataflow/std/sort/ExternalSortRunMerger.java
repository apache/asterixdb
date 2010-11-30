/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

public class ExternalSortRunMerger {
    private final IHyracksContext ctx;
    private final FrameSorter frameSorter;
    private final List<File> runs;
    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDesc;
    private final int framesLimit;
    private final IFrameWriter writer;
    private List<ByteBuffer> inFrames;
    private ByteBuffer outFrame;
    private FrameTupleAppender outFrameAppender;

    public ExternalSortRunMerger(IHyracksContext ctx, FrameSorter frameSorter, List<File> runs, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDesc, int framesLimit,
            IFrameWriter writer) {
        this.ctx = ctx;
        this.frameSorter = frameSorter;
        this.runs = runs;
        this.sortFields = sortFields;
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.recordDesc = recordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;
    }

    public void process(boolean doFinalPass) throws HyracksDataException {
        if (doFinalPass) {
            writer.open();
        }
        try {
            if (runs.size() <= 0) {
                if (frameSorter != null) {
                    frameSorter.flushFrames(writer);
                }
            } else {
                inFrames = new ArrayList<ByteBuffer>();
                outFrame = ctx.getResourceManager().allocateFrame();
                outFrameAppender = new FrameTupleAppender(ctx);
                outFrameAppender.reset(outFrame, true);
                for (int i = 0; i < framesLimit - 1; ++i) {
                    inFrames.add(ctx.getResourceManager().allocateFrame());
                }
                int passCount = 0;
                while (runs.size() > 0) {
                    passCount++;
                    try {
                        doPass(runs, passCount, doFinalPass);
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
            }
        } finally {
            if (doFinalPass) {
                writer.close();
            }
        }
    }

    public void process() throws HyracksDataException {
        process(true);
    }

    // creates a new run from runs that can fit in memory.
    private void doPass(List<File> runs, int passCount, boolean doFinalPass) throws HyracksDataException, IOException {
        File newRun = null;
        IFrameWriter writer = this.writer;
        boolean finalPass = false;
        if (runs.size() + 1 <= framesLimit) { // + 1 outFrame
            if (!doFinalPass) {
                return;
            }
            finalPass = true;
            for (int i = inFrames.size() - 1; i >= runs.size(); i--) {
                inFrames.remove(i);
            }
        } else {
            newRun = ctx.getResourceManager().createFile(ExternalSortOperatorDescriptor.class.getSimpleName(), ".run");
            writer = new RunFileWriter(newRun);
            writer.open();
        }
        try {
            RunFileReader[] runCursors = new RunFileReader[inFrames.size()];
            FrameTupleAccessor[] tupleAccessors = new FrameTupleAccessor[inFrames.size()];
            Comparator<ReferenceEntry> comparator = createEntryComparator(comparators);
            ReferencedPriorityQueue topTuples = new ReferencedPriorityQueue(ctx, recordDesc, inFrames.size(),
                    comparator);
            int[] tupleIndexes = new int[inFrames.size()];
            for (int i = 0; i < inFrames.size(); i++) {
                tupleIndexes[i] = 0;
                int runIndex = topTuples.peek().getRunid();
                runCursors[runIndex] = new RunFileReader(runs.get(runIndex));
                runCursors[runIndex].open();
                if (runCursors[runIndex].nextFrame(inFrames.get(runIndex))) {
                    tupleAccessors[runIndex] = new FrameTupleAccessor(ctx, recordDesc);
                    tupleAccessors[runIndex].reset(inFrames.get(runIndex));
                    setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);
                } else {
                    closeRun(runIndex, runCursors, tupleAccessors);
                }
            }

            while (!topTuples.areRunsExhausted()) {
                ReferenceEntry top = topTuples.peek();
                int runIndex = top.getRunid();
                FrameTupleAccessor fta = top.getAccessor();
                int tupleIndex = top.getTupleIndex();

                if (!outFrameAppender.append(fta, tupleIndex)) {
                    FrameUtils.flushFrame(outFrame, writer);
                    outFrameAppender.reset(outFrame, true);
                    if (!outFrameAppender.append(fta, tupleIndex)) {
                        throw new IllegalStateException();
                    }
                }

                ++tupleIndexes[runIndex];
                setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);
            }
            if (outFrameAppender.getTupleCount() > 0) {
                FrameUtils.flushFrame(outFrame, writer);
                outFrameAppender.reset(outFrame, true);
            }
            runs.subList(0, inFrames.size()).clear();
            if (!finalPass) {
                runs.add(0, newRun);
            }
        } finally {
            if (!finalPass) {
                writer.close();
            }
        }
    }

    private void setNextTopTuple(int runIndex, int[] tupleIndexes, RunFileReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors, ReferencedPriorityQueue topTuples) throws IOException {
        boolean exists = hasNextTuple(runIndex, tupleIndexes, runCursors, tupleAccessors);
        if (exists) {
            topTuples.popAndReplace(tupleAccessors[runIndex], tupleIndexes[runIndex]);
        } else {
            topTuples.pop();
            closeRun(runIndex, runCursors, tupleAccessors);
        }
    }

    private boolean hasNextTuple(int runIndex, int[] tupleIndexes, RunFileReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors) throws IOException {
        if (tupleAccessors[runIndex] == null || runCursors[runIndex] == null) {
            return false;
        } else if (tupleIndexes[runIndex] >= tupleAccessors[runIndex].getTupleCount()) {
            ByteBuffer buf = tupleAccessors[runIndex].getBuffer(); // same-as-inFrames.get(runIndex)
            if (runCursors[runIndex].nextFrame(buf)) {
                tupleIndexes[runIndex] = 0;
                return hasNextTuple(runIndex, tupleIndexes, runCursors, tupleAccessors);
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private void closeRun(int index, RunFileReader[] runCursors, IFrameTupleAccessor[] tupleAccessor)
            throws HyracksDataException {
        runCursors[index].close();
        runCursors[index] = null;
        tupleAccessor[index] = null;
    }

    private Comparator<ReferenceEntry> createEntryComparator(final IBinaryComparator[] comparators) {
        return new Comparator<ReferenceEntry>() {
            public int compare(ReferenceEntry tp1, ReferenceEntry tp2) {
                FrameTupleAccessor fta1 = (FrameTupleAccessor) tp1.getAccessor();
                FrameTupleAccessor fta2 = (FrameTupleAccessor) tp2.getAccessor();
                int j1 = tp1.getTupleIndex();
                int j2 = tp2.getTupleIndex();
                byte[] b1 = fta1.getBuffer().array();
                byte[] b2 = fta2.getBuffer().array();
                for (int f = 0; f < sortFields.length; ++f) {
                    int fIdx = sortFields[f];
                    int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength()
                            + fta1.getFieldStartOffset(j1, fIdx);
                    int l1 = fta1.getFieldEndOffset(j1, fIdx) - fta1.getFieldStartOffset(j1, fIdx);
                    int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength()
                            + fta2.getFieldStartOffset(j2, fIdx);
                    int l2 = fta2.getFieldEndOffset(j2, fIdx) - fta2.getFieldStartOffset(j2, fIdx);
                    int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            }
        };
    }
}