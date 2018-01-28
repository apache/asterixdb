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
package org.apache.hyracks.dataflow.std.sort;

import java.util.Comparator;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameReader;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.utils.NormalizedKeyUtils;
import org.apache.hyracks.dataflow.std.sort.util.GroupFrameAccessor;
import org.apache.hyracks.dataflow.std.util.ReferenceEntry;
import org.apache.hyracks.dataflow.std.util.ReferencedPriorityQueue;

public class RunMergingFrameReader implements IFrameReader {
    private final IHyracksTaskContext ctx;
    private final List<? extends IFrameReader> runCursors;
    private final List<? extends IFrame> inFrames;
    private final int[] sortFields;
    private final IBinaryComparator[] comparators;
    private final INormalizedKeyComputer nmkComputer;
    private final int normalizedKeyLength;
    private final boolean normalizedKeyDecisive;
    private final RecordDescriptor recordDesc;
    private final int topK;
    private int tupleCount;
    private FrameTupleAppender outFrameAppender;
    private ReferencedPriorityQueue topTuples;
    private int[] tupleIndexes;
    private IFrameTupleAccessor[] tupleAccessors;

    public RunMergingFrameReader(IHyracksTaskContext ctx, List<? extends IFrameReader> runs,
            List<? extends IFrame> inFrames, int[] sortFields, IBinaryComparator[] comparators,
            INormalizedKeyComputer nmkComputer, RecordDescriptor recordDesc) {
        this(ctx, runs, inFrames, sortFields, comparators, nmkComputer, recordDesc, Integer.MAX_VALUE);
    }

    public RunMergingFrameReader(IHyracksTaskContext ctx, List<? extends IFrameReader> runs,
            List<? extends IFrame> inFrames, int[] sortFields, IBinaryComparator[] comparators,
            INormalizedKeyComputer nmkComputer, RecordDescriptor recordDesc, int topK) {
        this.ctx = ctx;
        this.runCursors = runs;
        this.inFrames = inFrames;
        this.sortFields = sortFields;
        this.comparators = comparators;
        this.nmkComputer = nmkComputer;
        this.normalizedKeyLength =
                nmkComputer != null ? nmkComputer.getNormalizedKeyProperties().getNormalizedKeyLength() : 0;
        // right now we didn't take multiple key normalizers for frame merger, since during this step it won't be
        // too many cache misses (merging multiple runs sequentially).
        // but still, we can apply a special optimization if there is only 1 sort field
        this.normalizedKeyDecisive = nmkComputer != null
                ? nmkComputer.getNormalizedKeyProperties().isDecisive() && comparators.length == 1 : false;
        this.recordDesc = recordDesc;
        this.topK = topK;
    }

    @Override
    public void open() throws HyracksDataException {
        tupleCount = 0;
        tupleAccessors = new IFrameTupleAccessor[runCursors.size()];
        outFrameAppender = new FrameTupleAppender();
        Comparator<ReferenceEntry> comparator = createEntryComparator(comparators);
        topTuples = new ReferencedPriorityQueue(runCursors.size(), comparator, sortFields, nmkComputer);
        tupleIndexes = new int[runCursors.size()];
        for (int i = 0; i < runCursors.size(); i++) {
            tupleIndexes[i] = 0;
            int runIndex = topTuples.peek().getRunid();
            runCursors.get(runIndex).open();
            if (runCursors.get(runIndex).nextFrame(inFrames.get(runIndex))) {
                tupleAccessors[runIndex] = new GroupFrameAccessor(ctx.getInitialFrameSize(), recordDesc);
                tupleAccessors[runIndex].reset(inFrames.get(runIndex).getBuffer());
                setNextTopTuple(runIndex, tupleIndexes, runCursors, inFrames, tupleAccessors, topTuples);
            } else {
                closeRun(runIndex, runCursors, tupleAccessors);
                topTuples.pop();
            }
        }
    }

    @Override
    public boolean nextFrame(IFrame outFrame) throws HyracksDataException {
        outFrameAppender.reset(outFrame, true);
        while (!topTuples.areRunsExhausted() && tupleCount < topK) {
            ReferenceEntry top = topTuples.peek();
            int runIndex = top.getRunid();
            IFrameTupleAccessor fta = top.getAccessor();
            int tupleIndex = top.getTupleIndex();

            if (!outFrameAppender.append(fta, tupleIndex)) {
                return true;
            } else {
                tupleCount++;
            }
            ++tupleIndexes[runIndex];
            setNextTopTuple(runIndex, tupleIndexes, runCursors, inFrames, tupleAccessors, topTuples);
        }

        if (outFrameAppender.getTupleCount() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < runCursors.size(); ++i) {
            closeRun(i, runCursors, tupleAccessors);
        }
    }

    private static void setNextTopTuple(int runIndex, int[] tupleIndexes, List<? extends IFrameReader> runCursors,
            List<? extends IFrame> inFrames, IFrameTupleAccessor[] tupleAccessors, ReferencedPriorityQueue topTuples)
            throws HyracksDataException {
        boolean exists = hasNextTuple(runIndex, tupleIndexes, runCursors, inFrames, tupleAccessors);
        if (exists) {
            topTuples.popAndReplace(tupleAccessors[runIndex], tupleIndexes[runIndex]);
        } else {
            topTuples.pop();
            closeRun(runIndex, runCursors, tupleAccessors);
        }
    }

    private static boolean hasNextTuple(int runIndex, int[] tupleIndexes, List<? extends IFrameReader> runCursors,
            List<? extends IFrame> inFrames, IFrameTupleAccessor[] tupleAccessors) throws HyracksDataException {
        if (tupleAccessors[runIndex] == null || runCursors.get(runIndex) == null) {
            return false;
        } else if (tupleIndexes[runIndex] >= tupleAccessors[runIndex].getTupleCount()) {
            IFrame frame = inFrames.get(runIndex);
            if (runCursors.get(runIndex).nextFrame(frame)) {
                tupleIndexes[runIndex] = 0;
                tupleAccessors[runIndex].reset(frame.getBuffer());
                return hasNextTuple(runIndex, tupleIndexes, runCursors, inFrames, tupleAccessors);
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private static void closeRun(int index, List<? extends IFrameReader> runCursors,
            IFrameTupleAccessor[] tupleAccessors) throws HyracksDataException {
        if (runCursors.get(index) != null) {
            runCursors.get(index).close();
            runCursors.set(index, null);
            tupleAccessors[index] = null;
        }
    }

    private Comparator<ReferenceEntry> createEntryComparator(final IBinaryComparator[] comparators) {
        return new Comparator<ReferenceEntry>() {
            @Override
            public int compare(ReferenceEntry tp1, ReferenceEntry tp2) {
                int[] tPointers1 = tp1.getTPointers();
                int[] tPointers2 = tp2.getTPointers();
                int cmp = NormalizedKeyUtils.compareNormalizeKeys(tPointers1, 0, tPointers2, 0, normalizedKeyLength);
                if (cmp != 0) {
                    return cmp;
                } else if (normalizedKeyDecisive) {
                    // we further compare the run id
                    return compareRun(tp1, tp2);
                }
                IFrameTupleAccessor fta1 = tp1.getAccessor();
                IFrameTupleAccessor fta2 = tp2.getAccessor();
                byte[] b1 = fta1.getBuffer().array();
                byte[] b2 = fta2.getBuffer().array();

                for (int f = 0; f < sortFields.length; ++f) {
                    int c;
                    try {
                        c = comparators[f].compare(b1, tPointers1[2 * f + normalizedKeyLength],
                                tPointers1[2 * f + normalizedKeyLength + 1], b2,
                                tPointers2[2 * f + normalizedKeyLength], tPointers2[2 * f + normalizedKeyLength + 1]);
                        if (c != 0) {
                            return c;
                        }
                    } catch (HyracksDataException e) {
                        throw new IllegalArgumentException(e);
                    }
                }
                return compareRun(tp1, tp2);
            }

            private int compareRun(ReferenceEntry tp1, ReferenceEntry tp2) {
                int runid1 = tp1.getRunid();
                int runid2 = tp2.getRunid();
                return runid1 < runid2 ? -1 : (runid1 == runid2 ? 0 : 1);
            }
        };
    }
}
