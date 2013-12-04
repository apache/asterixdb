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
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.util.IntSerDeUtils;

public class FrameSorterMergeSort implements IFrameSorter {
    private final IHyracksTaskContext ctx;
    private final int[] sortFields;
    private final INormalizedKeyComputer nkc;
    private final IBinaryComparator[] comparators;
    private final List<ByteBuffer> buffers;

    private final FrameTupleAccessor fta1;
    private final FrameTupleAccessor fta2;

    private final FrameTupleAppender appender;

    private final ByteBuffer outFrame;

    private int dataFrameCount;
    private int[] tPointers;
    private int[] tPointersTemp;
    private int tupleCount;

    public FrameSorterMergeSort(IHyracksTaskContext ctx, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        this.ctx = ctx;
        this.sortFields = sortFields;
        nkc = firstKeyNormalizerFactory == null ? null : firstKeyNormalizerFactory.createNormalizedKeyComputer();
        comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        buffers = new ArrayList<ByteBuffer>();
        fta1 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        fta2 = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        appender = new FrameTupleAppender(ctx.getFrameSize());
        outFrame = ctx.allocateFrame();

        dataFrameCount = 0;
    }

    @Override
    public void reset() {
        dataFrameCount = 0;
        tupleCount = 0;
    }

    @Override
    public int getFrameCount() {
        return dataFrameCount;
    }

    @Override
    public void insertFrame(ByteBuffer buffer) throws HyracksDataException {
        ByteBuffer copyFrame;
        if (dataFrameCount == buffers.size()) {
            copyFrame = ctx.allocateFrame();
            buffers.add(copyFrame);
        } else {
            copyFrame = buffers.get(dataFrameCount);
        }
        FrameUtils.copy(buffer, copyFrame);
        ++dataFrameCount;
    }

    @Override
    public void sortFrames() {
        int nBuffers = dataFrameCount;
        tupleCount = 0;
        for (int i = 0; i < nBuffers; ++i) {
            fta1.reset(buffers.get(i));
            tupleCount += fta1.getTupleCount();
        }
        int sfIdx = sortFields[0];
        tPointers = tPointers == null || tPointers.length < tupleCount * 4 ? new int[tupleCount * 4] : tPointers;
        int ptr = 0;
        for (int i = 0; i < nBuffers; ++i) {
            fta1.reset(buffers.get(i));
            int tCount = fta1.getTupleCount();
            byte[] array = fta1.getBuffer().array();
            for (int j = 0; j < tCount; ++j) {
                int tStart = fta1.getTupleStartOffset(j);
                int tEnd = fta1.getTupleEndOffset(j);
                tPointers[ptr * 4] = i;
                tPointers[ptr * 4 + 1] = tStart;
                tPointers[ptr * 4 + 2] = tEnd;
                int f0StartRel = fta1.getFieldStartOffset(j, sfIdx);
                int f0EndRel = fta1.getFieldEndOffset(j, sfIdx);
                int f0Start = f0StartRel + tStart + fta1.getFieldSlotsLength();
                tPointers[ptr * 4 + 3] = nkc == null ? 0 : nkc.normalize(array, f0Start, f0EndRel - f0StartRel);
                ++ptr;
            }
        }
        if (tupleCount > 0) {
            tPointersTemp = new int[tPointers.length];
            sort(0, tupleCount);
        }
    }

    @Override
    public void flushFrames(IFrameWriter writer) throws HyracksDataException {
        appender.reset(outFrame, true);
        for (int ptr = 0; ptr < tupleCount; ++ptr) {
            int i = tPointers[ptr * 4];
            int tStart = tPointers[ptr * 4 + 1];
            int tEnd = tPointers[ptr * 4 + 2];
            ByteBuffer buffer = buffers.get(i);
            fta1.reset(buffer);
            if (!appender.append(fta1, tStart, tEnd)) {
                FrameUtils.flushFrame(outFrame, writer);
                appender.reset(outFrame, true);
                if (!appender.append(fta1, tStart, tEnd)) {
                    throw new HyracksDataException("Record size (" + (tEnd - tStart) + ") larger than frame size ("
                            + appender.getBuffer().capacity() + ")");
                }
            }
        }
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outFrame, writer);
        }
    }

    private void sort(int offset, int length) {
        int step = 1;
        int len = length;
        int end = offset + len;
        /** bottom-up merge */
        while (step < len) {
            /** merge */
            for (int i = offset; i < end; i += 2 * step) {
                int next = i + step;
                if (next < end) {
                    merge(i, next, step, Math.min(step, end - next));
                } else {
                    System.arraycopy(tPointers, i * 4, tPointersTemp, i * 4, (end - i) * 4);
                }
            }
            /** prepare next phase merge */
            step *= 2;
            int[] tmp = tPointersTemp;
            tPointersTemp = tPointers;
            tPointers = tmp;
        }
    }

    /** Merge two subarrays into one */
    private void merge(int start1, int start2, int len1, int len2) {
        int targetPos = start1;
        int pos1 = start1;
        int pos2 = start2;
        int end1 = start1 + len1 - 1;
        int end2 = start2 + len2 - 1;
        while (pos1 <= end1 && pos2 <= end2) {
            int cmp = compare(pos1, pos2);
            if (cmp <= 0) {
                copy(pos1, targetPos);
                pos1++;
            } else {
                copy(pos2, targetPos);
                pos2++;
            }
            targetPos++;
        }
        if (pos1 <= end1) {
            int rest = end1 - pos1 + 1;
            System.arraycopy(tPointers, pos1 * 4, tPointersTemp, targetPos * 4, rest * 4);
        }
        if (pos2 <= end2) {
            int rest = end2 - pos2 + 1;
            System.arraycopy(tPointers, pos2 * 4, tPointersTemp, targetPos * 4, rest * 4);
        }
    }

    private void copy(int src, int dest) {
        tPointersTemp[dest * 4] = tPointers[src * 4];
        tPointersTemp[dest * 4 + 1] = tPointers[src * 4 + 1];
        tPointersTemp[dest * 4 + 2] = tPointers[src * 4 + 2];
        tPointersTemp[dest * 4 + 3] = tPointers[src * 4 + 3];
    }

    private int compare(int tp1, int tp2) {
        int i1 = tPointers[tp1 * 4];
        int j1 = tPointers[tp1 * 4 + 1];
        int v1 = tPointers[tp1 * 4 + 3];

        int tp2i = tPointers[tp2 * 4];
        int tp2j = tPointers[tp2 * 4 + 1];
        int tp2v = tPointers[tp2 * 4 + 3];

        if (v1 != tp2v) {
            return ((((long) v1) & 0xffffffffL) < (((long) tp2v) & 0xffffffffL)) ? -1 : 1;
        }
        int i2 = tp2i;
        int j2 = tp2j;
        ByteBuffer buf1 = buffers.get(i1);
        ByteBuffer buf2 = buffers.get(i2);
        byte[] b1 = buf1.array();
        byte[] b2 = buf2.array();
        fta1.reset(buf1);
        fta2.reset(buf2);
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(buf1.array(), j1 + (fIdx - 1) * 4);
            int f1End = IntSerDeUtils.getInt(buf1.array(), j1 + fIdx * 4);
            int s1 = j1 + fta1.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(buf2.array(), j2 + (fIdx - 1) * 4);
            int f2End = IntSerDeUtils.getInt(buf2.array(), j2 + fIdx * 4);
            int s2 = j2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @Override
    public void close() {
        this.buffers.clear();
    }
}
