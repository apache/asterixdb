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

public class FrameSorterQuickSort implements IFrameSorter {
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
    private int tupleCount;

    public FrameSorterQuickSort(IHyracksTaskContext ctx, int[] sortFields,
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
            sort(tPointers, 0, tupleCount);
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

    private void sort(int[] tPointers, int offset, int length) {
        int m = offset + (length >> 1);
        int mi = tPointers[m * 4];
        int mj = tPointers[m * 4 + 1];
        int mv = tPointers[m * 4 + 3];

        int a = offset;
        int b = a;
        int c = offset + length - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int cmp = compare(tPointers, b, mi, mj, mv);
                if (cmp > 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointers, a++, b);
                }
                ++b;
            }
            while (c >= b) {
                int cmp = compare(tPointers, c, mi, mj, mv);
                if (cmp < 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointers, c, d--);
                }
                --c;
            }
            if (b > c)
                break;
            swap(tPointers, b++, c--);
        }

        int s;
        int n = offset + length;
        s = Math.min(a - offset, b - a);
        vecswap(tPointers, offset, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswap(tPointers, b, n - s, s);

        if ((s = b - a) > 1) {
            sort(tPointers, offset, s);
        }
        if ((s = d - c) > 1) {
            sort(tPointers, n - s, s);
        }
    }

    private void swap(int x[], int a, int b) {
        for (int i = 0; i < 4; ++i) {
            int t = x[a * 4 + i];
            x[a * 4 + i] = x[b * 4 + i];
            x[b * 4 + i] = t;
        }
    }

    private void vecswap(int x[], int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(x, a, b);
        }
    }

    private int compare(int[] tPointers, int tp1, int tp2i, int tp2j, int tp2v) {
        int i1 = tPointers[tp1 * 4];
        int j1 = tPointers[tp1 * 4 + 1];
        int v1 = tPointers[tp1 * 4 + 3];
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
            int f1Start = fIdx == 0 ? 0 : buf1.getInt(j1 + (fIdx - 1) * 4);
            int f1End = buf1.getInt(j1 + fIdx * 4);
            int s1 = j1 + fta1.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : buf2.getInt(j2 + (fIdx - 1) * 4);
            int f2End = buf2.getInt(j2 + fIdx * 4);
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