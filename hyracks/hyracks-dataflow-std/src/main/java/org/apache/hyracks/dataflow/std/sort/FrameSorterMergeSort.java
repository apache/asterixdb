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

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.util.IntSerDeUtils;
import org.apache.hyracks.dataflow.std.sort.buffermanager.IFrameBufferManager;

public class FrameSorterMergeSort extends AbstractFrameSorter {

    private int[] tPointersTemp;
    private FrameTupleAccessor fta2;

    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        this(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                Integer.MAX_VALUE);
    }

    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, int outputLimit) throws HyracksDataException {
        super(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                outputLimit);
        fta2 = new FrameTupleAccessor(recordDescriptor);
    }

    @Override
    void sortTupleReferences() throws HyracksDataException {
        if (tPointersTemp == null || tPointersTemp.length < tPointers.length) {
            tPointersTemp = new int[tPointers.length];
        }
        sort(0, tupleCount);
    }

    @Override
    public void close() {
        super.close();
        tPointersTemp = null;
    }

    void sort(int offset, int length) throws HyracksDataException {
        int step = 1;
        int end = offset + length;
        /** bottom-up merge */
        while (step < length) {
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

    /**
     * Merge two subarrays into one
     */
    private void merge(int start1, int start2, int len1, int len2) throws HyracksDataException {
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

    private int compare(int tp1, int tp2) throws HyracksDataException {
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
        ByteBuffer buf1 = super.bufferManager.getFrame(i1);
        ByteBuffer buf2 = super.bufferManager.getFrame(i2);
        byte[] b1 = buf1.array();
        byte[] b2 = buf2.array();
        inputTupleAccessor.reset(buf1);
        fta2.reset(buf2);
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(b1, j1 + (fIdx - 1) * 4);
            int f1End = IntSerDeUtils.getInt(b1, j1 + fIdx * 4);
            int s1 = j1 + inputTupleAccessor.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(b2, j2 + (fIdx - 1) * 4);
            int f2End = IntSerDeUtils.getInt(b2, j2 + fIdx * 4);
            int s2 = j2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

}
