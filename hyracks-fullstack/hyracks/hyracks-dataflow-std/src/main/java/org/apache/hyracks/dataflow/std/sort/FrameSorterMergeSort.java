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

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;

public class FrameSorterMergeSort extends AbstractFrameSorter {

    private int[] tPointersTemp;

    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor)
            throws HyracksDataException {
        this(ctx, bufferManager, maxSortFrames, sortFields, keyNormalizerFactories, comparatorFactories,
                recordDescriptor, Integer.MAX_VALUE);
    }

    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, int outputLimit)
            throws HyracksDataException {
        super(ctx, bufferManager, maxSortFrames, sortFields, keyNormalizerFactories, comparatorFactories,
                recordDescriptor, outputLimit);
    }

    @Override
    void sortTupleReferences() throws HyracksDataException {
        if (tPointersTemp == null || tPointersTemp.length < tPointers.length) {
            tPointersTemp = new int[tPointers.length];
        }
        sort(0, tupleCount);
    }

    @Override
    protected long getRequiredMemory(FrameTupleAccessor frameAccessor) {
        return super.getRequiredMemory(frameAccessor) + ptrSize * frameAccessor.getTupleCount() * Integer.BYTES;
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
                    copy(tPointers, i, tPointersTemp, i, end - i);
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
                copy(tPointers, pos1, tPointersTemp, targetPos);
                pos1++;
            } else {
                copy(tPointers, pos2, tPointersTemp, targetPos);
                pos2++;
            }
            targetPos++;
        }
        if (pos1 <= end1) {
            int rest = end1 - pos1 + 1;
            copy(tPointers, pos1, tPointersTemp, targetPos, rest);
        }
        if (pos2 <= end2) {
            int rest = end2 - pos2 + 1;
            copy(tPointers, pos2, tPointersTemp, targetPos, rest);
        }
    }

    private void copy(int src[], int srcPos, int dest[], int destPos) {
        System.arraycopy(src, srcPos * ptrSize, dest, destPos * ptrSize, ptrSize);
    }

    private void copy(int src[], int srcPos, int dest[], int destPos, int n) {
        System.arraycopy(src, srcPos * ptrSize, dest, destPos * ptrSize, n * ptrSize);
    }
}
