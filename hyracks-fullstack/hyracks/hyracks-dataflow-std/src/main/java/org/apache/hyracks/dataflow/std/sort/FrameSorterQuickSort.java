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
import org.apache.hyracks.dataflow.std.buffermanager.IFrameBufferManager;

class FrameSorterQuickSort extends AbstractFrameSorter {

    FrameSorterQuickSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int maxSortFrames,
            int[] sortFields, INormalizedKeyComputerFactory[] keyNormalizerFactories,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, int outputLimit)
            throws HyracksDataException {
        super(ctx, bufferManager, maxSortFrames, sortFields, keyNormalizerFactories, comparatorFactories,
                recordDescriptor, outputLimit);
    }

    @Override
    void sortTupleReferences() throws HyracksDataException {
        sort(0, tupleCount);
    }

    private void sort(int offset, int length) throws HyracksDataException {
        int m = offset + (length >> 1);

        int a = offset;
        int b = a;
        int c = offset + length - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int cmp = compare(b, m);
                if (cmp > 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointers, a++, tPointers, b);
                }
                ++b;
            }
            while (c >= b) {
                int cmp = compare(c, m);
                if (cmp < 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointers, c, tPointers, d--);
                }
                --c;
            }
            if (b > c)
                break;
            swap(tPointers, b++, tPointers, c--);
        }

        int s;
        int n = offset + length;
        s = Math.min(a - offset, b - a);
        vecswap(tPointers, offset, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswap(tPointers, b, n - s, s);

        if ((s = b - a) > 1) {
            sort(offset, s);
        }
        if ((s = d - c) > 1) {
            sort(n - s, s);
        }
    }

    private void vecswap(int x[], int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(x, a, x, b);
        }
    }

    private void swap(int pointers1[], int pos1, int pointers2[], int pos2) {
        System.arraycopy(pointers1, pos1 * ptrSize, tmpPointer, 0, ptrSize);
        System.arraycopy(pointers2, pos2 * ptrSize, pointers1, pos1 * ptrSize, ptrSize);
        System.arraycopy(tmpPointer, 0, pointers2, pos2 * ptrSize, ptrSize);
    }
}
