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

package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public class TreeTupleSorter extends EnforcedIndexCursor implements ITreeIndexCursor {
    private final static int INITIAL_SIZE = 1000000;
    private int numTuples;
    private int currentTupleIndex;
    private int[] tPointers;
    private IBufferCache bufferCache;
    private final ITreeIndexFrame leafFrame1;
    private final ITreeIndexFrame leafFrame2;
    private ITreeIndexTupleReference frameTuple1;
    private ITreeIndexTupleReference frameTuple2;
    private final int fileId;
    private final static int ARRAY_GROWTH = 1000000; // Must be at least of size 2
    private final int[] comparatorFields;
    private final MultiComparator cmp;

    public TreeTupleSorter(int fileId, IBinaryComparatorFactory[] comparatorFactories, ITreeIndexFrame leafFrame1,
            ITreeIndexFrame leafFrame2, IBufferCache bufferCache, int[] comparatorFields) {
        this.fileId = fileId;
        this.leafFrame1 = leafFrame1;
        this.leafFrame2 = leafFrame2;
        this.bufferCache = bufferCache;
        this.comparatorFields = comparatorFields;
        tPointers = new int[INITIAL_SIZE * 2];
        frameTuple1 = leafFrame1.createTupleReference();
        frameTuple2 = leafFrame2.createTupleReference();
        currentTupleIndex = 0;
        cmp = MultiComparator.create(comparatorFactories);
    }

    @Override
    public void doClose() {
        numTuples = 0;
        currentTupleIndex = 0;
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        if (numTuples <= currentTupleIndex) {
            return false;
        }
        // We don't latch pages since this code is only used by flush () before
        // bulk-loading the r-tree to disk and flush is not concurrent.
        //
        ICachedPage node1 =
                bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, tPointers[currentTupleIndex * 2]), false);
        try {
            leafFrame1.setPage(node1);
            frameTuple1.resetByTupleOffset(leafFrame1.getBuffer().array(), tPointers[currentTupleIndex * 2 + 1]);
        } finally {
            bufferCache.unpin(node1);
        }
        return true;
    }

    @Override
    public void doNext() {
        currentTupleIndex++;
    }

    @Override
    public ITupleReference doGetTuple() {
        return frameTuple1;
    }

    public void insertTupleEntry(int pageId, int tupleOffset) {
        if (numTuples * 2 == tPointers.length) {
            int[] newData = new int[tPointers.length + ARRAY_GROWTH];
            System.arraycopy(tPointers, 0, newData, 0, tPointers.length);
            tPointers = newData;
        }

        tPointers[numTuples * 2] = pageId;
        tPointers[numTuples * 2 + 1] = tupleOffset;
        numTuples++;
    }

    public void sort() throws HyracksDataException {
        sort(tPointers, 0, numTuples);
    }

    private void sort(int[] tPointers, int offset, int length) throws HyracksDataException {
        int m = offset + (length >> 1);
        int mi = tPointers[m * 2];
        int mj = tPointers[m * 2 + 1];

        int a = offset;
        int b = a;
        int c = offset + length - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int cmp = compare(tPointers, b, mi, mj);
                if (cmp > 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointers, a++, b);
                }
                ++b;
            }
            while (c >= b) {
                int cmp = compare(tPointers, c, mi, mj);
                if (cmp < 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointers, c, d--);
                }
                --c;
            }
            if (b > c) {
                break;
            }
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
        for (int i = 0; i < 2; ++i) {
            int t = x[a * 2 + i];
            x[a * 2 + i] = x[b * 2 + i];
            x[b * 2 + i] = t;
        }
    }

    private void vecswap(int x[], int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(x, a, b);
        }
    }

    private int compare(int[] tPointers, int tp1, int tp2i, int tp2j) throws HyracksDataException {
        int i1 = tPointers[tp1 * 2];
        int j1 = tPointers[tp1 * 2 + 1];
        int i2 = tp2i;
        int j2 = tp2j;
        ICachedPage node1 = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, i1), false);
        try {
            leafFrame1.setPage(node1);
            ICachedPage node2 = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, i2), false);
            try {
                leafFrame2.setPage(node2);
                frameTuple1.resetByTupleOffset(leafFrame1.getBuffer().array(), j1);
                frameTuple2.resetByTupleOffset(leafFrame2.getBuffer().array(), j2);
                return cmp.selectiveFieldCompare(frameTuple1, frameTuple2, comparatorFields);
            } finally {
                bufferCache.unpin(node2);
            }
        } finally {
            bufferCache.unpin(node1);
        }
    }

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        // do nothing
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        // do nothing
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        // do nothing
    }

    @Override
    public void setFileId(int fileId) {
        // do nothing
    }

    @Override
    public boolean isExclusiveLatchNodes() {
        return false;
    }
}
