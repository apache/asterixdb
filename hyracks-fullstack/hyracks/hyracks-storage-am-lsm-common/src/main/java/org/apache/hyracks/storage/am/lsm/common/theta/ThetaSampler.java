/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.lsm.common.theta;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.storage.common.IComponentSampler;

import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongHeapPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongPriorityQueue;

/**
 * Implements a Theta Sketch sampler for LSM disk components using the K-Minimum Values (KMV) algorithm.
 */
public class ThetaSampler implements IComponentSampler {

    public static IComponentSampler createSampler(int[] keyFields, int k) {
        return new ThetaSampler(keyFields, k);
    }

    public static final int DEFAULT_K = 1024;

    private final int k;
    private final ArrayBackedValueStorage serializedTheta;

    private final LongPriorityQueue insertHeap;
    private final LongPriorityQueue deleteHeap;

    // O(1) tracking structures to ensure strict distinct hash counts (preventing duplicates)
    private final LongOpenHashSet insertSet;
    private final LongOpenHashSet deleteSet;

    private final int[] keyFields;
    private final long[] hashes;

    public static final LongComparator thresholdComparator = (f, s) -> Long.compare(s, f);

    private long insertThetaLimit = Long.MAX_VALUE;
    private long deleteThetaLimit = Long.MAX_VALUE;
    private static final long SEED = 0L;

    public ThetaSampler(int[] keyFields) {
        this(keyFields, DEFAULT_K);
    }

    public ThetaSampler(int[] keyFields, int k) {
        this.k = k;
        this.insertHeap = new LongHeapPriorityQueue(k, thresholdComparator);
        this.deleteHeap = new LongHeapPriorityQueue(k, thresholdComparator);

        // Capacity k + 1 to hold elements before eviction without resizing
        this.insertSet = new LongOpenHashSet(k + 1);
        this.deleteSet = new LongOpenHashSet(k + 1);

        this.keyFields = keyFields;
        this.serializedTheta = new ArrayBackedValueStorage();
        this.hashes = new long[2];
    }

    public void addTuple(ITupleReference tuple) {
        long hash = computeHash(tuple);
        boolean isAntiMatter =
                tuple instanceof ILSMTreeTupleReference && ((ILSMTreeTupleReference) tuple).isAntimatter();

        if (isAntiMatter) {
            updateHeap(deleteHeap, deleteSet, hash, deleteThetaLimit, true);
        } else {
            updateHeap(insertHeap, insertSet, hash, insertThetaLimit, false);
        }
    }

    private long computeHash(ITupleReference tuple) {
        MurmurHash128Bit.hash3_x64_128(tuple, keyFields, SEED, hashes);
        return (hashes[0] ^ hashes[1]) & 0x7FFFFFFFFFFFFFFFL;
    }

    private void updateHeap(LongPriorityQueue heap, LongOpenHashSet set, long hash, long limit, boolean isDelete) {
        if (heap.size() >= k && hash >= limit) {
            return;
        }

        // Strict distinct item requirement: skip duplicates
        if (!set.add(hash)) {
            return;
        }
        heap.enqueue(hash);

        if (heap.size() > k) {
            long removedHash = heap.dequeueLong();
            set.remove(removedHash); // Keep parallel set synchronized

            long newLimit = heap.firstLong();
            if (isDelete) {
                deleteThetaLimit = newLimit;
            } else {
                insertThetaLimit = newLimit;
            }
        }
    }

    // Non-destructive: serializePQ dequeues into a temp array and re-enqueues, so the heaps are left intact
    // and this sampler instance remains usable after serialization.
    @Override
    public IValueReference serialize() throws IOException {
        serializedTheta.reset();
        DataOutput out = serializedTheta.getDataOutput();
        out.writeInt(k);
        serializePQ(insertHeap, out);
        serializePQ(deleteHeap, out);
        return serializedTheta;
    }

    public static ThetaEstimator.ComponentStats deserialize(ArrayBackedValueStorage thetaStorage) {
        int start = thetaStorage.getStartOffset();
        byte[] thetaBytes = thetaStorage.getByteArray();
        int K = IntegerPointable.getInteger(thetaBytes, start);
        start += Integer.BYTES;

        int insertCount = IntegerPointable.getInteger(thetaBytes, start);
        start += Integer.BYTES;

        long[] insertSamples = new long[insertCount];
        for (int i = 0; i < insertCount; i++) {
            insertSamples[i] = LongPointable.getLong(thetaBytes, start);
            start += Long.BYTES;
        }

        int deleteCount = IntegerPointable.getInteger(thetaBytes, start);
        start += Integer.BYTES;

        long[] deleteSamples = new long[deleteCount];
        for (int i = 0; i < deleteCount; i++) {
            deleteSamples[i] = LongPointable.getLong(thetaBytes, start);
            start += Long.BYTES;
        }

        // Removed Arrays.sort() calls: data is already strictly sorted in ascending
        // order due to the reverse-dequeue approach in serializePQ.
        return new ThetaEstimator.ComponentStats(insertSamples, deleteSamples, K);
    }

    private void serializePQ(LongPriorityQueue heap, DataOutput out) throws IOException {
        int size = heap.size();
        out.writeInt(size);

        long[] temp = new long[size];
        for (int i = 0; i < size; i++) {
            temp[i] = heap.dequeueLong();
        }
        for (long v : temp) {
            heap.enqueue(v);
        }

        for (int i = size - 1; i >= 0; i--) {
            out.writeLong(temp[i]);
        }
    }
}