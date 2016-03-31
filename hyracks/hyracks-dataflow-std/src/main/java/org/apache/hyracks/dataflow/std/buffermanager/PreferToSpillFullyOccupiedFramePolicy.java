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

package org.apache.hyracks.dataflow.std.buffermanager;

import java.util.BitSet;
import java.util.function.IntUnaryOperator;

/**
 * This policy is used to decide which partition in {@link VPartitionTupleBufferManager} should be a victim when
 * there is not enough space to insert new element.
 */
public class PreferToSpillFullyOccupiedFramePolicy {

    private final IPartitionedTupleBufferManager bufferManager;
    private final BitSet spilledStatus;
    private final int minFrameSize;

    public PreferToSpillFullyOccupiedFramePolicy(IPartitionedTupleBufferManager bufferManager, BitSet spilledStatus,
            int minFrameSize) {
        this.bufferManager = bufferManager;
        this.spilledStatus = spilledStatus;
        this.minFrameSize = minFrameSize;
    }

    public int selectVictimPartition(int failedToInsertPartition) {
        // To avoid flush the half-full frame, it's better to spill itself.
        if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
            return failedToInsertPartition;
        }
        int partitionToSpill = findSpilledPartitionWithMaxMemoryUsage();
        int maxToSpillPartSize = 0;
        // if we couldn't find the already spilled partition, or it is too small to flush that one,
        // try to flush an in memory partition.
        if (partitionToSpill < 0
                || (maxToSpillPartSize = bufferManager.getPhysicalSize(partitionToSpill)) == minFrameSize) {
            int partitionInMem = findInMemPartitionWithMaxMemoryUsage();
            if (partitionInMem >= 0 && bufferManager.getPhysicalSize(partitionInMem) > maxToSpillPartSize) {
                partitionToSpill = partitionInMem;
            }
        }
        return partitionToSpill;
    }

    public int findInMemPartitionWithMaxMemoryUsage() {
        return findMaxSize(spilledStatus.nextClearBit(0), (i) -> spilledStatus.nextClearBit(i + 1));
    }

    public int findSpilledPartitionWithMaxMemoryUsage() {
        return findMaxSize(spilledStatus.nextSetBit(0), (i) -> spilledStatus.nextSetBit(i + 1));
    }

    private int findMaxSize(int startIndex, IntUnaryOperator nextIndexOp) {
        int pid = -1;
        int max = 0;
        for (int i = startIndex; i >= 0 && i < bufferManager.getNumPartitions(); i = nextIndexOp.applyAsInt(i)) {
            int partSize = bufferManager.getPhysicalSize(i);
            if (partSize > max) {
                max = partSize;
                pid = i;
            }
        }
        return pid;
    }

    /**
     * Create an constrain for the already spilled partition that it can only use at most one frame.
     * 
     * @param spillStatus
     * @return
     */
    public static IPartitionedMemoryConstrain createAtMostOneFrameForSpilledPartitionConstrain(BitSet spillStatus) {
        return new IPartitionedMemoryConstrain() {
            @Override
            public int frameLimit(int partitionId) {
                if (spillStatus.get(partitionId)) {
                    return 1;
                }
                return Integer.MAX_VALUE;
            }
        };
    }
}
