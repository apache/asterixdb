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

    public PreferToSpillFullyOccupiedFramePolicy(IPartitionedTupleBufferManager bufferManager, BitSet spilledStatus) {
        this.bufferManager = bufferManager;
        this.spilledStatus = spilledStatus;
    }

    /**
     * This method tries to find a victim partition.
     * We want to keep in-memory partitions (not spilled to the disk yet) as long as possible to reduce the overhead
     * of writing to and reading from the disk.
     * If the given partition contains one or more tuple, then try to spill the given partition.
     * If not, try to flush another an in-memory partition.
     * Note: right now, the createAtMostOneFrameForSpilledPartitionConstrain we are using for a spilled partition
     * enforces that the number of maximum frame for a spilled partition is 1.
     */
    public int selectVictimPartition(int failedToInsertPartition) {
        // To avoid flushing another partition with the last half-full frame, it's better to spill the given partition
        // since one partition needs to be spilled to the disk anyway. Another reason is that we know that
        // the last frame in this partition is full.
        if (bufferManager.getNumTuples(failedToInsertPartition) > 0) {
            return failedToInsertPartition;
        }
        // If the given partition doesn't contain any tuple in memory, try to flush a different in-memory partition.
        // We are not trying to steal a frame from another spilled partition since once spilled, a partition can only
        // have only one frame and we don't know whether the frame is fully occupied or not.
        // TODO: Once we change this policy (spilled partition can have only one frame in memory),
        //       we need to revise this method, too.
        return findInMemPartitionWithMaxMemoryUsage();
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
     * Create a constrain for the already spilled partition that it can only use at most one frame.
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
