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
package org.apache.hyracks.dataflow.std.join;

import java.util.BitSet;

import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;

public class HybridHashJoinUtil {

    private HybridHashJoinUtil() {
    }

    /**
     * Prints out the detailed information for partitions: in-memory and spilled partitions.
     * This method exists for a debug purpose.
     */
    public String printPartitionInfo(BitSet spilledStatus, OptimizedHybridHashJoin.SIDE whichSide, int numOfPartitions,
            int[] probePSizeInTups, int[] buildPSizeInTups, RunFileWriter[] probeRFWriters,
            RunFileWriter[] buildRFWriters, IPartitionedTupleBufferManager bufferManager) {
        StringBuilder buf = new StringBuilder();
        buf.append(">>> " + this + " " + Thread.currentThread().getId() + " printInfo():" + "\n");
        if (whichSide == OptimizedHybridHashJoin.SIDE.BUILD) {
            buf.append("BUILD side" + "\n");
        } else {
            buf.append("PROBE side" + "\n");
        }
        buf.append("# of partitions:\t" + numOfPartitions + "\t#spilled:\t" + spilledStatus.cardinality()
                + "\t#in-memory:\t" + (numOfPartitions - spilledStatus.cardinality()) + "\n");
        buf.append("(A) Spilled partitions" + "\n");
        int spilledTupleCount = 0;
        int spilledPartByteSize = 0;
        for (int pid = spilledStatus.nextSetBit(0); pid >= 0 && pid < numOfPartitions; pid =
                spilledStatus.nextSetBit(pid + 1)) {
            if (whichSide == OptimizedHybridHashJoin.SIDE.BUILD) {
                spilledTupleCount += buildPSizeInTups[pid];
                spilledPartByteSize += buildRFWriters[pid].getFileSize();
                buf.append("part:\t" + pid + "\t#tuple:\t" + buildPSizeInTups[pid] + "\tsize(MB):\t"
                        + ((double) buildRFWriters[pid].getFileSize() / 1048576) + "\n");
            } else {
                spilledTupleCount += probePSizeInTups[pid];
                spilledPartByteSize += probeRFWriters[pid].getFileSize();
            }
        }
        if (spilledStatus.cardinality() > 0) {
            buf.append("# of spilled tuples:\t" + spilledTupleCount + "\tsize(MB):\t"
                    + ((double) spilledPartByteSize / 1048576) + "avg #tuples per spilled part:\t"
                    + (spilledTupleCount / spilledStatus.cardinality()) + "\tavg size per part(MB):\t"
                    + ((double) spilledPartByteSize / 1048576 / spilledStatus.cardinality()) + "\n");
        }
        buf.append("(B) In-memory partitions" + "\n");
        int inMemoryTupleCount = 0;
        int inMemoryPartByteSize = 0;
        for (int pid = spilledStatus.nextClearBit(0); pid >= 0 && pid < numOfPartitions; pid =
                spilledStatus.nextClearBit(pid + 1)) {
            if (whichSide == OptimizedHybridHashJoin.SIDE.BUILD) {
                inMemoryTupleCount += buildPSizeInTups[pid];
                inMemoryPartByteSize += bufferManager.getPhysicalSize(pid);
            } else {
                inMemoryTupleCount += probePSizeInTups[pid];
                inMemoryPartByteSize += bufferManager.getPhysicalSize(pid);
            }
        }
        if (spilledStatus.cardinality() > 0) {
            buf.append("# of in-memory tuples:\t" + inMemoryTupleCount + "\tsize(MB):\t"
                    + ((double) inMemoryPartByteSize / 1048576) + "avg #tuples per spilled part:\t"
                    + (inMemoryTupleCount / spilledStatus.cardinality()) + "\tavg size per part(MB):\t"
                    + ((double) inMemoryPartByteSize / 1048576 / (numOfPartitions - spilledStatus.cardinality()))
                    + "\n");
        }
        if (inMemoryTupleCount + spilledTupleCount > 0) {
            buf.append("# of all tuples:\t" + (inMemoryTupleCount + spilledTupleCount) + "\tsize(MB):\t"
                    + ((double) (inMemoryPartByteSize + spilledPartByteSize) / 1048576) + " ratio of spilled tuples:\t"
                    + (spilledTupleCount / (inMemoryTupleCount + spilledTupleCount)) + "\n");
        } else {
            buf.append("# of all tuples:\t" + (inMemoryTupleCount + spilledTupleCount) + "\tsize(MB):\t"
                    + ((double) (inMemoryPartByteSize + spilledPartByteSize) / 1048576) + " ratio of spilled tuples:\t"
                    + "N/A" + "\n");
        }
        return buf.toString();
    }
}
