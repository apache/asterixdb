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
package org.apache.asterix.runtime.operators.joins;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

public class IntervalPartitionUtil {

    public static int determineK() {
        return 4;
    }

    public static void main(String[] args) {
        int k = 4;
        printPartitionMap(k);
        IIntervalMergeJoinChecker test = null;
        //        test = new OverlappingIntervalMergeJoinChecker(new int[1], new int[1], 0);
        //        System.err.println("Build - Overlapping");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));
        //
        //        test = new OverlapsIntervalMergeJoinChecker(new int[1], new int[1]);
        //        System.err.println("Build - Overlaps");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));
        //
        //        test = new BeforeIntervalMergeJoinChecker(new int[1], new int[1]);
        //        System.err.println("Build - Before");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));
        //
        //        test = new CoversIntervalMergeJoinChecker(new int[1], new int[1]);
        //        System.err.println("Build - Covers");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));
        //
        //        test = new CoveredByIntervalMergeJoinChecker(new int[1], new int[1]);
        //        System.err.println("Build - Covered By");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));
        //
        //        test = new EndsIntervalMergeJoinChecker(new int[1], new int[1]);
        //        System.err.println("Build - Ends");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));
        //
        //        test = new EndedByIntervalMergeJoinChecker(new int[1], new int[1]);
        //        System.err.println("Build - Ended By");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));
        //
        //        test = new StartsIntervalMergeJoinChecker(new int[1], new int[1]);
        //        System.err.println("Build - Starts");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));
        //
        //        test = new MeetsIntervalMergeJoinChecker(new int[1], new int[1]);
        //        System.err.println("Build - Meets");
        //        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));

        test = new EqualsIntervalMergeJoinChecker(new int[1], new int[1]);
        System.err.println("Build - Equal");
        printJoinPartitionMap(getJoinPartitionListOfSets(k, test));

        ArrayList<HashSet<Integer>> mapBuild = getJoinPartitionListOfSets(k, test);
        System.err.println("Build");
        printJoinPartitionMap(mapBuild);
        System.err.println("Spill Order");
        printPartitionSpillOrder(getPartitionSpillOrder(mapBuild));
        System.err.println("Build");
        printJoinPartitionMap(mapBuild);

        System.err.println("Probe");
        ArrayList<HashSet<Integer>> mapProbe = getProbeJoinPartitionListOfSets(mapBuild);
        printJoinPartitionMap(mapProbe);
    }

    public static int getMaxPartitions(int k) {
        return (k * k + k) / 2;
    }

    public static IGrowableIntArray[] getJoinPartitionIntArray(int k, IIntervalMergeJoinChecker imjc) {
        IGrowableIntArray[] buildPartitionMap = new IntArrayList[getMaxPartitions(k)];
        for (int buildStart = 0; buildStart < k; ++buildStart) {
            for (int buildEnd = buildStart; buildEnd < k; ++buildEnd) {
                //                System.out.println("Outer Partition: (" + buildStart + ", " + buildEnd + ") "
                //                        + intervalPartitionMap(buildStart, buildEnd, k));
                buildPartitionMap[intervalPartitionMap(buildStart, buildEnd, k)] = new IntArrayList(5, 5);
                for (int probeStart = 0; probeStart < k; ++probeStart) {
                    for (int probeEnd = probeStart; probeEnd < k; ++probeEnd) {
                        //                        System.out.print("  Inner Partition: (" + probeStart + ", " + probeEnd + ") "
                        //                                + intervalPartitionMap(probeStart, probeEnd, k));
                        if (imjc.compareInterval(buildStart, buildEnd + 1, probeStart, probeEnd + 1)) {
                            //                            System.out.print(" matches");
                            buildPartitionMap[intervalPartitionMap(buildStart, buildEnd, k)]
                                    .add(intervalPartitionMap(probeStart, probeEnd, k));
                        }
                        //                        System.out.println();
                    }
                }
            }
        }
        return buildPartitionMap;
    }

    public static ArrayList<HashSet<Integer>> getJoinPartitionListOfSets(int k, IIntervalMergeJoinChecker imjc) {
        ArrayList<HashSet<Integer>> buildPartitionMap = new ArrayList<HashSet<Integer>>();
        for (int i = 0; i < getMaxPartitions(k); ++i) {
            buildPartitionMap.add(new HashSet<Integer>());
        }
        // Build partitions
        for (int buildStart = 0; buildStart < k; ++buildStart) {
            for (int buildEnd = buildStart; buildEnd < k; ++buildEnd) {
                // Probe partitions
                for (int probeStart = 0; probeStart < k; ++probeStart) {
                    for (int probeEnd = probeStart; probeEnd < k; ++probeEnd) {
                        // Join partitions
                        if (imjc.compareIntervalPartition(buildStart, buildEnd, probeStart, probeEnd)) {
                            buildPartitionMap.get(intervalPartitionMap(buildStart, buildEnd, k))
                                    .add(intervalPartitionMap(probeStart, probeEnd, k));
                        }
                    }
                }
            }
        }
        return buildPartitionMap;
    }

    public static void printJoinPartitionMap(IGrowableIntArray[] partitionMap) {
        for (int i = 0; i < partitionMap.length; ++i) {
            System.out.print("Build partition " + i + " must join with prode partition(s): ");
            for (int j = 0; j < partitionMap[i].size(); ++j) {
                System.out.print(partitionMap[i].get(j) + " ");
            }
            System.out.println("");
        }
    }

    public static void printJoinPartitionMap(ArrayList<HashSet<Integer>> partitionMap) {
        for (int i = 0; i < partitionMap.size(); ++i) {
            System.out.print("(hashset) Partition " + i + " must join with partition(s): ");
            for (Integer map : partitionMap.get(i)) {
                System.out.print(map + " ");
            }
            System.out.println("");
        }
    }

    public static void printPartitionSpillOrder(ArrayList<Integer> spillOrder) {
        System.out.print("Spill partition in the following order: ");
        for (int i = 0; i < spillOrder.size(); ++i) {
            System.out.print(spillOrder.get(i) + " ");
        }
        System.out.println("");
    }

    public static ArrayList<Integer> getPartitionSpillOrder(ArrayList<HashSet<Integer>> partitionMap) {
        // Make a copy of the partitionMap.
        ArrayList<HashSet<Integer>> tempMap = new ArrayList<HashSet<Integer>>();
        for (int i = 0; i < partitionMap.size(); ++i) {
            tempMap.add(new HashSet<Integer>());
            for (Integer map : partitionMap.get(i)) {
                tempMap.get(i).add(map.intValue());
            }
        }

        ArrayList<Integer> spillOrder = new ArrayList<Integer>();

        int p = findLeastJoinedPartitionInMemory(tempMap, spillOrder);
        while (p != -1) {
            spillOrder.add(p);
            removePartitionFromMap(tempMap, p);
            p = findLeastJoinedPartitionInMemory(tempMap, spillOrder);
        }
        return spillOrder;
    }

    public static BitSet getProbePartitionSpillList(ArrayList<HashSet<Integer>> partitionMap, BitSet buildSpill) {
        BitSet probeSpill = new BitSet(buildSpill.size());
        for (int i = buildSpill.nextSetBit(0); i >= 0; i = buildSpill.nextSetBit(i + 1)) {
            for (Integer map : partitionMap.get(i)) {
                probeSpill.set(map.intValue());
            }
        }
        return probeSpill;
    }

    public static ArrayList<HashSet<Integer>> getProbeJoinPartitionListOfSets(
            ArrayList<HashSet<Integer>> buildPartitionMap) {
        ArrayList<HashSet<Integer>> probePartitionMap = new ArrayList<HashSet<Integer>>();
        for (int i = 0; i < buildPartitionMap.size(); ++i) {
            probePartitionMap.add(new HashSet<Integer>());
        }
        for (int i = 0; i < buildPartitionMap.size(); ++i) {
            for (Integer map : buildPartitionMap.get(i)) {
                probePartitionMap.get(map.intValue()).add(i);
            }
        }
        return probePartitionMap;
    }

    public static int findLeastJoinedPartitionInMemory(ArrayList<HashSet<Integer>> partitionMap,
            ArrayList<Integer> ignoreList) {
        int minPartition = -1;
        int minCount = Integer.MAX_VALUE;
        for (int i = 0; i < partitionMap.size(); ++i) {
            if (!ignoreList.contains(i) && partitionMap.get(i).size() < minCount) {
                minPartition = i;
                minCount = partitionMap.get(i).size();
            }
        }
        return minPartition;
    }

    public static void removePartitionFromMap(ArrayList<HashSet<Integer>> partitionMap, int partition) {
        for (HashSet<Integer> map : partitionMap) {
            map.remove(partition);
        }
    }

    public static void printPartitionMap(int k) {
        for (int i = 0; i < k; ++i) {
            for (int j = i; j < k; ++j) {
                System.out.println(
                        "Map partition (" + i + ", " + j + ") to partition id: " + intervalPartitionMap(i, j, k));
            }
        }
    }

    public static int intervalPartitionMap(long i, long j, int k) {
        int p = 0;
        for (long duration = j - i; duration > 0; --duration) {
            p += k - duration + 1;
        }
        p += i;
        return p;
    }

    public static long getIntervalPartitionI(IFrameTupleAccessor accessor, int tIndex, int fieldId, long partitionStart,
            long partitionDuration, int k) throws HyracksDataException {
        long i = Math.floorDiv((getIntervalStart(accessor, tIndex, fieldId) - partitionStart), partitionDuration);
        return Math.max(0, Math.min(i, k - 1l));
    }

    public static long getIntervalPartitionJ(IFrameTupleAccessor accessor, int tIndex, int fieldId, long partitionStart,
            long partitionDuration, int k) throws HyracksDataException {
        long j = Math.floorDiv((getIntervalEnd(accessor, tIndex, fieldId) - partitionStart), partitionDuration);
        return Math.max(0, Math.min(j, k - 1l));
    }

    public static long getIntervalStart(IFrameTupleAccessor accessor, int tupleId, int fieldId)
            throws HyracksDataException {
        int start = accessor.getTupleStartOffset(tupleId) + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(tupleId, fieldId) + 1;
        long intervalStart = AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), start);
        return intervalStart;
    }

    public static long getIntervalEnd(IFrameTupleAccessor accessor, int tupleId, int fieldId)
            throws HyracksDataException {
        int start = accessor.getTupleStartOffset(tupleId) + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(tupleId, fieldId) + 1;
        long intervalEnd = AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), start);
        return intervalEnd;
    }

    public static long getStartOfPartition(IRangeMap rangeMap, int partition) { //throws HyracksDataException {
        int fieldIndex = 0;
        //        if (ATypeTag.INT64.serialize() != rangeMap.getTag(0, 0)) {
        //            throw new HyracksDataException("Invalid range map type for interval merge join checker.");
        //        }
        long partitionStart = Long.MIN_VALUE;
        if (partition != 0 && partition <= rangeMap.getSplitCount()) {
            partitionStart = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, partition - 1),
                    rangeMap.getStartOffset(fieldIndex, partition - 1) + 1);
        } else if (partition > rangeMap.getSplitCount()) {
            partitionStart = Long.MAX_VALUE;
        }
        return partitionStart;
    }

    public static long getEndOfPartition(IRangeMap rangeMap, int partition) { //throws HyracksDataException {
        int fieldIndex = 0;
        //        if (ATypeTag.INT64.serialize() != rangeMap.getTag(0, 0)) {
        //            throw new HyracksDataException("Invalid range map type for interval merge join checker.");
        //        }
        long partitionEnd = Long.MAX_VALUE;
        if (partition < rangeMap.getSplitCount()) {
            partitionEnd = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, partition),
                    rangeMap.getStartOffset(fieldIndex, partition) + 1);
        }
        return partitionEnd;
    }

    public static ArrayList<HashSet<Integer>> getProbeJoinPartitionsInMemory(ArrayList<HashSet<Integer>> probeJoinMap,
            BitSet spilledStatus) {
        return getProbeJoinPartitions(probeJoinMap, spilledStatus, false);
    }

    public static ArrayList<HashSet<Integer>> getProbeJoinPartitionsSpilled(ArrayList<HashSet<Integer>> probeJoinMap,
            BitSet spilledStatus) {
        return getProbeJoinPartitions(probeJoinMap, spilledStatus, true);
    }

    private static ArrayList<HashSet<Integer>> getProbeJoinPartitions(ArrayList<HashSet<Integer>> probeJoinMap,
            BitSet spilledStatus, Boolean spilled) {
        ArrayList<HashSet<Integer>> probeJoinMapSpilled = new ArrayList<HashSet<Integer>>();
        for (int i = 0; i < probeJoinMap.size(); ++i) {
            probeJoinMapSpilled.add(new HashSet<Integer>());
        }
        for (int i = 0; i < probeJoinMap.size(); ++i) {
            for (Integer map : probeJoinMap.get(i)) {
                if (spilledStatus.get(map.intValue()) == spilled) {
                    probeJoinMapSpilled.get(i).add(map.intValue());
                }
            }
        }
        return probeJoinMapSpilled;
    }

}