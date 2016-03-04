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
package org.apache.asterix.runtime.operators.joins.intervalpartition;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import org.apache.asterix.runtime.operators.joins.EqualsIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;

public class IntervalPartitionUtil {
    public static final double C_CPU = 0.5;
    public static final double C_IO = 10;

    public static int determineK(int countR, int maxDurationR, int countS, int maxDurationS, int avgTuplePerFrame) {
        double deltaR = 1.0 / maxDurationR;
        double deltaS = 1.0 / maxDurationS;

        int knMinusTwo = 0;
        int knMinusOne = 0;
        int kn = 1;

        int prn = determinePn(kn, countR, deltaR);
        double tn = determineTn(kn, determinePn(kn, countS, deltaS));

        while ((kn != knMinusOne) && (kn != knMinusTwo)) {
            knMinusTwo = knMinusOne;
            knMinusOne = kn;
            kn = determineKn(countR, countS, avgTuplePerFrame, prn, tn);
            prn = determinePn(kn, countR, deltaR);
            tn = determineTn(kn, determinePn(kn, countS, deltaS));
        }
        return kn;
    }

    public static int determineKn(int countR, int countS, int avgTuplePerFrame, int prn, double tn) {
        double factorS = (3.0 * countS) / (2 * (C_IO + 2 * C_CPU) * tn);
        double factorR = (C_IO / avgTuplePerFrame) + ((4.0 * countR * C_CPU) / prn);
        return (int) Math.cbrt(factorS * factorR);
    }

    public static int determinePn(int kn, int count, double delta) {
        long knDelta = (long) Math.ceil(kn * delta);
        return Math.min((int) ((kn * knDelta) + kn - ((knDelta * knDelta) / 2.0) - (knDelta / 2.0)), count);
    }

    public static double determineTn(int kn, int Pn) {
        return Pn / ((kn * kn + kn) / 2.0);
    }

    public static int getMaxPartitions(int k) {
        return (k * k + k) / 2;
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

    public static void printPartitionMap(int k) {
        for (int i = 0; i < k; ++i) {
            for (int j = i; j < k; ++j) {
                int pid = intervalPartitionMap(i, j, k);
                Pair<Integer, Integer> partition = getIntervalPartition(pid, k);
                System.out.println("Map partition (" + i + ", " + j + ") to partition id: " + pid + " back to pair ("
                        + partition.first + ", " + partition.second + ")");
            }
        }
    }

    /**
     * Map the partition start and end points to a single value.
     * The mapped partitions are sorted in interval starting at 0.
     *
     * @param partitionI
     *            start point
     * @param partitionJ
     *            end point
     * @param k
     *            granules
     * @return mapping
     */
    public static int intervalPartitionMap(int partitionI, int partitionJ, int k) {
        int p = ((partitionI * (k + k - partitionI + 1)) / 2);
        return p + partitionJ - partitionI;
    }

    /**
     * Reverse the map to individual start and end points.
     *
     * @param i
     *            map id
     * @param k
     *            granules
     * @return start and end points
     */
    public static Pair<Integer, Integer> getIntervalPartition(int pid, int k) {
        int i = 0;
        int sum = 0;
        for (int p = k; p <= pid; p += k - i) {
            ++i;
            sum = p;
        }
        int j = i + pid - sum;
        return new Pair<Integer, Integer>(i, j);
    }

    public static long getStartOfPartition(IRangeMap rangeMap, int partition) { //throws HyracksDataException {
        int fieldIndex = 0;
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
        long partitionEnd = Long.MAX_VALUE;
        if (partition < rangeMap.getSplitCount()) {
            partitionEnd = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, partition),
                    rangeMap.getStartOffset(fieldIndex, partition) + 1);
        }
        return partitionEnd;
    }

    public static LinkedHashSet<Integer> getProbeJoinPartitions(int pid, int[] buildPSizeInTups,
            IIntervalMergeJoinChecker imjc, int k) {
        LinkedHashSet<Integer> joinMap = new LinkedHashSet<Integer>();
        Pair<Integer, Integer> map = getIntervalPartition(pid, k);
        int probeStart = map.first;
        int probeEnd = map.second;
        // Build partitions with data
        for (int buildStart = 0; buildStart < k; ++buildStart) {
            for (int buildEnd = buildStart; buildEnd < k; ++buildEnd) {
                int buildId = intervalPartitionMap(buildStart, buildEnd, k);
                if (buildPSizeInTups[buildId] > 0) {
                    // Join partitions for probe's pid
                    if (imjc.compareIntervalPartition(buildStart, buildEnd, probeStart, probeEnd)) {
                        joinMap.add(buildId);
                    }
                }
            }
        }
        return joinMap;
    }

    public static LinkedHashMap<Integer, LinkedHashSet<Integer>> getInMemorySpillJoinMap(
            LinkedHashMap<Integer, LinkedHashSet<Integer>> probeJoinMap, BitSet buildInMemoryStatus,
            BitSet probeSpilledStatus) {
        LinkedHashMap<Integer, LinkedHashSet<Integer>> inMemoryMap = new LinkedHashMap<Integer, LinkedHashSet<Integer>>();
        for (Entry<Integer, LinkedHashSet<Integer>> entry : probeJoinMap.entrySet()) {
            if (probeSpilledStatus.get(entry.getKey())) {
                for (Integer i : entry.getValue()) {
                    if (buildInMemoryStatus.get(i)) {
                        if (!inMemoryMap.containsKey(entry.getKey())) {
                            inMemoryMap.put(entry.getKey(), new LinkedHashSet<Integer>());
                        }
                        inMemoryMap.get(entry.getKey()).add(i);
                    }
                }
            }
        }
        return inMemoryMap;
    }

}