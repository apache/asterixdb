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

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.LongPointable;

public class IntervalPartitionUtil {
    public static final double C_CPU = 0.5;
    public static final double C_IO = 100000;
    public static final int ITERATION_LIMIT = 20;

    private IntervalPartitionUtil() {
    }

    public static void main(String[] args) {
        PhysicalOptimizationConfig poc = new PhysicalOptimizationConfig();
        long[] countList = { poc.getMaxFramesForJoinLeftInput(), 2441, 9766, 39063, 156250, 625000, 2500000, 10000000 };
        long[] maxDurationList = { poc.getMaxIntervalDuration(), 1, 3, 30, 300, 3000, 30000, 300000 };
        int[] tuplesList = { poc.getMaxRecordsPerFrame(), 5, 50, 300, 900 };

        int k;
        for (long count : countList) {
            for (long maxDuration : maxDurationList) {
                for (int tuples : tuplesList) {
                    k = determineK(count, maxDuration, count, maxDuration, tuples);
                    System.err.println(
                            "size: " + count + " duration: " + maxDuration + " tuples: " + tuples + " k: " + k);
                }
            }
        }
    }

    public static int determineK(long countR, long maxDurationR, long countS, long maxDurationS, int avgTuplePerFrame) {
        double deltaR = 1.0 / maxDurationR;
        double deltaS = 1.0 / maxDurationS;

        long knMinusTwo = 0;
        long knMinusOne = 0;
        long kn = 1;

        long prn = determinePn(kn, countR, deltaR);
        double tn = determineTn(kn, determinePn(kn, countS, deltaS));

        int count = 0;
        while ((kn != knMinusOne) && (kn != knMinusTwo) && count < ITERATION_LIMIT) {
            knMinusTwo = knMinusOne;
            knMinusOne = kn;
            kn = determineKn(countR, countS, avgTuplePerFrame, prn, tn);
            prn = determinePn(kn, countR, deltaR);
            tn = determineTn(kn, determinePn(kn, countS, deltaS));
            count++;
        }
        if (count == ITERATION_LIMIT) {
            kn = (kn + knMinusOne + knMinusTwo) / 3;
        } else if (kn == knMinusTwo) {
            kn = (kn + knMinusTwo) / 2;
        }
        if (kn > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) kn;
        }
    }

    public static long determineKn(long countR, long countS, int avgTuplePerFrame, long prn, double tn) {
        double factorS = (3.0 * countS) / (2 * (C_IO + 2 * C_CPU) * tn);
        double factorR = (C_IO / avgTuplePerFrame) + ((4.0 * countR * C_CPU) / prn);
        return (long) Math.cbrt(factorS * factorR);
    }

    public static long determinePn(long kn, long count, double delta) {
        double knDelta = Math.ceil(kn * delta);
        return Math.min((long) ((kn * knDelta) + kn - ((knDelta * knDelta) / 2.0) - (knDelta / 2.0)), count);
    }

    public static double determineTn(long kn, long pn) {
        return pn / ((kn * kn + kn) / 2.0);
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
        int p = (partitionI * (k + k - partitionI + 1)) / 2;
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
        return new Pair<>(i, j);
    }

    public static long getStartOfPartition(IRangeMap rangeMap, int partition) {
        int fieldIndex = 0;
        long partitionStart = LongPointable.getLong(rangeMap.getMinByteArray(fieldIndex),
                rangeMap.getMinStartOffset(fieldIndex) + 1);
        if (partition != 0 && partition <= rangeMap.getSplitCount()) {
            partitionStart = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, partition - 1),
                    rangeMap.getStartOffset(fieldIndex, partition - 1) + 1);
        } else if (partition > rangeMap.getSplitCount()) {
            partitionStart = LongPointable.getLong(rangeMap.getMaxByteArray(fieldIndex),
                    rangeMap.getMaxStartOffset(fieldIndex) + 1);
        }
        return partitionStart;
    }

    public static long getEndOfPartition(IRangeMap rangeMap, int partition) {
        int fieldIndex = 0;
        long partitionEnd = LongPointable.getLong(rangeMap.getMaxByteArray(fieldIndex),
                rangeMap.getMaxStartOffset(fieldIndex) + 1);
        if (partition < rangeMap.getSplitCount()) {
            partitionEnd = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, partition),
                    rangeMap.getStartOffset(fieldIndex, partition) + 1);
        }
        return partitionEnd;
    }

    public static LinkedHashSet<Integer> getProbeJoinPartitions(int pid, int[] buildPSizeInTups,
            IIntervalMergeJoinChecker imjc, int k) {
        LinkedHashSet<Integer> joinMap = new LinkedHashSet<>();
        Pair<Integer, Integer> map = getIntervalPartition(pid, k);
        int probeStart = map.first;
        int probeEnd = map.second;
        // Build partitions with data
        for (int buildStart = 0; buildStart < k; ++buildStart) {
            for (int buildEnd = k - 1; buildStart <= buildEnd; --buildEnd) {
                int buildId = intervalPartitionMap(buildStart, buildEnd, k);
                if (buildPSizeInTups[buildId] > 0) {
                    // Join partitions for probe's pid
                    if (!(buildStart == 0 && probeStart == 0)
                            && imjc.compareIntervalPartition(buildStart, buildEnd, probeStart, probeEnd)) {
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
        LinkedHashMap<Integer, LinkedHashSet<Integer>> inMemoryMap = new LinkedHashMap<>();
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

    public static long getPartitionDuration(long partitionStart, long partitionEnd, int k) throws HyracksDataException {
        if (k <= 2) {
            throw new HyracksDataException("k is to small for interval partitioner.");
        }
        long duration = (partitionEnd - partitionStart) / (k - 2);
        if (duration <= 0) {
            duration = 1;
        }
        return duration;
    }

    public static int getIntervalPartition(long point, long partitionStart, long partitionDuration, int k)
            throws HyracksDataException {
        if (point < partitionStart) {
            return 0;
        }
        long pointFloor = Math.floorDiv(point - partitionStart, partitionDuration);
        // Add one to the partition, since 0 represents any point before the start partition point.
        return (int) Math.min(pointFloor + 1, k - 1L);
    }

}