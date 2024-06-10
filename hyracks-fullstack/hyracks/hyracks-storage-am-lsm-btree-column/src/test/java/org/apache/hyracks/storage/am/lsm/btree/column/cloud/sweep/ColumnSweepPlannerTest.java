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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep;

import static org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType.QUERY;
import static org.apache.hyracks.util.StorageUtil.getIntSizeInBytes;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

import org.apache.hyracks.storage.am.lsm.btree.column.dummy.DummyColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.dummy.DummySweepClock;
import org.apache.hyracks.util.StorageUtil;
import org.junit.Assert;
import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class ColumnSweepPlannerTest {
    private static final int MAX_MEGA_LEAF_NODE_SIZE = getIntSizeInBytes(10, StorageUtil.StorageUnit.MEGABYTE);
    private static final Random RANDOM = new Random(0);
    private final DummySweepClock clock = new DummySweepClock();

    @Test
    public void test10Columns() {
        int numberOfPrimaryKeys = 1;
        int numberOfColumns = numberOfPrimaryKeys + 10;
        int[] columnSizes = createNormalColumnSizes(numberOfPrimaryKeys, numberOfColumns);
        ColumnSweepPlanner planner = new ColumnSweepPlanner(numberOfPrimaryKeys, clock);
        IntList projectedColumns = new IntArrayList();
        DummyColumnProjectionInfo info = new DummyColumnProjectionInfo(numberOfPrimaryKeys, QUERY, projectedColumns);

        // Adjust sizes
        planner.adjustColumnSizes(columnSizes, numberOfColumns);

        // Project 3 columns
        projectedColumns(numberOfPrimaryKeys, numberOfColumns, 3, projectedColumns);
        // access the projected columns (max 10 times)
        access(planner, info, 10);

        // Advance clock
        clock.advance(10);

        // Plan for eviction
        BitSet keptColumns = new BitSet();
        planner.plan();
        computeKeptColumns(planner.getPlanCopy(), keptColumns, numberOfColumns);

        // Project another 3 columns
        projectedColumns(numberOfPrimaryKeys, numberOfColumns, 3, projectedColumns);
        // access the projected columns
        access(planner, info, 100);

        // At this point, the plan should change
        BitSet newKeptColumns = new BitSet();
        computeKeptColumns(planner.getPlanCopy(), newKeptColumns, numberOfColumns);

        Assert.assertNotEquals(keptColumns, newKeptColumns);
    }

    private void computeKeptColumns(BitSet plan, BitSet keptColumns, int numberOfColumns) {
        keptColumns.clear();
        for (int i = 0; i < numberOfColumns; i++) {
            if (!plan.get(i)) {
                keptColumns.set(i);
            }
        }

        System.out.println("Kept columns: " + keptColumns);
    }

    private void access(ColumnSweepPlanner planner, DummyColumnProjectionInfo info, int bound) {
        int numberOfAccesses = RANDOM.nextInt(1, bound);
        for (int i = 0; i < numberOfAccesses; i++) {
            planner.access(info);
            clock.advance(1);
        }

        System.out.println("Accessed: " + info + " " + numberOfAccesses + " time");
    }

    private void projectedColumns(int numberPrimaryKeys, int numberOfColumns, int numberOfProjectedColumns,
            IntList projectedColumns) {
        IntSet alreadyProjectedColumns = new IntOpenHashSet();
        projectedColumns.clear();
        for (int i = 0; i < numberOfProjectedColumns; i++) {
            int columnIndex = RANDOM.nextInt(numberPrimaryKeys, numberOfColumns);
            while (alreadyProjectedColumns.contains(columnIndex)) {
                columnIndex = RANDOM.nextInt(numberPrimaryKeys, numberOfColumns);
            }
            projectedColumns.add(columnIndex);
            alreadyProjectedColumns.add(columnIndex);
        }
    }

    private int[] createNormalColumnSizes(int numberOfPrimaryKeys, int numberOfColumns) {
        int[] columnSizes = new int[numberOfColumns];
        double[] normalDistribution = new double[numberOfColumns];
        double sum = 0.0d;
        for (int i = 0; i < numberOfColumns; i++) {
            double value = Math.abs(RANDOM.nextGaussian());
            normalDistribution[i] = value;
            sum += value;
        }

        for (int i = numberOfPrimaryKeys; i < numberOfColumns; i++) {
            int size = (int) Math.round((normalDistribution[i] / sum) * MAX_MEGA_LEAF_NODE_SIZE);
            columnSizes[i] = size;
        }

        System.out.println("Column sizes:");
        for (int i = 0; i < numberOfColumns; i++) {
            System.out.println(i + ": " + StorageUtil.toHumanReadableSize(columnSizes[i]));
        }
        System.out.println("TotalSize:" + StorageUtil.toHumanReadableSize(Arrays.stream(columnSizes).sum()));
        return columnSizes;
    }

}
