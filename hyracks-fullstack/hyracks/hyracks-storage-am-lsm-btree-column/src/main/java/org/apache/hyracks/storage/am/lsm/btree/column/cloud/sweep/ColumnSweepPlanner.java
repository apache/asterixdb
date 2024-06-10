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

import static org.apache.hyracks.storage.am.lsm.common.util.LSMComponentIdUtils.isMergedComponent;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.ColumnRanges;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongArrays;

public final class ColumnSweepPlanner {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final double SIZE_WEIGHT = 0.3d;
    private static final double LAST_ACCESS_WEIGHT = 1.0d - SIZE_WEIGHT;
    private static final double INITIAL_PUNCHABLE_THRESHOLD = 0.7d;
    private static final double PUNCHABLE_THRESHOLD_DECREMENT = 0.7d;
    private static final int MAX_ITERATION_COUNT = 5;
    private static final int REEVALUATE_PLAN_THRESHOLD = 50;

    private final AtomicBoolean active;
    private final int numberOfPrimaryKeys;
    private final BitSet plan;
    private final BitSet reevaluatedPlan;
    private final IntSet indexedColumns;
    private final ISweepClock clock;
    private int numberOfColumns;
    private long lastAccess;
    private int maxSize;
    private int[] sizes;
    private long[] lastAccesses;

    private double punchableThreshold;
    private int numberOfSweptColumns;
    private int numberOfCloudRequests;

    public ColumnSweepPlanner(int numberOfPrimaryKeys, ISweepClock clock) {
        this.clock = clock;
        active = new AtomicBoolean(false);
        this.numberOfPrimaryKeys = numberOfPrimaryKeys;
        sizes = new int[0];
        lastAccesses = new long[0];
        indexedColumns = new IntOpenHashSet();
        plan = new BitSet();
        reevaluatedPlan = new BitSet();
        punchableThreshold = INITIAL_PUNCHABLE_THRESHOLD;
    }

    public boolean isActive() {
        return active.get();
    }

    public int getNumberOfPrimaryKeys() {
        return numberOfPrimaryKeys;
    }

    public void onActivate(int numberOfColumns, List<ILSMDiskComponent> diskComponents,
            IColumnTupleProjector sweepProjector, IBufferCache bufferCache) throws HyracksDataException {
        resizeStatsArrays(numberOfColumns);
        setInitialSizes(diskComponents, sweepProjector, bufferCache);
        active.set(true);
    }

    public void setIndexedColumns(IColumnProjectionInfo projectionInfo) {
        indexedColumns.clear();
        for (int i = 0; i < projectionInfo.getNumberOfProjectedColumns(); i++) {
            int columnIndex = projectionInfo.getColumnIndex(i);
            indexedColumns.add(columnIndex);
        }
    }

    public IntSet getIndexedColumnsCopy() {
        return new IntOpenHashSet(indexedColumns);
    }

    public synchronized void access(IColumnProjectionInfo projectionInfo) {
        resetPlanIfNeeded();
        long accessTime = clock.getCurrentTime();
        lastAccess = accessTime;
        int numberOfColumns = projectionInfo.getNumberOfProjectedColumns();
        boolean requireCloudAccess = false;
        for (int i = 0; i < numberOfColumns; i++) {
            int columnIndex = projectionInfo.getColumnIndex(i);
            // columnIndex can be -1 when accessing a non-existing column (i.e., not known by the schema)
            if (columnIndex >= 0) {
                lastAccesses[columnIndex] = accessTime;
                requireCloudAccess |= numberOfSweptColumns > 0 && plan.get(columnIndex);
            }
        }

        numberOfCloudRequests += requireCloudAccess ? 1 : 0;
    }

    public synchronized void adjustColumnSizes(int[] newSizes, int numberOfColumns) {
        resizeStatsArrays(numberOfColumns);
        for (int i = 0; i < numberOfColumns; i++) {
            int newSize = newSizes[i];
            sizes[i] = Math.max(sizes[i], newSize);
            maxSize = Math.max(maxSize, newSize);
        }
    }

    public synchronized boolean plan() {
        plan.clear();
        int numberOfEvictableColumns = 0;
        int iter = 0;
        // Calculate weights: Ensure the plan contains new columns that never been swept
        while (iter < MAX_ITERATION_COUNT && numberOfEvictableColumns < numberOfColumns) {
            if (numberOfEvictableColumns > 0) {
                // Do not reiterate if we found columns to evict
                break;
            }

            // Find evictable columns
            numberOfEvictableColumns += findEvictableColumns(plan);

            // The next iteration/plan will be more aggressive
            punchableThreshold *= PUNCHABLE_THRESHOLD_DECREMENT;
            iter++;
        }
        // Add the number of evictable columns
        numberOfSweptColumns += numberOfEvictableColumns;
        if (numberOfEvictableColumns > 0) {
            LOGGER.info("Planning to evict {} columns. The evictable columns are {}", numberOfEvictableColumns, plan);
            return true;
        }

        LOGGER.info("Couldn't find columns to evict after {} iteration", iter);
        return false;
    }

    public synchronized BitSet getPlanCopy() {
        return (BitSet) plan.clone();
    }

    private double getWeight(int i, IntSet indexedColumns, int numberOfPrimaryKeys) {
        if (i < numberOfPrimaryKeys || indexedColumns.contains(i)) {
            return -1.0;
        }

        double sizeWeight = sizes[i] / (double) maxSize * SIZE_WEIGHT;
        double lasAccessWeight = (lastAccess - lastAccesses[i]) / (double) lastAccess * LAST_ACCESS_WEIGHT;

        return sizeWeight + lasAccessWeight;
    }

    private void resizeStatsArrays(int numberOfColumns) {
        sizes = IntArrays.ensureCapacity(sizes, numberOfColumns);
        lastAccesses = LongArrays.ensureCapacity(lastAccesses, numberOfColumns);
        this.numberOfColumns = numberOfColumns - numberOfPrimaryKeys;
    }

    private void setInitialSizes(List<ILSMDiskComponent> diskComponents, IColumnTupleProjector sweepProjector,
            IBufferCache bufferCache) throws HyracksDataException {
        // This runs when activating an index (no need to synchronize on the opTracker)
        if (diskComponents.isEmpty()) {
            return;
        }

        IColumnProjectionInfo columnProjectionInfo =
                ColumnSweeperUtil.createColumnProjectionInfo(diskComponents, sweepProjector);
        ILSMDiskComponent latestComponent = diskComponents.get(0);
        ILSMDiskComponent oldestComponent = diskComponents.get(diskComponents.size() - 1);
        ColumnBTreeReadLeafFrame leafFrame = ColumnSweeperUtil.createLeafFrame(columnProjectionInfo, latestComponent);
        ColumnRanges ranges = new ColumnRanges(columnProjectionInfo.getNumberOfPrimaryKeys());
        // Get the column sizes from the freshest component, which has the columns of the most recent schema
        setColumnSizes(latestComponent, leafFrame, ranges, bufferCache);
        if (isMergedComponent(oldestComponent.getId())) {
            // Get the column sizes from the oldest merged component, which probably has the largest columns
            setColumnSizes(oldestComponent, leafFrame, ranges, bufferCache);
        }
    }

    private void setColumnSizes(ILSMDiskComponent diskComponent, ColumnBTreeReadLeafFrame leafFrame,
            ColumnRanges ranges, IBufferCache bufferCache) throws HyracksDataException {
        ColumnBTree columnBTree = (ColumnBTree) diskComponent.getIndex();
        long dpid = BufferedFileHandle.getDiskPageId(columnBTree.getFileId(), columnBTree.getBulkloadLeafStart());
        ICachedPage page = bufferCache.pin(dpid);
        try {
            leafFrame.setPage(page);
            ranges.reset(leafFrame);
            for (int i = 0; i < leafFrame.getNumberOfColumns(); i++) {
                sizes[i] = Math.max(sizes[i], ranges.getColumnLength(i));
                maxSize = Math.max(maxSize, sizes[i]);
            }
        } finally {
            bufferCache.unpin(page);
        }
    }

    private int findEvictableColumns(BitSet plan) {
        int numberOfEvictableColumns = 0;
        for (int i = 0; i < sizes.length; i++) {
            if (!plan.get(i) && getWeight(i, indexedColumns, numberOfPrimaryKeys) >= punchableThreshold) {
                // Column reached a punchable threshold; include it in the eviction plan.
                plan.set(i);
                numberOfEvictableColumns++;
            }
        }

        return numberOfEvictableColumns;
    }

    private void resetPlanIfNeeded() {
        if (numberOfCloudRequests < REEVALUATE_PLAN_THRESHOLD) {
            return;
        }

        numberOfCloudRequests = 0;
        reevaluatedPlan.clear();
        int numberOfEvictableColumns = findEvictableColumns(reevaluatedPlan);
        for (int i = 0; i < numberOfEvictableColumns; i++) {
            int columnIndex = reevaluatedPlan.nextSetBit(i);
            if (!plan.get(columnIndex)) {
                // the plan contains a stale column. Invalidate!
                LOGGER.info("Re-planning to evict {} columns. Old plan: {} new plan: {}", numberOfEvictableColumns,
                        plan, reevaluatedPlan);
                plan.clear();
                plan.or(reevaluatedPlan);
                break;
            }
        }
    }
}
