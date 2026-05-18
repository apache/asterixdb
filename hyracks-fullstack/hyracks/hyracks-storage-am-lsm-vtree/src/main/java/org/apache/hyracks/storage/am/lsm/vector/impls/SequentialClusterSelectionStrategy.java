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

package org.apache.hyracks.storage.am.lsm.vector.impls;

import java.util.Collections;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Sequential cluster selection strategy.
 * Iterates through all clusters in order (0 → 1 → 2 → ...) without early termination.
 * Used for:
 * - Merge operations (process ALL data from all components)
 * - Index-driven KNN (scan all clusters with bidirectional pruning within each)
 *
 * Key differences from NprobeClusterSelectionStrategy:
 * - No level-wise or DFS cluster selection
 * - No nprobe/K-based early termination (shouldStopAdvancing always returns false)
 * - Sequential cluster iteration via first cursor's advanceToNextCluster()
 * - All components advance through clusters TOGETHER in lock-step
 */
public class SequentialClusterSelectionStrategy implements IClusterSelectionStrategy {

    private static final Logger LOGGER = LogManager.getLogger();

    // First component cursor for sequential cluster iteration
    private VTreeSearchCursor firstCursor;

    public SequentialClusterSelectionStrategy() {
    }

    @Override
    public void initialize(VTree vTree, double[] queryVector, IVTreeDistanceFunction distFunc, int k)
            throws HyracksDataException {
        // Sequential strategy doesn't need tree/query/distance info.
        // Cluster iteration is handled by the underlying cursor in full-scan mode.
    }

    @Override
    public ClusterSearchResult getNextCluster() throws HyracksDataException {
        if (firstCursor == null) {
            return null;
        }

        // Check if there are more clusters available
        if (!firstCursor.hasMoreClusters()) {
            LOGGER.trace("No more clusters");
            return null;
        }

        // Advance the first cursor to the next cluster (sequential iteration)
        boolean advanced = firstCursor.advanceToNextCluster();
        if (!advanced) {
            LOGGER.trace("Failed to advance to next cluster");
            return null;
        }

        // Get the cluster result after advancement
        ClusterSearchResult next = firstCursor.getCurrentClusterResult();

        if (next != null) {
            LOGGER.trace("Next cluster: cid={}, dirPage={}", next.centroidId, next.directoryPageId);
        } else {
            LOGGER.trace("advanceToNextCluster succeeded but getCurrentClusterResult is null");
        }

        return next;
    }

    @Override
    public boolean hasMoreClusters() {
        return firstCursor != null && firstCursor.hasMoreClusters();
    }

    @Override
    public boolean shouldStopAdvancing(int minClustersExplored, int resultsCollected) {
        // Never stop early in merge mode - must process ALL data
        return false;
    }

    @Override
    public Set<Integer> getVisitedCentroidIds() {
        // No deduplication in sequential (full-scan) mode — each cluster is visited exactly once in order.
        return Collections.emptySet();
    }

    @Override
    public void setFirstCursorForDFS(VTreeSearchCursor firstCursor) {
        this.firstCursor = firstCursor;
    }

    @Override
    public ClusterSearchResult getFirstCluster() {
        // First cluster is already opened by the cursor during doOpen()
        // Return current cluster result from first cursor
        return firstCursor != null ? firstCursor.getCurrentClusterResult() : null;
    }

    @Override
    public void setQuantizer(double[] quantizedQueryVector, IVTreeQuantizer quantizer) {
        // Sequential (full-scan) strategy does not use quantized distance.
    }

    @Override
    public void deferSeedCluster(ClusterSearchResult seed) {
        // Sequential (full-scan) strategy has no greedy seed to defer.
    }

    @Override
    public void reset() {
        // No per-run state to reset in sequential mode.
    }

    @Override
    public int getLevelWiseClusterCount() {
        // No level-wise in sequential mode
        return 0;
    }

    @Override
    public boolean isLevelWisePhaseComplete() {
        // Always "complete" since we don't use level-wise
        return true;
    }
}
