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

import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchCursor;

/**
 * Strategy interface for cluster selection in multi-cluster vector search.
 *
 * Encapsulates the logic for determining which clusters to explore and when to stop.
 * This allows different search strategies (nprobe-based, beam search, adaptive) to be
 * plugged into the LSM cursor without modifying the cursor itself.
 */
public interface IClusterSelectionStrategy {

    /**
     * Initialize the strategy with search context.
     * Called once at the beginning of a search.
     *
     * @param vTree The VTree (first component's tree for level-wise computation)
     * @param queryVector The query vector
     * @param distFunc Distance function
     * @param k Target number of results
     * @throws HyracksDataException if initialization fails
     */
    void initialize(VTree vTree, double[] queryVector, IVTreeDistanceFunction distFunc, int k)
            throws HyracksDataException;

    /**
     * Get the next cluster to explore.
     * Returns clusters based on the strategy (e.g., level-wise first, then DFS fallback).
     *
     * @return ClusterSearchResult with directoryPageId for O(1) access, or null if no more clusters
     * @throws HyracksDataException if an error occurs
     */
    ClusterSearchResult getNextCluster() throws HyracksDataException;

    /**
     * Check if there are more clusters available to explore.
     *
     * @return true if more clusters can be retrieved via getNextCluster()
     */
    boolean hasMoreClusters();

    /**
     * Check if we should stop advancing to next cluster based on results collected.
     *
     * @param minClustersExplored Minimum clusters explored across all components
     * @param resultsCollected Number of reconciled results collected so far
     * @return true if we should stop advancing (e.g., nprobe satisfied AND K reached)
     */
    boolean shouldStopAdvancing(int minClustersExplored, int resultsCollected);

    /**
     * Get the shared visited set for cross-component deduplication.
     * This set is passed to all component cursors to avoid revisiting clusters.
     *
     * @return Set of visited centroid IDs
     */
    Set<Integer> getVisitedCentroidIds();

    /**
     * Set the first cursor for DFS fallback.
     * Called after all component cursors are opened.
     * The first cursor maintains NavigationState for DFS traversal.
     *
     * @param firstCursor First component's VTreeSearchCursor
     */
    void setFirstCursorForDFS(VTreeSearchCursor firstCursor);

    /**
     * Get the first cluster result (computed during initialization).
     * Used to synchronize all component cursors to the same initial cluster.
     *
     * @return The first cluster to explore, or null if not yet computed
     */
    ClusterSearchResult getFirstCluster();

    /**
     * Set quantizer state for computing quantized D(q,C) in ClusterSearchResult.
     * Call before initialize() so that level-wise navigation can populate
     * ClusterSearchResult.quantizedDistance for each leaf centroid found.
     *
     * @param quantizedQueryVector Dequantized form of query vector (nullable — pass null to skip)
     * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable — pass null to skip)
     */
    void setQuantizer(double[] quantizedQueryVector, IVTreeQuantizer quantizer);

    /**
     * Hand over the greedy-DFS seed cluster that the level-wise re-open displaced, so the strategy can
     * probe it once after the level-wise phase. {@code initializeClusterIterator} already consumed it
     * from the DFS iterator and marked it visited, so otherwise it is never scanned and its records are
     * dropped (with {@code cross_pollination_m=1} there is no replica to cover them).
     *
     * @param seed the displaced seed cluster, or {@code null} to clear
     */
    void deferSeedCluster(ClusterSearchResult seed);

    /**
     * Reset the strategy for a new search.
     * Called when the cursor is reopened.
     */
    void reset();

    /**
     * Get the number of level-wise clusters computed.
     * Used for logging/debugging.
     *
     * @return Number of level-wise clusters, or 0 if not computed
     */
    int getLevelWiseClusterCount();

    /**
     * Check if level-wise phase is complete.
     * Used for logging/debugging.
     *
     * @return true if level-wise exploration is done
     */
    boolean isLevelWisePhaseComplete();
}
