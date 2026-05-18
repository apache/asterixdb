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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchCursor;
import org.apache.hyracks.storage.am.vector.utils.VTreeNavigationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Nprobe-based cluster selection strategy.
 *
 * Explores clusters in two phases:
 * 1. Level-wise: Computes clusters within epsilon distance threshold, sorted globally by distance
 * 2. DFS fallback: When level-wise exhausted, uses DFS backtracking from first component
 *
 * Stopping condition: minClustersExplored >= nprobe AND resultsCollected >= K
 */
public class NprobeClusterSelectionStrategy implements IClusterSelectionStrategy {
    private static final Logger LOGGER = LogManager.getLogger();

    // Parameters
    private final double minProbeFraction;
    private final double epsilon;
    private int nprobe; // Computed from minProbeFraction * totalLeafClusters at initialize time
    private int K;

    // Level-wise state
    private List<ClusterSearchResult> globalLevelWiseClusters;
    private int globalClusterIndex;
    private boolean levelWisePhaseComplete;

    // Shared state for cross-component deduplication
    private Set<Integer> visitedCentroidIds;

    // Quantizer state for computing quantized D(q,C) in ClusterSearchResult
    private double[] quantizedQueryVector;
    private IVTreeQuantizer quantizer;

    // For DFS fallback
    private VTreeSearchCursor firstCursor;

    // Seed cluster displaced by the level-wise re-open; probed once after the level-wise phase (see
    // deferSeedCluster).
    private ClusterSearchResult deferredSeedCluster;

    public NprobeClusterSelectionStrategy(double minProbeFraction, double epsilon) {
        this.minProbeFraction = minProbeFraction;
        this.epsilon = epsilon;
        this.nprobe = 1; // Will be computed in initialize() from minProbeFraction * totalLeafClusters
        this.visitedCentroidIds = new HashSet<>();
    }

    @Override
    public void setQuantizer(double[] quantizedQueryVector, IVTreeQuantizer quantizer) {
        this.quantizedQueryVector = quantizedQueryVector;
        this.quantizer = quantizer;
    }

    @Override
    public void initialize(VTree vTree, double[] queryVector, IVTreeDistanceFunction distFunc, int k)
            throws HyracksDataException {
        this.K = k;
        this.globalClusterIndex = 0;
        this.levelWisePhaseComplete = false;

        // Compute level-wise clusters using VTreeNavigationUtils
        if (queryVector != null && epsilon > 0.0 && vTree != null) {
            try {
                globalLevelWiseClusters = VTreeNavigationUtils.findCloseCentroidsLevelWiseGlobalSort(
                        vTree.getNavigationBufferCache(), vTree.getNavigationFileId(), vTree.getNavigationRootPageId(),
                        vTree.getInteriorFrameFactory(), vTree.getLeafFrameFactory(), queryVector, distFunc, epsilon,
                        quantizedQueryVector, quantizer);

                // Compute nprobe from minProbeFraction * totalLeafClusters
                int totalLeafClusters = globalLevelWiseClusters != null ? globalLevelWiseClusters.size() : 1;
                // Always probe at least one cluster: for small indexes floor(total*fraction) can round to
                // 0, which would make the search entirely K-gated. The max(1,...) clamp is intentional and
                // makes nprobe non-monotonic vs the configured fraction for tiny datasets.
                this.nprobe = Math.max(1, (int) Math.floor(totalLeafClusters * minProbeFraction));

                // Mark first cluster as visited and start getNextCluster() from index 1
                // The cursor handles index 0 separately via getFirstCluster()
                if (globalLevelWiseClusters != null && !globalLevelWiseClusters.isEmpty()) {
                    visitedCentroidIds.add(globalLevelWiseClusters.get(0).centroidId);
                    globalClusterIndex = 1; // Skip first cluster in getNextCluster()
                }

                LOGGER.trace("Computed {} level-wise clusters with epsilon={}, minProbeFraction={}, nprobe={}",
                        totalLeafClusters, epsilon, minProbeFraction, nprobe);
            } catch (HyracksDataException e) {
                // Level-wise probing is an optimization over the DFS fallback; on a storage error we can
                // still answer via DFS, but that reduces recall, so surface it at WARN with the cause
                // rather than hiding a degraded search at TRACE. Only HyracksDataException is tolerated
                // here — an unexpected RuntimeException (NPE, contract violation) propagates so real bugs
                // are not masked as a silent recall drop.
                LOGGER.warn("Level-wise cluster computation failed; falling back to DFS-only cluster "
                        + "selection (recall may be reduced)", e);
                globalLevelWiseClusters = null;
            }
        }
    }

    @Override
    public ClusterSearchResult getNextCluster() throws HyracksDataException {
        // Phase 1: Level-wise clusters (pre-computed, globally sorted by distance)
        if (!levelWisePhaseComplete && globalLevelWiseClusters != null
                && globalClusterIndex < globalLevelWiseClusters.size()) {

            ClusterSearchResult nextCluster = globalLevelWiseClusters.get(globalClusterIndex);
            globalClusterIndex++;

            // Mark visited for DFS fallback deduplication
            visitedCentroidIds.add(nextCluster.centroidId);

            LOGGER.trace("Level-wise: cluster {}/{} (cid={}, distance={}, dirPage={})", globalClusterIndex,
                    globalLevelWiseClusters.size(), nextCluster.centroidId, nextCluster.distance,
                    nextCluster.directoryPageId);

            if (globalClusterIndex >= globalLevelWiseClusters.size()) {
                levelWisePhaseComplete = true;
                LOGGER.trace("Level-wise phase complete. Visited centroids: {}", visitedCentroidIds);
            }

            return nextCluster;
        }

        // Phase 2: DFS fallback - get next from first component's DFS
        levelWisePhaseComplete = true;

        // After the level-wise clusters and before the DFS fallback, yield the displaced seed once.
        // The DFS below can't reach it (already consumed and marked visited), so it must be explicit.
        if (deferredSeedCluster != null) {
            ClusterSearchResult seed = deferredSeedCluster;
            deferredSeedCluster = null;
            visitedCentroidIds.add(seed.centroidId);
            LOGGER.trace("Probing deferred greedy-seed cluster cid={} (dirPage={})", seed.centroidId,
                    seed.directoryPageId);
            return seed;
        }

        if (firstCursor == null) {
            return null;
        }

        ClusterSearchResult next = firstCursor.findNextClusterDFS();

        if (next == null) {
            LOGGER.trace("DFS exhausted, no more clusters. Visited centroids: {}", visitedCentroidIds);
            return null;
        }

        LOGGER.trace("DFS fallback: cluster cid={}, distance={}, dirPage={}", next.centroidId, next.distance,
                next.directoryPageId);

        return next;
    }

    @Override
    public boolean hasMoreClusters() {
        // Check level-wise first
        if (!levelWisePhaseComplete && globalLevelWiseClusters != null
                && globalClusterIndex < globalLevelWiseClusters.size()) {
            return true;
        }

        // Deferred seed still pending.
        if (deferredSeedCluster != null) {
            return true;
        }

        // Check DFS via first cursor
        if (firstCursor != null) {
            return firstCursor.hasMoreClusters();
        }

        return false;
    }

    @Override
    public void deferSeedCluster(ClusterSearchResult seed) {
        if (seed == null || isInLevelWiseList(seed.centroidId)) {
            // Nothing to rescue: a seed already in the level-wise window is scanned by the level-wise
            // phase, so deferring it too would double-scan the cluster.
            this.deferredSeedCluster = null;
        } else {
            // Out-of-window seed: the level-wise phase will not scan it, so probe it afterwards.
            this.deferredSeedCluster = seed;
        }
    }

    private boolean isInLevelWiseList(int centroidId) {
        if (globalLevelWiseClusters == null) {
            return false;
        }
        for (ClusterSearchResult c : globalLevelWiseClusters) {
            if (c.centroidId == centroidId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean shouldStopAdvancing(int minClustersExplored, int resultsCollected) {
        // Stop when: explored at least nprobe clusters AND collected at least K results
        return minClustersExplored >= nprobe && resultsCollected >= K;
    }

    /**
     * Returns the live visited-centroid set, which {@code getNextCluster()} also mutates. This strategy
     * is single-threaded per cursor: the set is shared with the component cursors for dedup and must not
     * be iterated concurrently with a {@code getNextCluster()} call. Not safe for parallel component probing.
     */
    @Override
    public Set<Integer> getVisitedCentroidIds() {
        return visitedCentroidIds;
    }

    @Override
    public void setFirstCursorForDFS(VTreeSearchCursor firstCursor) {
        this.firstCursor = firstCursor;
    }

    @Override
    public ClusterSearchResult getFirstCluster() {
        if (globalLevelWiseClusters != null && !globalLevelWiseClusters.isEmpty()) {
            return globalLevelWiseClusters.get(0);
        }
        return null;
    }

    @Override
    public void reset() {
        this.globalLevelWiseClusters = null;
        this.globalClusterIndex = 0;
        this.levelWisePhaseComplete = false;
        this.visitedCentroidIds.clear();
        this.firstCursor = null;
        this.deferredSeedCluster = null;
        this.quantizedQueryVector = null;
        this.quantizer = null;
    }

    @Override
    public int getLevelWiseClusterCount() {
        return globalLevelWiseClusters != null ? globalLevelWiseClusters.size() : 0;
    }

    @Override
    public boolean isLevelWisePhaseComplete() {
        return levelWisePhaseComplete;
    }

    // Getters for logging/debugging
    public int getNprobe() {
        return nprobe;
    }

    public double getMinProbeFraction() {
        return minProbeFraction;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public int getK() {
        return K;
    }
}
