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
package org.apache.hyracks.storage.am.vector.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.am.vector.api.VTreeStaticTupleConstants;
import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Static helpers for navigating a VTree. Provides:
 * <ul>
 *   <li>{@link #findClosestCentroid} - root-to-leaf descent picking the single closest centroid.</li>
 *   <li>{@link #initializeClusterIterator} / {@link #findNextClosestCluster} - iterative DFS
 *       over leaf centroids in non-decreasing distance order with visited-id dedup.</li>
 *   <li>{@link #findCloseCentroidsLevelWiseGlobalSort} - level-wise probing with an epsilon
 *       window followed by a global sort and threshold filter at the leaf layer.</li>
 * </ul>
 * All methods pin/unpin buffer-cache pages internally; callers must not hold latches on the
 * traversed pages. Stateless and thread-safe.
 */
public class VTreeNavigationUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Maximum number of root-to-leaf hops in {@link #findClosestCentroid}; serves as a safety net
     * against malformed trees / cyclic child pointers. The real VTree is shallow (~3-5 levels).
     */
    private static final int MAX_TREE_DEPTH = 10;

    /**
     * Find the closest centroid by traversing the tree from root to leaf,
     * optionally computing a quantized distance for the best result.
     *
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param rootPageId Root page ID to start traversal
     * @param interiorFrameFactory Factory for creating interior frames
     * @param leafFrameFactory Factory for creating leaf frames
     * @param queryVector Query vector to find closest centroid for
     * @param distanceFunction Distance function to use for centroid finding
     * @param quantizedQueryVector Query vector in dequantized/reconstructed space — i.e. quantized then
     *        dequantized so it lives in the same space as the reconstructed leaf centroids it is compared against
     *        (nullable — pass null to skip quantized distance)
     * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable — pass null to skip)
     * @return ClusterSearchResult containing closest centroid information (with quantizedDistance if quantizer provided)
     * @throws HyracksDataException if any error occurs during traversal
     */
    public static ClusterSearchResult findClosestCentroid(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory, double[] queryVector,
            IVTreeDistanceFunction distanceFunction, double[] quantizedQueryVector, IVTreeQuantizer quantizer)
            throws HyracksDataException {

        int currentPageId = rootPageId;
        int hops = 0;

        while (true) {
            if (++hops > MAX_TREE_DEPTH) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Infinite loop detected in tree traversal");
            }

            PageScan scan = scanPage(bufferCache, fileId, currentPageId, queryVector, interiorFrameFactory,
                    leafFrameFactory, distanceFunction, quantizedQueryVector, quantizer);

            if (scan.isLeaf()) {
                // Leaf level - the closest centroid is the sort's first entry
                if (scan.leafCentroids.isEmpty()) {
                    throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No closest cluster found");
                }
                return scan.leafCentroids.get(0);
            }

            // Interior level - descend into the closest child
            if (scan.children.isEmpty()) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                        "No valid centroid found in interior cluster");
            }
            currentPageId = scan.children.get(0).childPageId();
        }
    }

    /**
     * Extract the full-precision centroid embedding from a static-structure tuple. The embedding sits at
     * {@link VTreeStaticTupleConstants#EMBEDDING_FIELD} in every interior and leaf layout, so only that
     * field is decoded; the surrounding fields (centroid id, child/metadata pointer, optional quantized
     * bytes / neighbor list) are left untouched.
     */
    private static double[] extractCentroid(ITreeIndexTupleReference tuple) throws HyracksDataException {
        int field = VTreeStaticTupleConstants.EMBEDDING_FIELD;
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(tuple.getFieldData(field),
                tuple.getFieldStart(field), tuple.getFieldLength(field)));
        return DoubleArraySerializerDeserializer.INSTANCE.deserialize(dis);
    }

    /**
     * Collects every child of an interior page (including its overflow chain) and returns them
     * sorted by distance to {@code queryVector}, closest first. {@code initialFrame} must already
     * be set to {@code startPageId} and that page must be pinned/latched by the caller; overflow
     * pages are pinned and released internally. Malformed tuples are skipped.
     */
    private static List<VTreeChildCentroid> collectAllChildCentroids(IBufferCache bufferCache, int fileId,
            double[] queryVector, int startPageId, IVTreeInteriorFrame initialFrame,
            ITreeIndexFrameFactory interiorFrameFactory, IVTreeDistanceFunction distanceFunction)
            throws HyracksDataException {

        List<VTreeChildCentroid> children = new ArrayList<>();
        int currentPageId = startPageId;
        IVTreeInteriorFrame currentFrame = initialFrame;
        boolean isFirstPage = true;
        ICachedPage currentPage = null;

        while (currentPageId != -1) {
            try {
                if (!isFirstPage) {
                    currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    currentPage.acquireReadLatch();
                    currentFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
                    currentFrame.setPage(currentPage);
                }

                int tupleCount = currentFrame.getTupleCount();
                boolean hasOverflow = currentFrame.getOverflowFlagBit();
                int nextPageId = hasOverflow ? currentFrame.getNextPage() : -1;

                for (int i = 0; i < tupleCount; i++) {
                    try {
                        ITreeIndexTupleReference tuple = currentFrame.createTupleReference();
                        tuple.resetByTupleIndex(currentFrame, i);
                        double[] centroid = extractCentroid(tuple);

                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = distanceFunction.apply(queryVector, centroid);
                        int childPageId = currentFrame.getChildPageId(i);
                        children.add(new VTreeChildCentroid(childPageId, distance, i));
                    } catch (HyracksDataException e) {
                        // Skip only genuinely malformed interior tuples (decode/accessor failures);
                        // unexpected runtime failures (NPE, contract violations) propagate so silent
                        // recall loss is surfaced. Logged at WARN with page id + tuple index.
                        LOGGER.log(Level.WARN, "Skipping malformed interior tuple {} on page {}: {}", i, currentPageId,
                                e.getMessage());
                    }
                }

                currentPageId = nextPageId;
                isFirstPage = false;

            } finally {
                if (!isFirstPage && currentPage != null) {
                    currentPage.releaseReadLatch();
                    bufferCache.unpin(currentPage);
                    currentPage = null;
                }
            }
        }

        children.sort(Comparator.comparingDouble(VTreeChildCentroid::distance));

        return children;
    }

    /**
     * Collects every centroid of a leaf page (including its overflow chain) and returns them
     * sorted by distance to {@code queryVector}, closest first. {@code initialFrame} must already
     * be set to {@code startPageId} and that page must be pinned/latched by the caller; overflow
     * pages are pinned and released internally.
     * <p>
     * When both {@code quantizer} and {@code quantizedQueryVector} are non-null, the quantized
     * distance D(q̃, C̃) is computed for each centroid and placed in the resulting
     * {@link ClusterSearchResult#quantizedDistance}; otherwise that field is {@code NaN}.
     * Malformed tuples are skipped.
     */
    private static List<ClusterSearchResult> collectAllLeafCentroids(IBufferCache bufferCache, int fileId,
            double[] queryVector, int startPageId, IVTreeLeafFrame initialFrame,
            ITreeIndexFrameFactory leafFrameFactory, IVTreeDistanceFunction distanceFunction,
            double[] quantizedQueryVector, IVTreeQuantizer quantizer) throws HyracksDataException {

        List<ClusterSearchResult> centroids = new ArrayList<>();
        int currentPageId = startPageId;
        IVTreeLeafFrame currentFrame = initialFrame;
        boolean isFirstPage = true;
        ICachedPage currentPage = null;

        while (currentPageId != -1) {
            try {
                if (!isFirstPage) {
                    currentPage = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, currentPageId));
                    currentPage.acquireReadLatch();
                    currentFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                    currentFrame.setPage(currentPage);
                }

                int tupleCount = currentFrame.getTupleCount();
                boolean hasOverflow = currentFrame.getOverflowFlagBit();
                int nextPageId = hasOverflow ? currentFrame.getNextLeaf() : -1;

                for (int i = 0; i < tupleCount; i++) {
                    try {
                        ITreeIndexTupleReference frameTuple = currentFrame.createTupleReference();
                        frameTuple.resetByTupleIndex(currentFrame, i);
                        double[] centroid = extractCentroid(frameTuple);
                        int centroidId = currentFrame.getCentroidId(i);
                        long directoryPageId = currentFrame.getMetadataPagePointer(i);

                        if (centroid.length != queryVector.length) {
                            continue;
                        }

                        double distance = distanceFunction.apply(queryVector, centroid);
                        double quantizedDistance = quantizedDistanceOrNaN(currentFrame, i, quantizedQueryVector,
                                quantizer, distanceFunction);

                        centroids.add(ClusterSearchResult.create(currentPageId, i, centroid.clone(), distance,
                                centroidId, directoryPageId, quantizedDistance));
                    } catch (HyracksDataException e) {
                        // Skip only genuinely malformed leaf tuples (decode/accessor failures);
                        // unexpected runtime failures (NPE, contract violations) propagate so silent
                        // recall loss is surfaced. Logged at WARN with page id + tuple index.
                        LOGGER.log(Level.WARN, "Skipping malformed leaf tuple {} on page {}: {}", i, currentPageId,
                                e.getMessage());
                    }
                }

                currentPageId = nextPageId;
                isFirstPage = false;

            } finally {
                if (!isFirstPage && currentPage != null) {
                    currentPage.releaseReadLatch();
                    bufferCache.unpin(currentPage);
                    currentPage = null;
                }
            }
        }

        centroids.sort(Comparator.comparingDouble(c -> c.distance));
        return centroids;
    }

    /**
     * Quantized distance D(q̃, C̃) for the leaf centroid at {@code tupleIndex}: dequantize its stored
     * quantized bytes and compare against {@code quantizedQueryVector} (the query already reconstructed
     * into the same dequantized space). Returns {@code Double.NaN} when not in quantized mode
     * ({@code quantizer}/{@code quantizedQueryVector} null) or the centroid carries no quantized bytes —
     * the sentinel {@link ClusterSearchResult#quantizedDistance} readers treat as "not computed".
     */
    private static double quantizedDistanceOrNaN(IVTreeLeafFrame frame, int tupleIndex, double[] quantizedQueryVector,
            IVTreeQuantizer quantizer, IVTreeDistanceFunction distanceFunction) throws HyracksDataException {
        if (quantizer == null || quantizedQueryVector == null) {
            return Double.NaN;
        }
        byte[] quantizedCentroidBytes = frame.getQuantizedCentroidBytes(tupleIndex);
        if (quantizedCentroidBytes == null) {
            return Double.NaN;
        }
        double[] dequantizedCentroid = quantizer.dequantize(quantizedCentroidBytes);
        return distanceFunction.apply(quantizedQueryVector, dequantizedCentroid);
    }

    /**
     * Result of scanning one static-structure page under a single read latch. A leaf page yields its
     * centroids (distance-sorted, closest first) in {@link #leafCentroids}; an interior page yields its
     * children (distance-sorted) in {@link #children}. Exactly one of the two is non-null — use
     * {@link #isLeaf()} to discriminate.
     */
    private static final class PageScan {
        private final List<ClusterSearchResult> leafCentroids;
        private final List<VTreeChildCentroid> children;

        // One private canonical constructor; the two factories below name leaf-vs-interior explicitly.
        // (Two single-arg constructors would collide under generic erasure — both erase to PageScan(List).)
        private PageScan(List<ClusterSearchResult> leafCentroids, List<VTreeChildCentroid> children) {
            this.leafCentroids = leafCentroids;
            this.children = children;
        }

        /** Leaf page: carries its distance-sorted centroids. */
        private static PageScan forLeaf(List<ClusterSearchResult> leafCentroids) {
            return new PageScan(leafCentroids, null);
        }

        /** Interior page: carries its distance-sorted children. */
        private static PageScan forInterior(List<VTreeChildCentroid> children) {
            return new PageScan(null, children);
        }

        private boolean isLeaf() {
            return leafCentroids != null;
        }
    }

    /**
     * Pin {@code pageId}, read-latch it once, and collect its entries: for a leaf page its centroids, for
     * an interior page its children — each distance-sorted, closest first, including the page's overflow
     * chain. Determining leaf-vs-interior and collecting happen under the same latch (one pin per hop), and
     * the page (plus any overflow pages pinned by the collectors) is released before returning. Shared by
     * the three root-to-leaf descent walkers ({@link #findClosestCentroid}, {@link #initializeClusterIterator},
     * {@link #descendToLeaf}).
     */
    private static PageScan scanPage(IBufferCache bufferCache, int fileId, int pageId, double[] queryVector,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            IVTreeDistanceFunction distanceFunction, double[] quantizedQueryVector, IVTreeQuantizer quantizer)
            throws HyracksDataException {
        ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
        try {
            page.acquireReadLatch();
            // isLeaf() reads the level byte from the shared header, so a leaf frame can classify any page.
            IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
            leafFrame.setPage(page);
            if (leafFrame.isLeaf()) {
                return PageScan.forLeaf(collectAllLeafCentroids(bufferCache, fileId, queryVector, pageId, leafFrame,
                        leafFrameFactory, distanceFunction, quantizedQueryVector, quantizer));
            }
            IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
            interiorFrame.setPage(page);
            return PageScan.forInterior(collectAllChildCentroids(bufferCache, fileId, queryVector, pageId,
                    interiorFrame, interiorFrameFactory, distanceFunction));
        } finally {
            page.releaseReadLatch();
            bufferCache.unpin(page);
        }
    }

    // ==================== Multi-Cluster Iterative DFS Support ====================

    /**
     * Initialize the cluster iterator by building navigation stack from root to first leaf.
     * This performs DFS to find the closest cluster and sets up the stack for backtracking.
     *
     * @param state Navigation state to initialize
     * @return The first (closest) cluster, or null if tree is empty
     * @throws HyracksDataException if any error occurs
     */
    public static ClusterSearchResult initializeClusterIterator(VTreeNavigationState state,
            IVTreeDistanceFunction distanceFunction) throws HyracksDataException {
        if (state.initialized) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Iterator already initialized");
        }

        state.stack.clear();
        state.initialized = true;

        // Start DFS from root
        int currentPageId = state.rootPageId;

        while (true) {
            PageScan scan = scanPage(state.bufferCache, state.fileId, currentPageId, state.queryVector,
                    state.interiorFrameFactory, state.leafFrameFactory, distanceFunction, null, null);

            if (scan.isLeaf()) {
                if (scan.leafCentroids.isEmpty()) {
                    return null; // Empty tree
                }
                // Push leaf frame; return its first centroid as the closest cluster, marking it visited
                VTreeNavigationFrame leafNavFrame =
                        VTreeNavigationFrame.newLeafFrame(currentPageId, scan.leafCentroids);
                state.stack.push(leafNavFrame);
                ClusterSearchResult first = leafNavFrame.nextCentroid();
                state.markVisited(first.centroidId);
                return first;
            }

            // Interior level: push the frame and descend into the closest child
            if (scan.children.isEmpty()) {
                throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "Interior node has no valid children");
            }
            VTreeNavigationFrame interiorNavFrame = VTreeNavigationFrame.newInteriorFrame(currentPageId, scan.children);
            state.stack.push(interiorNavFrame);
            currentPageId = interiorNavFrame.nextChild().childPageId();
        }
    }

    /**
     * Find the next closest cluster using DFS with backtracking.
     *
     * Algorithm:
     * 1. Try next centroid on current leaf page
     * 2. If leaf exhausted, pop stack (backtrack to parent)
     * 3. Try next child from parent
     * 4. Descend to new leaf
     * 5. Return next centroid
     *
     * @param state Navigation state with stack
     * @return Next closest cluster, or null if all clusters exhausted
     * @throws HyracksDataException if any error occurs
     */
    public static ClusterSearchResult findNextClosestCluster(VTreeNavigationState state,
            IVTreeDistanceFunction distanceFunction) throws HyracksDataException {
        if (!state.initialized) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "Iterator not initialized. Call initializeClusterIterator() first");
        }

        while (!state.stack.isEmpty()) {
            VTreeNavigationFrame topFrame = state.stack.peek();

            if (topFrame.isLeaf()) {
                // At leaf level: try next centroid in current page, skipping visited ones
                while (topFrame.hasNextCentroid()) {
                    ClusterSearchResult next = topFrame.nextCentroid();

                    if (state.isVisited(next.centroidId)) {
                        LOGGER.log(Level.TRACE, "[DFS] skipping visited cid={} d={}", next.centroidId, next.distance);
                        continue;
                    }

                    state.markVisited(next.centroidId);
                    LOGGER.log(Level.TRACE, "[DFS] return cid={} d={} pageId={} idx={}/{}", next.centroidId,
                            next.distance, topFrame.pageId(), topFrame.emittedCount(), topFrame.centroidCount());
                    return next;
                }

                // All centroids in this leaf exhausted or visited, backtrack
                state.stack.pop();
                continue;

            } else {
                // At interior level: try next child
                if (topFrame.hasNextChild()) {
                    VTreeChildCentroid nextChild = topFrame.nextChild();

                    // Descend to this child and navigate to leaf
                    ClusterSearchResult result = descendToLeaf(state, nextChild.childPageId(), distanceFunction);

                    if (result != null) {
                        return result;
                    }
                    // If descend failed, continue with next child
                    continue;

                } else {
                    // All children explored, backtrack
                    state.stack.pop();
                    continue;
                }
            }
        }

        LOGGER.log(Level.TRACE, "[DFS] stack exhausted");
        return null;
    }

    /**
     * Descend from given page to leaf level, building stack along the way.
     * Always picks closest child at each interior level.
     *
     * @param state Navigation state
     * @param startPageId Page to start descent from
     * @return First centroid at leaf level, or null if no valid path
     * @throws HyracksDataException if any error occurs
     */
    private static ClusterSearchResult descendToLeaf(VTreeNavigationState state, int startPageId,
            IVTreeDistanceFunction distanceFunction) throws HyracksDataException {

        int currentPageId = startPageId;

        while (true) {
            PageScan scan = scanPage(state.bufferCache, state.fileId, currentPageId, state.queryVector,
                    state.interiorFrameFactory, state.leafFrameFactory, distanceFunction, null, null);

            if (scan.isLeaf()) {
                if (scan.leafCentroids.isEmpty()) {
                    return null; // Empty leaf
                }
                VTreeNavigationFrame leafNavFrame =
                        VTreeNavigationFrame.newLeafFrame(currentPageId, scan.leafCentroids);
                state.stack.push(leafNavFrame);

                // Return the first unvisited centroid on this leaf
                while (leafNavFrame.hasNextCentroid()) {
                    ClusterSearchResult first = leafNavFrame.nextCentroid();
                    if (!state.isVisited(first.centroidId)) {
                        state.markVisited(first.centroidId);
                        return first;
                    }
                    LOGGER.log(Level.TRACE, "[DFS descendToLeaf] skipping visited cid={}", first.centroidId);
                }
                return null;
            }

            // Interior: push the frame and descend into the closest child
            if (scan.children.isEmpty()) {
                return null; // No valid children
            }
            VTreeNavigationFrame interiorNavFrame = VTreeNavigationFrame.newInteriorFrame(currentPageId, scan.children);
            state.stack.push(interiorNavFrame);
            currentPageId = interiorNavFrame.nextChild().childPageId();
        }
    }

    // ==================== Level-Wise Cluster Selection Support ====================

    /**
     * Multiplicative-epsilon distance threshold relative to {@code |closestDistance|}: yields
     * {@code (1+epsilon)*d} for positive {@code d} and {@code (1-epsilon)*d} for negative {@code d}
     * (negated dot product, where smaller is better). An additive {@code d+epsilon} form is a near
     * no-op for euclidean_squared / high-dim L2 where distances are O(10)-O(10^3), which collapses the
     * search to ~1 cluster regardless of nprobe/epsilon.
     */
    private static double epsilonThreshold(double closestDistance, double epsilon) {
        return closestDistance + Math.abs(closestDistance) * epsilon;
    }

    /**
     * Find close centroids using level-by-level cross-pollination with global sorting.
     * At each interior node, explores all children within closestDistance * (1 + epsilon)
     * (for positive distances) / closestDistance * (1 - epsilon) (for negative distances
     * such as negated dot product).
     * At leaf level, collects ALL centroids, then sorts globally and filters by epsilon.
     * <p>
     * 1. Traverse tree using epsilon threshold at interior levels
     * 2. Collect ALL reachable leaf centroids
     * 3. Sort globally by distance to query
     * 4. Filter by global closest distance + epsilon
     *
     * @param bufferCache Buffer cache for page access
     * @param fileId File ID for page identification
     * @param rootPageId Root page ID to start traversal
     * @param interiorFrameFactory Factory for creating interior frames
     * @param leafFrameFactory Factory for creating leaf frames
     * @param queryVector Query vector to find closest centroids for
     * @param distanceFunction Distance function to use
     * @param epsilon Relative distance threshold (multiplicative). Threshold for level/global
     *                pruning is computed as {@code closestDistance + |closestDistance| * epsilon},
     *                i.e. (1+epsilon)*d for positive d and (1-epsilon)*d for negative d.
     * @return List of ClusterSearchResult containing all qualifying centroids, sorted by distance
     * @throws HyracksDataException if any error occurs during traversal
     */
    public static List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(IBufferCache bufferCache, int fileId,
            int rootPageId, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector, IVTreeDistanceFunction distanceFunction, double epsilon) throws HyracksDataException {
        return findCloseCentroidsLevelWiseGlobalSort(bufferCache, fileId, rootPageId, interiorFrameFactory,
                leafFrameFactory, queryVector, distanceFunction, epsilon, null, null);
    }

    /**
     * Overload that accepts quantizer parameters for computing quantized D(q,C).
     * Navigation still uses full-precision distances; quantizedDistance is extra metadata
     * populated in each ClusterSearchResult for triangle inequality pruning at the cursor level.
     *
     * @param quantizedQueryVector Query vector in dequantized/reconstructed space — quantized then
     *        dequantized so it lives in the same space as the reconstructed leaf centroids (nullable)
     * @param quantizer Quantizer for dequantizing leaf centroid bytes (nullable)
     */
    public static List<ClusterSearchResult> findCloseCentroidsLevelWiseGlobalSort(IBufferCache bufferCache, int fileId,
            int rootPageId, ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector, IVTreeDistanceFunction distanceFunction, double epsilon,
            double[] quantizedQueryVector, IVTreeQuantizer quantizer) throws HyracksDataException {

        List<ClusterSearchResult> allCentroids = new ArrayList<>();
        Set<Integer> visitedLeafPages = new HashSet<>();
        Queue<VTreeLevelNode> queue = new ArrayDeque<>();
        queue.add(new VTreeLevelNode(rootPageId, 0));

        // Phase 1: Collect all centroids from all reachable leaf pages
        while (!queue.isEmpty()) {
            int currentLevel = queue.peek().level();

            List<VTreeLevelNode> currentLevelNodes = new ArrayList<>();

            // Collect all nodes at current level
            while (!queue.isEmpty() && queue.peek().level() == currentLevel) {

                currentLevelNodes.add(queue.poll());
            }

            // Process all nodes at current level
            for (VTreeLevelNode node : currentLevelNodes) {
                ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, node.pageId()));

                try {
                    page.acquireReadLatch();

                    IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                    leafFrame.setPage(page);
                    boolean isLeaf = leafFrame.isLeaf();

                    if (isLeaf) {
                        // Leaf node processing - collect ALL centroids (no threshold filtering yet)
                        if (!visitedLeafPages.add(node.pageId())) {

                            continue; // Already visited
                        }

                        List<ClusterSearchResult> leafCentroids =
                                collectAllLeafCentroids(bufferCache, fileId, queryVector, node.pageId(), leafFrame,

                                        leafFrameFactory, distanceFunction, quantizedQueryVector, quantizer);

                        if (leafCentroids.isEmpty()) {
                            continue;
                        }

                        // Add ALL centroids from this leaf page to global collection
                        allCentroids.addAll(leafCentroids);

                    } else {
                        // Interior node processing - explore children within epsilon
                        IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
                        interiorFrame.setPage(page);
                        queue.addAll(childrenWithinEpsilon(bufferCache, fileId, queryVector, node, interiorFrame,
                                interiorFrameFactory, distanceFunction, epsilon));
                    }

                } finally {
                    page.releaseReadLatch();
                    bufferCache.unpin(page);
                }
            }
        }

        if (allCentroids.isEmpty()) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE, "No closest clusters found");
        }

        // Phase 2: Sort ALL centroids globally by distance to query vector
        allCentroids.sort(Comparator.comparingDouble(r -> r.distance));

        // Phase 3: Apply epsilon threshold based on the globally closest centroid
        return applyGlobalEpsilonFilter(allCentroids, epsilon);
    }

    /**
     * Interior-node frontier expansion for the level-wise BFS: collect this interior page's children
     * (closest first) and return the next-level nodes whose distance is within the epsilon window of the
     * closest child. {@code interiorFrame} must be set to {@code node}'s page and that page must be
     * pinned/latched by the caller (collectAllChildCentroids pins only the overflow chain). Empty if the
     * page has no valid children.
     */
    private static List<VTreeLevelNode> childrenWithinEpsilon(IBufferCache bufferCache, int fileId,
            double[] queryVector, VTreeLevelNode node, IVTreeInteriorFrame interiorFrame,
            ITreeIndexFrameFactory interiorFrameFactory, IVTreeDistanceFunction distanceFunction, double epsilon)
            throws HyracksDataException {
        List<VTreeChildCentroid> sortedChildren = collectAllChildCentroids(bufferCache, fileId, queryVector,
                node.pageId(), interiorFrame, interiorFrameFactory, distanceFunction);
        if (sortedChildren.isEmpty()) {
            return List.of();
        }
        double localThreshold = epsilonThreshold(sortedChildren.get(0).distance(), epsilon);
        List<VTreeLevelNode> nextLevel = new ArrayList<>();
        for (VTreeChildCentroid child : sortedChildren) {
            if (child.distance() <= localThreshold) {
                nextLevel.add(new VTreeLevelNode(child.childPageId(), node.level() + 1));
            } else {
                break; // Children are sorted, no more qualify
            }
        }
        return nextLevel;
    }

    /**
     * Phase 3 of level-wise search: given all collected centroids already sorted by distance ascending,
     * keep only those within the epsilon window of the globally closest centroid. Returns the input list
     * unchanged when {@code epsilon <= 0.0} (no filtering). Pure list operation — no page access.
     */
    private static List<ClusterSearchResult> applyGlobalEpsilonFilter(List<ClusterSearchResult> sortedCentroids,
            double epsilon) {
        if (epsilon <= 0.0) {
            return sortedCentroids;
        }
        double globalThreshold = epsilonThreshold(sortedCentroids.get(0).distance, epsilon);
        List<ClusterSearchResult> filteredCentroids = new ArrayList<>();
        for (ClusterSearchResult result : sortedCentroids) {
            if (result.distance <= globalThreshold) {
                filteredCentroids.add(result);
            } else {
                break; // Centroids are sorted, so we can break early
            }
        }
        return filteredCentroids;
    }

}
