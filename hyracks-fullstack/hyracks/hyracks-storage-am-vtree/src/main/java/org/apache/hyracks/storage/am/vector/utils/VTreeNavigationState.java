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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Mutable navigation state for the DFS cluster iterator: holds the buffer cache / frame factories
 * needed to pin pages, the navigation stack of {@link VTreeNavigationFrame}s, and a (possibly
 * shared) set of visited centroid ids used to deduplicate when level-wise selection and DFS
 * fallback are combined.
 * <p>
 * Not thread-safe: an instance is confined to a single search / operation context and must not be
 * shared concurrently across threads.
 */
public class VTreeNavigationState {

    public final IBufferCache bufferCache;
    public final int fileId;
    public final int rootPageId;
    public final ITreeIndexFrameFactory interiorFrameFactory;
    public final ITreeIndexFrameFactory leafFrameFactory;
    public final double[] queryVector;
    public final Deque<VTreeNavigationFrame> stack;
    public boolean initialized;

    /** Visited centroid ids; may be shared across LSM components for cross-component dedup. */
    private Set<Integer> visitedCentroidIds;

    public VTreeNavigationState(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory,
            double[] queryVector) {
        this(bufferCache, fileId, rootPageId, interiorFrameFactory, leafFrameFactory, queryVector, new HashSet<>());
    }

    /**
     * Constructor accepting a shared visited-id set. Use this when the same dedup set must be
     * observed across multiple LSM components. A {@code null} {@code sharedVisitedSet} falls back
     * to a fresh local set.
     */
    public VTreeNavigationState(IBufferCache bufferCache, int fileId, int rootPageId,
            ITreeIndexFrameFactory interiorFrameFactory, ITreeIndexFrameFactory leafFrameFactory, double[] queryVector,
            Set<Integer> sharedVisitedSet) {
        this.bufferCache = bufferCache;
        this.fileId = fileId;
        this.rootPageId = rootPageId;
        this.interiorFrameFactory = interiorFrameFactory;
        this.leafFrameFactory = leafFrameFactory;
        this.queryVector = queryVector;
        this.stack = new ArrayDeque<>();
        this.initialized = false;
        this.visitedCentroidIds = sharedVisitedSet != null ? sharedVisitedSet : new HashSet<>();
    }

    public boolean isVisited(int centroidId) {
        return visitedCentroidIds.contains(centroidId);
    }

    public void markVisited(int centroidId) {
        visitedCentroidIds.add(centroidId);
    }

    /** Swap in an external visited set (e.g. to share it across LSM components). */
    public void setVisitedSet(Set<Integer> visitedSet) {
        this.visitedCentroidIds = visitedSet;
    }
}
