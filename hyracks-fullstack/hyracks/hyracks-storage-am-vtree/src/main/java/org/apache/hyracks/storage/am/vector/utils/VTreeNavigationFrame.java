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

import java.util.List;

import org.apache.hyracks.storage.am.vector.impls.ClusterSearchResult;

/**
 * One stack frame for the DFS cluster iterator. A frame represents either an interior page
 * (carrying its children sorted by distance to the query) or a leaf page (carrying its centroids).
 * An internal cursor tracks the next entry to emit; advance it through {@link #nextChild()} /
 * {@link #nextCentroid()}. Build frames with {@link #newInteriorFrame} / {@link #newLeafFrame}.
 * <p>
 * Not thread-safe: the cursor is mutated during iteration; a frame is confined to a single
 * search / operation context.
 */
public class VTreeNavigationFrame {

    private final int pageId;
    private final boolean isLeaf;
    /** Children sorted by distance to query; non-null iff interior. */
    private final List<VTreeChildCentroid> sortedChildren;
    /** Leaf centroids sorted by distance to query; non-null iff leaf. */
    private final List<ClusterSearchResult> sortedCentroids;
    /** Index of the next child / centroid to emit. */
    private int nextIndex;

    private VTreeNavigationFrame(int pageId, boolean isLeaf, List<VTreeChildCentroid> sortedChildren,
            List<ClusterSearchResult> sortedCentroids) {
        this.pageId = pageId;
        this.isLeaf = isLeaf;
        this.sortedChildren = sortedChildren;
        this.sortedCentroids = sortedCentroids;
        this.nextIndex = 0;
    }

    /** Frame over an interior page's distance-sorted children. */
    public static VTreeNavigationFrame newInteriorFrame(int pageId, List<VTreeChildCentroid> sortedChildren) {
        return new VTreeNavigationFrame(pageId, false, sortedChildren, null);
    }

    /** Frame over a leaf page's distance-sorted centroids. */
    public static VTreeNavigationFrame newLeafFrame(int pageId, List<ClusterSearchResult> sortedCentroids) {
        return new VTreeNavigationFrame(pageId, true, null, sortedCentroids);
    }

    public int pageId() {
        return pageId;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    /** Number of entries already emitted from this frame (the current cursor position). */
    public int emittedCount() {
        return nextIndex;
    }

    /** Total centroids on this (leaf) frame. */
    public int centroidCount() {
        return sortedCentroids.size();
    }

    public boolean hasNextChild() {
        return sortedChildren != null && nextIndex < sortedChildren.size();
    }

    public boolean hasNextCentroid() {
        return sortedCentroids != null && nextIndex < sortedCentroids.size();
    }

    public VTreeChildCentroid nextChild() {
        if (hasNextChild()) {
            return sortedChildren.get(nextIndex++);
        }
        return null;
    }

    public ClusterSearchResult nextCentroid() {
        if (hasNextCentroid()) {
            return sortedCentroids.get(nextIndex++);
        }
        return null;
    }
}
