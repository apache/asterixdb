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

package org.apache.hyracks.storage.am.vector.impls;

import java.util.Objects;

import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * What a memory component needs to navigate another component's static clustering structure: that
 * component's buffer cache, file id and root page, plus the centroid&rarr;directory-page mapping allocated in
 * this component's own virtual buffer cache. A component holds it as one reference published by a single
 * volatile write, so attachment is atomic and a reader that sees the reference sees complete contents.
 *
 * @param bufferCache        the static structure's buffer cache, read-only from here
 * @param fileId             the static structure's file id
 * @param rootPageId         the static structure's root page
 * @param centroidDirPageMap leaf-centroid index &rarr; directory page id, in this component's own cache
 * @param firstLeafCentroidId centroid id that {@code centroidDirPageMap[0]} corresponds to
 * @param numLeafCentroids   number of leaf centroids in the static structure
 */
record StaticStructureRef(IBufferCache bufferCache, int fileId, int rootPageId, int[] centroidDirPageMap,
        int firstLeafCentroidId, int numLeafCentroids) {

    /** Returned for a centroid the mapping does not cover; {@code IPageManager#takePage} never returns it. */
    static final long NO_DIRECTORY_PAGE = -1;

    StaticStructureRef {
        Objects.requireNonNull(bufferCache, "bufferCache");
        Objects.requireNonNull(centroidDirPageMap, "centroidDirPageMap");
    }

    /**
     * The directory page holding a leaf centroid's data pages, or {@link #NO_DIRECTORY_PAGE} for a centroid
     * this component allocated no directory for, in which case the caller falls back to its own resolution.
     */
    long directoryPageFor(int centroidId) {
        int index = centroidId - firstLeafCentroidId;
        return index >= 0 && index < centroidDirPageMap.length ? centroidDirPageMap[index] : NO_DIRECTORY_PAGE;
    }
}
