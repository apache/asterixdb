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
package org.apache.hyracks.api.io;

import java.util.Locale;

/**
 * What a workspace file holds, which is to say why it was spilled to disk. Workspace files are counted by kind, so
 * this set is deliberately small and closed: add a value only for a genuinely new reason to spill, not for each new
 * operator.
 */
public enum WorkspaceFileKind {
    /** Runs of an external sort, including a top-k sort and the merge passes. */
    SORT,
    /** Partitions or runs of a hash or sort-based group-by. */
    GROUP,
    /** Partitions of a hash join, and the build side of a nested-loop, interval or spatial join. */
    JOIN,
    /** Frames of a window partition that did not fit in memory. */
    WINDOW,
    /** The input of an aggregate function holding its values on disk, such as a median. */
    AGGREGATE,
    /** A materialized intermediate result, read by more than one consumer or more than once. */
    MATERIALIZE,
    /** Intermediate results of an index search, such as candidates of an inverted index or vector search. */
    INDEX_SEARCH,
    /** Frames of a materializing connector between operators. */
    EXCHANGE,
    /** A job's result held for the client to fetch. */
    RESULT,
    /** Anything else. */
    OTHER;

    private final String label = name().toLowerCase(Locale.ROOT);

    /**
     * @return the lower-case name of this kind, suitable as a metric label value
     */
    public String label() {
        return label;
    }
}
