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

import java.io.Serializable;

/**
 * Immutable cross-pollination parameters threaded from the index DDL (WITH clause) down to the
 * storage-layer {@code VTree}, so that incremental insert/delete reproduce the exact multi-cluster
 * placement that bulk-load performs.
 * <p>
 * At bulk-load a record is written into the {@code m} closest leaf centroids, after eps-filtering
 * ({@code epsilon}) the candidate list and thinning it by the SPTAG-style RNG diversity rule
 * ({@code rngFactor}; see {@link RngAcceptanceFilter}). For incremental delete to cancel <em>every</em>
 * replica, the DML path must resolve the same centroid set — which means it must use the same three
 * parameters. {@code m == 1} reproduces the legacy single-closest behavior and bypasses the
 * level-wise/RNG machinery entirely.
 *
 * @param m         replica count (>= 1). 1 disables cross-pollination.
 * @param rngFactor RNG diversity multiplier (canonical SPTAG = 1.0; non-finite disables the rule).
 * @param epsilon   level-wise candidate window used to gather centroids before RNG thinning.
 */
public record CrossPollinationConfig(int m, double rngFactor, double epsilon) implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Legacy single-closest placement: no cross-pollination. Matches the WITH-clause defaults. */
    public static final CrossPollinationConfig LEGACY = new CrossPollinationConfig(1, 1.0, 0.3);

    public CrossPollinationConfig {
        m = Math.max(1, m);
        // epsilon is a multiplicative window width; a negative value is meaningless (it would
        // shrink rather than widen the candidate window). Reject it up front.
        if (epsilon < 0.0) {
            throw new IllegalArgumentException("epsilon must be >= 0, got " + epsilon);
        }
        // rngFactor is intentionally NOT validated: a non-finite value (NaN / +Infinity) is a
        // legal input that disables the RNG diversity rule in RngAcceptanceFilter (degrading to a
        // pure top-cap slice). Only m is clamped; rngFactor semantics are the caller's contract.
    }


    /** @return true when records are replicated into more than one cluster. */
    public boolean enabled() {
        return m > 1;
    }
}
