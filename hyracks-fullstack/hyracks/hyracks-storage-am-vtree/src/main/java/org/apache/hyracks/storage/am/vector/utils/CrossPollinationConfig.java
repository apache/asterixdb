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
 * Immutable cross-pollination parameters threaded from the index DDL down to the storage-layer
 * {@code VTree}, so that incremental insert and delete reproduce the multi-cluster placement bulk-load
 * performs. A record is written into the {@code m} closest leaf centroids that survive the SPTAG-style
 * RNG diversity rule (see {@link RngAcceptanceFilter}). A delete cancels every replica only when it
 * resolves the same centroid set, so both paths must hold the same config.
 * <p>
 * There is deliberately no default instance and no default for either component: every holder requires
 * a non-null config and every persisted resource carries both values, so the index's own DDL is the only
 * source. A fallback here would be a second source of truth, and the two silently diverging is what once
 * let a delete resolve a different leaf cluster than the matter it had to cancel.
 *
 * @param m         replica count; 1 disables cross-pollination.
 * @param rngFactor RNG diversity multiplier (canonical SPTAG = 1.0; non-finite disables the rule).
 *                  Inert at {@code m == 1}, where the diversity test never runs.
 */
public record CrossPollinationConfig(int m, double rngFactor) implements Serializable {

    private static final long serialVersionUID = 1L;

    public CrossPollinationConfig {
        // The DDL validates m to [1, MAX_CROSS_POLLINATION_M], so a smaller value is a wiring bug or a
        // corrupt resource. Clamping it would produce a placement disagreeing with the other path's.
        if (m < 1) {
            throw new IllegalArgumentException("m must be >= 1, got " + m);
        }
        // rngFactor is intentionally NOT validated: a non-finite value is a legal input that disables the
        // RNG diversity rule in RngAcceptanceFilter, degrading it to a pure top-cap slice.
    }
}
