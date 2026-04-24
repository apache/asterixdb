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
package org.apache.asterix.common.vector;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single source of truth for the vector distance metrics the VTree stack understands, together with
 * the string aliases each metric is written as in {@code ann_distance(...)} and in the index
 * {@code similarity} option. The canonical name is the first alias.
 * <p>
 * This taxonomy backs the metric string handling in {@code VectorDistanceFunctionFactory} (the
 * index-internal distance function) and {@code VectorIndexAccessMethod.normalizeDistanceMetric}
 * (optimizer index selection). Keeping the
 * alias set in one place removes the previous requirement that those maps be edited in lockstep --
 * an alias that resolved in one but not another would trip an NPE on the index search path.
 */
public enum VectorSimilarityMetric {
    EUCLIDEAN("euclidean", "l2"),
    EUCLIDEAN_SQUARED("euclidean_squared", "l2_squared"),
    COSINE("cosine", "cosine similarity"),
    DOT("dot");

    private final List<String> aliases;

    VectorSimilarityMetric(String... aliases) {
        this.aliases = List.of(aliases);
    }

    /** The canonical (preferred) name for this metric: the first alias. */
    public String canonical() {
        return aliases.get(0);
    }

    /** Every accepted spelling for this metric (lowercase), canonical first. */
    public List<String> aliases() {
        return aliases;
    }

    // Lowercase alias -> metric.
    private static final Map<String, VectorSimilarityMetric> BY_ALIAS;
    static {
        Map<String, VectorSimilarityMetric> byAlias = new HashMap<>();
        for (VectorSimilarityMetric metric : values()) {
            for (String alias : metric.aliases) {
                byAlias.put(alias, metric);
            }
        }
        BY_ALIAS = Collections.unmodifiableMap(byAlias);
    }

    /**
     * Resolves an alias to its metric, case-insensitively and trimmed.
     *
     * @return the matching metric, or {@code null} if the alias is not recognized.
     */
    public static VectorSimilarityMetric fromAlias(String alias) {
        if (alias == null) {
            return null;
        }
        return BY_ALIAS.get(alias.toLowerCase().trim());
    }
}
