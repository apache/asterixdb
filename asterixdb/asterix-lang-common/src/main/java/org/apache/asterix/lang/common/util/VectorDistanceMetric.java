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

package org.apache.asterix.lang.common.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Maps {@code vector_distance()} metric string literals to internal hyphenated builtin names.
 * <p>
 * Accepted metrics (case-insensitive): {@code COSINE}, {@code DOT}, {@code L2}, {@code EUCLIDEAN},
 * {@code L2_SQUARED}, {@code EUCLIDEAN_SQUARED}.
 * <p>
 * {@code COSINE} and {@code DOT} resolve to distance semantics ({@code cosine-distance} = 1 − similarity,
 * {@code dot-distance} = −dot product), not the public {@code cosine_similarity()} / {@code dot_product()} builtins.
 */
@AiProvenance(agent = AiProvenance.Agent.GPT_5_3, tool = AiProvenance.Tool.CURSOR, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Restrict to six canonical metric literals")
public final class VectorDistanceMetric {

    private static final String SUPPORTED_METRICS = "COSINE, DOT, L2, EUCLIDEAN, L2_SQUARED, EUCLIDEAN_SQUARED";

    private static final Map<String, String> METRIC_TO_BUILTIN = new HashMap<>();

    static {
        register("euclidean-distance", "l2", "euclidean");
        register("euclidean-squared-distance", "l2_squared", "euclidean_squared");
        register("cosine-distance", "cosine");
        register("dot-distance", "dot");
    }

    private VectorDistanceMetric() {
    }

    public static Optional<String> resolve(String metric) {
        if (metric == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(METRIC_TO_BUILTIN.get(normalize(metric)));
    }

    public static String supportedMetricsMessage() {
        return SUPPORTED_METRICS;
    }

    private static void register(String builtinName, String... aliases) {
        for (String alias : aliases) {
            METRIC_TO_BUILTIN.put(normalize(alias), builtinName);
        }
    }

    private static String normalize(String metric) {
        return metric.toLowerCase().replace('-', '_');
    }
}
