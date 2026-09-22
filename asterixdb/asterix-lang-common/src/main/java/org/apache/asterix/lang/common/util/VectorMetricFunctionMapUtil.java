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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.asterix.om.functions.BuiltinFunctions;

/**
 * Maps {@code vector_distance()} / {@code ann_distance()} metric string literals to the internal
 * distance builtins that implement them, named from {@link BuiltinFunctions}.
 * <p>
 * The literals come from {@link VectorSimilarityMetric}, the same taxonomy
 * {@code CREATE INDEX ... WITH {"similarity"}} resolves through: a metric the DDL accepts is one a
 * query accepts.
 * <p>
 * {@code COSINE} and {@code DOT} resolve to distance semantics ({@code cosine-distance} = 1 - similarity,
 * {@code dot-distance} = -dot product), not the public {@code cosine_similarity()} / {@code dot_product()} builtins.
 */
public final class VectorMetricFunctionMapUtil {

    private static final Map<VectorSimilarityMetric, String> METRIC_TO_BUILTIN =
            new EnumMap<>(VectorSimilarityMetric.class);

    static {
        METRIC_TO_BUILTIN.put(VectorSimilarityMetric.EUCLIDEAN, BuiltinFunctions.EUCLIDEAN_DISTANCE.getName());
        METRIC_TO_BUILTIN.put(VectorSimilarityMetric.EUCLIDEAN_SQUARED,
                BuiltinFunctions.EUCLIDEAN_SQUARED_DISTANCE.getName());
        METRIC_TO_BUILTIN.put(VectorSimilarityMetric.COSINE, BuiltinFunctions.COSINE_DISTANCE.getName());
        METRIC_TO_BUILTIN.put(VectorSimilarityMetric.DOT, BuiltinFunctions.DOT_DISTANCE.getName());
    }

    // Derived from the taxonomy, so it lists exactly what resolve() takes.
    private static final String SUPPORTED_METRICS =
            Arrays.stream(VectorSimilarityMetric.values()).flatMap(m -> m.aliases().stream())
                    .map(alias -> alias.toUpperCase(Locale.ROOT)).collect(Collectors.joining(", "));

    private VectorMetricFunctionMapUtil() {
    }

    public static Optional<String> resolve(String metric) {
        VectorSimilarityMetric resolved = VectorSimilarityMetric.fromAlias(metric);
        return resolved == null ? Optional.empty() : Optional.ofNullable(METRIC_TO_BUILTIN.get(resolved));
    }

    public static String supportedMetricsMessage() {
        return SUPPORTED_METRICS;
    }
}
