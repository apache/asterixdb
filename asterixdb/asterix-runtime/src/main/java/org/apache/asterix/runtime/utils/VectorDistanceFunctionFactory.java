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
package org.apache.asterix.runtime.utils;

import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Factory for creating IVectorDistanceFunction implementations that wrap VectorDistanceCalculation methods.
 * This factory allows passing VectorDistanceCalculation implementations from AsterixDB to Hyracks modules
 * without creating circular dependencies.
 * The factory is serializable and can be passed through the job pipeline.
 */
public class VectorDistanceFunctionFactory implements IVTreeDistanceFunctionFactory {
    private static final long serialVersionUID = 1L;

    private static final String METRIC_FIELD = "metric";

    // The distance metric is fixed at index creation (determined by the embedding model) and baked into
    // this factory; Hyracks builds the distance function without ever seeing a metric string.
    private final VectorSimilarityMetric metric;

    public VectorDistanceFunctionFactory(VectorSimilarityMetric metric) {
        this.metric = metric;
    }

    // Built fresh on the NC (createDistanceFunction runs at runtime), so these are never Java-serialized.
    // The implementations live in VectorDistanceCalculation next to the calculations they wrap, keeping this
    // Serializable factory a plain metric-to-function lookup.
    private static IVTreeDistanceFunction functionFor(VectorSimilarityMetric metric) {
        return switch (metric) {
            case EUCLIDEAN -> VectorDistanceCalculation.EUCLIDEAN_FN;
            case EUCLIDEAN_SQUARED -> VectorDistanceCalculation.EUCLIDEAN_SQUARED_FN;
            case COSINE -> VectorDistanceCalculation.COSINE_DISTANCE_FN;
            case DOT ->
                // dotDistance returns -dot(a,b) so that minimizing "distance" equals maximizing dot product (MIPS).
                    VectorDistanceCalculation.DOT_DISTANCE_FN;
        };
    }

    @Override
    public IVTreeDistanceFunction createDistanceFunction() {
        return functionFor(metric);
    }

    // Persisted form: the class identifier plus the metric this factory was created with, so the metric
    // survives NC restart as part of the factory (it is not stored anywhere else on the resource).
    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put(METRIC_FIELD, metric.canonical());
        return json;
    }

    @SuppressWarnings("unused")
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        if (!json.has(METRIC_FIELD)) {
            throw HyracksDataException.create(ErrorCode.ILLEGAL_STATE,
                    "VectorDistanceFunctionFactory is missing its distance metric; resource is corrupt");
        }
        return new VectorDistanceFunctionFactory(VectorSimilarityMetric.fromAlias(json.get(METRIC_FIELD).asText()));
    }
}
