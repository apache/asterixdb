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

package org.apache.hyracks.storage.am.vector;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunctionFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Minimal distance-function factory for storage-layer unit tests, since {@code hyracks-*} test
 * modules cannot depend on the AsterixDB implementation. Keep it numerically consistent with
 * asterix-runtime's {@code VectorDistanceFunctionFactory}.
 */
public class TestVTreeDistanceFunctionFactory implements IVTreeDistanceFunctionFactory {

    private static final long serialVersionUID = 1L;

    public static final TestVTreeDistanceFunctionFactory INSTANCE = new TestVTreeDistanceFunctionFactory("euclidean");

    private final String metric;

    public TestVTreeDistanceFunctionFactory(String metric) {
        this.metric = metric == null ? "euclidean" : metric.toLowerCase().trim();
    }

    @Override
    public IVTreeDistanceFunction createDistanceFunction() {
        switch (metric) {
            case "euclidean_squared":
            case "l2_squared":
                return TestVTreeDistanceFunctionFactory::euclideanSquared;
            case "cosine":
                return TestVTreeDistanceFunctionFactory::cosineDistance;
            case "dot":
                return TestVTreeDistanceFunctionFactory::negDotProduct;
            default: // euclidean / l2
                return (a, b) -> Math.sqrt(euclideanSquared(a, b));
        }
    }

    private static double euclideanSquared(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    /** 1 - cos(a, b), so smaller = more similar. */
    private static double cosineDistance(double[] a, double[] b) {
        double dot = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0.0 || normB == 0.0) {
            return 1.0;
        }
        return 1.0 - dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /** -dot(a, b), so smaller = more similar (MIPS convention). */
    private static double negDotProduct(double[] a, double[] b) {
        double dot = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
        }
        return -dot;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = (ObjectNode) registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("metric", metric);
        return json;
    }

    @SuppressWarnings("unused")
    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
        return new TestVTreeDistanceFunctionFactory(json.has("metric") ? json.get("metric").asText() : "euclidean");
    }
}
