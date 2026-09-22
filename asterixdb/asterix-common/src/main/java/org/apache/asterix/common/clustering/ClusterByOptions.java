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
package org.apache.asterix.common.clustering;

import java.util.Objects;

/**
 * The CLUSTER BY options after validation, immutable and shared by reference: the language rewrite
 * validates the WITH record and builds one instance, the clause carries it (surviving the AST clones a
 * view or function body goes through), and the translator hands the same instance to
 * {@code ClusterByOperator}.
 * <p>
 * The options mirror the two tiers of the WITH contract: what any clustering has (the algorithm and the
 * declared vector width) lives here; what is specific to one algorithm lives in that algorithm's block
 * ({@link KmeansOptions}), non-null exactly when the algorithm selects it.
 */
public final class ClusterByOptions {

    // The vocabulary the language layer resolves to and the expansion dispatches on, defined once so the
    // writer, the rule and the translator cannot drift: a typo is a compile error.
    public static final String ALGORITHM_KMEANS = "kmeans";
    public static final String INIT_MODE_KMEANS_PARALLEL = "kmeans_parallel";
    public static final String INIT_MODE_RANDOM = "random";
    // The two fields of the cluster descriptor record.
    public static final String FIELD_CLUSTER_ID = "cluster_id";
    public static final String FIELD_CENTROID = "centroid";

    private final String algorithm;
    private final int dimension;
    private final KmeansOptions kmeans;

    public ClusterByOptions(String algorithm, int dimension, KmeansOptions kmeans) {
        this.algorithm = algorithm;
        this.dimension = dimension;
        this.kmeans = kmeans;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public int getDimension() {
        return dimension;
    }

    public KmeansOptions getKmeans() {
        return kmeans;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClusterByOptions)) {
            return false;
        }
        ClusterByOptions that = (ClusterByOptions) o;
        return dimension == that.dimension && Objects.equals(algorithm, that.algorithm)
                && Objects.equals(kmeans, that.kmeans);
    }

    @Override
    public int hashCode() {
        return Objects.hash(algorithm, dimension, kmeans);
    }

    @Override
    public String toString() {
        return "{algorithm: " + algorithm + ", dimension: " + dimension + (kmeans == null ? "" : ", " + kmeans) + "}";
    }

    /** The K-Means tier of the WITH contract; every field here is meaningless for any other algorithm. */
    public static final class KmeansOptions {

        private final int numClusters;
        private final String initMode;
        private final String metric;
        // The query's seed, or null when absent; the expansion turns null into its own fixed defaults.
        private final Integer seed;
        private final Integer numIterations;

        public KmeansOptions(int numClusters, String initMode, String metric, Integer seed, Integer numIterations) {
            this.numClusters = numClusters;
            this.initMode = initMode;
            this.metric = metric;
            this.seed = seed;
            this.numIterations = numIterations;
        }

        public int getNumClusters() {
            return numClusters;
        }

        public String getInitMode() {
            return initMode;
        }

        public String getMetric() {
            return metric;
        }

        public Integer getSeed() {
            return seed;
        }

        public Integer getNumIterations() {
            return numIterations;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof KmeansOptions)) {
                return false;
            }
            KmeansOptions that = (KmeansOptions) o;
            return numClusters == that.numClusters && Objects.equals(initMode, that.initMode)
                    && Objects.equals(metric, that.metric) && Objects.equals(seed, that.seed)
                    && Objects.equals(numIterations, that.numIterations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(numClusters, initMode, metric, seed, numIterations);
        }

        @Override
        public String toString() {
            return "kmeans: {num_clusters: " + numClusters + ", init_mode: " + initMode + ", metric: " + metric
                    + ", seed: " + seed + ", num_iterations: " + numIterations + "}";
        }
    }
}
