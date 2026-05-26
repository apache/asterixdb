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
package org.apache.asterix.common.annotations;

import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

/**
 * Marks a vector-distance function call as an approximate-nearest-neighbor (ANN) request, i.e. the desugared
 * form of {@code ann_distance(...)}. Its presence tells {@code IntroduceTopKAccessMethodRule} that the call is
 * eligible to be served by a vector index (a plain {@code vector_distance} call carries no such annotation and
 * stays exact). The annotation also carries the ANN-only search parameters that have no place on the 2-arg
 * distance builtin the call is rewritten into.
 */
public final class AnnSearchPreferenceAnnotation implements IExpressionAnnotation {

    private final String metric;
    private final double minProbeFraction;
    private final int kMultiplier;

    public AnnSearchPreferenceAnnotation(String metric, double minProbeFraction, int kMultiplier) {
        this.metric = metric;
        this.minProbeFraction = minProbeFraction;
        this.kMultiplier = kMultiplier;
    }

    /** The distance metric as written in the query (used at compile time for index selection). */
    public String getMetric() {
        return metric;
    }

    /** Fraction of clusters to probe during the ANN search; default 0.1. */
    public double getMinProbeFraction() {
        return minProbeFraction;
    }

    /** Over-fetch factor applied to k during the ANN search; default 1. */
    public int getKMultiplier() {
        return kMultiplier;
    }

    @Override
    public String toString() {
        return String.format("ann-search:%s,%f,%d", metric, minProbeFraction, kMultiplier);
    }
}
