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

package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import org.apache.asterix.lang.common.util.VectorDistanceMetric;
import org.junit.Assert;
import org.junit.Test;

public class VectorDistanceMetricTest {

    @Test
    public void resolvesEuclideanMetrics() {
        Assert.assertEquals("euclidean-distance", VectorDistanceMetric.resolve("L2").orElseThrow());
        Assert.assertEquals("euclidean-distance", VectorDistanceMetric.resolve("EUCLIDEAN").orElseThrow());
    }

    @Test
    public void resolvesEuclideanSquaredMetrics() {
        Assert.assertEquals("euclidean-squared-distance", VectorDistanceMetric.resolve("L2_SQUARED").orElseThrow());
        Assert.assertEquals("euclidean-squared-distance",
                VectorDistanceMetric.resolve("EUCLIDEAN_SQUARED").orElseThrow());
    }

    @Test
    public void resolvesCosineMetric() {
        Assert.assertEquals("cosine-distance", VectorDistanceMetric.resolve("COSINE").orElseThrow());
    }

    @Test
    public void resolvesDotMetric() {
        Assert.assertEquals("dot-distance", VectorDistanceMetric.resolve("DOT").orElseThrow());
    }

    @Test
    public void rejectsRemovedAliases() {
        Assert.assertTrue(VectorDistanceMetric.resolve("cosine_similarity").isEmpty());
        Assert.assertTrue(VectorDistanceMetric.resolve("dot_product").isEmpty());
        Assert.assertTrue(VectorDistanceMetric.resolve("l2_distance").isEmpty());
        Assert.assertTrue(VectorDistanceMetric.resolve("euclidean_distance").isEmpty());
        Assert.assertTrue(VectorDistanceMetric.resolve("cosine_distance").isEmpty());
        Assert.assertTrue(VectorDistanceMetric.resolve("dot_distance").isEmpty());
    }

    @Test
    public void rejectsUnknownMetric() {
        Assert.assertTrue(VectorDistanceMetric.resolve("unknown").isEmpty());
    }
}
