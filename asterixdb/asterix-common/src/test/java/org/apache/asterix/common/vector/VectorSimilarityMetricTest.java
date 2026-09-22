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

import java.util.Locale;

import org.junit.Assert;
import org.junit.Test;

/**
 * The alias table is the single source of truth for every metric spelling the system accepts, on the
 * DDL path and the query path alike.
 */
public class VectorSimilarityMetricTest {

    @Test
    public void everyDeclaredAliasResolvesToItsMetric() {
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            for (String alias : metric.aliases()) {
                Assert.assertSame(alias, metric, VectorSimilarityMetric.fromAlias(alias));
            }
        }
    }

    @Test
    public void canonicalIsTheFirstAliasAndResolvesToItself() {
        for (VectorSimilarityMetric metric : VectorSimilarityMetric.values()) {
            Assert.assertEquals(metric.aliases().get(0), metric.canonical());
            Assert.assertSame(metric, VectorSimilarityMetric.fromAlias(metric.canonical()));
        }
    }

    @Test
    public void caseAndSurroundingWhitespaceAreIgnored() {
        Assert.assertSame(VectorSimilarityMetric.EUCLIDEAN, VectorSimilarityMetric.fromAlias("L2"));
        Assert.assertSame(VectorSimilarityMetric.EUCLIDEAN, VectorSimilarityMetric.fromAlias("  l2  "));
        Assert.assertSame(VectorSimilarityMetric.EUCLIDEAN, VectorSimilarityMetric.fromAlias("\tEuclidean\n"));
        Assert.assertSame(VectorSimilarityMetric.EUCLIDEAN_SQUARED,
                VectorSimilarityMetric.fromAlias(" EUCLIDEAN_SQUARED "));
    }

    /**
     * A space is not part of any metric name: {@link VectorSimilarityMetric#normalize} does not
     * collapse internal whitespace.
     */
    @Test
    public void cosineSimilarityIsNotAnAlias() {
        Assert.assertNull(VectorSimilarityMetric.fromAlias("cosine similarity"));
        Assert.assertNull(VectorSimilarityMetric.fromAlias("cosine_similarity"));
    }

    /**
     * A hyphen is not a separator: {@link VectorSimilarityMetric#normalize} maps nothing onto
     * {@code _}, so an underscore alias cannot be spelled with a hyphen.
     */
    @Test
    public void hyphenIsNotASeparator() {
        Assert.assertNull(VectorSimilarityMetric.fromAlias("euclidean-squared"));
        Assert.assertNull(VectorSimilarityMetric.fromAlias("l2-squared"));
    }

    @Test
    public void unknownAndNullResolveToNull() {
        Assert.assertNull(VectorSimilarityMetric.fromAlias(null));
        Assert.assertNull(VectorSimilarityMetric.fromAlias("manhattan"));
        Assert.assertNull(VectorSimilarityMetric.fromAlias(""));
    }

    /**
     * Lowercasing must not follow the default locale: under {@code tr_TR} the dotless lowercase of
     * {@code I} would make every metric containing one unresolvable, on the DDL path, the query path
     * and in CLUSTER BY.
     */
    @Test
    public void resolutionIsLocaleIndependent() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            Assert.assertSame(VectorSimilarityMetric.EUCLIDEAN, VectorSimilarityMetric.fromAlias("EUCLIDEAN"));
            Assert.assertSame(VectorSimilarityMetric.EUCLIDEAN_SQUARED,
                    VectorSimilarityMetric.fromAlias("EUCLIDEAN_SQUARED"));
            Assert.assertSame(VectorSimilarityMetric.COSINE, VectorSimilarityMetric.fromAlias("COSINE"));
        } finally {
            Locale.setDefault(previous);
        }
    }
}
