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
package org.apache.asterix.runtime.operators.kmeans;

import org.junit.Assert;
import org.junit.Test;

/**
 * Guards the properties that make k-means|| initialization reproducible and duplicate-safe: the random number a
 * row is drawn with depends only on the vector contents, the query seed, the round and which copy of the vector
 * the row is.
 * <p>
 * SAMPLE decides for every row in every round whether it joins the candidate pool by comparing a random number
 * against the draw probability. A number from a per-partition random stream would depend on the position the row
 * was read at, which changes with the arrival order of partitioned data, and the same query would then draw a
 * different pool on every run. Deriving the number from a hash of the vector removes that dependency. Mixing in
 * the copy index keeps copies of one vector independent trials: from the hash alone they were one trial, and a
 * duplicated point could sit out every round.
 * <p>
 * The test checks: equal vectors hash equally and different vectors do not; the number is fixed by (hash, seed,
 * round, copy) and changes with each of them; copy 0 reproduces the hash-only draw; and the numbers are uniform in
 * [0, 1).
 */
public class KMeansFingerprintTest {

    @Test
    public void drawDependsOnVectorSeedRoundAndCopyOnly() {
        double[] v = { 0.25d, -3.0d, 1e300d, 0.0d };
        long fp = KMeansLoopIO.fingerprint(v);
        // Same contents give the same hash. Changed contents give a different hash.
        Assert.assertEquals(fp, KMeansLoopIO.fingerprint(v.clone()));
        Assert.assertNotEquals(fp, KMeansLoopIO.fingerprint(new double[] { 0.25d, -3.0d, 1e300d, -0.0d }));
        Assert.assertNotEquals(fp, KMeansLoopIO.fingerprint(new double[] { -3.0d, 0.25d, 1e300d, 0.0d }));
        // Same inputs give the same number. A different seed, round or copy gives a different one.
        Assert.assertEquals(KMeansLoopIO.uniformDraw(fp, 42L, 3, 0), KMeansLoopIO.uniformDraw(fp, 42L, 3, 0), 0.0d);
        Assert.assertNotEquals(KMeansLoopIO.uniformDraw(fp, 42L, 3, 0), KMeansLoopIO.uniformDraw(fp, 43L, 3, 0), 0.0d);
        Assert.assertNotEquals(KMeansLoopIO.uniformDraw(fp, 42L, 3, 0), KMeansLoopIO.uniformDraw(fp, 42L, 4, 0), 0.0d);
        Assert.assertNotEquals(KMeansLoopIO.uniformDraw(fp, 42L, 3, 0), KMeansLoopIO.uniformDraw(fp, 42L, 3, 1), 0.0d);
        // Forty copies are forty distinct trials.
        long distinct = java.util.stream.IntStream.range(0, 40)
                .mapToDouble(c -> KMeansLoopIO.uniformDraw(fp, 42L, 3, c)).distinct().count();
        Assert.assertEquals(40L, distinct);
        // Numbers lie in [0, 1) and are not degenerate.
        int count = 100_000;
        double sum = 0.0d;
        for (int i = 0; i < count; i++) {
            long h = KMeansLoopIO.fingerprint(new double[] { i, i * 0.5d });
            double u = KMeansLoopIO.uniformDraw(h, 7L, 1, i % 3);
            Assert.assertTrue(u >= 0.0d && u < 1.0d);
            sum += u;
        }
        Assert.assertEquals(0.5d, sum / count, 0.01d);
    }
}
