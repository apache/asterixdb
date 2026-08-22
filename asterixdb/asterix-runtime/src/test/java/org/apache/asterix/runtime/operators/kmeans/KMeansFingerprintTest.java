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

import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

/**
 * Guards the property that makes k-means|| initialization reproducible: the random number a vector is drawn
 * with depends only on the vector contents, the query seed and the round.
 * <p>
 * SAMPLE decides for every vector in every round whether it joins the candidate pool by comparing a random
 * number against the draw probability. If that number came from a per-partition random stream it would depend
 * on which partition the vector landed in and at which position. Both change whenever the input is
 * repartitioned and a repartition through RANDOM_PARTITION_EXCHANGE is itself random. The same query would then
 * produce a different candidate pool and a different clustering on every run. Deriving the number from a hash
 * of the vector removes that dependency: a vector is drawn or not drawn no matter where it is stored.
 * <p>
 * The test therefore checks the three things the guarantee rests on: equal vectors hash equally and different
 * vectors do not; the number is fixed by (hash, seed, round) and changes with each of them; and the numbers are
 * uniform in [0, 1) so they are valid against a probability.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED)
public class KMeansFingerprintTest {

    @Test
    public void drawDependsOnVectorSeedAndRoundOnly() {
        double[] v = { 0.25d, -3.0d, 1e300d, 0.0d };
        long fp = KMeansLoopIO.fingerprint(v);
        // Same contents give the same hash. Changed contents give a different hash.
        Assert.assertEquals(fp, KMeansLoopIO.fingerprint(v.clone()));
        Assert.assertNotEquals(fp, KMeansLoopIO.fingerprint(new double[] { 0.25d, -3.0d, 1e300d, -0.0d }));
        Assert.assertNotEquals(fp, KMeansLoopIO.fingerprint(new double[] { -3.0d, 0.25d, 1e300d, 0.0d }));
        // Same inputs give the same number. A different seed or round gives a different one.
        Assert.assertEquals(KMeansLoopIO.uniformDraw(fp, 42L, 3), KMeansLoopIO.uniformDraw(fp, 42L, 3), 0.0d);
        Assert.assertNotEquals(KMeansLoopIO.uniformDraw(fp, 42L, 3), KMeansLoopIO.uniformDraw(fp, 43L, 3), 0.0d);
        Assert.assertNotEquals(KMeansLoopIO.uniformDraw(fp, 42L, 3), KMeansLoopIO.uniformDraw(fp, 42L, 4), 0.0d);
        // Numbers lie in [0, 1) and are not degenerate.
        int count = 100_000;
        double sum = 0.0d;
        for (int i = 0; i < count; i++) {
            double u = KMeansLoopIO.uniformDraw(KMeansLoopIO.fingerprint(new double[] { i, i * 0.5d }), 7L, 1);
            Assert.assertTrue(u >= 0.0d && u < 1.0d);
            sum += u;
        }
        Assert.assertEquals(0.5d, sum / count, 0.01d);
    }
}
