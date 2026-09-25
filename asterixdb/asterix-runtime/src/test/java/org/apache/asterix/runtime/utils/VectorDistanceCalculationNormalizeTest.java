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

import org.junit.Assert;
import org.junit.Test;

public class VectorDistanceCalculationNormalizeTest {

    @Test
    public void fourComponentVector() {
        double[] v = { 1, 2, 3, 4 };
        Assert.assertTrue(VectorDistanceCalculation.normalizeInPlace(v));
        Assert.assertEquals(0.18257418583505536, v[0], 0.0);
        Assert.assertEquals(0.3651483716701107, v[1], 0.0);
        Assert.assertEquals(0.5477225575051661, v[2], 0.0);
        Assert.assertEquals(0.7302967433402214, v[3], 0.0);
    }

    @Test
    public void unitAndPythagorean() {
        double[] unit = { 0, 1 };
        Assert.assertTrue(VectorDistanceCalculation.normalizeInPlace(unit));
        Assert.assertArrayEquals(new double[] { 0.0, 1.0 }, unit, 0.0);

        double[] pythagorean = { 3, 4 };
        Assert.assertTrue(VectorDistanceCalculation.normalizeInPlace(pythagorean));
        // 3*(1/5) is 1 ulp above 0.6; compare the same way the helper divides.
        Assert.assertEquals(3.0 / 5.0, pythagorean[0], 1e-15);
        Assert.assertEquals(4.0 / 5.0, pythagorean[1], 1e-15);
    }

    @Test
    public void negativePreservesDirection() {
        double[] v = { -3, 4 };
        Assert.assertTrue(VectorDistanceCalculation.normalizeInPlace(v));
        Assert.assertEquals(-3.0 * (1.0 / 5.0), v[0], 0.0);
        Assert.assertEquals(4.0 * (1.0 / 5.0), v[1], 0.0);
    }

    @Test
    public void rejectsEmptyZeroAndNonFinite() {
        Assert.assertFalse(VectorDistanceCalculation.normalizeInPlace(new double[0]));
        Assert.assertFalse(VectorDistanceCalculation.normalizeInPlace(new double[] { 0, 0, 0 }));
        Assert.assertFalse(VectorDistanceCalculation.normalizeInPlace(new double[] { Double.NaN, 1 }));
        Assert.assertFalse(VectorDistanceCalculation.normalizeInPlace(new double[] { Double.POSITIVE_INFINITY }));
        Assert.assertFalse(VectorDistanceCalculation.normalizeInPlace(null));
    }

    @Test
    public void failureLeavesArrayUnchanged() {
        double[] zero = { 0, 0 };
        Assert.assertFalse(VectorDistanceCalculation.normalizeInPlace(zero));
        Assert.assertArrayEquals(new double[] { 0, 0 }, zero, 0.0);

        double[] nan = { Double.NaN, 1 };
        Assert.assertFalse(VectorDistanceCalculation.normalizeInPlace(nan));
        Assert.assertTrue(Double.isNaN(nan[0]));
        Assert.assertEquals(1.0, nan[1], 0.0);
    }
}
