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

package org.apache.hyracks.dataflow.common.data.normalizers;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.dataflow.common.utils.NormalizedKeyUtils;
import org.junit.Assert;
import org.junit.Test;

public abstract class AbstractNormalizedKeyComputerFactoryTest {

    @Test
    public void testPositive() {
        IPointable large = getLargePositive();
        IPointable small = getSmallPositive();

        normalizeAndCompare(large, small, 1);
        normalizeAndCompare(small, large, -1);
        normalizeAndCompare(large, large, 0);
    }

    @Test
    public void testNegative() {
        IPointable small = getSmallNegative();
        IPointable large = getLargeNegative();

        normalizeAndCompare(large, small, 1);
        normalizeAndCompare(small, large, -1);
        normalizeAndCompare(large, large, 0);
    }

    @Test
    public void testPositiveAndNegative() {
        IPointable smallNegative = getSmallNegative();
        IPointable largeNegative = getLargeNegative();
        IPointable smallPositive = getSmallPositive();
        IPointable largePositive = getLargePositive();

        normalizeAndCompare(smallNegative, smallPositive, -1);
        normalizeAndCompare(smallNegative, largePositive, -1);
        normalizeAndCompare(largeNegative, smallPositive, -1);
        normalizeAndCompare(largeNegative, largePositive, -1);
    }

    protected void normalizeAndCompare(IPointable p1, IPointable p2, int result) {
        int[] key1 = normalize(p1);
        int[] key2 = normalize(p2);
        int comp = NormalizedKeyUtils.compareNormalizeKeys(key1, 0, key2, 0, key1.length);
        if (result > 0) {
            Assert.assertTrue(comp > 0);
        } else if (result == 0) {
            Assert.assertTrue(comp == 0);
        } else {
            Assert.assertTrue(comp < 0);
        }
    }

    protected abstract IPointable getLargePositive();

    protected abstract IPointable getSmallPositive();

    protected abstract IPointable getLargeNegative();

    protected abstract IPointable getSmallNegative();

    protected abstract int[] normalize(IPointable value);
}
