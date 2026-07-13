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
package org.apache.hyracks.storage.am.lsm.btree;

import java.util.Random;

import org.apache.hyracks.storage.am.btree.impls.BitmapLeafIds;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link BitmapLeafIds} (ASTERIXDB-3702): the bitmap+select encoding must be exactly
 * equivalent to the {@code int[]} it replaces — {@code size()} = number of ids, {@code get(i)} = the
 * i-th id in ascending order (which is how the sample cursor indexes leaves).
 */
public class BitmapLeafIdsTest {

    @Test
    public void getEqualsSortedArrayAcrossDensities() {
        // dense (row-like ~99%), sparse (column-like), and edge sizes
        check(buildDense(200_000, 0.99, 1));
        check(buildDense(50_000, 0.98, 7));
        check(buildSparse(2_000, 320)); // column mega-leaf spacing
        check(new int[] { 0 });
        check(new int[] { 5 });
        check(new int[] { 0, 63, 64, 65, 127, 128 }); // word boundaries
    }

    // reference: a sorted, distinct id array is the ground truth; get(i) must equal ref[i].
    private static void check(int[] ref) {
        long universe = ref[ref.length - 1] + 1L;
        BitmapLeafIds b = new BitmapLeafIds(universe);
        for (int id : ref) {
            b.set(id);
        }
        b.build();
        Assert.assertEquals("size", ref.length, b.size());
        for (int i = 0; i < ref.length; i++) {
            Assert.assertEquals("get(" + i + ")", ref[i], b.get(i));
        }
    }

    // sorted near-dense ids with occasional holes (models bulk-loaded row leaf ids: leaves + interiors)
    private static int[] buildDense(int n, double density, long seed) {
        int[] a = new int[n];
        Random r = new Random(seed);
        int cur = 0;
        double holeProb = (1.0 / density) - 1.0;
        for (int i = 0; i < n; i++) {
            a[i] = cur;
            cur += 1 + (r.nextDouble() < holeProb ? 1 + r.nextInt(4) : 0);
        }
        return a;
    }

    // sorted ids spaced by ~stride (models columnar mega-leaf page0s scattered among column pages)
    private static int[] buildSparse(int n, int stride) {
        int[] a = new int[n];
        for (int i = 0; i < n; i++) {
            a[i] = i * stride;
        }
        return a;
    }
}
