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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read;

import java.util.List;

import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IntPairUtil;
import org.junit.Assert;
import org.junit.Test;

import it.unimi.dsi.fastutil.Pair;

public class MergedPageRagesTest {
    private MergedPageRanges mergedPageRanges;
    private final CloudMegaPageReadContext cloudMegaPageReadContext = null;

    @Test
    public void mergePageRanges1() {
        int[] pageRanges = new int[] { 1, 3, 5, 7, 8, 12, 16, 19, 23, 26 };

        int requiredRangeCount = 3;
        mergedPageRanges = new MergedPageRanges(cloudMegaPageReadContext, requiredRangeCount);
        for (int i = 0; i < pageRanges.length; i += 2) {
            mergedPageRanges.addRange(pageRanges[i], pageRanges[i + 1]);
        }

        mergedPageRanges.mergeRanges();
        // since the gaps are in following order
        // ( 2, 1, 4, 4 )
        // since we need 3 ranges, 5 - 3 = 2 merges should be done.
        // hence the resultant ranges := ( 0, 2 ), ( 3, 3 ), ( 4, 4 )
        List<Pair<Integer, Integer>> ranges = List.of(Pair.of(0, 2), Pair.of(3, 3), Pair.of(4, 4));

        for (int i = 0; i < requiredRangeCount; i++) {
            long nextRange = mergedPageRanges.getNextRange();
            Assert.assertEquals(ranges.get(i).first().intValue(), IntPairUtil.getFirst(nextRange));
            Assert.assertEquals(ranges.get(i).second().intValue(), IntPairUtil.getSecond(nextRange));
        }
    }

    @Test
    public void allMerges() {
        int[] pageRanges = new int[] { 1, 3, 5, 7, 8, 12, 16, 19, 23, 26 };

        int requiredRangeCount = 1;
        mergedPageRanges = new MergedPageRanges(cloudMegaPageReadContext, requiredRangeCount);
        for (int i = 0; i < pageRanges.length; i += 2) {
            mergedPageRanges.addRange(pageRanges[i], pageRanges[i + 1]);
        }

        mergedPageRanges.mergeRanges();
        // since the gaps are in following order
        // ( 2, 1, 4, 4 )
        // since we need 1 ranges, 5 - 4 = 1 merge should be done.
        // hence the resultant ranges := ( 0, 4)
        List<Pair<Integer, Integer>> ranges = List.of(Pair.of(0, 4));

        for (int i = 0; i < requiredRangeCount; i++) {
            long nextRange = mergedPageRanges.getNextRange();
            Assert.assertEquals(ranges.get(i).first().intValue(), IntPairUtil.getFirst(nextRange));
            Assert.assertEquals(ranges.get(i).second().intValue(), IntPairUtil.getSecond(nextRange));
        }
    }

    @Test
    public void noMerges() {
        int[] pageRanges = new int[] { 1, 3, 5, 7, 8, 12, 16, 19, 23, 26 };

        int requiredRangeCount = 8;
        mergedPageRanges = new MergedPageRanges(cloudMegaPageReadContext, requiredRangeCount);
        for (int i = 0; i < pageRanges.length; i += 2) {
            mergedPageRanges.addRange(pageRanges[i], pageRanges[i + 1]);
        }

        mergedPageRanges.mergeRanges();
        // since the gaps are in following order
        // ( 2, 1, 4, 4 )
        // since we need 8 ranges, no merge should be done.
        List<Pair<Integer, Integer>> ranges =
                List.of(Pair.of(0, 0), Pair.of(1, 1), Pair.of(2, 2), Pair.of(3, 3), Pair.of(4, 4));

        for (int i = 0; i < ranges.size(); i++) {
            long nextRange = mergedPageRanges.getNextRange();
            Assert.assertEquals(ranges.get(i).first().intValue(), IntPairUtil.getFirst(nextRange));
            Assert.assertEquals(ranges.get(i).second().intValue(), IntPairUtil.getSecond(nextRange));
        }
    }
}
