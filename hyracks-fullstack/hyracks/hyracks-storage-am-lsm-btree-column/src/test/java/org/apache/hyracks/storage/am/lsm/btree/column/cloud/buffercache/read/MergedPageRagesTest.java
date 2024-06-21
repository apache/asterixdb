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

import org.junit.Assert;
import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntList;

public class MergedPageRagesTest {

    @Test
    public void mergePageRanges1() {
        int[] pageRanges = new int[] { 1, 3, 5, 7, 8, 12, 16, 19, 23, 26 };

        int requiredRangeCount = 3;
        AbstractPageRangesComputer mergedPageRanges = AbstractPageRangesComputer.create(requiredRangeCount);
        for (int i = 0; i < pageRanges.length; i += 2) {
            mergedPageRanges.addRange(pageRanges[i], pageRanges[i + 1]);
        }

        // since the gaps are in following order
        // ( 2, 1, 4, 4 )
        // since we need 3 ranges, 5 - 3 = 2 merges should be done.
        // hence the resultant ranges := ( 0, 2 ), ( 3, 3 ), ( 4, 4 )
        int[] expectedRanges = new int[] { 1, 12, 16, 19, 23, 26 };
        assertResult(mergedPageRanges, expectedRanges);
    }

    @Test
    public void allMerges() {
        int[] pageRanges = new int[] { 1, 3, 5, 7, 8, 12, 16, 19, 23, 26 };

        int requiredRangeCount = 1;
        AbstractPageRangesComputer mergedPageRanges = AbstractPageRangesComputer.create(requiredRangeCount);
        for (int i = 0; i < pageRanges.length; i += 2) {
            mergedPageRanges.addRange(pageRanges[i], pageRanges[i + 1]);
        }

        // since the gaps are in following order
        // ( 2, 1, 4, 4 )
        // since we need 1 ranges, 5 - 4 = 1 merge should be done.
        // hence the resultant ranges := ( 0, 4)
        int[] expectedRanges = new int[] { 1, 26 };
        assertResult(mergedPageRanges, expectedRanges);
    }

    @Test
    public void noMerges() {
        int[] pageRanges = new int[] { 1, 3, 5, 7, 8, 12, 16, 19, 23, 26 };

        int requiredRangeCount = 8;
        AbstractPageRangesComputer mergedPageRanges = AbstractPageRangesComputer.create(requiredRangeCount);
        for (int i = 0; i < pageRanges.length; i += 2) {
            mergedPageRanges.addRange(pageRanges[i], pageRanges[i + 1]);
        }

        // since the gaps are in following order
        // ( 2, 1, 4, 4 )
        // since we need 8 ranges, no merge should be done.
        int[] expectedRanges = new int[] { 1, 3, 5, 7, 8, 12, 16, 19, 23, 26 };

        assertResult(mergedPageRanges, expectedRanges);
    }

    @Test
    public void singlePageGap() {
        int[] pageRanges = new int[] { 1, 3, 5, 7, 9, 10 };

        int requiredRangeCount = 3;
        AbstractPageRangesComputer mergedPageRanges = AbstractPageRangesComputer.create(requiredRangeCount);
        for (int i = 0; i < pageRanges.length; i += 2) {
            mergedPageRanges.addRange(pageRanges[i], pageRanges[i + 1]);
        }

        // Since the gaps between the ranges are all 1, the result should be a single range
        int[] expectedRanges = new int[] { 1, 10 };

        assertResult(mergedPageRanges, expectedRanges);
    }

    private void assertResult(AbstractPageRangesComputer mergedPageRanges, int[] expectedRanges) {
        if (mergedPageRanges.getMaxNumberOfRanges() == 1) {
            assertSinglePageRangeComputer((SinglePageRangeComputer) mergedPageRanges, expectedRanges);
            return;
        }
        PageRangesComputer pageRangesComputer = (PageRangesComputer) mergedPageRanges;
        int mergeResult = pageRangesComputer.mergeRanges();
        switch (mergeResult) {
            case PageRangesComputer.SINGLE_RANGE:
                assertSingleRange(pageRangesComputer, expectedRanges);
                break;
            case PageRangesComputer.EACH_RANGE:
                assertEachRange(pageRangesComputer, expectedRanges);
                break;
            default:
                assertBoundaries(pageRangesComputer, expectedRanges);
                break;
        }
    }

    private void assertSinglePageRangeComputer(SinglePageRangeComputer mergedPageRanges, int[] expectedRanges) {
        Assert.assertEquals(expectedRanges.length, 2);
        Assert.assertEquals(expectedRanges[0], mergedPageRanges.rangeStart);
        Assert.assertEquals(expectedRanges[1], mergedPageRanges.rangeEnd);
    }

    private void assertSingleRange(PageRangesComputer mergedPageRanges, int[] expectedRanges) {
        Assert.assertEquals(expectedRanges.length, 2);
        IntList pageRanges = mergedPageRanges.pageRanges;
        Assert.assertEquals(expectedRanges[0], pageRanges.getInt(0));
        Assert.assertEquals(expectedRanges[1], pageRanges.getInt(pageRanges.size() - 1));
    }

    private void assertEachRange(PageRangesComputer mergedPageRanges, int[] expectedRanges) {
        IntList pageRanges = mergedPageRanges.pageRanges;
        Assert.assertEquals(expectedRanges.length, pageRanges.size());
        for (int i = 0; i < expectedRanges.length; i++) {
            Assert.assertEquals(expectedRanges[i], pageRanges.getInt(i));
        }
    }

    private void assertBoundaries(PageRangesComputer mergedPageRanges, int[] expectedRanges) {
        int[] rangeBoundaries = mergedPageRanges.rangeBoundaries;
        Assert.assertEquals(expectedRanges.length / 2, rangeBoundaries.length);
        IntList pageRanges = mergedPageRanges.pageRanges;
        int startIndex = 0;
        int expectedRange = 0;
        for (int i = 0; i < rangeBoundaries.length; i++) {
            int endIndex = rangeBoundaries[i];
            int rangeStart = pageRanges.getInt(startIndex);
            int rangeEnd = pageRanges.getInt(endIndex);

            Assert.assertEquals(expectedRanges[expectedRange++], rangeStart);
            Assert.assertEquals(expectedRanges[expectedRange++], rangeEnd);

            // Start from the next cut
            startIndex = endIndex + 1;
        }
    }
}
