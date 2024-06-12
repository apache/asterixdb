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

import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IntPairUtil;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongPriorityQueue;

/**
 * Merge the given ranges such that the maximum number of ranges <= N.
 * Merge should be greedy as the range having lower gaps should be given priority.
 */
public class MergedPageRanges {
    public static final BitSet EMPTY = new BitSet();
    private final CloudMegaPageReadContext columnCtx;
    private final int numRequiredRanges;
    private final IntList pageRanges;
    private final LongPriorityQueue gapRanges;
    // indicates the index of the ranges which are merged
    private final BitSet mergedIndex = new BitSet();
    // indicates a page is requested or not
    private final BitSet unwantedPages = new BitSet();
    // indicates the extra pages got included while a merge
    private int currentIndex = 0;
    private int numRanges;

    MergedPageRanges(CloudMegaPageReadContext columnCtx, int numRequiredRanges) {
        this.numRequiredRanges = numRequiredRanges;
        this.pageRanges = new IntArrayList(40);
        this.gapRanges = new LongArrayPriorityQueue(IntPairUtil.FIRST_COMPARATOR);
        this.columnCtx = columnCtx;
        this.numRanges = 0;
    }

    public void reset() {
        mergedIndex.clear();
        pageRanges.clear();
        gapRanges.clear();
        numRanges = 0;
        currentIndex = 0;
    }

    public void addRange(int rangeStart, int rangeEnd) {
        pageRanges.add(rangeStart);
        pageRanges.add(rangeEnd);
        numRanges++;
    }

    public void mergeRanges() {
        // totalMerges = totalRanges - MAXIMUM_RESULTANT_RANGES
        int merges = numRanges - numRequiredRanges;
        for (int i = 2; i < pageRanges.size(); i += 2) {
            int previousRangeEnd = pageRanges.getInt(i - 1);
            int currentRangeStart = pageRanges.getInt(i);
            // this could be optimized to just enqueue "merges" ranges,
            // but won't be much diff as the number of ranges gonna be small
            long gap = IntPairUtil.of(currentRangeStart - previousRangeEnd, i / 2);
            gapRanges.enqueue(gap);
        }

        int count = 0;
        while (count < merges) {
            // extract the lower 32 bits for the index.
            int index = IntPairUtil.getSecond(gapRanges.dequeueLong());
            // set the bit from [index - 1, index] indicating
            // the index and index-1 are merged.
            mergedIndex.set(index - 1, index + 1);
            count++;
        }
    }

    public void pin(int fileId, int pageZeroId, IBufferCache bufferCache) throws HyracksDataException {
        // since the numRanges are already within set threshold
        if (numRanges <= numRequiredRanges) {
            pinWithoutMerge(fileId, pageZeroId, bufferCache);
            return;
        }
        pinWithMerge(fileId, pageZeroId, bufferCache);
    }

    private void pinWithoutMerge(int fileId, int pageZeroId, IBufferCache bufferCache) throws HyracksDataException {
        for (int pageIndex = 1; pageIndex < pageRanges.size(); pageIndex += 2) {
            int lastPageIndex = pageRanges.getInt(pageIndex);
            int firstPageIndex = pageRanges.getInt(pageIndex - 1);
            int numberOfPages = lastPageIndex - firstPageIndex + 1;
            columnCtx.pin(bufferCache, fileId, pageZeroId, firstPageIndex, numberOfPages, numberOfPages, EMPTY);
        }
    }

    private void pinWithMerge(int fileId, int pageZeroId, IBufferCache bufferCache) throws HyracksDataException {
        // merge the range based on the numRequiredRanges.
        mergeRanges();
        // go through page ranges and pin the required ranges.
        int rangeCnt = 0;
        while (rangeCnt < numRequiredRanges) {
            unwantedPages.clear();
            long mergedRange = getNextRange();

            int firstRangeIdx = IntPairUtil.getFirst(mergedRange);
            int lastRangeIdx = IntPairUtil.getSecond(mergedRange);

            // since the ranges are flattened out in the pageRanges.
            // hence ith index's element would be at [2*i, 2*i + 1]
            int firstRangeStart = pageRanges.getInt(2 * firstRangeIdx);
            int firstRangeEnd = pageRanges.getInt(2 * firstRangeIdx + 1);
            int lastRangeStart = pageRanges.getInt(2 * lastRangeIdx);
            int lastRangeEnd = pageRanges.getInt(2 * lastRangeIdx + 1);

            int numberOfPages = lastRangeEnd - firstRangeStart + 1;
            // Number of unwanted pages will be zero, when there is just a single range (i.e. no merge)
            boolean areUnwantedPages = firstRangeIdx != lastRangeIdx;
            // and when the there is no extra page being fetched. eg: [1 2] [3 4]
            // for: [ 1 2 ] [ 4 5 ] [ 7 8 ] -> [ 1 8 ] ( fromIndex = 0, toIndex = 2 )
            // numberOfUnwantedPages = (4 - 2 - 1) + (7 - 5 -1) = 2
            areUnwantedPages = areUnwantedPages && (lastRangeStart - firstRangeEnd > 1);
            int numberOfUnwantedPages = 0;
            if (areUnwantedPages) {
                // iterate through the index and mark the gaps
                for (int fromIndex = firstRangeIdx; fromIndex < lastRangeIdx; fromIndex++) {
                    // Gap = V (2 * (fromIndex+1) ) - V(fromIndex * 2 + 1)
                    // V(index) = value at the index
                    int fromRangeEnd = pageRanges.getInt(2 * fromIndex + 1);
                    int toRangeStart = pageRanges.getInt(2 * (fromIndex + 1));
                    // fromRangeEnd != toRangeStart, as they would have been merged already
                    int rangeGap = (fromRangeEnd == toRangeStart) ? 0 : toRangeStart - fromRangeEnd - 1;
                    if (rangeGap > 0) {
                        unwantedPages.set(fromRangeEnd + 1, toRangeStart);
                    }
                    numberOfUnwantedPages += rangeGap;
                }
            }

            columnCtx.pin(bufferCache, fileId, pageZeroId, firstRangeStart, numberOfPages,
                    numberOfPages - numberOfUnwantedPages, unwantedPages);
            rangeCnt++;
        }
    }

    // making package-private for MergedPageRangesTest
    long getNextRange() {
        int fromIndex = currentIndex;
        int endIndex = currentIndex;
        int toIndex;

        // move till we have a set index, indicating all the indexes
        // are merged into one range.
        while (endIndex < numRanges && mergedIndex.get(endIndex)) {
            endIndex++;
        }

        if (fromIndex == endIndex) {
            currentIndex = endIndex + 1;
            toIndex = endIndex;
        } else {
            currentIndex = endIndex;
            toIndex = endIndex - 1;
        }

        return IntPairUtil.of(fromIndex, toIndex);
    }
}
