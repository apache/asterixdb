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

import java.util.Arrays;

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
final class PageRangesComputer extends AbstractPageRangesComputer {
    static final int SINGLE_RANGE = 0;
    static final int EACH_RANGE = 1;
    static final int BOUNDARIES = 2;

    private final int maxNumberOfRanges;
    private final LongPriorityQueue gapRanges;
    final IntList pageRanges;
    final int[] rangeBoundaries;

    PageRangesComputer(int maxNumberOfRanges) {
        this.maxNumberOfRanges = maxNumberOfRanges;
        pageRanges = new IntArrayList(INITIAL_SIZE);
        gapRanges = new LongArrayPriorityQueue(IntPairUtil.FIRST_COMPARATOR);
        rangeBoundaries = new int[maxNumberOfRanges];
    }

    @Override
    int getMaxNumberOfRanges() {
        return maxNumberOfRanges;
    }

    @Override
    void clear() {
        pageRanges.clear();
        gapRanges.clear();
        requestedPages.clear();
    }

    @Override
    void addRange(int rangeStart, int rangeEnd) {
        int previousEnd = pageRanges.size() - 1;

        pageRanges.add(rangeStart);
        pageRanges.add(rangeEnd);
        requestedPages.set(rangeStart, rangeEnd + 1);

        if (previousEnd > 0) {
            int maxNumberOfCuts = maxNumberOfRanges - 1;
            int gapSize = rangeStart - pageRanges.getInt(previousEnd) - 1;
            if (gapRanges.size() < maxNumberOfCuts) {
                // Didn't reach the maximum number cuts, add this gap
                gapRanges.enqueue(IntPairUtil.of(gapSize, previousEnd));
            } else if (IntPairUtil.getFirst(gapRanges.firstLong()) < gapSize) {
                // Found a bigger gap. Remove the smallest and add this new bigger gap
                gapRanges.dequeueLong();
                gapRanges.enqueue(IntPairUtil.of(gapSize, previousEnd));
            }
            // This gap is smaller than the smallest gap in the queue, ignore
            // A smaller gap than the smallest gap. Ignore it
        }
    }

    @Override
    void pin(CloudMegaPageReadContext ctx, IBufferCache bufferCache, int fileId, int pageZeroId)
            throws HyracksDataException {
        int mergeResult = mergeRanges();
        switch (mergeResult) {
            case SINGLE_RANGE:
                pinAsSingleRange(ctx, bufferCache, fileId, pageZeroId);
                break;
            case EACH_RANGE:
                pinEachRange(ctx, bufferCache, fileId, pageZeroId);
                break;
            default:
                pinMergedRanges(ctx, bufferCache, fileId, pageZeroId);
                break;
        }
    }

    int mergeRanges() {
        int i = 0;
        int maxGap = 0;
        while (!gapRanges.isEmpty()) {
            long pair = gapRanges.dequeueLong();
            maxGap = Math.max(maxGap, IntPairUtil.getFirst(pair));
            rangeBoundaries[i] = IntPairUtil.getSecond(pair);
            i++;
        }

        if (maxGap == 1) {
            // The biggest gap is 1, merge the ranges in a single range
            return SINGLE_RANGE;
        }

        if (getNumberOfRanges() <= maxNumberOfRanges) {
            // the number of ranges are within the limit, pin each range separately
            return EACH_RANGE;
        }

        // Set the last boundary
        rangeBoundaries[maxNumberOfRanges - 1] = pageRanges.size() - 1;

        // Sort cuts smallest to largest
        Arrays.sort(rangeBoundaries);

        // Use the boundaries to cut the ranges into separate ones
        return BOUNDARIES;
    }

    private int getNumberOfRanges() {
        return pageRanges.size() / 2;
    }

    private void pinAsSingleRange(CloudMegaPageReadContext ctx, IBufferCache bufferCache, int fileId, int pageZeroId)
            throws HyracksDataException {
        int start = pageRanges.getInt(0);
        int end = pageRanges.getInt(pageRanges.size() - 1);
        int numberOfPages = end - start + 1;
        ctx.pin(bufferCache, fileId, pageZeroId, start, numberOfPages, requestedPages);
    }

    private void pinEachRange(CloudMegaPageReadContext ctx, IBufferCache bufferCache, int fileId, int pageZeroId)
            throws HyracksDataException {
        for (int i = 0; i < pageRanges.size(); i += 2) {
            int start = pageRanges.getInt(i);
            int end = pageRanges.getInt(i + 1);
            int numberOfPages = end - start + 1;
            ctx.pin(bufferCache, fileId, pageZeroId, start, numberOfPages, requestedPages);
        }
    }

    private void pinMergedRanges(CloudMegaPageReadContext ctx, IBufferCache bufferCache, int fileId, int pageZeroId)
            throws HyracksDataException {
        int startIndex = 0;
        for (int i = 0; i < rangeBoundaries.length; i++) {
            int endIndex = rangeBoundaries[i];
            int rangeStart = pageRanges.getInt(startIndex);
            int rangeEnd = pageRanges.getInt(endIndex);
            int numberOfPages = rangeEnd - rangeStart + 1;
            ctx.pin(bufferCache, fileId, pageZeroId, rangeStart, numberOfPages, requestedPages);

            // Start from the next cut
            startIndex = endIndex + 1;
        }
    }

    @Override
    public String toString() {
        return "{rangeBoundaries: " + Arrays.toString(rangeBoundaries) + ", pageRanges: " + pageRanges
                + ", requestedPages: " + requestedPages + "}";
    }
}
