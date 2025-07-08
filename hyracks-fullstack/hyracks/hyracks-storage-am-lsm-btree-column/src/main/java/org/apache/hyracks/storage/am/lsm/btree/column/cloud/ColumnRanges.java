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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud;

import static org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep.ColumnSweeperUtil.EMPTY;
import static org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil.getColumnPageIndex;
import static org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil.getColumnStartOffset;
import static org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil.getNumberOfRemainingPages;

import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read.CloudColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep.ColumnSweepPlanner;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.sweep.ColumnSweeper;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongComparator;

/**
 * Computes columns offsets, lengths, and pages
 */
public final class ColumnRanges {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final LongComparator OFFSET_COMPARATOR = IntPairUtil.FIRST_COMPARATOR;
    private final int numberOfPrimaryKeys;

    // For eviction
    private final BitSet nonEvictablePages;

    // For Query
    private final BitSet evictablePages;
    private final BitSet cloudOnlyPages;

    private ColumnBTreeReadLeafFrame leafFrame;
    private long[] offsetColumnIndexPairs;
    private int[] lengths;
    private int[] columnsOrder;
    private int pageZeroId;

    public ColumnRanges(int numberOfPrimaryKeys) {
        this.numberOfPrimaryKeys = numberOfPrimaryKeys;

        offsetColumnIndexPairs = new long[0];
        lengths = new int[0];
        columnsOrder = new int[0];

        nonEvictablePages = new BitSet();

        evictablePages = new BitSet();
        cloudOnlyPages = new BitSet();
    }

    /**
     * @return number of primary keys
     */
    public int getNumberOfPrimaryKeys() {
        return numberOfPrimaryKeys;
    }

    /**
     * Reset ranges for initializing {@link ColumnSweepPlanner}
     *
     * @param leafFrame to compute the ranges for
     */
    public void reset(ColumnBTreeReadLeafFrame leafFrame) throws HyracksDataException {
        reset(leafFrame, EMPTY, EMPTY, EMPTY);
    }

    /**
     * Reset column ranges for {@link ColumnSweeper}
     *
     * @param leafFrame to compute the ranges for
     * @param plan      eviction plan
     */
    public void reset(ColumnBTreeReadLeafFrame leafFrame, BitSet plan) throws HyracksDataException {
        reset(leafFrame, plan, EMPTY, EMPTY);
    }

    /**
     * Reset ranges for {@link CloudColumnReadContext}
     *
     * @param leafFrame        to compute the ranges for
     * @param requestedColumns required columns
     * @param evictableColumns columns that are or will be evicted
     * @param cloudOnlyColumns locked columns that cannot be read from a local disk
     */
    public void reset(ColumnBTreeReadLeafFrame leafFrame, BitSet requestedColumns, BitSet evictableColumns,
            BitSet cloudOnlyColumns) throws HyracksDataException {
        try {
            // Set leafFrame
            this.leafFrame = leafFrame;
            // Ensure arrays capacities (given the leafFrame's columns and pages)
            init();

            // Set the first 32-bits to the offset and the second 32-bits to columnIndex
            int numberOfPresentColumnsInLeaf = leafFrame.populateOffsetColumnIndexPairs(offsetColumnIndexPairs);

            // Set artificial offset to determine the last column's length
            int megaLeafLength = leafFrame.getMegaLeafNodeLengthInBytes();
            offsetColumnIndexPairs[numberOfPresentColumnsInLeaf] =
                    IntPairUtil.of(megaLeafLength, numberOfPresentColumnsInLeaf);

            // Sort the pairs by offset (i.e., lowest offset first)
            LongArrays.stableSort(offsetColumnIndexPairs, 0, numberOfPresentColumnsInLeaf, OFFSET_COMPARATOR);

            int columnOrdinal = 0;
            for (int i = 0; i < numberOfPresentColumnsInLeaf; i++) {
                if (offsetColumnIndexPairs[i] == 0) {
                    //Any requested column's offset can't be zero
                    //In case a column is not being present in the accessed pageZero segments, it will be defaulted to 0
                    continue;
                }
                int columnIndex = getColumnIndexFromPair(offsetColumnIndexPairs[i]);
                int offset = getOffsetFromPair(offsetColumnIndexPairs[i]);
                int nextOffset = getOffsetFromPair(offsetColumnIndexPairs[i + 1]);

                // Compute the column's length in bytes (set 0 for PKs)
                int length = columnIndex < numberOfPrimaryKeys ? 0 : nextOffset - offset;
                // In case of sparse columns, few columnIndexes can be greater than the total sparse column count.
                ensureCapacity(columnIndex);
                lengths[columnIndex] = length;

                // Get start page ID (given the computed length above)
                int startPageId = getColumnStartPageIndex(columnIndex);
                // Get the number of pages (given the computed length above)
                int numberOfPages = getColumnNumberOfPages(columnIndex);

                if (columnIndex >= numberOfPrimaryKeys && requestedColumns.get(columnIndex)) {
                    // Set column index
                    columnsOrder[columnOrdinal++] = columnIndex;
                    // Compute cloud-only and evictable pages
                    setCloudOnlyAndEvictablePages(columnIndex, cloudOnlyColumns, evictableColumns, startPageId,
                            numberOfPages);
                    // A requested column. Keep its pages as requested
                    continue;
                }

                // Mark the page as non-evictable
                for (int j = startPageId; j < startPageId + numberOfPages; j++) {
                    nonEvictablePages.set(j);
                }
            }

            // Bound the nonRequestedPages to the number of pages in the mega leaf node
            nonEvictablePages.set(leafFrame.getMegaLeafNodeNumberOfPages());
            // to indicate the end
            columnsOrder[columnOrdinal] = -1;
        } finally {
            //Unpin the not required segment pages
            leafFrame.unPinNotRequiredPageZeroSegments();
        }
    }

    /**
     * First page of a column
     *
     * @param columnIndex column index
     * @return pageID
     */
    public int getColumnStartPageIndex(int columnIndex) throws HyracksDataException {
        int pageSize = leafFrame.getBuffer().capacity();
        return getColumnPageIndex(leafFrame.getColumnOffset(columnIndex), pageSize);
    }

    /**
     * The number of pages the column occupies
     *
     * @param columnIndex column index
     * @return number of pages
     */
    public int getColumnNumberOfPages(int columnIndex) throws HyracksDataException {
        int pageSize = leafFrame.getBuffer().capacity();
        int offset = getColumnStartOffset(leafFrame.getColumnOffset(columnIndex), pageSize);
        int firstBufferLength = pageSize - offset;
        int remainingLength = getColumnLength(columnIndex) - firstBufferLength;
        // 1 for the first page + the number of remaining pages
        return 1 + getNumberOfRemainingPages(remainingLength, pageSize);
    }

    /**
     * Length of a column in bytes
     *
     * @param columnIndex column index
     * @return number of bytes
     */
    public int getColumnLength(int columnIndex) {
        return lengths[columnIndex];
    }

    /**
     * Returns true if the page is meant to be read from the cloud only
     *
     * @param pageId page ID
     * @return true of the page should be read from the cloud, false otherwise
     * @see #reset(ColumnBTreeReadLeafFrame, BitSet, BitSet, BitSet)
     */
    public boolean isCloudOnly(int pageId) {
        // Compute the relative page ID for this mega leaf node
        int relativePageId = pageId - pageZeroId;
        return cloudOnlyPages.get(relativePageId);
    }

    /**
     * Whether the page has been or will be evicted
     *
     * @param pageId page ID
     * @return true of the page was or will be evicted, false otherwise
     */
    public boolean isEvictable(int pageId) {
        int relativePageId = pageId - pageZeroId;
        return evictablePages.get(relativePageId);
    }

    /**
     * @return Bitset of all non-requested pages
     */
    public BitSet getNonEvictablePages() {
        return nonEvictablePages;
    }

    /**
     * @return you the order of columns that should be read in order to ensure (semi) sequential access.
     * Sequential means page X is read before page Y, forall X and Y where X < Y
     */
    public int[] getColumnsOrder() {
        return columnsOrder;
    }

    public int getTotalNumberOfPages() {
        return leafFrame.getMegaLeafNodeNumberOfPages();
    }

    private void init() {
        int numberOfColumns = leafFrame.getNumberOfColumns();
        offsetColumnIndexPairs = LongArrays.ensureCapacity(offsetColumnIndexPairs, numberOfColumns + 1, 0);
        lengths = IntArrays.ensureCapacity(lengths, numberOfColumns, 0);
        columnsOrder = IntArrays.ensureCapacity(columnsOrder, numberOfColumns + 1, 0);
        nonEvictablePages.clear();
        evictablePages.clear();
        cloudOnlyPages.clear();
        pageZeroId = leafFrame.getPageId();
    }

    private static int getOffsetFromPair(long pair) {
        return IntPairUtil.getFirst(pair);
    }

    private static int getColumnIndexFromPair(long pair) {
        return IntPairUtil.getSecond(pair);
    }

    private void setCloudOnlyAndEvictablePages(int columnIndex, BitSet cloudOnlyColumns, BitSet evictableColumns,
            int startPageId, int numberOfPages) {
        if (evictableColumns == EMPTY && cloudOnlyColumns == EMPTY) {
            return;
        }

        // Find pages that meant to be read from the cloud only or are evictable
        boolean cloudOnly = cloudOnlyColumns.get(columnIndex);
        boolean evictable = evictableColumns.get(columnIndex);
        if (cloudOnly || evictable) {
            for (int j = startPageId; j < startPageId + numberOfPages; j++) {
                if (cloudOnly) {
                    cloudOnlyPages.set(j);
                } else {
                    evictablePages.set(j);
                }
            }
        }
    }

    private void ensureCapacity(int columnIndex) {
        if (columnIndex >= lengths.length) {
            lengths = IntArrays.grow(lengths, columnIndex + 1);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        int numberOfPages = leafFrame.getMegaLeafNodeNumberOfPages();
        builder.append("       ");
        for (int i = 0; i < numberOfPages; i++) {
            builder.append(String.format("%02d", i));
            builder.append("  ");
        }

        builder.append('\n');
        for (int i = 0; i < leafFrame.getNumberOfColumns(); i++) {
            builder.append(String.format("%03d", i));
            builder.append(":");
            int startPageId = 0;
            try {
                startPageId = getColumnStartPageIndex(i);
            } catch (HyracksDataException e) {
                throw new RuntimeException(e);
            }
            int columnPagesCount = 0;
            try {
                columnPagesCount = getColumnNumberOfPages(i);
            } catch (HyracksDataException e) {
                throw new RuntimeException(e);
            }
            printColumnPages(builder, numberOfPages, startPageId, columnPagesCount);
        }

        builder.append("nonEvictablePages: ");
        builder.append(nonEvictablePages);
        builder.append('\n');
        builder.append("evictablePages: ");
        builder.append(evictablePages);
        builder.append('\n');
        builder.append("cloudOnlyPages: ");
        builder.append(cloudOnlyPages);

        return builder.toString();
    }

    private void printColumnPages(StringBuilder builder, int numberOfPages, int startPageId, int columnPagesCount) {
        for (int i = 0; i < numberOfPages; i++) {
            builder.append("   ");
            if (i >= startPageId && i < startPageId + columnPagesCount) {
                builder.append(1);
            } else {
                builder.append(0);
            }
        }
        builder.append('\n');
    }
}
