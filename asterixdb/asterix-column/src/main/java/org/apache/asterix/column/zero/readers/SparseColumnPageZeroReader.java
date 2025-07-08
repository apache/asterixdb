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
package org.apache.asterix.column.zero.readers;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.asterix.column.zero.writers.SparseColumnPageZeroWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IntPairUtil;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class SparseColumnPageZeroReader extends DefaultColumnPageZeroReader {
    private final Int2IntOpenHashMap columnIndexToRelativeColumnIndex;

    public SparseColumnPageZeroReader() {
        columnIndexToRelativeColumnIndex = new Int2IntOpenHashMap();
        columnIndexToRelativeColumnIndex.defaultReturnValue(-1);
    }

    @Override
    public void reset(ByteBuffer pageZeroBuf, int headerSize) {
        super.reset(pageZeroBuf, headerSize);
        columnIndexToRelativeColumnIndex.clear();
    }

    @Override
    public void reset(ByteBuffer pageZeroBuf, int numberOfPresentColumns, int headerSize) {
        super.reset(pageZeroBuf, numberOfPresentColumns, headerSize);
        columnIndexToRelativeColumnIndex.clear();
    }

    @Override
    public int getColumnOffset(int columnIndex) {
        int relativeColumnIndex = getRelativeColumnIndex(columnIndex);
        return pageZeroBuf.getInt(
                headerSize + relativeColumnIndex * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE + Integer.BYTES);
    }

    @Override
    public int getColumnFilterOffset(int columnIndex) {
        int relativeColumnIndex = getRelativeColumnIndex(columnIndex);
        int columnsOffsetEnd = headerSize + numberOfPresentColumns * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        return columnsOffsetEnd + relativeColumnIndex * DefaultColumnPageZeroWriter.FILTER_SIZE;
    }

    @Override
    public void skipFilters() {
        int filterEndOffset = headerSize + numberOfPresentColumns
                * (SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE + SparseColumnPageZeroWriter.FILTER_SIZE);
        pageZeroBuf.position(filterEndOffset);
    }

    @Override
    public void skipColumnOffsets() {
        int columnsOffsetEnd = headerSize + numberOfPresentColumns * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        pageZeroBuf.position(columnsOffsetEnd);
    }

    // x + 0, 8, 16, .... , 8*(n-1)
    @Override
    public int getRelativeColumnIndex(int columnIndex) {
        // if the entry is in cache, return it
        int cachedIndex = columnIndexToRelativeColumnIndex.get(columnIndex);
        if (cachedIndex != -1) {
            return cachedIndex;
        }
        int startColumnIndex = getColumnIndex(0);
        int startColumn = pageZeroBuf.getInt(startColumnIndex);
        if (startColumn == columnIndex) {
            columnIndexToRelativeColumnIndex.put(columnIndex, 0);
            return 0;
        }

        int totalColumns = numberOfPresentColumns;
        int lastColumnIndex = getColumnIndex(totalColumns - 1);
        int lastColumn = pageZeroBuf.getInt(lastColumnIndex);
        if (lastColumn == columnIndex) {
            columnIndexToRelativeColumnIndex.put(columnIndex, totalColumns - 1);
            return totalColumns - 1;
        }

        int start = 0;
        int end = totalColumns - 1;
        while (start <= end) {
            int mid = start + (end - start) / 2;
            int midIndex = getColumnIndex(mid);
            int midColumnIndex = pageZeroBuf.getInt(midIndex);
            if (midColumnIndex == columnIndex) {
                columnIndexToRelativeColumnIndex.put(columnIndex, mid);
                return mid; // this is the relative index
            } else if (midColumnIndex < columnIndex) {
                start = mid + 1;
            } else {
                end = mid - 1;
            }
        }

        return -1;
    }

    private int getColumnIndex(int index) {
        return headerSize + index * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
    }

    @Override
    public boolean isValidColumn(int columnIndex) {
        int relativeColumnIndex = getRelativeColumnIndex(columnIndex);
        return relativeColumnIndex != -1;
    }

    @Override
    public void getAllColumns(BitSet presentColumns) {
        if (numberOfPresentColumns == 0) {
            return;
        }

        int columnIndex = headerSize;
        int limit = columnIndex + numberOfPresentColumns * SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;

        while (columnIndex < limit) {
            int column = pageZeroBuf.getInt(columnIndex);
            presentColumns.set(column);
            columnIndex += SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        }
    }

    @Override
    public int populateOffsetColumnIndexPairs(long[] offsetColumnIndexPairs) {
        int columnIndex = getColumnIndex(0);
        for (int i = 0; i < numberOfPresentColumns; i++) {
            int column = pageZeroBuf.getInt(columnIndex);
            int offset = pageZeroBuf.getInt(columnIndex + SparseColumnPageZeroWriter.COLUMN_INDEX_SIZE);
            offsetColumnIndexPairs[i] = IntPairUtil.of(offset, column);
            columnIndex += SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        }
        return numberOfPresentColumns;
    }
}
