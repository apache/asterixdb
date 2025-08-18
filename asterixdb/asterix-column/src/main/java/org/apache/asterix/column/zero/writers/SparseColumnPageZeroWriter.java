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
package org.apache.asterix.column.zero.writers;

import static org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter.FILTER_SIZE;

import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IValuesWriter;

/**
 * Sparse implementation of page zero writer that only allocates space for present columns.
 * <p>
 * This writer optimizes space usage for sparse datasets by storing only the columns
 * that actually contain data. Each column entry includes both the column index and
 * its data offset, allowing for efficient lookup while minimizing space overhead.
 *<p>
 * Memory layout in page zero:
 * 1. Column entries: 8 bytes per present column (4 bytes index + 4 bytes offset)
 * 2. Column filters: 16 bytes per present column (min/max values)
 * 3. Primary key data: variable size, written sequentially
 * <p>
 * This layout is particularly beneficial when the number of present columns is
 * significantly smaller than the total schema size.
 */
public class SparseColumnPageZeroWriter extends DefaultColumnPageZeroWriter {
    /** Size in bytes for storing a column index */
    public static final int COLUMN_INDEX_SIZE = Integer.BYTES;
    /** Size in bytes for storing a column entry (index + offset) */
    public static final int COLUMN_OFFSET_SIZE = Integer.BYTES + COLUMN_INDEX_SIZE;

    private int[] presentColumns;
    private int numberOfPresentColumns;

    public SparseColumnPageZeroWriter() {
        super();
    }

    @Override
    public void resetBasedOnColumns(int[] presentColumns, int numberOfColumns /* not being used */, int headerSize) {
        this.presentColumns = presentColumns;
        this.numberOfPresentColumns = presentColumns.length;
        this.primaryKeysOffset = headerSize;
        this.headerSize = headerSize;
        pageZero.position(headerSize);
    }

    public void resetInnerBasedOnColumns(int[] presentColumns, int numberOfPresentColumns, int headerSize) {
        this.presentColumns = presentColumns;
        this.numberOfPresentColumns = numberOfPresentColumns;
        this.primaryKeysOffset = headerSize; // Reset primary keys offset for sparse layout
        pageZero.position(headerSize);
    }

    @Override
    public byte flagCode() {
        return SPARSE_WRITER_FLAG;
    }

    /**
     * Allocates space in page zero for present column metadata only.
     * 
     * The allocation strategy reserves space only for columns that contain data:
     * - Column entries: Array of (index, offset) pairs for present columns
     * - Column filters: Array of min/max pairs for present columns only
     * - Primary keys: Variable-size data written after metadata
     */
    @Override
    public void allocateColumns() {
        // allocate space for columns' offset (8 * numberOfPresentColumns)
        columnsOffset = primaryKeysOffset;
        primaryKeysOffset += COLUMN_OFFSET_SIZE * numberOfPresentColumns;

        // allocate space for filters'
        filtersOffset = primaryKeysOffset;
        primaryKeysOffset += FILTER_SIZE * numberOfPresentColumns;

        // reset the position for pageZero,
        // the primary keys will be written from this offset
        pageZero.position(primaryKeysOffset);
        primaryKeys.reset(pageZero);
    }

    /**
     * Records a column's data offset along with its absolute column index.
     * 
     * In the sparse layout, each entry stores both the original column index
     * and the data offset, enabling lookup of sparse columns.
     * 
     * @param absoluteColumnIndex The absolute column index in the schema
     * @param relativeColumnIndex The position within the present columns array
     * @param offset The byte offset where the column's data begins
     */
    @Override
    public void putColumnOffset(int absoluteColumnIndex, int relativeColumnIndex, int offset) {
        int columnOffset = columnsOffset + COLUMN_OFFSET_SIZE * relativeColumnIndex;
        // Store the absolute column index first
        pageZero.putInt(columnOffset, absoluteColumnIndex);
        // Then store the data offset
        pageZero.putInt(columnOffset + Integer.BYTES, offset);
    }

    /**
     * Stores column filter information for present columns only.
     * Uses the relative column index to position within the sparse filter array.
     * 
     * @param relativeColumnIndex The position within the present columns array
     * @param normalizedMinValue The normalized minimum value in the column
     * @param normalizedMaxValue The normalized maximum value in the column
     */
    @Override
    public void putColumnFilter(int relativeColumnIndex, long normalizedMinValue, long normalizedMaxValue) {
        int offset = filtersOffset + relativeColumnIndex * FILTER_SIZE;
        pageZero.putLong(offset, normalizedMinValue);
        pageZero.putLong(offset + Long.BYTES, normalizedMaxValue);
    }

    /**
     * Writes primary key columns directly to page zero.
     * Primary keys are always present and stored similarly to the default layout.
     * 
     * @param primaryKeyWriters Array of writers containing primary key data
     * @throws HyracksDataException If an error occurs during writing
     */
    @Override
    public void writePrimaryKeyColumns(IValuesWriter[] primaryKeyWriters) throws HyracksDataException {
        for (int i = 0; i < primaryKeyWriters.length; i++) {
            IValuesWriter writer = primaryKeyWriters[i];
            // Record the offset where this primary key column starts
            putColumnOffset(i, i, primaryKeysOffset + primaryKeys.size());
            // Write the actual primary key data
            writer.flush(primaryKeys);
        }
    }

    /**
     * Performs binary search to find the relative position of a column index
     * within the sorted present columns array.
     * 
     * @param columnIndex The absolute column index to find
     * @return the relative position within present columns, or -1 if not found
     */
    public int findColumnIndex(int[] presentColumns, int numberOfPresentColumns, int columnIndex) {
        int low = 0;
        int high = numberOfPresentColumns - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = presentColumns[mid];
            if (midVal == columnIndex) {
                return mid;
            } else if (midVal < columnIndex) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return -1;
    }

    /**
     * Determines whether a column should be included based on presence in the sparse set.
     * 
     * For sparse layouts, only explicitly present columns or child columns
     * (when includeChildrenColumns is true) are included.
     * 
     * @param presentColumns Set of columns present in this page
     * @param columnIndex The column index to check
     * @param includeChildrenColumns Whether to include child columns for complex types
     * @return true if the column should be included
     */
    @Override
    public boolean includeOrderedColumn(BitSet presentColumns, int columnIndex, boolean includeChildrenColumns) {
        return includeChildrenColumns || presentColumns.get(columnIndex);
    }

    @Override
    public int getNumberOfColumns() {
        return numberOfPresentColumns;
    }

    /**
     * Maps an absolute column index to its relative position within the present columns array.
     * 
     * This mapping is essential for sparse layouts where the storage position
     * differs from the schema position.
     * 
     * @param columnIndex The absolute column index in the schema
     * @return the relative position within the present columns array
     * @throws IllegalStateException if the column index is not found in present columns
     */
    @Override
    public int getRelativeColumnIndex(int columnIndex) {
        int columnRelativeIndex = findColumnIndex(presentColumns, numberOfPresentColumns, columnIndex);
        if (columnRelativeIndex == -1) {
            throw new IllegalStateException("Column index " + columnIndex + " does not exist in present columns.");
        }
        return columnRelativeIndex;
    }

    @Override
    public int getColumnOffsetsSize() {
        return numberOfPresentColumns * COLUMN_OFFSET_SIZE;
    }
}
