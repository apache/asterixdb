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

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.asterix.column.bytes.stream.out.ByteBufferOutputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IValuesWriter;

/**
 * Default implementation of page zero writer that allocates space for all columns in the schema.
 * 
 * This writer uses a fixed layout where every column in the schema has a reserved slot,
 * regardless of whether data is present for that column. This approach is optimal for
 * dense datasets where most columns contain data.
 * 
 * Memory layout in page zero:
 * 1. Column offsets: 4 bytes per column (numberOfColumns * 4 bytes)
 * 2. Column filters: 16 bytes per column (numberOfColumns * 16 bytes) - min/max values
 * 3. Primary key data: variable size, written sequentially
 */
public class DefaultColumnPageZeroWriter implements IColumnPageZeroWriter {
    /** Size in bytes for storing a column offset */
    public static final int COLUMN_OFFSET_SIZE = Integer.BYTES;
    /** Size in bytes for storing column filter (min + max values) */
    public static final int FILTER_SIZE = Long.BYTES * 2; // min and max

    private final ByteBufferOutputStream primaryKeys;
    private ByteBuffer pageZero;
    private int numberOfColumns;

    // Offset positions within page zero buffer
    private int primaryKeysOffset; // Where primary key data starts
    private int columnsOffset; // Where column offset array starts  
    private int filtersOffset; // Where column filter array starts

    public DefaultColumnPageZeroWriter() {
        primaryKeys = new ByteBufferOutputStream();
    }

    @Override
    public void reset(ByteBuffer pageZeroBuf, int[] presentColumns, int numberOfColumns) {
        this.pageZero = pageZeroBuf;
        this.numberOfColumns = numberOfColumns;
        this.primaryKeysOffset = pageZeroBuf.position();
    }

    @Override
    public byte flagCode() {
        return DEFAULT_WRITER_FLAG;
    }

    /**
     * Allocates space in page zero for all column metadata.
     * 
     * The allocation strategy reserves space for all columns in the schema:
     * - Column offsets: Fixed array of 4-byte integers
     * - Column filters: Fixed array of 16-byte min/max pairs
     * - Primary keys: Variable-size data written after metadata
     */
    @Override
    public void allocateColumns() {
        // allocate space for columns' offset (4 * numberOfColumns)
        columnsOffset = primaryKeysOffset;
        primaryKeysOffset += COLUMN_OFFSET_SIZE * numberOfColumns;

        // allocate space for columns' filter (8 + 8) * numberOfColumns
        filtersOffset = primaryKeysOffset;
        primaryKeysOffset += FILTER_SIZE * numberOfColumns;

        // reset the position for pageZero,
        // the primary keys will be written from this offset
        pageZero.position(primaryKeysOffset);
        primaryKeys.reset(pageZero);
    }

    /**
     * Records a column's data offset using direct array indexing.
     * In the default layout, the column index directly maps to the array position.
     * 
     * @param absoluteColumnIndex The absolute column index (unused in default layout)
     * @param relativeColumnIndex The column index used for array positioning
     * @param offset The byte offset where the column's data begins
     */
    @Override
    public void putColumnOffset(int absoluteColumnIndex, int relativeColumnIndex, int offset) {
        pageZero.putInt(columnsOffset + COLUMN_OFFSET_SIZE * relativeColumnIndex, offset);
    }

    /**
     * Stores column filter information using direct array indexing.
     * Filters enable efficient column pruning during query execution.
     * 
     * @param relativeColumnIndex The column index used for array positioning
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
     * Primary keys are stored in page zero for fast access during operations.
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

    @Override
    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    /**
     * In the default layout, all columns are always included since space is pre-allocated.
     * 
     * @param presentColumns Set of columns present in this page (unused)
     * @param columnIndex The column index to check (unused)
     * @param includeChildrenColumns Whether to include child columns (unused)
     * @return always true for default layout
     */
    @Override
    public boolean includeOrderedColumn(BitSet presentColumns, int columnIndex, boolean includeChildrenColumns) {
        return true;
    }

    @Override
    public ByteBuffer getPageZeroBuffer() {
        return pageZero;
    }

    /**
     * In the default layout, the relative index is the same as the absolute index.
     * 
     * @param columnIndex The absolute column index
     * @return the same column index (identity mapping)
     */
    @Override
    public int getRelativeColumnIndex(int columnIndex) {
        return columnIndex;
    }

    @Override
    public int getColumnOffsetsSize() {
        return numberOfColumns * COLUMN_OFFSET_SIZE;
    }
}
