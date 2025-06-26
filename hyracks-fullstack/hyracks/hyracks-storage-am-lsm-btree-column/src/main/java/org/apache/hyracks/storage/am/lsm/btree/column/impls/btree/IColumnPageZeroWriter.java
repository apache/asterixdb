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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;

/**
 * Interface for writing column metadata to page zero of a column page.
 * This abstraction supports different page zero layouts including:
 * - Default layout: stores all columns with fixed offsets
 * - Sparse layout: stores only present columns with variable offsets
 * 
 * The writer handles column offsets, filters (min/max values), and primary key data.
 */
public interface IColumnPageZeroWriter {

    /** Flag code for default page zero writer */
    byte DEFAULT_WRITER_FLAG = 0;

    /** Flag code for sparse page zero writer */
    byte SPARSE_WRITER_FLAG = 1;

    byte MULTI_PAGE_DEFAULT_WRITER_FLAG = 2;

    byte MULTI_PAGE_SPARSE_WRITER_FLAG = 3;

    int MIN_COLUMN_SPACE = 4 + 16; // offset + filter size

    enum ColumnPageZeroWriterType {
        DEFAULT((byte) 2), // multi-page default page zero
        SPARSE((byte) 3), // multi-page sparse page zero
        ADAPTIVE((byte) -1); // adaptive writer that switches between default and sparse based on space efficiency

        private final byte writerFlag;

        ColumnPageZeroWriterType(byte writerFlag) {
            this.writerFlag = writerFlag;
        }

        public byte getWriterFlag() {
            return writerFlag;
        }
    }

    /**
     * Initializes the writer with page zero buffer and column information.
     *
     * @param presentColumns Array of column indexes that are present in this page
     * @param numberOfColumns Total number of columns in the schema (may be larger than presentColumns)
     */
    void resetBasedOnColumns(int[] presentColumns, int numberOfColumns, int headerSize) throws HyracksDataException;

    default void resetBasedOnColumns(int[] presentColumns, int numberOfColumns) throws HyracksDataException {
        resetBasedOnColumns(presentColumns, numberOfColumns, AbstractColumnBTreeLeafFrame.HEADER_SIZE);
    }

    /**
     * Returns the flag code that identifies this writer type.
     * Used for selecting the appropriate reader during deserialization.
     * 
     * @return flag code (DEFAULT_WRITER_FLAG for default, SPARSE_WRITER_FLAG for sparse)
     */
    byte flagCode();

    /**
     * Allocates space in page zero for column metadata.
     * This includes space for column offsets, filters, and primary keys.
     */
    void allocateColumns();

    /**
     * Records the offset of a column's data within the page.
     * 
     * @param absoluteColumnIndex The absolute column index in the schema
     * @param relativeColumnIndex The relative column index within this page (for sparse layouts)
     * @param offset The byte offset where the column's data begins
     */
    void putColumnOffset(int absoluteColumnIndex, int relativeColumnIndex, int offset) throws HyracksDataException;

    /**
     * Stores filter information (min/max values) for a column.
     * This enables efficient filtering during query execution.
     * 
     * @param relativeColumnIndex The relative column index within this page
     * @param normalizedMinValue The normalized minimum value in the column
     * @param normalizedMaxValue The normalized maximum value in the column
     */
    void putColumnFilter(int relativeColumnIndex, long normalizedMinValue, long normalizedMaxValue)
            throws HyracksDataException;

    /**
     * Writes primary key column data to page zero.
     * Primary keys are stored directly in page zero for fast access.
     * 
     * @param primaryKeyWriters Array of writers containing primary key data
     * @throws HyracksDataException If an error occurs during writing
     */
    void writePrimaryKeyColumns(IValuesWriter[] primaryKeyWriters) throws HyracksDataException;

    /**
     * Returns the number of columns handled by this writer.
     * For default writers, this is the total schema size.
     * For sparse writers, this is the number of present columns.
     * 
     * @return number of columns
     */
    int getNumberOfColumns();

    /**
     * Determines whether a column should be included in ordered processing.
     * This is particularly important for sparse columns where not all columns may be present.
     * 
     * @param presentColumns Set of columns present in this page
     * @param columnIndex The column index to check
     * @param includeChildrenColumns Whether to include child columns for complex types
     * @return true if the column should be included
     */
    boolean includeOrderedColumn(BitSet presentColumns, int columnIndex, boolean includeChildrenColumns);

    /**
     * Returns the page zero buffer being written to.
     * 
     * @return the page zero buffer
     */
    int getPageZeroBufferCapacity();

    /**
     * Maps an absolute column index to a relative index within this page.
     * For default layouts, this is typically an identity mapping.
     * For sparse layouts, this maps to the position within the present columns array.
     * 
     * @param columnIndex The absolute column index in the schema
     * @return the relative column index within this page
     */
    int getRelativeColumnIndex(int columnIndex);

    /**
     * Returns the total size in bytes used for storing column offsets.
     * 
     * @return size in bytes of column offset storage
     */
    int getColumnOffsetsSize();

    void setPageZero(ByteBuffer pageZero);

    void flush(ByteBuffer buf, int numberOfTuples, ITupleReference minKey, ITupleReference maxKey,
            AbstractColumnTupleWriter columnWriter, ITreeIndexTupleWriter rowTupleWriter) throws HyracksDataException;

    int getHeaderSize();
}
