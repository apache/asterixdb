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
package org.apache.asterix.column.values.writer;

import java.util.PriorityQueue;

import org.apache.asterix.column.bytes.stream.out.MultiPersistentBufferBytesOutputStream;
import org.apache.asterix.column.bytes.stream.out.pointer.IReservedPointer;
import org.apache.asterix.column.values.IColumnBatchWriter;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;

/**
 * A writer for a batch columns' values.
 * This implementation abstracts the page zero operations using IColumnPageZeroWriter,
 * which allows for supporting different column formats including sparse columns.
 */
public final class ColumnBatchWriter implements IColumnBatchWriter {
    private final MultiPersistentBufferBytesOutputStream columns;
    private final int pageSize;
    private final double tolerance;
    private final IColumnWriteContext writeContext;
    private final IReservedPointer columnLengthPointer;
    // The writer for page zero, which handles all page zero operations including 
    // column offsets and filters. This abstraction supports both default and sparse column formats.
    private IColumnPageZeroWriter pageZeroWriter;
    private int nonKeyColumnStartOffset;

    public ColumnBatchWriter(Mutable<IColumnWriteMultiPageOp> multiPageOpRef, int pageSize, double tolerance,
            IColumnWriteContext writeContext) {
        this.pageSize = pageSize;
        this.tolerance = tolerance;
        this.writeContext = writeContext;
        columns = new MultiPersistentBufferBytesOutputStream(multiPageOpRef);
        columnLengthPointer = columns.createPointer();
    }

    /**
     * Sets the page zero writer implementation and initializes it.
     * This method replaces the direct page zero buffer manipulation with a more abstracted approach,
     * which allows for different page zero layouts (default or sparse).
     *
     * @param pageZeroWriter The writer implementation for page zero operations
     * @param presentColumnsIndexes Array containing the indexes of columns present in this batch
     * @param numberOfColumns Total number of columns in the schema
     */
    public void setPageZeroWriter(IColumnPageZeroWriter pageZeroWriter, int[] presentColumnsIndexes,
            int numberOfColumns) throws HyracksDataException {
        this.pageZeroWriter = pageZeroWriter;
        pageZeroWriter.resetBasedOnColumns(presentColumnsIndexes, numberOfColumns);
        pageZeroWriter.allocateColumns();
        nonKeyColumnStartOffset = pageZeroWriter.getPageZeroBufferCapacity();
    }

    @Override
    public void writePrimaryKeyColumns(IColumnValuesWriter[] primaryKeyWriters) throws HyracksDataException {
        // Delegate primary key column writing to the page zero writer
        pageZeroWriter.writePrimaryKeyColumns(primaryKeyWriters);
    }

    @Override
    public int writeColumns(PriorityQueue<IColumnValuesWriter> nonKeysColumnWriters) throws HyracksDataException {
        columns.reset();
        while (!nonKeysColumnWriters.isEmpty()) {
            IColumnValuesWriter writer = nonKeysColumnWriters.poll();
            writeColumn(writer);
        }

        // compute the final length
        return nonKeyColumnStartOffset + columns.size();
    }

    @Override
    public void close() {
        writeContext.close();
    }

    /**
     * Writes a column's data to the batch.
     * This method handles column data placement, ensuring optimal space usage and minimizing page splits.
     * It also records column offsets and filter values in page zero through the pageZeroWriter.
     * 
     * @param writer The column values writer containing data to be written
     * @throws HyracksDataException If an error occurs during writing
     */
    private void writeColumn(IColumnValuesWriter writer) throws HyracksDataException {
        boolean overlapping = true;
        if (!hasEnoughSpace(columns.getCurrentBufferPosition(), writer)) {
            /*
             * We reset the columns stream to write all pages and confiscate a new buffer to minimize splitting
             * the columns value into multiple pages.
             */
            overlapping = false;
            nonKeyColumnStartOffset += columns.capacity();
            columns.reset();
        }

        int columnIndex = writer.getColumnIndex();
        writeContext.startWritingColumn(columnIndex, overlapping);
        int columnRelativeOffset = columns.size();
        columns.reserveInteger(columnLengthPointer);

        // Get the relative column index within the current page zero layout
        // This mapping is particularly important for sparse columns where not all columns may be present
        int relativeColumnIndex = pageZeroWriter.getRelativeColumnIndex(columnIndex);

        // Record the column's absolute offset in page zero using the writer abstraction
        pageZeroWriter.putColumnOffset(columnIndex, relativeColumnIndex,
                nonKeyColumnStartOffset + columnRelativeOffset);

        // Store column filter information (min/max values) in page zero
        // This allows for faster filtering during query execution
        pageZeroWriter.putColumnFilter(relativeColumnIndex, writer.getNormalizedMinValue(),
                writer.getNormalizedMaxValue());

        // Write the actual column data
        writer.flush(columns);

        int length = columns.size() - columnRelativeOffset;
        columnLengthPointer.setInteger(length);
        writeContext.endWritingColumn(columnIndex, length);
    }

    /**
     * Determines if there is enough space in the current buffer for a column.
     * This method implements a space management strategy that balances between
     * optimal buffer utilization and minimizing column splits across pages.
     *
     * @param bufferPosition Current position in the buffer
     * @param columnWriter The column writer with data to be written
     * @return true if there is enough space, false otherwise
     */
    private boolean hasEnoughSpace(int bufferPosition, IColumnValuesWriter columnWriter) {
        if (bufferPosition == 0) {
            // if the current buffer is empty, then use it
            return true;
        } else if (tolerance == 1.0d) {
            // if tolerance is 100%, then it should avoid doing any calculations and start a with a new page
            return false;
        }

        // Estimated size mostly overestimate the size
        int columnSize = columnWriter.getEstimatedSize();
        float remainingPercentage = (pageSize - bufferPosition) / (float) pageSize;
        if (columnSize > pageSize) {
            /*
             * If the column size is larger than the page size, we check whether the remaining space is less than
             * the tolerance percentage
             * - true  --> allocate new buffer and tolerate empty space
             * - false --> we split the column into two pages
             */
            return remainingPercentage >= tolerance;
        }

        int freeSpace = pageSize - (bufferPosition + columnSize);

        /*
         * Check if the free space is enough to fit the column or the free space is less that the tolerance percentage
         * - true  --> we allocate new buffer and tolerate empty space
         * - false --> we split the column into two pages
         */
        return freeSpace > columnSize || remainingPercentage >= tolerance;
    }
}
