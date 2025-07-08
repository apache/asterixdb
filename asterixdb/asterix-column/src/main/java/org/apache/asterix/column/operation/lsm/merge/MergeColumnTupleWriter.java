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
package org.apache.asterix.column.operation.lsm.merge;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWriter;
import org.apache.asterix.column.tuple.MergeColumnTupleReference;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.writer.ColumnBatchWriter;
import org.apache.asterix.column.zero.PageZeroWriterFlavorSelector;
import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.asterix.column.zero.writers.SparseColumnPageZeroWriter;
import org.apache.asterix.column.zero.writers.multipage.DefaultColumnMultiPageZeroWriter;
import org.apache.asterix.column.zero.writers.multipage.SparseColumnMultiPageZeroWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnWriteContext;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroWriterFlavorSelector;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class MergeColumnTupleWriter extends AbstractColumnTupleWriter {
    private final MergeColumnWriteMetadata columnMetadata;
    private final int maxLeafNodeSize;
    private final MergeColumnTupleReference[] componentsTuples;
    private final RunLengthIntArray writtenComponents;

    private final IColumnValuesWriter[] primaryKeyWriters;
    private final PriorityQueue<IColumnValuesWriter> orderedColumns;
    private final ColumnBatchWriter writer;
    private final IColumnPageZeroWriterFlavorSelector pageZeroWriterFlavorSelector;
    protected final BitSet presentColumnsIndexes;
    private final int maxNumberOfTuples;
    private int primaryKeysEstimatedSize;
    private int numberOfAntiMatter;
    private int numberOfTuples;

    public MergeColumnTupleWriter(MergeColumnWriteMetadata columnMetadata, int pageSize, int maxNumberOfTuples,
            double tolerance, int maxLeafNodeSize, IColumnWriteContext writeContext) {
        this.columnMetadata = columnMetadata;
        this.pageZeroWriterFlavorSelector = new PageZeroWriterFlavorSelector();
        this.maxLeafNodeSize = maxLeafNodeSize;
        this.presentColumnsIndexes = new BitSet();
        List<IColumnTupleIterator> componentsTuplesList = columnMetadata.getComponentsTuples();
        this.componentsTuples = new MergeColumnTupleReference[componentsTuplesList.size()];
        int totalLength = 0;
        int totalNumberOfTuples = 0;
        for (int i = 0; i < componentsTuplesList.size(); i++) {
            MergeColumnTupleReference mergeTuple = (MergeColumnTupleReference) componentsTuplesList.get(i);
            this.componentsTuples[i] = mergeTuple;
            mergeTuple.registerEndOfPageCallBack(this::writeAllColumns);
            mergeTuple.setColumnIndexes(presentColumnsIndexes);
            totalNumberOfTuples += mergeTuple.getTupleCount();
            totalLength += mergeTuple.getMergingLength();
        }
        this.maxNumberOfTuples = getMaxNumberOfTuples(maxNumberOfTuples, totalNumberOfTuples, totalLength);
        this.writtenComponents = new RunLengthIntArray();
        writer = new ColumnBatchWriter(columnMetadata.getMultiPageOpRef(), pageSize, tolerance, writeContext);
        writtenComponents.reset();
        primaryKeyWriters = new IColumnValuesWriter[columnMetadata.getNumberOfPrimaryKeys()];
        for (int i = 0; i < primaryKeyWriters.length; i++) {
            primaryKeyWriters[i] = columnMetadata.getWriter(i);
        }
        orderedColumns = new PriorityQueue<>(Comparator.comparingInt(x -> -x.getEstimatedSize()));
        numberOfAntiMatter = 0;
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        int primaryKeysSize = 0;
        for (int i = 0; i < columnMetadata.getNumberOfPrimaryKeys(); i++) {
            primaryKeysSize += primaryKeyWriters[i].getEstimatedSize(tuple.getFieldLength(i));
        }

        return primaryKeysSize;
    }

    @Override
    public void init(IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException {
        columnMetadata.init(multiPageOp);
    }

    @Override
    public int getAbsoluteNumberOfColumns(boolean includeCurrentTupleColumns) {
        return columnMetadata.getNumberOfColumns();
    }

    @Override
    public int getMaxNumberOfTuples() {
        return maxNumberOfTuples;
    }

    @Override
    public int getPrimaryKeysEstimatedSize() {
        return primaryKeysEstimatedSize;
    }

    @Override
    public void writeTuple(ITupleReference tuple) throws HyracksDataException {
        MergeColumnTupleReference columnTuple = (MergeColumnTupleReference) tuple;
        if (numberOfTuples == 0) {
            // fill with the columnIndexes
            for (MergeColumnTupleReference componentsTuple : componentsTuples) {
                componentsTuple.fillColumnIndexes();
            }
        }
        numberOfTuples++;
        int componentIndex = columnTuple.getComponentIndex();
        int skipCount = columnTuple.getAndResetSkipCount();
        if (skipCount > 0) {
            writtenComponents.add(setAntimatterIndicator(componentIndex), skipCount);
        }

        if (!columnTuple.isAntimatter()) {
            // anti matters contain only the primary keys, and none of the other columns
            writtenComponents.add(componentIndex);
        } else {
            // counter for logging purposes
            numberOfAntiMatter++;
        }

        writePrimaryKeys(columnTuple);
    }

    @Override
    public IColumnPageZeroWriterFlavorSelector getColumnPageZeroWriterFlavorSelector() {
        return pageZeroWriterFlavorSelector;
    }

    @Override
    public int getPageZeroWriterOccupiedSpace(int maxColumnsInPageZerothSegment, int bufferCapacity,
            boolean includeCurrentTupleColumns, IColumnPageZeroWriter.ColumnPageZeroWriterType writerType) {
        int spaceOccupiedByDefaultWriter;
        int spaceOccupiedBySparseWriter;

        if (writerType == IColumnPageZeroWriter.ColumnPageZeroWriterType.DEFAULT) {
            // go for default multi-page writer
            spaceOccupiedByDefaultWriter =
                    getSpaceOccupiedByDefaultWriter(maxColumnsInPageZerothSegment, includeCurrentTupleColumns);
            return spaceOccupiedByDefaultWriter;
        } else if (writerType == IColumnPageZeroWriter.ColumnPageZeroWriterType.SPARSE) {
            // Maximum space occupied by the columns = maxColumnsInPageZerothSegment * (offset + filter size)
            spaceOccupiedBySparseWriter = getSpaceOccupiedBySparseWriter(maxColumnsInPageZerothSegment, bufferCapacity);
            return spaceOccupiedBySparseWriter;
        }

        spaceOccupiedBySparseWriter = getSpaceOccupiedBySparseWriter(maxColumnsInPageZerothSegment, bufferCapacity);
        spaceOccupiedByDefaultWriter =
                getSpaceOccupiedByDefaultWriter(maxColumnsInPageZerothSegment, includeCurrentTupleColumns);
        pageZeroWriterFlavorSelector.switchPageZeroWriterIfNeeded(spaceOccupiedByDefaultWriter,
                spaceOccupiedBySparseWriter);
        return Math.min(spaceOccupiedBySparseWriter, spaceOccupiedByDefaultWriter);
    }

    private int getSpaceOccupiedByDefaultWriter(int maxColumnsInPageZerothSegment, boolean includeCurrentTupleColumns) {
        int spaceOccupiedByDefaultWriter;
        int totalNumberOfColumns = getAbsoluteNumberOfColumns(includeCurrentTupleColumns);
        totalNumberOfColumns = Math.min(totalNumberOfColumns, maxColumnsInPageZerothSegment);
        spaceOccupiedByDefaultWriter = DefaultColumnMultiPageZeroWriter.EXTENDED_HEADER_SIZE + totalNumberOfColumns
                * (DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE + DefaultColumnPageZeroWriter.FILTER_SIZE);
        return spaceOccupiedByDefaultWriter;
    }

    private int getSpaceOccupiedBySparseWriter(int maxColumnsInPageZerothSegment, int bufferCapacity) {
        int presentColumns = presentColumnsIndexes.cardinality();
        int maximumNumberOfColumnsInASegment =
                SparseColumnMultiPageZeroWriter.getMaximumNumberOfColumnsInAPage(bufferCapacity);
        int numberOfExtraPagesRequired = presentColumns <= maxColumnsInPageZerothSegment ? 0
                : (int) Math.ceil(
                        (double) (presentColumns - maxColumnsInPageZerothSegment) / maximumNumberOfColumnsInASegment);
        int headerSpace = SparseColumnMultiPageZeroWriter.getHeaderSpace(numberOfExtraPagesRequired);
        presentColumns = Math.min(presentColumns, maxColumnsInPageZerothSegment);

        // space occupied by the sparse writer
        return headerSpace + presentColumns
                * (SparseColumnPageZeroWriter.COLUMN_OFFSET_SIZE + DefaultColumnPageZeroWriter.FILTER_SIZE);
    }

    @Override
    public int flush(IColumnPageZeroWriter pageZeroWriter) throws HyracksDataException {
        // here the numberOfColumns is the total number of columns present in the LSM Index (across all disk components)
        // Hence, a merge will fail if union(NumberOfColumns(D1) + NumberOfColumns(D2) + ... + NumberOfColumns(DN)) >
        // pageZero space, and since the merged page contains this many number of columns, the first flush will fail.
        int numberOfColumns = columnMetadata.getNumberOfColumns();
        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();

        // If writtenComponents is not empty, process non-key columns
        if (writtenComponents.getSize() > 0) {
            writeNonKeyColumns();
            writtenComponents.reset();
        }

        // Iterate over the BitSet (presentColumnsIndexes) to get the indexes of the set bits
        for (int columnIndex = presentColumnsIndexes.nextSetBit(0); columnIndex >= 0; columnIndex =
                presentColumnsIndexes.nextSetBit(columnIndex + 1)) {
            if (columnIndex < numberOfPrimaryKeys) {
                continue; // Skip primary key columns
            }
            orderedColumns.add(columnMetadata.getWriter(columnIndex));
        }

        // Reset pageZeroWriter based on the writer
        writer.setPageZeroWriter(pageZeroWriter, toIndexArray(presentColumnsIndexes), numberOfColumns);

        // Write primary key columns
        writer.writePrimaryKeyColumns(primaryKeyWriters);

        // Write the other columns and get the total length
        int totalLength = writer.writeColumns(orderedColumns);

        // Reset numberOfAntiMatter (assuming this is part of the logic)
        numberOfAntiMatter = 0;

        return totalLength;
    }

    public static int[] toIndexArray(BitSet bitSet) {
        return FlushColumnTupleWriter.toIndexArray(bitSet);
    }

    @Override
    public void close() {
        columnMetadata.close();
        writer.close();
    }

    @Override
    public void reset() {
        presentColumnsIndexes.clear();
        numberOfTuples = 0;
    }

    private void writePrimaryKeys(MergeColumnTupleReference columnTuple) throws HyracksDataException {
        int primaryKeySize = 0;
        for (int i = 0; i < columnMetadata.getNumberOfPrimaryKeys(); i++) {
            IColumnValuesReader columnReader = columnTuple.getReader(i);
            IColumnValuesWriter columnWriter = primaryKeyWriters[i];
            columnReader.write(columnWriter, false);
            primaryKeySize += columnWriter.getEstimatedSize();
        }
        primaryKeysEstimatedSize = primaryKeySize;
    }

    private void writeNonKeyColumns() throws HyracksDataException {
        for (int i = 0; i < writtenComponents.getNumberOfBlocks(); i++) {
            int componentIndex = writtenComponents.getBlockValue(i);
            if (componentIndex < 0) {
                // Skip writing values of deleted tuples
                componentIndex = clearAntimatterIndicator(componentIndex);
                skipReaders(componentIndex, writtenComponents.getBlockSize(i));
                continue;
            }
            MergeColumnTupleReference componentTuple = componentsTuples[componentIndex];
            int count = writtenComponents.getBlockSize(i);

            // Iterate over the set bits in presentColumnsIndexes
            for (int columnIndex = presentColumnsIndexes.nextSetBit(0); columnIndex >= 0; columnIndex =
                    presentColumnsIndexes.nextSetBit(columnIndex + 1)) {
                if (columnIndex < columnMetadata.getNumberOfPrimaryKeys()) {
                    continue;
                }
                IColumnValuesReader columnReader = componentTuple.getReader(columnIndex);
                IColumnValuesWriter columnWriter = columnMetadata.getWriter(columnIndex);
                writeColumn(i, componentIndex, columnReader, columnWriter, count);
            }
        }
    }

    private void writeColumn(int blockIndex, int componentIndex, IColumnValuesReader columnReader,
            IColumnValuesWriter columnWriter, int count) throws HyracksDataException {
        try {
            columnReader.write(columnWriter, count);
        } catch (ColumnarValueException e) {
            ObjectNode node = e.createNode(getClass().getSimpleName());
            appendExceptionCommonInfo(node, componentIndex, count);
            node.put("blockIndex", blockIndex);
            throw e;
        }
    }

    private void skipReaders(int componentIndex, int count) throws HyracksDataException {
        MergeColumnTupleReference componentTuple = componentsTuples[componentIndex];
        try {
            // Iterate over the set bits in presentColumnsIndexes
            for (int columnIndex = presentColumnsIndexes.nextSetBit(0); columnIndex >= 0; columnIndex =
                    presentColumnsIndexes.nextSetBit(columnIndex + 1)) {
                if (columnIndex < columnMetadata.getNumberOfPrimaryKeys()) {
                    continue;
                }
                IColumnValuesReader columnReader = componentTuple.getReader(columnIndex);
                columnReader.skip(count);
            }
        } catch (ColumnarValueException e) {
            ObjectNode node = e.createNode(getClass().getSimpleName());
            appendExceptionCommonInfo(node, componentIndex, count);
            throw e;
        }
    }

    private void writeAllColumns(MergeColumnTupleReference columnTuple) throws HyracksDataException {
        /*
         * The last tuple from one of the components was reached. Since we are going to the next leaf, we will not be
         * able to access the readers of this component's leaf after this tuple. So, we are going to write
         * the values of all columns as recorded in writtenComponents
         */
        int skipCount = columnTuple.getAndResetSkipCount();
        if (skipCount > 0) {
            writtenComponents.add(setAntimatterIndicator(columnTuple.getComponentIndex()), skipCount);
        }
        writeNonKeyColumns();
        writtenComponents.reset();
    }

    private static int setAntimatterIndicator(int componentIndex) {
        // This is to avoid -0, where the '-' is the antimatter indicator
        return -(componentIndex + 1);
    }

    private static int clearAntimatterIndicator(int componentIndex) {
        return -componentIndex - 1;
    }

    private int getMaxNumberOfTuples(int maxNumberOfTuples, int totalNumberOfTuples, int totalLength) {
        int numberOfTuplesUsingMaxSize = Integer.MAX_VALUE;
        if (totalLength > maxLeafNodeSize && totalNumberOfTuples > 0) {
            int bytesPerTuple = (int) Math.ceil(totalLength / (double) totalNumberOfTuples);
            numberOfTuplesUsingMaxSize = maxLeafNodeSize / bytesPerTuple;
        }
        return Math.min(maxNumberOfTuples, numberOfTuplesUsingMaxSize);
    }

    private void appendExceptionCommonInfo(ObjectNode node, int componentIndex, int count) {
        node.put("numberOfWrittenPrimaryKeys", primaryKeyWriters[0].getCount());
        node.put("writtenComponents", writtenComponents.toString());
        node.put("numberOFAntiMatters", numberOfAntiMatter);
        node.put("componentIndex", componentIndex);
        node.put("count", count);
    }

    public void updateColumnMetadataForCurrentTuple(ITupleReference tuple) throws HyracksDataException {
    }
}
