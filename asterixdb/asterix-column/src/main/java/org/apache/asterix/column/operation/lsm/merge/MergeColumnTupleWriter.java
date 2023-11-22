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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.asterix.column.tuple.MergeColumnTupleReference;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.writer.ColumnBatchWriter;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class MergeColumnTupleWriter extends AbstractColumnTupleWriter {
    private final MergeColumnWriteMetadata columnMetadata;
    private final int maxLeafNodeSize;
    private final MergeColumnTupleReference[] componentsTuples;
    private final RunLengthIntArray writtenComponents;

    private final IColumnValuesWriter[] primaryKeyWriters;
    private final PriorityQueue<IColumnValuesWriter> orderedColumns;
    private final ColumnBatchWriter writer;
    private final int maxNumberOfTuples;
    private int primaryKeysEstimatedSize;
    private int numberOfAntiMatter;

    public MergeColumnTupleWriter(MergeColumnWriteMetadata columnMetadata, int pageSize, int maxNumberOfTuples,
            double tolerance, int maxLeafNodeSize) {
        this.columnMetadata = columnMetadata;
        this.maxLeafNodeSize = maxLeafNodeSize;
        List<IColumnTupleIterator> componentsTuplesList = columnMetadata.getComponentsTuples();
        this.componentsTuples = new MergeColumnTupleReference[componentsTuplesList.size()];
        int totalLength = 0;
        int totalNumberOfTuples = 0;
        for (int i = 0; i < componentsTuplesList.size(); i++) {
            MergeColumnTupleReference mergeTuple = (MergeColumnTupleReference) componentsTuplesList.get(i);
            this.componentsTuples[i] = mergeTuple;
            mergeTuple.registerEndOfPageCallBack(this::writeAllColumns);
            totalNumberOfTuples += mergeTuple.getTupleCount();
            totalLength += mergeTuple.getMergingLength();
        }
        this.maxNumberOfTuples = getMaxNumberOfTuples(maxNumberOfTuples, totalNumberOfTuples, totalLength);
        this.writtenComponents = new RunLengthIntArray();
        writer = new ColumnBatchWriter(columnMetadata.getMultiPageOpRef(), pageSize, tolerance);
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
    public int getNumberOfColumns() {
        return columnMetadata.getNumberOfColumns();
    }

    @Override
    public int getMaxNumberOfTuples() {
        return maxNumberOfTuples;
    }

    @Override
    public int getOccupiedSpace() {
        int numberOfColumns = getNumberOfColumns();
        int filterSize = numberOfColumns * AbstractColumnFilterWriter.FILTER_SIZE;
        return primaryKeysEstimatedSize + filterSize;
    }

    @Override
    public void writeTuple(ITupleReference tuple) throws HyracksDataException {
        MergeColumnTupleReference columnTuple = (MergeColumnTupleReference) tuple;
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
    public int flush(ByteBuffer pageZero) throws HyracksDataException {
        int numberOfColumns = columnMetadata.getNumberOfColumns();
        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();
        if (writtenComponents.getSize() > 0) {
            writeNonKeyColumns();
            writtenComponents.reset();
        }
        for (int i = numberOfPrimaryKeys; i < numberOfColumns; i++) {
            orderedColumns.add(columnMetadata.getWriter(i));
        }
        writer.setPageZeroBuffer(pageZero, numberOfColumns, numberOfPrimaryKeys);
        int allocatedSpace = writer.writePrimaryKeyColumns(primaryKeyWriters);
        allocatedSpace += writer.writeColumns(orderedColumns);

        numberOfAntiMatter = 0;
        return allocatedSpace;
    }

    @Override
    public void close() {
        columnMetadata.close();
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
                //Skip writing values of deleted tuples
                componentIndex = clearAntimatterIndicator(componentIndex);
                skipReaders(componentIndex, writtenComponents.getBlockSize(i));
                continue;
            }
            MergeColumnTupleReference componentTuple = componentsTuples[componentIndex];
            int count = writtenComponents.getBlockSize(i);
            for (int j = columnMetadata.getNumberOfPrimaryKeys(); j < columnMetadata.getNumberOfColumns(); j++) {
                IColumnValuesReader columnReader = componentTuple.getReader(j);
                IColumnValuesWriter columnWriter = columnMetadata.getWriter(j);
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
            node.put("numberOfWrittenPrimaryKeys", primaryKeyWriters[0].getCount());
            node.put("writtenComponents", writtenComponents.toString());
            node.put("blockIndex", blockIndex);
            node.put("componentIndex", componentIndex);
            node.put("count", count);
            node.put("numberOFAntiMatters", numberOfAntiMatter);
            throw e;
        }
    }

    private void skipReaders(int componentIndex, int count) throws HyracksDataException {
        MergeColumnTupleReference componentTuple = componentsTuples[componentIndex];
        for (int j = columnMetadata.getNumberOfPrimaryKeys(); j < columnMetadata.getNumberOfColumns(); j++) {
            IColumnValuesReader columnReader = componentTuple.getReader(j);
            columnReader.skip(count);
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
}
