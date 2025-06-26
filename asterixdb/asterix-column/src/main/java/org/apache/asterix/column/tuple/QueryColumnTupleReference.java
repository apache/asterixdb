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
package org.apache.asterix.column.tuple;

import java.util.List;

import org.apache.asterix.column.assembler.value.MissingValueGetter;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.bytes.stream.in.ByteBufferInputStream;
import org.apache.asterix.column.bytes.stream.in.DummyBytesInputStream;
import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.filter.IFilterApplier;
import org.apache.asterix.column.filter.TrueColumnFilterEvaluator;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.filter.range.IColumnRangeFilterValueAccessor;
import org.apache.asterix.column.operation.query.ColumnAssembler;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

public final class QueryColumnTupleReference extends AbstractAsterixColumnTupleReference {
    private final ColumnAssembler assembler;
    private final IColumnFilterEvaluator rangeFilterEvaluator;
    private final List<IColumnRangeFilterValueAccessor> filterValueAccessors;
    private final IColumnIterableFilterEvaluator columnFilterEvaluator;
    private final IFilterApplier filterApplier;
    private final List<IColumnValuesReader> filterColumnReaders;
    private final AbstractBytesInputStream[] filteredColumnStreams;
    private int previousIndex;

    public QueryColumnTupleReference(int componentIndex, ColumnBTreeReadLeafFrame frame,
            QueryColumnMetadata columnMetadata, IColumnReadMultiPageOp multiPageOp) {
        super(componentIndex, frame, columnMetadata, multiPageOp);
        assembler = columnMetadata.getAssembler();

        rangeFilterEvaluator = columnMetadata.getRangeFilterEvaluator();
        filterValueAccessors = columnMetadata.getFilterValueAccessors();

        columnFilterEvaluator = columnMetadata.getColumnFilterEvaluator();
        filterColumnReaders = columnMetadata.getFilterColumnReaders();
        filterApplier = createFilterApplier();

        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();
        filteredColumnStreams = new AbstractBytesInputStream[columnMetadata.getNumberOfFilteredColumns()];
        for (int i = 0; i < filteredColumnStreams.length; i++) {
            int columnIndex = filterColumnReaders.get(i).getColumnIndex();
            if (columnIndex < 0) {
                filteredColumnStreams[i] = DummyBytesInputStream.INSTANCE;
            } else if (columnIndex >= numberOfPrimaryKeys) {
                filteredColumnStreams[i] = new MultiByteBufferInputStream();
            } else {
                filteredColumnStreams[i] = new ByteBufferInputStream();
            }
        }
        previousIndex = -1;
    }

    @Override
    protected PrimitiveColumnValuesReader[] getPrimaryKeyReaders(IColumnProjectionInfo info) {
        return ((QueryColumnMetadata) info).getPrimaryKeyReaders();
    }

    @Override
    protected void skipMegaLeafNode() {
        assembler.reset(0);
    }

    @Override
    protected boolean startNewPage(int numberOfTuples) throws HyracksDataException {
        //Skip to filters
        frame.skipColumnOffsets();
        //Set filters' values
        FilterAccessorProvider.setFilterValues(filterValueAccessors, frame);
        //Skip filters
        frame.skipFilters();
        //Check if we should read all column pages
        boolean readColumns = rangeFilterEvaluator.evaluate();
        assembler.reset(readColumns ? numberOfTuples : 0);
        columnFilterEvaluator.reset(numberOfTuples);
        previousIndex = -1;
        return readColumns;
    }

    @Override
    protected void startColumnFilter(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        AbstractBytesInputStream columnStream = filteredColumnStreams[ordinal];
        columnStream.reset(buffersProvider);
        filterColumnReaders.get(ordinal).reset(columnStream, numberOfTuples);
    }

    @Override
    protected boolean evaluateFilter() throws HyracksDataException {
        return columnFilterEvaluator.evaluate();
    }

    @Override
    protected void startColumn(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        AbstractBytesInputStream columnStream = columnStreams[ordinal];
        columnStream.reset(buffersProvider);
        assembler.resetColumn(columnStream, ordinal);
    }

    @Override
    public void initSkip(int tupleIndex, int count) throws HyracksDataException {
        columnFilterEvaluator.setAt(tupleIndex);
        skip(count);
    }

    @Override
    public void skipCurrentTuple() throws HyracksDataException {
        if (isFilterApplied()) {
            // since cursorIndex is markedAsDeleted
            // move filter evaluator to the next valid tuple
            columnFilterEvaluator.setAt(tupleIndex + 1);
            assembler.setAt(columnFilterEvaluator.getValueIndex());
        } else {
            skip(1);
        }
    }

    private boolean isFilterApplied() {
        return columnFilterEvaluator.getTupleIndex() != -1;
    }

    private void skip(int count) throws HyracksDataException {
        assembler.skip(count);
    }

    public IValueReference getAssembledValue() throws HyracksDataException {
        try {
            if (previousIndex == tupleIndex) {
                return assembler.getPreviousValue();
            }
            previousIndex = tupleIndex;
            return filterApplier.getTuple();
        } catch (ColumnarValueException e) {
            appendExceptionInformation(e, previousIndex);
            throw e;
        }

    }

    private IFilterApplier createFilterApplier() {
        if (columnFilterEvaluator == TrueColumnFilterEvaluator.INSTANCE) {
            return assembler::nextValue;
        } else {
            return this::getFilteredAssembledValue;
        }
    }

    private IValueReference getFilteredAssembledValue() throws HyracksDataException {
        try {
            int index = columnFilterEvaluator.getTupleIndex();
            // index == -1 if the normalized filter indicated that a mega leaf node
            // is filtered
            if (index == tupleIndex) {
                // setAt in the assembler expect the value index (i.e., tupleCount - antiMatterCount)
                assembler.setAt(columnFilterEvaluator.getValueIndex());
                // set the next tuple index that satisfies the filter
                columnFilterEvaluator.evaluate();
                return assembler.nextValue();
            }
            return MissingValueGetter.MISSING;
        } catch (ColumnarValueException e) {
            columnFilterEvaluator.appendInformation(e);
            e.getNode().put("cursorTupleIndex", tupleIndex);
            throw e;
        }
    }
}
