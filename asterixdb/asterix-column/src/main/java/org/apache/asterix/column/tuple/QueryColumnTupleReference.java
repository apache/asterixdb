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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.asterix.column.assembler.value.MissingValueGetter;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.bytes.stream.in.ByteBufferInputStream;
import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.filter.IFilterApplier;
import org.apache.asterix.column.filter.TrueColumnFilterEvaluator;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.filter.normalized.IColumnFilterNormalizedValueAccessor;
import org.apache.asterix.column.operation.query.ColumnAssembler;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

public final class QueryColumnTupleReference extends AbstractAsterixColumnTupleReference {
    private final ColumnAssembler assembler;
    private final IColumnFilterEvaluator normalizedFilterEvaluator;
    private final List<IColumnFilterNormalizedValueAccessor> filterValueAccessors;
    private final IColumnIterableFilterEvaluator columnFilterEvaluator;
    private final IFilterApplier filterApplier;
    private final List<IColumnValuesReader> filterColumnReaders;
    private final AbstractBytesInputStream[] filteredColumnStreams;

    public QueryColumnTupleReference(int componentIndex, ColumnBTreeReadLeafFrame frame,
            QueryColumnMetadata columnMetadata, IColumnReadMultiPageOp multiPageOp) {
        super(componentIndex, frame, columnMetadata, multiPageOp);
        assembler = columnMetadata.getAssembler();

        normalizedFilterEvaluator = columnMetadata.getNormalizedFilterEvaluator();
        filterValueAccessors = columnMetadata.getFilterValueAccessors();

        columnFilterEvaluator = columnMetadata.getColumnFilterEvaluator();
        filterColumnReaders = columnMetadata.getFilterColumnReaders();
        filterApplier = createFilterApplier();

        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();
        filteredColumnStreams = new AbstractBytesInputStream[columnMetadata.getNumberOfFilteredColumns()];
        for (int i = 0; i < filteredColumnStreams.length; i++) {
            if (filterColumnReaders.get(i).getColumnIndex() >= numberOfPrimaryKeys) {
                filteredColumnStreams[i] = new MultiByteBufferInputStream();
            } else {
                filteredColumnStreams[i] = new ByteBufferInputStream();
            }
        }
    }

    @Override
    protected PrimitiveColumnValuesReader[] getPrimaryKeyReaders(IColumnProjectionInfo info) {
        return ((QueryColumnMetadata) info).getPrimaryKeyReaders();
    }

    @Override
    protected boolean startNewPage(ByteBuffer pageZero, int numberOfColumns, int numberOfTuples)
            throws HyracksDataException {
        //Skip to filters
        pageZero.position(pageZero.position() + numberOfColumns * Integer.BYTES);
        //Set filters' values
        FilterAccessorProvider.setFilterValues(filterValueAccessors, pageZero, numberOfColumns);
        //Skip filters
        pageZero.position(pageZero.position() + numberOfColumns * AbstractColumnFilterWriter.FILTER_SIZE);
        //Check if we should read all column pages
        boolean readColumns = normalizedFilterEvaluator.evaluate();
        assembler.reset(readColumns ? numberOfTuples : 0);
        columnFilterEvaluator.reset();
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
    public void skip(int count) throws HyracksDataException {
        columnFilterEvaluator.setAt(assembler.skip(count));
    }

    public IValueReference getAssembledValue() throws HyracksDataException {
        return filterApplier.getTuple();
    }

    private IFilterApplier createFilterApplier() {
        if (columnFilterEvaluator == TrueColumnFilterEvaluator.INSTANCE) {
            return assembler::nextValue;
        } else {
            return this::getFilteredAssembledValue;
        }
    }

    private IValueReference getFilteredAssembledValue() throws HyracksDataException {
        int index = columnFilterEvaluator.getTupleIndex();
        // index == -1 if the normalized filter indicated that a mega leaf node
        // is filtered
        if (index == tupleIndex) {
            assembler.setAt(index);
            // set the next tuple index that satisfies the filter
            columnFilterEvaluator.evaluate();
            return assembler.nextValue();
        }
        return MissingValueGetter.MISSING;
    }
}
