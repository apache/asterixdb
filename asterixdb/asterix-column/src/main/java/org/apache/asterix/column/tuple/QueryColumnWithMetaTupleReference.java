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

import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.operation.query.ColumnAssembler;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.operation.query.QueryColumnWithMetaMetadata;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.asterix.column.values.reader.filter.FilterAccessorProvider;
import org.apache.asterix.column.values.reader.filter.IColumnFilterEvaluator;
import org.apache.asterix.column.values.reader.filter.IColumnFilterValueAccessor;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

public final class QueryColumnWithMetaTupleReference extends AbstractAsterixColumnTupleReference {
    private final ColumnAssembler assembler;
    private final ColumnAssembler metaAssembler;
    private final IColumnFilterEvaluator filterEvaluator;
    private final List<IColumnFilterValueAccessor> filterValueAccessors;

    public QueryColumnWithMetaTupleReference(int componentIndex, ColumnBTreeReadLeafFrame frame,
            QueryColumnMetadata columnMetadata, IColumnReadMultiPageOp multiPageOp) {
        super(componentIndex, frame, columnMetadata, multiPageOp);
        assembler = columnMetadata.getAssembler();
        metaAssembler = ((QueryColumnWithMetaMetadata) columnMetadata).getMetaAssembler();
        filterEvaluator = columnMetadata.getFilterEvaluator();
        filterValueAccessors = columnMetadata.getFilterValueAccessors();
    }

    @Override
    protected PrimitiveColumnValuesReader[] getPrimaryKeyReaders(IColumnProjectionInfo info) {
        return ((QueryColumnMetadata) info).getPrimaryKeyReaders();
    }

    @Override
    protected boolean startNewPage(ByteBuffer pageZero, int numberOfColumns, int numberOfTuples) {
        //Skip to filters
        pageZero.position(pageZero.position() + numberOfColumns * Integer.BYTES);
        //Set filters' values
        FilterAccessorProvider.setFilterValues(filterValueAccessors, pageZero, numberOfColumns);
        //Skip filters
        pageZero.position(pageZero.position() + numberOfColumns * AbstractColumnFilterWriter.FILTER_SIZE);
        //Check if we should read all column pages
        boolean readColumns = filterEvaluator.evaluate();
        assembler.reset(readColumns ? numberOfTuples : 0);
        metaAssembler.reset(readColumns ? numberOfTuples : 0);
        return readColumns;
    }

    @Override
    protected void startColumn(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        AbstractBytesInputStream columnStream = columnStreams[ordinal];
        columnStream.reset(buffersProvider);
        int metaColumnCount = metaAssembler.getNumberOfColumns();
        if (ordinal >= metaColumnCount) {
            assembler.resetColumn(columnStream, ordinal - metaColumnCount);
        } else {
            metaAssembler.resetColumn(columnStream, ordinal);
        }
    }

    @Override
    public void skip(int count) throws HyracksDataException {
        metaAssembler.skip(count);
        assembler.skip(count);
    }

    public IValueReference getAssembledValue() throws HyracksDataException {
        return assembler.nextValue();
    }

    public IValueReference getMetaAssembledValue() throws HyracksDataException {
        return metaAssembler.nextValue();
    }
}
