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

import org.apache.asterix.column.bytes.stream.in.MultiByteBufferInputStream;
import org.apache.asterix.column.operation.lsm.merge.IEndOfPageCallBack;
import org.apache.asterix.column.operation.lsm.merge.MergeColumnReadMetadata;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.reader.PrimitiveColumnValuesReader;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

public final class MergeColumnTupleReference extends AbstractAsterixColumnTupleReference {
    // NoOP callback is for empty pages only
    private static final IEndOfPageCallBack EMPTY_PAGE_CALLBACK = createNoOpCallBack();
    private final IColumnValuesReader[] columnReaders;
    private int skipCount;
    private IEndOfPageCallBack endOfPageCallBack;
    private int mergingLength;

    public MergeColumnTupleReference(int componentIndex, ColumnBTreeReadLeafFrame frame,
            MergeColumnReadMetadata columnMetadata, IColumnReadMultiPageOp multiPageOp) {
        super(componentIndex, frame, columnMetadata, multiPageOp);
        this.columnReaders = columnMetadata.getColumnReaders();
        endOfPageCallBack = EMPTY_PAGE_CALLBACK;
    }

    @Override
    protected PrimitiveColumnValuesReader[] getPrimaryKeyReaders(IColumnProjectionInfo info) {
        MergeColumnReadMetadata columnMetadata = (MergeColumnReadMetadata) info;
        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();
        PrimitiveColumnValuesReader[] primaryKeyReaders = new PrimitiveColumnValuesReader[numberOfPrimaryKeys];
        IColumnValuesReader[] readers = columnMetadata.getColumnReaders();
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            primaryKeyReaders[i] = (PrimitiveColumnValuesReader) readers[i];
        }
        return primaryKeyReaders;
    }

    @Override
    protected boolean startNewPage(ByteBuffer pageZero, int numberOfColumns, int numberOfTuples) {
        //Skip filters
        pageZero.position(pageZero.position() + numberOfColumns * AbstractColumnFilterWriter.FILTER_SIZE);
        // skip count is always start from zero as no "search" is conducted during a merge
        this.skipCount = 0;
        mergingLength = 0;
        return true;
    }

    @Override
    protected void startColumn(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        int numberOfPrimaryKeys = primaryKeyStreams.length;
        if (ordinal < numberOfPrimaryKeys) {
            //Skip primary key
            return;
        }
        MultiByteBufferInputStream columnStream = (MultiByteBufferInputStream) columnStreams[ordinal];
        columnStream.reset(buffersProvider);
        IColumnValuesReader reader = columnReaders[ordinal];
        reader.reset(columnStream, numberOfTuples);
        mergingLength += buffersProvider.getLength();
    }

    @Override
    protected void startColumnFilter(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException {
        // NoOp
    }

    @Override
    protected boolean evaluateFilter() throws HyracksDataException {
        return true;
    }

    @Override
    public void skip(int count) throws HyracksDataException {
        skipCount += count;
    }

    @Override
    public void lastTupleReached() throws HyracksDataException {
        endOfPageCallBack.callEnd(this);
    }

    public int getAndResetSkipCount() {
        int currentSkipCount = skipCount;
        skipCount = 0;
        return currentSkipCount;
    }

    public IColumnValuesReader getReader(int columnIndex) {
        return columnReaders[columnIndex];
    }

    public void registerEndOfPageCallBack(IEndOfPageCallBack endOfPageCallBack) {
        this.endOfPageCallBack = endOfPageCallBack;
    }

    public int getMergingLength() {
        return mergingLength;
    }

    private static IEndOfPageCallBack createNoOpCallBack() {
        return columnTuple -> {
            if (!columnTuple.isEmpty()) {
                // safeguard against unset proper call back for non-empty pages
                throw new NullPointerException("endOfPageCallBack is null");
            }
        };
    }
}
