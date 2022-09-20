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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples;

import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.HEADER_SIZE;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

public abstract class AbstractColumnTupleReference implements IColumnTupleIterator {
    private static final String UNSUPPORTED_OPERATION_MSG = "Operation is not supported for column tuples";
    private final int componentIndex;
    private final ColumnBTreeReadLeafFrame frame;
    private final IColumnBufferProvider[] primaryKeyBufferProviders;
    private final IColumnBufferProvider[] buffersProviders;
    private final int numberOfPrimaryKeys;
    protected int tupleIndex;

    /**
     * Column tuple reference
     *
     * @param componentIndex LSM component index
     * @param frame          page0 frame
     * @param info           projection info
     */
    protected AbstractColumnTupleReference(int componentIndex, ColumnBTreeReadLeafFrame frame,
            IColumnProjectionInfo info, IColumnReadMultiPageOp multiPageOp) {
        this.componentIndex = componentIndex;
        this.frame = frame;
        numberOfPrimaryKeys = info.getNumberOfPrimaryKeys();

        primaryKeyBufferProviders = new IColumnBufferProvider[numberOfPrimaryKeys];

        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            primaryKeyBufferProviders[i] = new ColumnSingleBufferProvider(i);
        }

        int numberOfRequestedColumns = info.getNumberOfProjectedColumns();
        buffersProviders = new IColumnBufferProvider[numberOfRequestedColumns];
        for (int i = 0; i < numberOfRequestedColumns; i++) {
            int columnIndex = info.getColumnIndex(i);
            if (columnIndex >= numberOfPrimaryKeys) {
                buffersProviders[i] = new ColumnMultiBufferProvider(columnIndex, multiPageOp);
            } else {
                buffersProviders[i] = new ColumnSingleBufferProvider(columnIndex);
            }
        }
    }

    @Override
    public final void reset(int startIndex) throws HyracksDataException {
        tupleIndex = startIndex;
        ByteBuffer pageZero = frame.getBuffer();
        pageZero.clear();
        pageZero.position(HEADER_SIZE);

        int numberOfTuples = frame.getTupleCount();
        //Start new page and check whether we should skip reading non-key columns or not
        boolean readColumnPages = startNewPage(pageZero, frame.getNumberOfColumns(), numberOfTuples);

        //Start primary keys
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            IColumnBufferProvider provider = primaryKeyBufferProviders[i];
            provider.reset(frame);
            startPrimaryKey(provider, tupleIndex, i, numberOfTuples);
        }

        if (readColumnPages) {
            for (int i = 0; i < buffersProviders.length; i++) {
                IColumnBufferProvider provider = buffersProviders[i];
                //Release previous pinned pages if any
                provider.releaseAll();
                provider.reset(frame);
                startColumn(provider, tupleIndex, i, numberOfTuples);
            }
        }
    }

    protected abstract boolean startNewPage(ByteBuffer pageZero, int numberOfColumns, int numberOfTuples);

    protected abstract void startPrimaryKey(IColumnBufferProvider bufferProvider, int startIndex, int ordinal,
            int numberOfTuples) throws HyracksDataException;

    protected abstract void startColumn(IColumnBufferProvider buffersProvider, int startIndex, int ordinal,
            int numberOfTuples) throws HyracksDataException;

    protected abstract void onNext() throws HyracksDataException;

    @Override
    public final void next() throws HyracksDataException {
        onNext();
        tupleIndex++;
    }

    @Override
    public final void consume() {
        tupleIndex = frame.getTupleCount();
    }

    @Override
    public final boolean isConsumed() {
        return tupleIndex >= frame.getTupleCount();
    }

    @Override
    public final int getComponentIndex() {
        return componentIndex;
    }

    @Override
    public final void unpinColumnsPages() throws HyracksDataException {
        for (int i = 0; i < buffersProviders.length; i++) {
            buffersProviders[i].releaseAll();
        }
    }

    /* *************************************************************
     * Unsupported Operations
     * *************************************************************
     */

    @Override
    public final void setFieldCount(int fieldCount) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void setFieldCount(int fieldStartIndex, int fieldCount) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void resetByTupleOffset(byte[] buf, int tupleStartOffset) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }
}
