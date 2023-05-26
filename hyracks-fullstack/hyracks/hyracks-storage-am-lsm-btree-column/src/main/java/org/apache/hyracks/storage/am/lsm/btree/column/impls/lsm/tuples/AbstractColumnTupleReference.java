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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractColumnTupleReference implements IColumnTupleIterator {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String UNSUPPORTED_OPERATION_MSG = "Operation is not supported for column tuples";
    private final int componentIndex;
    private final ColumnBTreeReadLeafFrame frame;
    private final IColumnBufferProvider[] primaryKeyBufferProviders;
    private final IColumnBufferProvider[] filterBufferProviders;
    private final IColumnBufferProvider[] buffersProviders;
    private final int numberOfPrimaryKeys;
    private int totalNumberOfMegaLeafNodes;
    private int numOfSkippedMegaLeafNodes;
    private int endIndex;
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

        int numberOfFilteredColumns = info.getNumberOfFilteredColumns();
        filterBufferProviders = new IColumnBufferProvider[numberOfFilteredColumns];
        for (int i = 0; i < numberOfFilteredColumns; i++) {
            int columnIndex = info.getFilteredColumnIndex(i);
            if (columnIndex >= numberOfPrimaryKeys) {
                filterBufferProviders[i] = new ColumnMultiBufferProvider(columnIndex, multiPageOp);
            } else {
                filterBufferProviders[i] = new ColumnSingleBufferProvider(columnIndex);
            }
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
        totalNumberOfMegaLeafNodes = 0;
        numOfSkippedMegaLeafNodes = 0;
    }

    @Override
    public final void newPage() throws HyracksDataException {
        tupleIndex = 0;
        ByteBuffer pageZero = frame.getBuffer();
        pageZero.clear();
        pageZero.position(HEADER_SIZE);

        int numberOfTuples = frame.getTupleCount();

        //Start primary keys
        for (int i = 0; i < numberOfPrimaryKeys; i++) {
            IColumnBufferProvider provider = primaryKeyBufferProviders[i];
            provider.reset(frame);
            startPrimaryKey(provider, i, numberOfTuples);
        }
    }

    @Override
    public final void reset(int startIndex, int endIndex) throws HyracksDataException {
        tupleIndex = startIndex;
        this.endIndex = endIndex;
        ByteBuffer pageZero = frame.getBuffer();
        int numberOfTuples = frame.getTupleCount();
        //Start new page and check whether we should skip reading non-key columns or not
        boolean readColumnPages = startNewPage(pageZero, frame.getNumberOfColumns(), numberOfTuples);
        setPrimaryKeysAt(startIndex, startIndex);
        if (readColumnPages) {
            for (int i = 0; i < filterBufferProviders.length; i++) {
                IColumnBufferProvider provider = filterBufferProviders[i];
                //Release previous pinned pages if any
                provider.releaseAll();
                provider.reset(frame);
                startColumnFilter(provider, i, numberOfTuples);
            }
        }

        if (readColumnPages && evaluateFilter()) {
            for (int i = 0; i < buffersProviders.length; i++) {
                IColumnBufferProvider provider = buffersProviders[i];
                //Release previous pinned pages if any
                provider.releaseAll();
                provider.reset(frame);
                startColumn(provider, i, numberOfTuples);
            }
            // Skip until before startIndex (i.e. stop at startIndex - 1)
            skip(startIndex);
        } else {
            numOfSkippedMegaLeafNodes++;
        }
        totalNumberOfMegaLeafNodes++;
    }

    @Override
    public final void setAt(int startIndex) throws HyracksDataException {
        int skipCount = startIndex - tupleIndex;
        tupleIndex = startIndex;
        setPrimaryKeysAt(startIndex, skipCount);
        // -1 because next would be called for all columns
        skip(skipCount - 1);
    }

    protected abstract void setPrimaryKeysAt(int index, int skipCount) throws HyracksDataException;

    protected abstract boolean startNewPage(ByteBuffer pageZero, int numberOfColumns, int numberOfTuples)
            throws HyracksDataException;

    protected abstract void startPrimaryKey(IColumnBufferProvider bufferProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException;

    protected abstract void startColumn(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException;

    protected abstract void startColumnFilter(IColumnBufferProvider buffersProvider, int ordinal, int numberOfTuples)
            throws HyracksDataException;

    protected abstract boolean evaluateFilter() throws HyracksDataException;

    protected abstract void onNext() throws HyracksDataException;

    protected final int getTupleCount() {
        return frame.getTupleCount();
    }

    protected final boolean isEmpty() {
        return frame.getTupleCount() == 0;
    }

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
        return tupleIndex >= endIndex;
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

    @Override
    public final void close() {
        if (LOGGER.isInfoEnabled() && numOfSkippedMegaLeafNodes > 0) {
            LOGGER.info("Filtered {} disk mega-leaf nodes out of {} in total", numOfSkippedMegaLeafNodes,
                    totalNumberOfMegaLeafNodes);
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
