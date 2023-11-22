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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

public abstract class AbstractColumnTupleReference implements IColumnTupleIterator {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String UNSUPPORTED_OPERATION_MSG = "Operation is not supported for column tuples";
    private final int componentIndex;
    private final ColumnBTreeReadLeafFrame frame;
    private final IColumnBufferProvider[] primaryKeyBufferProviders;
    private final IColumnBufferProvider[] filterBufferProviders;
    private final IColumnBufferProvider[] buffersProviders;
    private final int numberOfPrimaryKeys;
    private int endIndex;
    protected int tupleIndex;

    // For logging
    private final LongSet pinnedPages;
    private int totalNumberOfMegaLeafNodes;
    private int numOfSkippedMegaLeafNodes;
    private int maxNumberOfPinnedPages;

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

        pinnedPages = new LongOpenHashSet();
        int numberOfFilteredColumns = info.getNumberOfFilteredColumns();
        filterBufferProviders = new IColumnBufferProvider[numberOfFilteredColumns];
        for (int i = 0; i < numberOfFilteredColumns; i++) {
            int columnIndex = info.getFilteredColumnIndex(i);
            if (columnIndex < 0) {
                filterBufferProviders[i] = DummyColumnBufferProvider.INSTANCE;
            } else if (columnIndex >= numberOfPrimaryKeys) {
                filterBufferProviders[i] = new ColumnMultiBufferProvider(columnIndex, multiPageOp, pinnedPages);
            } else {
                filterBufferProviders[i] = new ColumnSingleBufferProvider(columnIndex);
            }
        }

        int numberOfRequestedColumns = info.getNumberOfProjectedColumns();
        buffersProviders = new IColumnBufferProvider[numberOfRequestedColumns];
        for (int i = 0; i < numberOfRequestedColumns; i++) {
            int columnIndex = info.getColumnIndex(i);
            if (columnIndex >= numberOfPrimaryKeys) {
                buffersProviders[i] = new ColumnMultiBufferProvider(columnIndex, multiPageOp, pinnedPages);
            } else {
                buffersProviders[i] = DummyColumnBufferProvider.INSTANCE;
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
        //Release previous pinned pages if any
        unpinColumnsPages();
        /*
         * When startIndex = 0, a call to next() is performed to get the information of the PK
         * and 0 skips will be performed. If startIndex (for example) is 5, a call to next() will be performed
         * then 4 skips will be performed.
         */
        int skipCount = setPrimaryKeysAt(startIndex, startIndex);
        if (readColumnPages) {
            for (int i = 0; i < filterBufferProviders.length; i++) {
                IColumnBufferProvider provider = filterBufferProviders[i];
                provider.reset(frame);
                startColumnFilter(provider, i, numberOfTuples);
            }
        }

        if (readColumnPages && evaluateFilter()) {
            for (int i = 0; i < buffersProviders.length; i++) {
                IColumnBufferProvider provider = buffersProviders[i];
                provider.reset(frame);
                startColumn(provider, i, numberOfTuples);
            }

            /*
             * skipCount can be < 0 for cases when the tuples in the range [0, startIndex] are all anti-matters.
             * Consequently, tuples in the range [0, startIndex] do not have any non-key columns. Thus, the returned
             * skipCount from calling setPrimaryKeysAt(startIndex, startIndex) is a negative value. For that reason,
             * non-key column should not skip any value.
             */
            skip(Math.max(skipCount, 0));
        } else {
            numOfSkippedMegaLeafNodes++;
        }

        totalNumberOfMegaLeafNodes++;
    }

    @Override
    public final void setAt(int startIndex) throws HyracksDataException {
        if (tupleIndex == startIndex) {
            /*
             * This case happens when we ask for the same tuple again when utilizing a secondary index. To illustrate,
             * assume that the secondary index search yielded the following PKs [1, 1, 1, 2] -- keys are always sorted.
             * We see that the secondary index asked for the PK '1' three times. Asking for the same tuple multiple
             * times is possible when we do index nested-loop join (indexnl).
             * See ASTERIX-3311
             */
            return;
        }
        /*
         * Let say that tupleIndex = 5 and startIndex = 12
         * Then, skipCount = 12 - 5 - 1 = 6.
         */
        int skipCount = startIndex - tupleIndex - 1;
        tupleIndex = startIndex;
        /*
         * As in reset(int startIndex, int endIndex) above, a call to next() will be performed followed by 6 skips.
         * So, the reader will be moved forward 7 positions (5 + 7 = 12). Hence, the PK will be exactly at index 12.
         */
        skipCount = setPrimaryKeysAt(startIndex, skipCount);
        /*
         * For values, we need to do 6 skips, as next will be called later by the assembler
         * -- setting the position at 12 as well.
         */
        skip(skipCount);
    }

    protected abstract int setPrimaryKeysAt(int index, int skipCount) throws HyracksDataException;

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

    public final int getTupleCount() {
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
        for (int i = 0; i < filterBufferProviders.length; i++) {
            filterBufferProviders[i].releaseAll();
        }

        for (int i = 0; i < buffersProviders.length; i++) {
            buffersProviders[i].releaseAll();
        }

        maxNumberOfPinnedPages = Math.max(maxNumberOfPinnedPages, pinnedPages.size());
        pinnedPages.clear();
    }

    @Override
    public final void close() {
        if (!LOGGER.isDebugEnabled()) {
            return;
        }

        if (numOfSkippedMegaLeafNodes > 0) {
            LOGGER.debug("Filtered {} disk mega-leaf nodes out of {} in total", numOfSkippedMegaLeafNodes,
                    totalNumberOfMegaLeafNodes);
        }

        LOGGER.debug("Max number of pinned pages is {}", maxNumberOfPinnedPages + 1);
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
