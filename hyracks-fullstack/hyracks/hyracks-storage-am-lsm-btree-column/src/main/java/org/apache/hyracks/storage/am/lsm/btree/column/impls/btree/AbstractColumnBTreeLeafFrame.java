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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.btree;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ISlotManager;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

/**
 * Disable all unsupported/unused operations
 */
public abstract class AbstractColumnBTreeLeafFrame implements ITreeIndexFrame {
    private static final String UNSUPPORTED_OPERATION_MSG = "Operation is not supported";

    /*
     * Remap the BTreeNSMFrame pointers for columnar pages
     */
    //Same as before
    public static final int TUPLE_COUNT_OFFSET = Constants.TUPLE_COUNT_OFFSET;
    //Previously Renaming
    public static final int NUMBER_OF_COLUMNS_OFFSET = Constants.FREE_SPACE_OFFSET;
    //Previously first four byte of LSN.
    public static final int LEFT_MOST_KEY_OFFSET = Constants.RESERVED_HEADER_SIZE;
    //Previously last four byte of LSN.
    public static final int RIGHT_MOST_KEY_OFFSET = LEFT_MOST_KEY_OFFSET + 4;
    /**
     * Currently, a column offset takes 4-byte (fixed). But in the future, we can reformat the offsets. For example,
     * we can store index-offset pairs if we encounter a sparse columns (i.e., most columns are just nulls). This
     * reformatting could be indicated by the FLAG byte.
     *
     * @see AbstractColumnTupleWriter#getColumnOffsetsSize()
     */
    public static final int SIZE_OF_COLUMNS_OFFSETS_OFFSET = RIGHT_MOST_KEY_OFFSET + 4;
    //Total number of columns pages
    public static final int NUMBER_OF_COLUMN_PAGES = SIZE_OF_COLUMNS_OFFSETS_OFFSET + 4;
    //A flag (used in NSM to indicate small and large pages). We can reuse it as explained above
    public static final int FLAG_OFFSET = NUMBER_OF_COLUMN_PAGES + 4;
    public static final int NEXT_LEAF_OFFSET = FLAG_OFFSET + 1;
    public static final int HEADER_SIZE = NEXT_LEAF_OFFSET + 4;

    protected final ITreeIndexTupleWriter rowTupleWriter;

    protected MultiComparator cmp;
    protected ICachedPage page;
    protected ByteBuffer buf;

    AbstractColumnBTreeLeafFrame(ITreeIndexTupleWriter rowTupleWriter) {
        this.rowTupleWriter = rowTupleWriter;
    }

    /* ****************************************************************************
     * Needed by both read and write
     * ****************************************************************************
     */

    @Override
    public final ITreeIndexTupleWriter getTupleWriter() {
        return rowTupleWriter;
    }

    @Override
    public final void setMultiComparator(MultiComparator cmp) {
        this.cmp = cmp;
    }

    @Override
    public final void setPage(ICachedPage page) {
        this.page = page;
        // Duplicate to avoid interference when scanning the dataset twice
        this.buf = page.getBuffer().duplicate();
        buf.clear();
        buf.position(HEADER_SIZE);
    }

    @Override
    public final ICachedPage getPage() {
        return page;
    }

    @Override
    public final ByteBuffer getBuffer() {
        return buf;
    }

    @Override
    public final boolean isLeaf() {
        return true;
    }

    @Override
    public final boolean isInterior() {
        return false;
    }

    @Override
    public final int getPageHeaderSize() {
        return HEADER_SIZE;
    }

    /* ****************************************************************************
     * Operations that are needed by either read or write
     * ****************************************************************************
     */

    @Override
    public void initBuffer(byte level) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public int getTupleCount() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public ITupleReference getLeftmostTuple() throws HyracksDataException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public ITupleReference getRightmostTuple() throws HyracksDataException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    /* ****************************************************************************
     * Unsupported Operations
     * ****************************************************************************
     */

    @Override
    public final String printHeader() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final byte getLevel() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void setLevel(byte level) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void insert(ITupleReference tuple, int tupleIndex) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final FrameOpSpaceStatus hasSpaceUpdate(ITupleReference newTuple, int oldTupleIndex) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void update(ITupleReference newTuple, int oldTupleIndex, boolean inPlace) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void delete(ITupleReference tuple, int tupleIndex) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final boolean compact() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final boolean compress() throws HyracksDataException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final int getTupleOffset(int slotNum) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final int getTotalFreeSpace() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void setPageLsn(long pageLsn) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final long getPageLsn() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final int getMaxTupleSize(int pageSize) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final ISlotManager getSlotManager() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final int getSlotSize() {
        return 0;
    }

    @Override
    public final int getFreeSpaceOff() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }

    @Override
    public final void setFreeSpaceOff(int freeSpace) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }
}
