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
package org.apache.asterix.column.zero.readers;

import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.FLAG_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.LEFT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.MEGA_LEAF_NODE_LENGTH;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NEXT_LEAF_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NUMBER_OF_COLUMNS_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.RIGHT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IntPairUtil;
import org.apache.hyracks.storage.am.lsm.btree.column.error.ColumnarValueException;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class DefaultColumnPageZeroReader implements IColumnPageZeroReader {
    protected static Logger LOGGER = LogManager.getLogger();

    protected ByteBuffer pageZeroBuf;
    protected static final BitSet EMPTY_SEGMENTS = new BitSet();
    protected int numberOfPresentColumns;
    protected int headerSize;

    public DefaultColumnPageZeroReader() {
    }

    @Override
    public void reset(ByteBuffer pageZeroBuf, int headerSize) {
        this.pageZeroBuf = pageZeroBuf;
        this.numberOfPresentColumns = pageZeroBuf.getInt(NUMBER_OF_COLUMNS_OFFSET);
        this.headerSize = headerSize;
    }

    public void reset(ByteBuffer pageZeroBuf, int numberOfPresentColumns, int headerSize) {
        this.pageZeroBuf = pageZeroBuf;
        this.numberOfPresentColumns = numberOfPresentColumns;
        this.headerSize = headerSize;
    }

    @Override
    public int getColumnOffset(int columnIndex) {
        return pageZeroBuf.getInt(headerSize + columnIndex * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE);
    }

    protected int getColumnFilterOffset(int columnIndex) {
        int columnsOffsetEnd = headerSize + numberOfPresentColumns * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        return columnsOffsetEnd + columnIndex * DefaultColumnPageZeroWriter.FILTER_SIZE;
    }

    @Override
    public long getColumnFilterMin(int columnIndex) {
        int filterOffset = getColumnFilterOffset(columnIndex);
        return pageZeroBuf.getLong(filterOffset);
    }

    @Override
    public long getColumnFilterMax(int columnIndex) {
        int filterOffset = getColumnFilterOffset(columnIndex);
        return pageZeroBuf.getLong(filterOffset + Long.BYTES);
    }

    @Override
    public void skipFilters() {
        int filterEndOffset = getColumnFilterOffset(numberOfPresentColumns);
        pageZeroBuf.position(filterEndOffset);
    }

    @Override
    public void skipColumnOffsets() {
        int columnEndOffset = headerSize + numberOfPresentColumns * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        pageZeroBuf.position(columnEndOffset);
    }

    @Override
    public int getTupleCount() {
        return pageZeroBuf.getInt(TUPLE_COUNT_OFFSET);
    }

    @Override
    public int getNumberOfPageZeroSegments() {
        return 1;
    }

    @Override
    public int getLeftMostKeyOffset() {
        return pageZeroBuf.getInt(LEFT_MOST_KEY_OFFSET);
    }

    @Override
    public int getRightMostKeyOffset() {
        return pageZeroBuf.getInt(RIGHT_MOST_KEY_OFFSET);
    }

    @Override
    public int getNumberOfPresentColumns() {
        return pageZeroBuf.getInt(NUMBER_OF_COLUMNS_OFFSET);
    }

    @Override
    public int getRelativeColumnIndex(int columnIndex) {
        return columnIndex;
    }

    @Override
    public int getNextLeaf() {
        return pageZeroBuf.getInt(NEXT_LEAF_OFFSET);
    }

    @Override
    public int getMegaLeafNodeLengthInBytes() {
        return pageZeroBuf.getInt(MEGA_LEAF_NODE_LENGTH);
    }

    @Override
    public int getPageZeroCapacity() {
        return pageZeroBuf.capacity();
    }

    @Override
    public boolean isValidColumn(int columnIndex) {
        return columnIndex < numberOfPresentColumns;
    }

    @Override
    public void getAllColumns(BitSet presentColumns) {
        int numberOfColumns = numberOfPresentColumns;
        presentColumns.set(0, numberOfColumns);
    }

    @Override
    public ByteBuffer getPageZeroBuf() {
        return pageZeroBuf;
    }

    @Override
    public int populateOffsetColumnIndexPairs(long[] offsetColumnIndexPairs) {
        int columnOffsetStart = headerSize;
        for (int i = 0; i < numberOfPresentColumns; i++) {
            int offset = pageZeroBuf.getInt(columnOffsetStart);
            offsetColumnIndexPairs[i] = IntPairUtil.of(offset, i);
            columnOffsetStart += DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        }
        return numberOfPresentColumns;
    }

    @Override
    public BitSet getPageZeroSegmentsPages() {
        return EMPTY_SEGMENTS;
    }

    @Override
    public int getHeaderSize() {
        return headerSize;
    }

    @Override
    public void resetStream(IColumnBufferProvider pageZeroSegmentBufferProvider) {
        throw new UnsupportedOperationException("Not supported for DefaultColumnPageZeroReader");
    }

    @Override
    public BitSet markRequiredPageSegments(BitSet projectedColumns, int pageZeroId, boolean markAll) {
        return EMPTY_SEGMENTS;
    }

    @Override
    public void unPinNotRequiredPageZeroSegments() throws HyracksDataException {
        // No-OP
    }

    @Override
    public void printPageZeroReaderInfo() {
        ColumnarValueException ex = new ColumnarValueException();
        ObjectNode readerNode = ex.createNode(getClass().getSimpleName());
        readerNode.put("headerSize", headerSize);
        readerNode.put("numberOfPresentColumns", numberOfPresentColumns);
        readerNode.put("flag", pageZeroBuf.get(FLAG_OFFSET));
        LOGGER.debug("SingleColumnPageZeroReader Info: {}", readerNode.toPrettyString());
    }
}
