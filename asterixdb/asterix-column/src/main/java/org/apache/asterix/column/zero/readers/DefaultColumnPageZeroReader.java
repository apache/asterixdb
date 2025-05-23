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

import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.HEADER_SIZE;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.LEFT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.MEGA_LEAF_NODE_LENGTH;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NEXT_LEAF_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.NUMBER_OF_COLUMNS_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.RIGHT_MOST_KEY_OFFSET;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.TUPLE_COUNT_OFFSET;

import java.nio.ByteBuffer;

import org.apache.asterix.column.zero.writers.DefaultColumnPageZeroWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.IntPairUtil;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.IColumnPageZeroReader;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class DefaultColumnPageZeroReader implements IColumnPageZeroReader {
    protected ByteBuffer pageZeroBuf;

    @Override
    public void reset(ByteBuffer pageZeroBuf) {
        this.pageZeroBuf = pageZeroBuf;
    }

    @Override
    public int getColumnOffset(int columnIndex) {
        return pageZeroBuf.getInt(HEADER_SIZE + columnIndex * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE);
    }

    @Override
    public int getColumnFilterOffset(int columnIndex) {
        int columnsOffsetEnd =
                HEADER_SIZE + getNumberOfPresentColumns() * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        return columnsOffsetEnd + columnIndex * DefaultColumnPageZeroWriter.FILTER_SIZE;
    }

    @Override
    public long getLong(int offset) {
        return pageZeroBuf.getLong(offset);
    }

    @Override
    public void skipFilters() {
        int filterEndOffset = getColumnFilterOffset(getNumberOfPresentColumns());
        pageZeroBuf.position(filterEndOffset);
    }

    @Override
    public void skipColumnOffsets() {
        int columnEndOffset =
                HEADER_SIZE + getNumberOfPresentColumns() * DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        pageZeroBuf.position(columnEndOffset);
    }

    @Override
    public int getTupleCount() {
        return pageZeroBuf.getInt(TUPLE_COUNT_OFFSET);
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
        int relativeColumnIndex = getRelativeColumnIndex(columnIndex);
        return relativeColumnIndex < getNumberOfPresentColumns();
    }

    @Override
    public void getAllColumns(IntOpenHashSet presentColumns) {
        int numberOfColumns = getNumberOfPresentColumns();
        for (int i = 0; i < numberOfColumns; i++) {
            presentColumns.add(i);
        }
    }

    @Override
    public ByteBuffer getPageZeroBuf() {
        return pageZeroBuf;
    }

    @Override
    public void populateOffsetColumnIndexPairs(long[] offsetColumnIndexPairs) {
        int columnOffsetStart = HEADER_SIZE;
        for (int i = 0; i < offsetColumnIndexPairs.length; i++) {
            int offset = pageZeroBuf.getInt(columnOffsetStart);
            offsetColumnIndexPairs[i] = IntPairUtil.of(offset, i);
            columnOffsetStart += DefaultColumnPageZeroWriter.COLUMN_OFFSET_SIZE;
        }
    }
}
