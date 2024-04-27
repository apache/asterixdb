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

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleReader;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;

public final class ColumnBTreeReadLeafFrame extends AbstractColumnBTreeLeafFrame {
    private final AbstractColumnTupleReader columnarTupleReader;
    private final ITreeIndexTupleReference leftMostTuple;
    private final ITreeIndexTupleReference rightMostTuple;

    public ColumnBTreeReadLeafFrame(ITreeIndexTupleWriter rowTupleWriter,
            AbstractColumnTupleReader columnarTupleReader) {
        super(rowTupleWriter);
        this.columnarTupleReader = columnarTupleReader;
        leftMostTuple = rowTupleWriter.createTupleReference();
        rightMostTuple = rowTupleWriter.createTupleReference();
    }

    @Override
    public ITupleReference getLeftmostTuple() {
        if (getTupleCount() == 0) {
            return null;
        }

        leftMostTuple.setFieldCount(cmp.getKeyFieldCount());
        leftMostTuple.resetByTupleOffset(buf.array(), buf.getInt(LEFT_MOST_KEY_OFFSET));
        return leftMostTuple;
    }

    @Override
    public ITupleReference getRightmostTuple() {
        if (getTupleCount() == 0) {
            return null;
        }

        rightMostTuple.setFieldCount(cmp.getKeyFieldCount());
        rightMostTuple.resetByTupleOffset(buf.array(), buf.getInt(RIGHT_MOST_KEY_OFFSET));
        return rightMostTuple;
    }

    public IColumnTupleIterator createTupleReference(int index, IColumnReadMultiPageOp multiPageOp) {
        return columnarTupleReader.createTupleIterator(this, index, multiPageOp);
    }

    @Override
    public int getTupleCount() {
        return buf.getInt(Constants.TUPLE_COUNT_OFFSET);
    }

    public int getPageId() {
        return BufferedFileHandle.getPageId(((CachedPage) page).getDiskPageId());
    }

    public int getNumberOfColumns() {
        return buf.getInt(NUMBER_OF_COLUMNS_OFFSET);
    }

    public int getColumnOffset(int columnIndex) {
        if (columnIndex >= getNumberOfColumns()) {
            throw new IndexOutOfBoundsException(columnIndex + " >= " + getNumberOfColumns());
        }
        return columnarTupleReader.getColumnOffset(buf, columnIndex);
    }

    public int getNextLeaf() {
        return buf.getInt(NEXT_LEAF_OFFSET);
    }

    public long getMegaLeafNodeLengthInBytes() {
        return buf.getInt(MEGA_LEAF_NODE_LENGTH);
    }

    public int getMegaLeafNodeNumberOfPages() {
        return (int) Math.ceil((double) getMegaLeafNodeLengthInBytes() / buf.capacity());
    }

    public ColumnBTreeReadLeafFrame createCopy() {
        return new ColumnBTreeReadLeafFrame(rowTupleWriter, columnarTupleReader);
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        throw new IllegalArgumentException("Use createTupleReference(int)");
    }
}
