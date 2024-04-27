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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;

public class ColumnBTreeWriteLeafFrame extends AbstractColumnBTreeLeafFrame {
    private final AbstractColumnTupleWriter columnTupleWriter;

    public ColumnBTreeWriteLeafFrame(ITreeIndexTupleWriter rowTupleWriter,
            AbstractColumnTupleWriter columnTupleWriter) {
        super(rowTupleWriter);
        this.columnTupleWriter = columnTupleWriter;
    }

    @Override
    public void initBuffer(byte level) {
        buf.putInt(TUPLE_COUNT_OFFSET, 0);
        buf.put(Constants.LEVEL_OFFSET, level);
        buf.putInt(NUMBER_OF_COLUMNS_OFFSET, 0);
        buf.putInt(LEFT_MOST_KEY_OFFSET, -1);
        buf.putInt(RIGHT_MOST_KEY_OFFSET, -1);
        buf.putInt(SIZE_OF_COLUMNS_OFFSETS_OFFSET, 0);
        buf.putInt(MEGA_LEAF_NODE_LENGTH, 0);
        buf.put(FLAG_OFFSET, (byte) 0);
        buf.putInt(NEXT_LEAF_OFFSET, -1);
    }

    void flush(AbstractColumnTupleWriter columnWriter, int numberOfTuples, ITupleReference minKey,
            ITupleReference maxKey) throws HyracksDataException {
        // Prepare the space for writing the columns' information such as the primary keys
        buf.position(HEADER_SIZE);
        // Flush the columns to persistence pages and write the length of the mega leaf node in pageZero
        buf.putInt(MEGA_LEAF_NODE_LENGTH, columnWriter.flush(buf));

        // Write min and max keys
        int offset = buf.position();
        buf.putInt(LEFT_MOST_KEY_OFFSET, offset);
        offset += rowTupleWriter.writeTuple(minKey, buf.array(), offset);
        buf.putInt(RIGHT_MOST_KEY_OFFSET, offset);
        rowTupleWriter.writeTuple(maxKey, buf.array(), offset);

        // Write page information
        int numberOfColumns = columnWriter.getNumberOfColumns();
        buf.putInt(TUPLE_COUNT_OFFSET, numberOfTuples);
        buf.putInt(NUMBER_OF_COLUMNS_OFFSET, numberOfColumns);
        buf.putInt(SIZE_OF_COLUMNS_OFFSETS_OFFSET, columnWriter.getColumnOffsetsSize());
    }

    public AbstractColumnTupleWriter getColumnTupleWriter() {
        return columnTupleWriter;
    }

    void setNextLeaf(int pageId) {
        buf.putInt(NEXT_LEAF_OFFSET, pageId);
    }
}
