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
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ColumnBTreeWriteLeafFrame extends AbstractColumnBTreeLeafFrame {
    private final AbstractColumnTupleWriter columnTupleWriter;

    public ColumnBTreeWriteLeafFrame(ITreeIndexTupleWriter rowTupleWriter,
            AbstractColumnTupleWriter columnTupleWriter) {
        super(rowTupleWriter, columnTupleWriter.getColumnPageZeroWriterFlavorSelector());
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

    void flush(AbstractColumnTupleWriter columnWriter, int numberOfTuples, int zerothSegmentMaxColumns,
            ITupleReference minKey, ITupleReference maxKey, IColumnWriteMultiPageOp multiPageOpRef)
            throws HyracksDataException {
        IColumnPageZeroWriter pageZeroWriter = pageZeroWriterFlavorSelector.getPageZeroWriter(multiPageOpRef,
                zerothSegmentMaxColumns, getBuffer().capacity());
        pageZeroWriter.flush(buf, numberOfTuples, minKey, maxKey, columnWriter, rowTupleWriter);
    }

    public AbstractColumnTupleWriter getColumnTupleWriter() {
        return columnTupleWriter;
    }

    void setNextLeaf(int pageId) {
        buf.putInt(NEXT_LEAF_OFFSET, pageId);
    }

    public void dumpBuffer(ObjectNode bufNode) {
        bufNode.put("tupleCount", buf.getInt(TUPLE_COUNT_OFFSET));
        bufNode.put("level", buf.get(Constants.LEVEL_OFFSET));
        bufNode.put("numberOfColumns", buf.getInt(NUMBER_OF_COLUMNS_OFFSET));
        bufNode.put("leftMostKeyOffset", buf.getInt(LEFT_MOST_KEY_OFFSET));
        bufNode.put("rightMostKeyOffset", buf.getInt(RIGHT_MOST_KEY_OFFSET));
        bufNode.put("sizeOfColumns", buf.getInt(SIZE_OF_COLUMNS_OFFSETS_OFFSET));
        bufNode.put("megaLeafNodeLength", buf.getInt(MEGA_LEAF_NODE_LENGTH));
        bufNode.put("flagOffset", buf.get(FLAG_OFFSET));
        bufNode.put("nextLeafOffset", buf.getInt(NEXT_LEAF_OFFSET));
    }
}
