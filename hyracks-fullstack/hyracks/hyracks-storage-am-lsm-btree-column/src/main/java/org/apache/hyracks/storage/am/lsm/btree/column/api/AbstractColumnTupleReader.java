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
package org.apache.hyracks.storage.am.lsm.btree.column.api;

import java.nio.ByteBuffer;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

/**
 * Provided for columnar read tuple reference
 */
public abstract class AbstractColumnTupleReader extends AbstractTupleWriterDisabledMethods {
    public abstract IColumnTupleIterator createTupleIterator(ColumnBTreeReadLeafFrame frame, int componentIndex,
            IColumnReadMultiPageOp multiPageOp);

    /**
     * Currently fixed to 4-byte per offset
     *
     * @param buf         buffer of Page0
     * @param columnIndex column index
     * @return column offset
     * @see AbstractColumnTupleWriter#getColumnOffsetsSize()
     */
    public final int getColumnOffset(ByteBuffer buf, int columnIndex) {
        return buf.getInt(AbstractColumnBTreeLeafFrame.HEADER_SIZE + columnIndex * Integer.BYTES);
    }

    @Override
    public final int bytesRequired(ITupleReference tuple) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION_MSG);
    }
}
