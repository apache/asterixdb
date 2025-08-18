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

import java.nio.ByteBuffer;
import java.util.Queue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

/**
 * Buffer provider for Primary Keys
 */
public final class ColumnSingleBufferProvider implements IColumnBufferProvider {
    private final int columnIndex;
    private ByteBuffer buffer;

    public ColumnSingleBufferProvider(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public void reset(ColumnBTreeReadLeafFrame frame) throws HyracksDataException {
        int offset = frame.getColumnOffset(columnIndex);
        this.buffer = frame.getBuffer().duplicate();
        buffer.position(offset);
    }

    @Override
    public void readAll(Queue<ByteBuffer> buffers) {
        throw new UnsupportedOperationException("Use getBuffer() for single-buffer");
    }

    @Override
    public void releaseAll() throws HyracksDataException {
        //NoOp
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

}
