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
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnReadMultiPageOp;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;
import org.apache.hyracks.storage.common.buffercache.CachedPage;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

import it.unimi.dsi.fastutil.longs.LongSet;

public final class ColumnMultiBufferProvider implements IColumnBufferProvider {
    private final int columnIndex;
    private final IColumnReadMultiPageOp multiPageOp;
    private final Queue<ICachedPage> pages;
    private final LongSet pinnedPages;
    private int numberOfPages;
    private int startPage;
    private int startOffset;
    private int length;

    public ColumnMultiBufferProvider(int columnIndex, IColumnReadMultiPageOp multiPageOp, LongSet pinnedPages) {
        this.columnIndex = columnIndex;
        this.multiPageOp = multiPageOp;
        this.pinnedPages = pinnedPages;
        pages = new ArrayDeque<>();
    }

    @Override
    public void reset(ColumnBTreeReadLeafFrame frame) throws HyracksDataException {
        if (columnIndex >= frame.getNumberOfColumns()) {
            numberOfPages = 0;
            length = 0;
            return;
        }

        int offset = frame.getColumnOffset(columnIndex);
        startPage = frame.getPageId() + getColumnPageIndex(offset);
        startOffset = offset % multiPageOp.getPageSize();
        //Duplicate as the buffer could be shared by more than one column
        ByteBuffer firstPage = readNext().duplicate();
        firstPage.position(startOffset);
        //Read the length
        length = firstPage.getInt();
        int remainingLength = length - firstPage.remaining();
        numberOfPages = (int) Math.ceil((double) remainingLength / multiPageOp.getPageSize());
        //+4-bytes after reading the length
        startOffset += Integer.BYTES;
        //-4-bytes after reading the length
        length -= Integer.BYTES;
    }

    @Override
    public void readAll(Queue<ByteBuffer> buffers) throws HyracksDataException {
        ByteBuffer buffer = pages.peek().getBuffer().duplicate();
        buffer.clear();
        buffer.position(startOffset);
        buffers.add(buffer);
        for (int i = 0; i < numberOfPages; i++) {
            buffer = readNext().duplicate();
            buffer.clear();
            buffers.add(buffer);
        }
        numberOfPages = 0;
    }

    @Override
    public void releaseAll() throws HyracksDataException {
        while (!pages.isEmpty()) {
            ICachedPage page = pages.poll();
            multiPageOp.unpin(page);
        }
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public ByteBuffer getBuffer() {
        throw new UnsupportedOperationException("Use readAll() for multi-buffer");
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    private ByteBuffer readNext() throws HyracksDataException {
        ICachedPage columnPage = multiPageOp.pin(startPage++);
        pages.add(columnPage);
        pinnedPages.add(((CachedPage) columnPage).getDiskPageId());
        return columnPage.getBuffer();
    }

    private int getColumnPageIndex(int columnOffset) {
        return (int) Math.floor((double) columnOffset / multiPageOp.getPageSize());
    }
}
