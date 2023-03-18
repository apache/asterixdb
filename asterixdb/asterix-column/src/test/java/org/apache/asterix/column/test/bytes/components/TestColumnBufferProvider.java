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
package org.apache.asterix.column.test.bytes.components;

import static org.apache.asterix.column.test.bytes.AbstractBytesTest.PAGE_SIZE;
import static org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.AbstractColumnBTreeLeafFrame.HEADER_SIZE;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.asterix.column.common.buffer.DummyBufferCache;
import org.apache.asterix.column.common.buffer.DummyPage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTreeReadLeafFrame;

public class TestColumnBufferProvider implements IColumnBufferProvider {
    private final int fileId;
    private final int columnIndex;
    private final DummyBufferCache dummyBufferCache;
    private final Queue<DummyPage> pages;
    private int numberOfPages;
    private int startPage;
    private int startOffset;
    private int length;

    public TestColumnBufferProvider(int fileId, int columnIndex, DummyBufferCache dummyBufferCache) {
        this.fileId = fileId;
        this.dummyBufferCache = dummyBufferCache;
        this.columnIndex = columnIndex;
        pages = new ArrayDeque<>();
    }

    @Override
    public void reset(ColumnBTreeReadLeafFrame frame) throws HyracksDataException {
        //NoOp
    }

    public void reset(DummyPage pageZero) throws HyracksDataException {
        pages.clear();
        int offset = getColumnOffset(pageZero.getBuffer());
        startPage = pageZero.getPageId() + getColumnPageIndex(offset);
        startOffset = offset % PAGE_SIZE;
        ByteBuffer firstPage = readNext();
        firstPage.position(startOffset);
        //Read the length
        length = firstPage.getInt();
        //+1 for the first page
        numberOfPages = 1 + (int) Math.ceil((length - firstPage.remaining()) / (double) PAGE_SIZE);
        startOffset += Integer.BYTES;
        length -= Integer.BYTES;
    }

    @Override
    public void releaseAll() throws HyracksDataException {
        throw new IllegalAccessError("do not call");
    }

    @Override
    public void readAll(Queue<ByteBuffer> buffers) throws HyracksDataException {
        ByteBuffer buffer = pages.peek().getBuffer().duplicate();
        buffer.clear();
        buffer.position(startOffset);
        buffers.add(buffer);
        for (int i = 0; i < numberOfPages - 1; i++) {
            buffer = readNext().duplicate();
            buffer.clear();
            buffers.add(buffer);
        }
        numberOfPages = 0;
    }

    @Override
    public ByteBuffer getBuffer() {
        return null;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int getColumnIndex() {
        return columnIndex;
    }

    private ByteBuffer readNext() {
        DummyPage columnPage = dummyBufferCache.getBuffer(fileId, startPage++);
        pages.add(columnPage);
        return columnPage.getBuffer();
    }

    private int getColumnOffset(ByteBuffer pageZero) {
        return pageZero.getInt(HEADER_SIZE + Integer.BYTES * columnIndex);
    }

    private int getColumnPageIndex(int columnOffset) {
        return (int) Math.floor((double) columnOffset / PAGE_SIZE);
    }
}
