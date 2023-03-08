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
package org.apache.asterix.column.bytes.stream.out;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.column.bytes.stream.out.pointer.ByteBufferReservedPointer;
import org.apache.asterix.column.bytes.stream.out.pointer.IReservedPointer;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

abstract class AbstractMultiBufferBytesOutputStream extends AbstractBytesOutputStream {
    protected final Mutable<IColumnWriteMultiPageOp> multiPageOpRef;
    protected final List<ByteBuffer> buffers;
    protected int currentBufferIndex;
    protected int allocatedBytes;
    protected int position;
    protected ByteBuffer currentBuf;

    AbstractMultiBufferBytesOutputStream(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        this.multiPageOpRef = multiPageOpRef;
        buffers = new ArrayList<>();
    }

    protected abstract ByteBuffer confiscateNewBuffer() throws HyracksDataException;

    protected abstract void preReset() throws HyracksDataException;

    @Override
    public final void reset() throws HyracksDataException {
        preReset();
        position = 0;
        currentBufferIndex = 0;
        if (allocatedBytes == 0) {
            allocateBuffer();
        }
        currentBufferIndex = 0;
        currentBuf = buffers.get(0);
        currentBuf.clear();
    }

    @Override
    public final void write(int b) throws IOException {
        ensureCapacity(1);
        currentBuf.put((byte) b);
        position++;
    }

    @Override
    public final void write(byte[] b, int off, int len) throws IOException {
        ensureCapacity(len);
        int remaining = len;
        int offset = off;
        while (remaining > 0) {
            setNextBufferIfNeeded();
            int writeLength = Math.min(remaining, currentBuf.remaining());
            currentBuf.put(b, offset, writeLength);
            position += writeLength;
            offset += writeLength;
            remaining -= writeLength;
        }
    }

    @Override
    public void reserveByte(IReservedPointer pointer) throws IOException {
        ensureCapacity(Byte.BYTES);
        int offset = getCurrentBufferPosition();
        currentBuf.put((byte) 0);
        position += 1;
        ((ByteBufferReservedPointer) pointer).setPointer(currentBuf, offset);
    }

    @Override
    public final void reserveInteger(IReservedPointer pointer) throws HyracksDataException {
        ensureCapacity(Integer.BYTES);
        int offset = getCurrentBufferPosition();
        currentBuf.putInt(0);
        position += Integer.BYTES;
        ((ByteBufferReservedPointer) pointer).setPointer(currentBuf, offset);
    }

    @Override
    public final IReservedPointer createPointer() {
        return new ByteBufferReservedPointer();
    }

    public final int getCurrentBufferPosition() {
        return currentBuf.position();
    }

    @Override
    public final int size() {
        return position;
    }

    @Override
    public final int capacity() {
        return allocatedBytes;
    }

    @Override
    public final void finish() {
        currentBuf = null;
        buffers.clear();
        allocatedBytes = 0;
    }

    /* *************************************************
     * Helper methods
     * *************************************************
     */

    private void ensureCapacity(int length) throws HyracksDataException {
        if (position + length > allocatedBytes) {
            allocateMoreBuffers(length);
        } else if (length > 0) {
            setNextBufferIfNeeded();
        }
    }

    private void allocateMoreBuffers(int length) throws HyracksDataException {
        int neededSpace = length - currentBuf.remaining();
        while (neededSpace > 0) {
            neededSpace -= allocateBuffer();
        }
        setNextBufferIfNeeded();
    }

    private void setNextBufferIfNeeded() {
        if (currentBuf.remaining() == 0) {
            currentBuf = buffers.get(++currentBufferIndex);
            currentBuf.clear();
        }
    }

    private int allocateBuffer() throws HyracksDataException {
        ByteBuffer buffer = confiscateNewBuffer();
        buffers.add(buffer);
        buffer.clear();
        int size = buffer.capacity();
        allocatedBytes += size;
        return size;
    }
}
