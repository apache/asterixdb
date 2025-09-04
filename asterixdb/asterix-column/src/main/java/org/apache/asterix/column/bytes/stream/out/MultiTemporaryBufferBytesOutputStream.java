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
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

public final class MultiTemporaryBufferBytesOutputStream extends AbstractMultiBufferBytesOutputStream {
    private boolean isFirstBufferFromColumnPool = false;

    public MultiTemporaryBufferBytesOutputStream(Mutable<IColumnWriteMultiPageOp> multiPageOpRef) {
        super(multiPageOpRef);
    }

    // Don't initialize for temporary buffers, as they are confiscated from the pool
    @Override
    public void reset() throws HyracksDataException {
        preReset();
        position = 0;
        currentBufferIndex = 0;
        releaseColumnBuffer();
    }

    @Override
    protected void preReset() {
        //NoOp
    }

    @Override
    protected ByteBuffer confiscateNewBuffer() throws HyracksDataException {
        // When a buffer reset occurs, the 0th buffer is set to null.
        // If buffers are not empty, this means a buffer from the column pool was previously used.
        // If the 0th buffer is null, a new buffer must be confiscated.
        if (buffers.isEmpty() || buffers.get(0) == null) {
            /*
             * One buffer from the pool to avoid confiscating a whole page for a tiny stream.
             * This protects pressuring the buffer cache from confiscating pages for small columns. Think sparse
             * columns, which may take only a few hundreds of bytes to write.
             */
            isFirstBufferFromColumnPool = true;
            return multiPageOpRef.getValue().confiscateTemporary0thBuffer();
        }
        return multiPageOpRef.getValue().confiscateTemporary();
    }

    @Override
    protected void ensureCapacity(int length) throws HyracksDataException {
        ensure0thBufferAllocated();
        super.ensureCapacity(length);
    }

    private void ensure0thBufferAllocated() throws HyracksDataException {
        if ((currentBufferIndex == 0 && !isFirstBufferFromColumnPool)) {
            // grab one from pool, as the buffers has already been reserved.
            currentBuf = confiscateNewBuffer();
            boolean isBufferEmpty = buffers.isEmpty();
            currentBuf.clear();
            if (buffers.isEmpty()) {
                buffers.add(currentBuf);
            } else {
                buffers.set(0, currentBuf);
            }
            if (isBufferEmpty) {
                allocatedBytes += currentBuf.capacity();
            }
        }
    }

    @Override
    protected void releaseColumnBuffer() {
        if (!isFirstBufferFromColumnPool) {
            return;
        }
        multiPageOpRef.getValue().releaseTemporary0thBuffer(buffers.get(0));
        isFirstBufferFromColumnPool = false;
        buffers.set(0, null);
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        int writtenSize = 0;
        int numberOfUsedBuffers = currentBufferIndex + 1;
        // This can be the case where the empty columns are being written
        ensure0thBufferAllocated();
        for (int i = 0; i < numberOfUsedBuffers; i++) {
            ByteBuffer buffer = buffers.get(i);
            outputStream.write(buffer.array(), 0, buffer.position());
            writtenSize += buffer.position();
        }

        if (writtenSize != position) {
            // Sanity check
            StringBuilder builder = new StringBuilder();
            builder.append('[');
            for (int i = 0; i < numberOfUsedBuffers; i++) {
                ByteBuffer buffer = buffers.get(i);
                builder.append("{Buffer index: ").append(i);
                builder.append(" Position: ").append(buffer.position());
                builder.append(" Limit: ").append(buffer.limit());
                builder.append(" Capacity: ").append(buffer.capacity());
                builder.append("}, ");
            }
            builder.setLength(builder.length() - 2);
            builder.append(']');
            throw new IllegalStateException("Size is different (written: " + writtenSize + ", position: " + position
                    + ", allocatedBytes: " + allocatedBytes + ", currentBufferIndex: " + currentBufferIndex
                    + ", buffers: " + builder + ")");
        }
    }
}
