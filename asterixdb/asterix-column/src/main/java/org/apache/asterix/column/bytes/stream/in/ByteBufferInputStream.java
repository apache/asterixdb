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
package org.apache.asterix.column.bytes.stream.in;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;

public final class ByteBufferInputStream extends AbstractBytesInputStream {
    private ByteBuffer buffer;
    private int mark = -1;

    @Override
    public void reset(IColumnBufferProvider bufferProvider) {
        addBuffer(bufferProvider.getBuffer());
    }

    @Override
    protected void addBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
        mark = -1;
    }

    @Override
    public void resetAt(int bytesToSkip, AbstractBytesInputStream stream) throws IOException {
        ByteBufferInputStream in = (ByteBufferInputStream) stream;
        buffer = in.buffer.duplicate();
        buffer.position(buffer.position() + bytesToSkip);
        mark = -1;
    }

    @Override
    public void read(IPointable pointable, int length) throws EOFException {
        if (buffer.remaining() < length) {
            throw new EOFException();
        }

        pointable.set(buffer.array(), buffer.position(), length);
        buffer.position(buffer.position() + length);
    }

    @Override
    public int read() throws IOException {
        if (!buffer.hasRemaining()) {
            throw new EOFException();
        }
        return buffer.get() & 0xFF; // as unsigned
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        if (length == 0) {
            return 0;
        }

        int remaining = buffer.remaining();
        if (remaining <= 0) {
            return -1;
        }

        int bytesToRead = Math.min(remaining, length);
        buffer.get(bytes, offset, bytesToRead);

        return bytesToRead;
    }

    @Override
    public long skip(long n) {
        if (n == 0) {
            return 0;
        }

        if (!buffer.hasRemaining()) {
            return -1;
        }

        // buffer.remaining is an int, so this will always fit in an int
        int bytesToSkip = (int) Math.min(buffer.remaining(), n);
        buffer.position(buffer.position() + bytesToSkip);

        return bytesToSkip;
    }

    @Override
    public int read(ByteBuffer out) {
        int bytesToCopy;
        ByteBuffer copyBuffer;
        if (buffer.remaining() <= out.remaining()) {
            // copy the entire buffer
            bytesToCopy = buffer.remaining();
            copyBuffer = buffer;
        } else {
            // copy a slice of the current buffer
            bytesToCopy = out.remaining();
            copyBuffer = buffer.duplicate();
            copyBuffer.position(buffer.position());
            copyBuffer.limit(buffer.position() + bytesToCopy);
            buffer.position(buffer.position() + bytesToCopy);
        }

        out.put(copyBuffer);
        out.flip();

        return bytesToCopy;
    }

    @Override
    public AbstractBytesInputStream sliceStream(int length) throws EOFException {
        if (buffer.remaining() < length) {
            throw new EOFException();
        }
        ByteBuffer copy = buffer.duplicate();
        copy.position(buffer.position());
        copy.limit(buffer.position() + length);
        ByteBufferInputStream in = new ByteBufferInputStream();
        in.addBuffer(copy);
        buffer.position(buffer.position() + length);
        return in;
    }

    @Override
    public AbstractBytesInputStream remainingStream() {
        ByteBuffer remaining = buffer.duplicate();
        remaining.position(buffer.position());
        buffer.position(buffer.limit());
        ByteBufferInputStream in = new ByteBufferInputStream();
        in.addBuffer(remaining);
        return in;
    }

    @Override
    public void mark(int readlimit) {
        this.mark = buffer.position();
    }

    @Override
    public void reset() throws IOException {
        if (mark >= 0) {
            buffer.position(mark);
            this.mark = -1;
        } else {
            throw new IOException("No mark defined");
        }
    }

    @Override
    public int available() {
        return buffer.remaining();
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }
}
