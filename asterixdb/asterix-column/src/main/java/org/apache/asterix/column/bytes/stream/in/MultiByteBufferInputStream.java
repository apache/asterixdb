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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;

public final class MultiByteBufferInputStream extends AbstractBytesInputStream {
    private static final ByteBuffer EMPTY;

    static {
        EMPTY = ByteBuffer.allocate(0);
        EMPTY.limit(0);
    }

    private final Queue<ByteBuffer> buffers;
    private final ArrayBackedValueStorage tempPointableStorage;
    private int length;

    private ByteBuffer current;
    private int position;

    public MultiByteBufferInputStream() {
        this.buffers = new ArrayDeque<>();
        tempPointableStorage = new ArrayBackedValueStorage();
        this.current = EMPTY;
        this.position = 0;
        this.length = 0;

    }

    private MultiByteBufferInputStream(MultiByteBufferInputStream original, int len) throws EOFException {
        buffers = new ArrayDeque<>();
        tempPointableStorage = new ArrayBackedValueStorage();
        position = original.position;
        length = original.length;
        buffers.addAll(original.sliceBuffers(len));
        nextBuffer();
    }

    @Override
    public void reset(IColumnBufferProvider bufferProvider) throws HyracksDataException {
        reset();
        length = bufferProvider.getLength();
        if (length > 0) {
            bufferProvider.readAll(buffers);
            nextBuffer();
        }
    }

    @Override
    protected void addBuffer(ByteBuffer buffer) {
        buffers.add(buffer);
        length += buffer.remaining();
    }

    @Override
    public void resetAt(int bytesToSkip, AbstractBytesInputStream stream) throws IOException {
        MultiByteBufferInputStream original = (MultiByteBufferInputStream) stream;
        buffers.clear();
        position = original.position;
        length = original.length;
        current = original.current.duplicate();
        for (ByteBuffer buffer : original.buffers) {
            buffers.add(buffer.duplicate());
        }

        if (skip(bytesToSkip) != bytesToSkip) {
            throw new EOFException();
        }
    }

    @Override
    public long skip(long n) {
        if (n <= 0) {
            return 0;
        }

        if (current == null) {
            return -1;
        }

        long bytesSkipped = 0;
        while (bytesSkipped < n) {
            if (current.remaining() > 0) {
                long bytesToSkip = Math.min(n - bytesSkipped, current.remaining());
                current.position(current.position() + (int) bytesToSkip);
                bytesSkipped += bytesToSkip;
                this.position += bytesToSkip;
            } else if (!nextBuffer()) {
                // there are no more buffers
                return bytesSkipped > 0 ? bytesSkipped : -1;
            }
        }

        return bytesSkipped;
    }

    @Override
    public int read(ByteBuffer out) {
        int len = out.remaining();
        if (len <= 0) {
            return 0;
        }

        if (current == null) {
            return -1;
        }

        int bytesCopied = 0;
        while (bytesCopied < len) {
            if (current.remaining() > 0) {
                int bytesToCopy;
                ByteBuffer copyBuffer;
                if (current.remaining() <= out.remaining()) {
                    // copy all the current buffer
                    bytesToCopy = current.remaining();
                    copyBuffer = current;
                } else {
                    // copy a slice of the current buffer
                    bytesToCopy = out.remaining();
                    copyBuffer = current.duplicate();
                    copyBuffer.limit(copyBuffer.position() + bytesToCopy);
                    current.position(copyBuffer.position() + bytesToCopy);
                }

                out.put(copyBuffer);
                bytesCopied += bytesToCopy;
                this.position += bytesToCopy;

            } else if (!nextBuffer()) {
                // there are no more buffers
                return bytesCopied > 0 ? bytesCopied : -1;
            }
        }

        return bytesCopied;
    }

    @Override
    public AbstractBytesInputStream sliceStream(int length) throws EOFException {
        return new MultiByteBufferInputStream(this, length);
    }

    @Override
    public AbstractBytesInputStream remainingStream() throws EOFException {
        return new MultiByteBufferInputStream(this, length - position);
    }

    @Override
    public int read(byte[] bytes, int off, int len) {
        if (len <= 0) {
            if (len < 0) {
                throw new IndexOutOfBoundsException("Read length must be greater than 0: " + len);
            }
            return 0;
        }

        if (current == null) {
            return -1;
        }

        int bytesRead = 0;
        while (bytesRead < len) {
            if (current.remaining() > 0) {
                int bytesToRead = Math.min(len - bytesRead, current.remaining());
                current.get(bytes, off + bytesRead, bytesToRead);
                bytesRead += bytesToRead;
                this.position += bytesToRead;
            } else if (!nextBuffer()) {
                // there are no more buffers
                return bytesRead > 0 ? bytesRead : -1;
            }
        }

        return bytesRead;
    }

    @Override
    public int read(byte[] bytes) {
        return read(bytes, 0, bytes.length);
    }

    @Override
    public int read() throws IOException {
        if (current == null) {
            throw new EOFException();
        }

        while (true) {
            if (current.remaining() > 0) {
                this.position += 1;
                return current.get() & 0xFF; // as unsigned
            } else if (!nextBuffer()) {
                // there are no more buffers
                throw new EOFException();
            }
        }
    }

    @Override
    public void read(IPointable pointable, int length) throws EOFException {
        if (current.remaining() >= length) {
            pointable.set(current.array(), current.position(), length);
            current.position(current.position() + length);
            position += length;
        } else {
            tempPointableStorage.setSize(length);
            //Read first half part from the current buffer
            int bytesRead = read(tempPointableStorage.getByteArray(), 0, length);
            if (bytesRead != length) {
                throw new EOFException();
            }
            pointable.set(tempPointableStorage);
        }
    }

    @Override
    public int available() {
        return length - position;
    }

    @Override
    public void mark(int readLimit) {
        throw new UnsupportedOperationException("reset() is not supported");
    }

    @Override
    public void reset() {
        buffers.clear();
        this.current = EMPTY;
        this.position = 0;
        this.length = 0;
    }

    private List<ByteBuffer> sliceBuffers(long length) throws EOFException {
        if (length <= 0) {
            return Collections.emptyList();
        }

        if (current == null) {
            throw new EOFException();
        }

        List<ByteBuffer> sliceBuffers = new ArrayList<>();
        long bytesAccumulated = 0;
        while (bytesAccumulated < length) {
            if (current.remaining() > 0) {
                // get a slice of the current buffer to return
                // always fits in an int because remaining returns an int that is >= 0
                int bufLen = (int) Math.min(length - bytesAccumulated, current.remaining());
                ByteBuffer slice = current.duplicate();
                slice.limit(slice.position() + bufLen);
                sliceBuffers.add(slice);
                bytesAccumulated += bufLen;

                // update state; the bytes are considered read
                current.position(current.position() + bufLen);
                this.position += bufLen;
            } else if (!nextBuffer()) {
                // there are no more buffers
                throw new EOFException();
            }
        }

        return sliceBuffers;
    }

    private boolean nextBuffer() {
        if (buffers.isEmpty()) {
            return false;
        }
        current = buffers.poll();
        return true;
    }
}
