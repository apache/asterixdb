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
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;

public abstract class AbstractBytesInputStream extends InputStream {

    public abstract void resetAt(int bytesToSkip, AbstractBytesInputStream stream) throws IOException;

    protected abstract void addBuffer(ByteBuffer buffer);

    public abstract void read(IPointable pointable, int length) throws EOFException;

    @Override
    public abstract int read() throws IOException;

    @Override
    public abstract int read(byte[] bytes, int offset, int length) throws IOException;

    @Override
    public abstract long skip(long n);

    public abstract int read(ByteBuffer out);

    public abstract AbstractBytesInputStream remainingStream() throws EOFException;

    public abstract AbstractBytesInputStream sliceStream(int length) throws EOFException;

    @Override
    public abstract void mark(int readLimit);

    @Override
    public abstract void reset() throws IOException;

    public abstract void reset(IColumnBufferProvider bufferProvider) throws HyracksDataException;

    @Override
    public abstract int available();

    public ByteBuffer getBuffer() {
        throw new UnsupportedOperationException("Getting buffer is not supported");
    }

    public final void skipFully(long n) throws IOException {
        long skipped = skip(n);
        if (skipped < n) {
            throw new EOFException("Not enough bytes to skip: " + skipped + " < " + n);
        }
    }

    @Override
    public final boolean markSupported() {
        return true;
    }
}
