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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnBufferProvider;

public class DummyBytesInputStream extends AbstractBytesInputStream {
    public static final AbstractBytesInputStream INSTANCE = new DummyBytesInputStream();

    private DummyBytesInputStream() {
    }

    @Override
    public void reset(IColumnBufferProvider bufferProvider) throws HyracksDataException {

    }

    @Override
    public void resetAt(int bytesToSkip, AbstractBytesInputStream stream) throws IOException {

    }

    @Override
    protected void addBuffer(ByteBuffer buffer) {

    }

    @Override
    public void read(IPointable pointable, int length) throws EOFException {

    }

    @Override
    public int read() throws IOException {
        return 0;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        return 0;
    }

    @Override
    public long skip(long n) {
        return 0;
    }

    @Override
    public int read(ByteBuffer out) {
        return 0;
    }

    @Override
    public AbstractBytesInputStream remainingStream() throws EOFException {
        return null;
    }

    @Override
    public AbstractBytesInputStream sliceStream(int length) throws EOFException {
        return null;
    }

    @Override
    public void mark(int readLimit) {

    }

    @Override
    public void reset() throws IOException {

    }

    @Override
    public int available() {
        return 0;
    }
}
