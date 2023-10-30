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
package org.apache.asterix.cloud;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudResettableInputStream extends InputStream {
    private static final Logger LOGGER = LogManager.getLogger();
    // TODO: make configurable
    public static final int MIN_BUFFER_SIZE = 5 * 1024 * 1024;
    private final IWriteBufferProvider bufferProvider;
    private ByteBuffer writeBuffer;

    private final ICloudBufferedWriter bufferedWriter;

    public CloudResettableInputStream(ICloudBufferedWriter bufferedWriter, IWriteBufferProvider bufferProvider) {
        this.bufferedWriter = bufferedWriter;
        this.bufferProvider = bufferProvider;
    }

    private void open() {
        if (writeBuffer == null) {
            writeBuffer = bufferProvider.getBuffer();
            writeBuffer.clear();
        }
    }

    @Override
    public void reset() {
        writeBuffer.reset();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(int readLimit) {
        writeBuffer.mark();
    }

    public void write(ByteBuffer header, ByteBuffer page) throws HyracksDataException {
        write(header);
        write(page);
    }

    public int write(ByteBuffer page) throws HyracksDataException {
        open();
        return write(page.array(), 0, page.limit());
    }

    public void write(int b) throws HyracksDataException {
        if (writeBuffer.remaining() == 0) {
            uploadAndWait();
        }
        writeBuffer.put((byte) b);
    }

    public int write(byte[] b, int off, int len) throws HyracksDataException {
        open();

        // full buffer = upload -> write all
        if (writeBuffer.remaining() == 0) {
            uploadAndWait();
        }

        // write partial -> upload -> write -> upload -> ...
        int offset = off;
        int pageRemaining = len;
        while (pageRemaining > 0) {
            // enough to write all
            if (writeBuffer.remaining() > pageRemaining) {
                writeBuffer.put(b, offset, pageRemaining);
                return len;
            }

            int remaining = writeBuffer.remaining();
            writeBuffer.put(b, offset, remaining);
            pageRemaining -= remaining;
            offset += remaining;
            uploadAndWait();
        }

        return len;
    }

    public void finish() throws HyracksDataException {
        open();
        try {
            if (writeBuffer.position() > 0 || bufferedWriter.isEmpty()) {
                /*
                 * upload if:
                 * (1) the writeBuffer is not empty
                 * OR
                 * (2) nothing was written to the file at all to ensure writing empty file
                 */
                uploadAndWait();
            }
            bufferedWriter.finish();
        } finally {
            returnBuffer();
        }
    }

    public void abort() throws HyracksDataException {
        try {
            bufferedWriter.abort();
        } finally {
            returnBuffer();
        }
    }

    private void uploadAndWait() throws HyracksDataException {
        writeBuffer.flip();
        try {
            bufferedWriter.upload(this, writeBuffer.limit());
        } catch (Exception e) {
            LOGGER.error(e);
            throw HyracksDataException.create(e);
        }

        writeBuffer.clear();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (writeBuffer.remaining() == 0) {
            return -1;
        }

        int length = Math.min(len, writeBuffer.remaining());
        writeBuffer.get(b, off, length);
        return length;
    }

    @Override
    public int read() throws IOException {
        return writeBuffer.get();
    }

    private void returnBuffer() {
        if (writeBuffer != null) {
            bufferProvider.recycle(writeBuffer);
            writeBuffer = null;
        }
    }
}
