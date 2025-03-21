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
import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.io.request.ICloudBeforeRetryRequest;
import org.apache.hyracks.cloud.io.request.ICloudRequest;
import org.apache.hyracks.cloud.util.CloudRetryableRequestUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudResettableInputStream extends InputStream implements ICloudWriter {
    private static final Logger LOGGER = LogManager.getLogger();
    private final IWriteBufferProvider bufferProvider;
    private final ICloudBufferedWriter bufferedWriter;
    private ByteBuffer writeBuffer;
    private long writtenBytes;

    public CloudResettableInputStream(ICloudBufferedWriter bufferedWriter, IWriteBufferProvider bufferProvider) {
        this.bufferedWriter = bufferedWriter;
        this.bufferProvider = bufferProvider;
        writtenBytes = 0;
    }

    /* ************************************************************
     * InputStream methods
     * ************************************************************
     */

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

    /* ************************************************************
     * ICloudWriter methods
     * ************************************************************
     */

    @Override
    public int write(ByteBuffer header, ByteBuffer page) throws HyracksDataException {
        return write(header) + write(page);
    }

    @Override
    public int write(ByteBuffer page) throws HyracksDataException {
        open();
        return write(page.array(), page.position(), page.remaining());
    }

    @Override
    public void write(int b) throws HyracksDataException {
        if (writeBuffer.remaining() == 0) {
            uploadAndWait();
        }
        writeBuffer.put((byte) b);
        writtenBytes += 1;
    }

    @Override
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
                break;
            }

            int remaining = writeBuffer.remaining();
            writeBuffer.put(b, offset, remaining);
            pageRemaining -= remaining;
            offset += remaining;
            uploadAndWait();
        }

        writtenBytes += len;
        return len;
    }

    @Override
    public long position() {
        return writtenBytes;
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

    @Override
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
                writeBuffer.flip();
                try {
                    ICloudRequest request = () -> bufferedWriter.uploadLast(this, writeBuffer);
                    ICloudBeforeRetryRequest retry = () -> writeBuffer.position(0);
                    CloudRetryableRequestUtil.runWithNoRetryOnInterruption(request, retry);
                } catch (Exception e) {
                    LOGGER.error(e);
                    throw HyracksDataException.create(ErrorCode.FAILED_IO_OPERATION, e);
                }
            }
            bufferedWriter.finish();
        } finally {
            returnBuffer();
        }
        doClose();
    }

    @Override
    public void abort() throws HyracksDataException {
        try {
            bufferedWriter.abort();
        } finally {
            returnBuffer();
        }
        doClose();
    }

    private void open() {
        if (writeBuffer == null) {
            writeBuffer = bufferProvider.getBuffer();
            writeBuffer.clear();
            writtenBytes = 0;
        }
    }

    private void doClose() throws HyracksDataException {
        try {
            close();
        } catch (IOException e) {
            throw HyracksDataException.create(ErrorCode.FAILED_IO_OPERATION, e);
        }
    }

    private void uploadAndWait() throws HyracksDataException {
        writeBuffer.flip();
        try {
            ICloudRequest request = () -> bufferedWriter.upload(this, writeBuffer.limit());
            ICloudBeforeRetryRequest retry = () -> writeBuffer.position(0);
            // This will be interrupted and the interruption will be followed by a halt
            CloudRetryableRequestUtil.runWithNoRetryOnInterruption(request, retry);
        } catch (Exception e) {
            LOGGER.error(e);
            throw HyracksDataException.create(ErrorCode.FAILED_IO_OPERATION, e);
        }

        writeBuffer.clear();
    }

    private void returnBuffer() {
        if (writeBuffer != null) {
            bufferProvider.recycle(writeBuffer);
            writeBuffer = null;
        }
    }
}
