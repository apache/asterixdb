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
package org.apache.asterix.cloud.clients.google.gcs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

public class GCSWriter implements ICloudWriter {
    private static final Logger LOGGER = LogManager.getLogger();
    private final String bucket;
    private final String path;
    private final IRequestProfilerLimiter profiler;
    private final Storage gcsClient;
    private final ICloudGuardian guardian;
    private final int writeBufferSize;

    private WriteChannel writer = null;
    private long writtenBytes;

    public GCSWriter(String bucket, String path, Storage gcsClient, IRequestProfilerLimiter profiler,
            ICloudGuardian guardian, int writeBufferSize) {
        this.bucket = bucket;
        this.path = path;
        this.profiler = profiler;
        this.gcsClient = gcsClient;
        this.guardian = guardian;
        this.writeBufferSize = writeBufferSize;
        writtenBytes = 0;
    }

    @Override
    public int write(ByteBuffer page) throws HyracksDataException {
        guardian.checkIsolatedWriteAccess(bucket, path);
        // The GCS library triggers a new upload when its internal buffer is full, not on each call to writer.write().
        // uploadsToBeTriggered estimates upload count, and we acquire matching tokens from the limiter.
        long uploadsToBeTriggered =
                ((writtenBytes + page.remaining()) / writeBufferSize) - (writtenBytes / writeBufferSize);
        while (uploadsToBeTriggered-- > 0) {
            profiler.objectMultipartUpload();
        }

        int written = 0;
        try {
            setUploadId();
            while (page.hasRemaining()) {
                written += writer.write(page);
            }
        } catch (IOException | RuntimeException e) {
            throw HyracksDataException.create(ErrorCode.FAILED_IO_OPERATION, e);
        }

        writtenBytes += written;
        return written;
    }

    @Override
    public int write(byte[] b, int off, int len) throws HyracksDataException {
        return write(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public long position() {
        return writtenBytes;
    }

    @Override
    public void write(int b) throws HyracksDataException {
        write(ByteBuffer.wrap(new byte[] { (byte) b }));
    }

    @Override
    public void finish() throws HyracksDataException {
        guardian.checkWriteAccess(bucket, path);
        profiler.objectMultipartUpload();
        try {
            setUploadId();

            writer.close();
            writer = null;
        } catch (IOException | RuntimeException e) {
            throw HyracksDataException.create(ErrorCode.FAILED_IO_OPERATION, e);
        }
        log("FINISHED");
    }

    @Override
    public void abort() {
        // https://github.com/googleapis/java-storage/issues/202
        // Cannot abort. Upload Ids and data are discarded after a week
        writer = null;
        LOGGER.warn("Multipart upload for {} was aborted", path);
    }

    private void setUploadId() {
        if (writer == null) {
            writer = gcsClient.writer(BlobInfo.newBuilder(BlobId.of(bucket, path)).build());
            writer.setChunkSize(writeBufferSize);
            writtenBytes = 0;
            log("STARTED");
        }
    }

    private void log(String op) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} multipart upload for {}", op, path);
        }
    }
}
