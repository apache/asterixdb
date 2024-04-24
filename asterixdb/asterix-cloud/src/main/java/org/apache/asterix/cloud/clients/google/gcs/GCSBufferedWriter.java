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

import static org.apache.asterix.cloud.CloudResettableInputStream.MIN_BUFFER_SIZE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.profiler.IRequestProfiler;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

public class GCSBufferedWriter implements ICloudBufferedWriter {
    private static final Logger LOGGER = LogManager.getLogger();
    private final String bucket;
    private final String path;
    private final IRequestProfiler profiler;
    private final Storage gcsClient;
    private boolean uploadStarted = false;
    private int partNumber;
    private WriteChannel writer = null;

    public GCSBufferedWriter(String bucket, String path, Storage gcsClient, IRequestProfiler profiler) {
        this.bucket = bucket;
        this.path = path;
        this.profiler = profiler;
        this.gcsClient = gcsClient;
    }

    @Override
    public int upload(InputStream stream, int length) throws HyracksDataException {
        profiler.objectMultipartUpload();
        setUploadId();
        try {
            ByteBuffer buffer = ByteBuffer.wrap(stream.readNBytes(length));
            while (buffer.hasRemaining()) {
                writer.write(buffer);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return partNumber++;
    }

    @Override
    public boolean isEmpty() {
        return !uploadStarted;
    }

    @Override
    public void finish() throws HyracksDataException {
        if (!uploadStarted) {
            throw new IllegalStateException("Cannot finish without writing any bytes");
        }
        profiler.objectMultipartUpload();
        try {
            writer.close();
            writer = null;
            uploadStarted = false;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
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
        if (!uploadStarted) {
            uploadStarted = true;
            partNumber = 1;
            writer = gcsClient.writer(BlobInfo.newBuilder(BlobId.of(bucket, path)).build());
            writer.setChunkSize(MIN_BUFFER_SIZE);
            log("STARTED");
        }
    }

    private void log(String op) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{} multipart upload for {}", op, path);
        }
    }
}
