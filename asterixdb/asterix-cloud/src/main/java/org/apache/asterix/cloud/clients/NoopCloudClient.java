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
package org.apache.asterix.cloud.clients;

import java.io.FilenameFilter;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.asterix.cloud.clients.profiler.NoOpRequestProfilerLimiter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class NoopCloudClient implements ICloudClient {

    public static final ICloudClient INSTANCE = new NoopCloudClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private NoopCloudClient() {
    }

    @Override
    public void close() throws HyracksDataException {
        // no-op
    }

    @Override
    public int getWriteBufferSize() {
        return 0;
    }

    @Override
    public IRequestProfilerLimiter getProfilerLimiter() {
        return NoOpRequestProfilerLimiter.INSTANCE;
    }

    @Override
    public ICloudWriter createWriter(String bucket, String path, IWriteBufferProvider bufferProvider) {
        return new NoOpCloudWriter();
    }

    @Override
    public Set<CloudFile> listObjects(String bucket, String path, FilenameFilter filter) {
        return Set.of();
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
        return 0;
    }

    @Override
    public byte[] readAllBytes(String bucket, String path) throws HyracksDataException {
        return new byte[0];
    }

    @Override
    public InputStream getObjectStream(String bucket, String path, long offset, long length) {
        return InputStream.nullInputStream();
    }

    @Override
    public void write(String bucket, String path, byte[] data) {
    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) {
    }

    @Override
    public void deleteObjects(String bucket, Collection<String> paths) throws HyracksDataException {
    }

    @Override
    public long getObjectSize(String bucket, String path) throws HyracksDataException {
        return 0;
    }

    @Override
    public boolean exists(String bucket, String path) throws HyracksDataException {
        return false;
    }

    @Override
    public boolean isEmptyPrefix(String bucket, String path) throws HyracksDataException {
        return false;
    }

    @Override
    public IParallelDownloader createParallelDownloader(String bucket, IOManager ioManager)
            throws HyracksDataException {
        return NoOpParallelDownloader.INSTANCE;
    }

    @Override
    public Predicate<Exception> getObjectNotFoundExceptionPredicate() {
        return e -> true;
    }

    @Override
    public JsonNode listAsJson(ObjectMapper objectMapper, String bucket) {
        return OBJECT_MAPPER.createArrayNode();
    }

    private static class NoOpCloudWriter implements ICloudWriter {

        long position = 0L;

        public NoOpCloudWriter() {
        }

        @Override
        public void abort() throws HyracksDataException {
            position = 0L;
        }

        @Override
        public int write(ByteBuffer page) throws HyracksDataException {
            int written = page.remaining();
            position += written;
            page.position(page.limit());
            return written;
        }

        @Override
        public void write(int b) throws HyracksDataException {
            position++;
        }

        @Override
        public int write(byte[] b, int off, int len) throws HyracksDataException {
            position += len;
            return len;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void finish() throws HyracksDataException {
            position = 0;
        }
    }

    private static class NoOpParallelDownloader implements IParallelDownloader {
        private static final NoOpParallelDownloader INSTANCE = new NoOpParallelDownloader();

        @Override
        public void close() throws HyracksDataException {
        }

        @Override
        public void downloadFiles(Collection<FileReference> toDownload) throws HyracksDataException {
        }

        @Override
        public Collection<FileReference> downloadDirectories(Collection<FileReference> toDownload)
                throws HyracksDataException {
            return List.of();
        }
    }
}
