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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.asterix.cloud.CloudResettableInputStream;
import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.clients.aws.s3.S3CloudClient;
import org.apache.asterix.cloud.clients.google.gcs.GCSCloudClient;
import org.apache.asterix.cloud.clients.google.gcs.GCSWriter;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UnstableCloudClient implements ICloudClient {
    // 10% error rate
    public static final AtomicReference<Double> ERROR_RATE = new AtomicReference<>(0.11d);
    private static final Random RANDOM = new Random(0);
    private final ICloudClient cloudClient;

    public UnstableCloudClient(ICloudClient cloudClient) {
        this.cloudClient = cloudClient;
    }

    @Override
    public int getWriteBufferSize() {
        return cloudClient.getWriteBufferSize();
    }

    @Override
    public IRequestProfilerLimiter getProfilerLimiter() {
        return cloudClient.getProfilerLimiter();
    }

    @Override
    public ICloudWriter createWriter(String bucket, String path, IWriteBufferProvider bufferProvider) {
        if (cloudClient instanceof S3CloudClient) {
            return createUnstableWriter((S3CloudClient) cloudClient, bucket, path, bufferProvider);
        } else if (cloudClient instanceof GCSCloudClient) {
            return new UnstableGCSCloudWriter(cloudClient.createWriter(bucket, path, bufferProvider),
                    cloudClient.getWriteBufferSize());
        }
        return cloudClient.createWriter(bucket, path, bufferProvider);
    }

    @Override
    public Set<CloudFile> listObjects(String bucket, String path, FilenameFilter filter) throws HyracksDataException {
        return cloudClient.listObjects(bucket, path, filter);
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
        fail();
        return cloudClient.read(bucket, path, offset, buffer);
    }

    @Override
    public byte[] readAllBytes(String bucket, String path) throws HyracksDataException {
        fail();
        return cloudClient.readAllBytes(bucket, path);
    }

    @Override
    public InputStream getObjectStream(String bucket, String path, long offset, long length) {
        return cloudClient.getObjectStream(bucket, path, offset, length);
    }

    @Override
    public void write(String bucket, String path, byte[] data) throws HyracksDataException {
        cloudClient.write(bucket, path, data);
    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) throws HyracksDataException {
        cloudClient.copy(bucket, srcPath, destPath);
    }

    @Override
    public void deleteObjects(String bucket, Collection<String> paths) throws HyracksDataException {
        cloudClient.deleteObjects(bucket, paths);
    }

    @Override
    public long getObjectSize(String bucket, String path) throws HyracksDataException {
        fail();
        return cloudClient.getObjectSize(bucket, path);
    }

    @Override
    public boolean exists(String bucket, String path) throws HyracksDataException {
        fail();
        return cloudClient.exists(bucket, path);
    }

    @Override
    public boolean isEmptyPrefix(String bucket, String path) throws HyracksDataException {
        fail();
        return cloudClient.isEmptyPrefix(bucket, path);
    }

    @Override
    public IParallelDownloader createParallelDownloader(String bucket, IOManager ioManager)
            throws HyracksDataException {
        return cloudClient.createParallelDownloader(bucket, ioManager);
    }

    @Override
    public JsonNode listAsJson(ObjectMapper objectMapper, String bucket) throws HyracksDataException {
        return cloudClient.listAsJson(objectMapper, bucket);
    }

    @Override
    public void close() throws HyracksDataException {
        cloudClient.close();
    }

    @Override
    public Predicate<Exception> getObjectNotFoundExceptionPredicate() {
        return cloudClient.getObjectNotFoundExceptionPredicate();
    }

    private static void fail() throws HyracksDataException {
        double prob = RANDOM.nextInt(100) / 100.0d;
        if (prob < ERROR_RATE.get()) {
            throw HyracksDataException.create(ErrorCode.FAILED_IO_OPERATION, new IOException("Simulated error"));
        }
    }

    private static ICloudWriter createUnstableWriter(S3CloudClient cloudClient, String bucket, String path,
            IWriteBufferProvider bufferProvider) {
        ICloudBufferedWriter bufferedWriter =
                new UnstableCloudBufferedWriter(cloudClient.createBufferedWriter(bucket, path));
        return new CloudResettableInputStream(bufferedWriter, bufferProvider);
    }

    /**
     * An unstable cloud writer that mimics the functionality of {@link GCSWriter}
     */
    private static class UnstableGCSCloudWriter implements ICloudWriter {
        private final ICloudWriter writer;
        private final int writeBufferSize;

        UnstableGCSCloudWriter(ICloudWriter writer, int writeBufferSize) {
            this.writer = writer;
            this.writeBufferSize = writeBufferSize;
        }

        @Override
        public int write(ByteBuffer page) throws HyracksDataException {
            if (position() == 0) {
                fail();
            }
            long uploadsToBeTriggered =
                    ((position() + page.remaining()) / writeBufferSize) - (position() / writeBufferSize);
            while (uploadsToBeTriggered-- > 0) {
                fail();
            }
            return writer.write(page);
        }

        @Override
        public void write(int b) throws HyracksDataException {
            write(ByteBuffer.wrap(new byte[] { (byte) b }));
        }

        @Override
        public int write(byte[] b, int off, int len) throws HyracksDataException {
            return write(ByteBuffer.wrap(b, off, len));
        }

        @Override
        public long position() {
            return writer.position();
        }

        @Override
        public void finish() throws HyracksDataException {
            fail();
            writer.finish();
        }

        @Override
        public void abort() throws HyracksDataException {
            writer.abort();
        }
    }

    private static class UnstableCloudBufferedWriter implements ICloudBufferedWriter {
        private final ICloudBufferedWriter bufferedWriter;

        private UnstableCloudBufferedWriter(ICloudBufferedWriter bufferedWriter) {
            this.bufferedWriter = bufferedWriter;
        }

        @Override
        public void upload(InputStream stream, int length) throws HyracksDataException {
            fail();
            bufferedWriter.upload(stream, length);
        }

        @Override
        public void uploadLast(InputStream stream, ByteBuffer buffer) throws HyracksDataException {
            fail();
            bufferedWriter.uploadLast(stream, buffer);
        }

        @Override
        public boolean isEmpty() {
            return bufferedWriter.isEmpty();
        }

        @Override
        public void finish() throws HyracksDataException {
            bufferedWriter.finish();
        }

        @Override
        public void abort() throws HyracksDataException {
            bufferedWriter.abort();
        }
    }
}
