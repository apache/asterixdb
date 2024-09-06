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

import static org.apache.asterix.cloud.clients.google.gcs.GCSClientConfig.DELETE_BATCH_SIZE;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.clients.CloudFile;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.cloud.clients.profiler.CountRequestProfilerLimiter;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.asterix.cloud.clients.profiler.RequestLimiterNoOpProfiler;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.IOManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

public class GCSCloudClient implements ICloudClient {
    private final Storage gcsClient;
    private final GCSClientConfig config;
    private final ICloudGuardian guardian;
    private final IRequestProfilerLimiter profilerLimiter;
    private final int writeBufferSize;

    public GCSCloudClient(GCSClientConfig config, Storage gcsClient, ICloudGuardian guardian) {
        this.gcsClient = gcsClient;
        this.config = config;
        this.guardian = guardian;
        this.writeBufferSize = config.getWriteBufferSize();
        long profilerInterval = config.getProfilerLogInterval();
        GCSRequestRateLimiter limiter = new GCSRequestRateLimiter(config);
        if (profilerInterval > 0) {
            profilerLimiter = new CountRequestProfilerLimiter(profilerInterval, limiter);
        } else {
            profilerLimiter = new RequestLimiterNoOpProfiler(limiter);
        }
        guardian.setCloudClient(this);
    }

    public GCSCloudClient(GCSClientConfig config, ICloudGuardian guardian) throws HyracksDataException {
        this(config, buildClient(config), guardian);
    }

    @Override
    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    @Override
    public IRequestProfilerLimiter getProfilerLimiter() {
        return profilerLimiter;
    }

    @Override
    public ICloudWriter createWriter(String bucket, String path, IWriteBufferProvider bufferProvider) {
        return new GCSWriter(bucket, config.getPrefix() + path, gcsClient, profilerLimiter, guardian, writeBufferSize);
    }

    @Override
    public Set<CloudFile> listObjects(String bucket, String path, FilenameFilter filter) {
        guardian.checkReadAccess(bucket, path);
        profilerLimiter.objectsList();
        Page<Blob> blobs = gcsClient.list(bucket, BlobListOption.prefix(config.getPrefix() + path),
                BlobListOption.fields(Storage.BlobField.SIZE));

        Set<CloudFile> files = new HashSet<>();
        for (Blob blob : blobs.iterateAll()) {
            if (filter.accept(null, IoUtil.getFileNameFromPath(blob.getName()))) {
                files.add(CloudFile.of(stripCloudPrefix(blob.getName()), blob.getSize()));
            }
        }
        return files;
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profilerLimiter.objectGet();
        BlobId blobId = BlobId.of(bucket, config.getPrefix() + path);
        long readTo = offset + buffer.remaining();
        int totalRead = 0;
        try (ReadChannel from = gcsClient.reader(blobId).limit(readTo)) {
            while (buffer.remaining() > 0) {
                from.seek(offset + totalRead);
                totalRead += from.read(buffer);
            }
        } catch (IOException | StorageException ex) {
            throw HyracksDataException.create(ex);
        }

        if (buffer.remaining() != 0) {
            throw new IllegalStateException("Expected buffer remaining = 0, found: " + buffer.remaining());
        }
        return totalRead;
    }

    @Override
    public byte[] readAllBytes(String bucket, String path) {
        guardian.checkReadAccess(bucket, path);
        profilerLimiter.objectGet();
        BlobId blobId = BlobId.of(bucket, config.getPrefix() + path);
        try {
            return gcsClient.readAllBytes(blobId);
        } catch (StorageException e) {
            return null;
        }
    }

    @Override
    public InputStream getObjectStream(String bucket, String path, long offset, long length) {
        guardian.checkReadAccess(bucket, path);
        profilerLimiter.objectGet();
        ReadChannel reader = null;
        try {
            reader = gcsClient.reader(bucket, config.getPrefix() + path).limit(offset + length);
            reader.seek(offset);
            return Channels.newInputStream(reader);
        } catch (StorageException | IOException ex) {
            throw new RuntimeException(CleanupUtils.close(reader, ex));
        }
    }

    @Override
    public void write(String bucket, String path, byte[] data) {
        guardian.checkWriteAccess(bucket, path);
        profilerLimiter.objectWrite();
        BlobInfo blobInfo = BlobInfo.newBuilder(bucket, config.getPrefix() + path).build();
        gcsClient.create(blobInfo, data);
    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) {
        guardian.checkReadAccess(bucket, srcPath);
        profilerLimiter.objectsList();
        Page<Blob> blobs = gcsClient.list(bucket, BlobListOption.prefix(config.getPrefix() + srcPath));
        for (Blob blob : blobs.iterateAll()) {
            profilerLimiter.objectCopy();
            BlobId source = blob.getBlobId();
            String targetName = destPath.getChildPath(IoUtil.getFileNameFromPath(source.getName()));
            BlobId target = BlobId.of(bucket, targetName);
            guardian.checkWriteAccess(bucket, targetName);
            CopyRequest copyReq = CopyRequest.newBuilder().setSource(source).setTarget(target).build();
            gcsClient.copy(copyReq);
        }
    }

    @Override
    public void deleteObjects(String bucket, Collection<String> paths) {
        if (paths.isEmpty()) {
            return;
        }

        StorageBatch batchRequest;
        Iterator<String> pathIter = paths.iterator();
        while (pathIter.hasNext()) {
            batchRequest = gcsClient.batch();
            for (int i = 0; pathIter.hasNext() && i < DELETE_BATCH_SIZE; i++) {
                BlobId blobId = BlobId.of(bucket, config.getPrefix() + pathIter.next());
                guardian.checkWriteAccess(bucket, blobId.getName());
                batchRequest.delete(blobId);
            }

            batchRequest.submit();
            profilerLimiter.objectDelete();
        }
    }

    @Override
    public long getObjectSize(String bucket, String path) {
        guardian.checkReadAccess(bucket, path);
        profilerLimiter.objectGet();
        Blob blob =
                gcsClient.get(bucket, config.getPrefix() + path, Storage.BlobGetOption.fields(Storage.BlobField.SIZE));
        if (blob == null) {
            return 0;
        }
        return blob.getSize();
    }

    @Override
    public boolean exists(String bucket, String path) {
        guardian.checkReadAccess(bucket, path);
        profilerLimiter.objectGet();
        Blob blob = gcsClient.get(bucket, config.getPrefix() + path,
                Storage.BlobGetOption.fields(Storage.BlobField.values()));
        return blob != null && blob.exists();
    }

    @Override
    public boolean isEmptyPrefix(String bucket, String path) {
        guardian.checkReadAccess(bucket, path);
        profilerLimiter.objectsList();
        Page<Blob> blobs = gcsClient.list(bucket, BlobListOption.prefix(config.getPrefix() + path));
        return !blobs.hasNextPage();
    }

    @Override
    public IParallelDownloader createParallelDownloader(String bucket, IOManager ioManager)
            throws HyracksDataException {
        return new GCSParallelDownloader(bucket, ioManager, config, profilerLimiter);
    }

    @Override
    public JsonNode listAsJson(ObjectMapper objectMapper, String bucket) {
        guardian.checkReadAccess(bucket, "/");
        profilerLimiter.objectsList();
        Page<Blob> blobs = gcsClient.list(bucket, BlobListOption.fields(Storage.BlobField.SIZE));
        ArrayNode objectsInfo = objectMapper.createArrayNode();

        List<Blob> objects = new ArrayList<>();
        blobs.iterateAll().forEach(objects::add);
        objects.sort((x, y) -> String.CASE_INSENSITIVE_ORDER.compare(x.getName(), y.getName()));
        for (Blob blob : objects) {
            ObjectNode objectInfo = objectsInfo.addObject();
            objectInfo.put("path", blob.getName());
            objectInfo.put("size", blob.getSize());
        }
        return objectsInfo;
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            gcsClient.close();
        } catch (Exception ex) {
            throw HyracksDataException.create(ex);
        }
    }

    private static Storage buildClient(GCSClientConfig config) throws HyracksDataException {
        StorageOptions.Builder builder = StorageOptions.newBuilder().setCredentials(config.createCredentialsProvider());

        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            builder.setHost(config.getEndpoint());
        }
        return builder.build().getService();
    }

    private String stripCloudPrefix(String objectName) {
        return objectName.substring(config.getPrefix().length());
    }
}