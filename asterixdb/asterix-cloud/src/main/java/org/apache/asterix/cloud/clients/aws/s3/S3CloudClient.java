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
package org.apache.asterix.cloud.clients.aws.s3;

import static org.apache.asterix.cloud.clients.aws.s3.S3ClientConfig.DELETE_BATCH_SIZE;
import static org.apache.asterix.cloud.clients.aws.s3.S3ClientUtils.encodeURI;
import static org.apache.asterix.cloud.clients.aws.s3.S3ClientUtils.listS3Objects;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.asterix.cloud.CloudResettableInputStream;
import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.clients.CloudFile;
import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.cloud.clients.profiler.CountRequestProfilerLimiter;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.asterix.cloud.clients.profiler.RequestLimiterNoOpProfiler;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.AttributeMap;

@ThreadSafe
public final class S3CloudClient implements ICloudClient {
    private static final Logger LOGGER = LogManager.getLogger();
    private final S3ClientConfig config;
    private final S3Client s3Client;
    private final ICloudGuardian guardian;
    private final IRequestProfilerLimiter profiler;
    private final int writeBufferSize;

    public S3CloudClient(S3ClientConfig config, ICloudGuardian guardian) {
        this(config, buildClient(config), guardian);
    }

    public S3CloudClient(S3ClientConfig config, S3Client s3Client, ICloudGuardian guardian) {
        this.config = config;
        this.s3Client = s3Client;
        this.guardian = guardian;
        this.writeBufferSize = config.getWriteBufferSize();
        long profilerInterval = config.getProfilerLogInterval();
        S3RequestRateLimiter limiter = new S3RequestRateLimiter(config);
        if (profilerInterval > 0) {
            profiler = new CountRequestProfilerLimiter(profilerInterval, limiter);
        } else {
            profiler = new RequestLimiterNoOpProfiler(limiter);
        }
        guardian.setCloudClient(this);
    }

    @Override
    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    @Override
    public IRequestProfilerLimiter getProfilerLimiter() {
        return profiler;
    }

    @Override
    public ICloudWriter createWriter(String bucket, String path, IWriteBufferProvider bufferProvider) {
        ICloudBufferedWriter bufferedWriter =
                new S3BufferedWriter(s3Client, profiler, guardian, bucket, config.getPrefix() + path);
        return new CloudResettableInputStream(bufferedWriter, bufferProvider);
    }

    @Override
    public Set<CloudFile> listObjects(String bucket, String path, FilenameFilter filter) {
        guardian.checkReadAccess(bucket, path);
        profiler.objectsList();
        path = config.isLocalS3Provider() ? encodeURI(path) : path;
        return ensureListConsistent(filterAndGet(listS3Objects(s3Client, bucket, config.getPrefix() + path), filter),
                bucket, CloudFile::getPath);
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        long bytesToRead = buffer.remaining();
        long readTo = offset + buffer.remaining() - 1;
        GetObjectRequest rangeGetObjectRequest = GetObjectRequest.builder().range("bytes=" + offset + "-" + readTo)
                .bucket(bucket).key(config.getPrefix() + path).build();

        int totalRead = 0;
        int read;

        // TODO(htowaileb): add retry logic here
        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(rangeGetObjectRequest)) {
            while (buffer.remaining() > 0) {
                read = response.read(buffer.array(), buffer.position(), buffer.remaining());
                if (read == -1) {
                    throw new IllegalStateException("Unexpected EOF encountered. File: " + path + ", expected bytes: "
                            + bytesToRead + ", bytes read: " + totalRead);
                }
                buffer.position(buffer.position() + read);
                totalRead += read;
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }

        if (buffer.remaining() != 0) {
            throw new IllegalStateException("Expected buffer remaining = 0, found: " + buffer.remaining());
        }
        return totalRead;
    }

    @Override
    public byte[] readAllBytes(String bucket, String path) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucket).key(config.getPrefix() + path).build();

        try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(getReq)) {
            return stream.readAllBytes();
        } catch (NoSuchKeyException e) {
            return null;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public InputStream getObjectStream(String bucket, String path, long offset, long length) {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        long readTo = offset + length - 1;
        GetObjectRequest getReq = GetObjectRequest.builder().range("bytes=" + offset + "-" + readTo).bucket(bucket)
                .key(config.getPrefix() + path).build();
        try {
            return s3Client.getObject(getReq);
        } catch (NoSuchKeyException e) {
            // This should not happen at least from the only caller of this method
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void write(String bucket, String path, byte[] data) {
        guardian.checkWriteAccess(bucket, path);
        profiler.objectWrite();
        PutObjectRequest putReq = PutObjectRequest.builder().bucket(bucket).key(config.getPrefix() + path).build();
        s3Client.putObject(putReq, RequestBody.fromBytes(data));
    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) {
        guardian.checkReadAccess(bucket, srcPath);
        srcPath = config.getPrefix() + srcPath;
        srcPath = config.isLocalS3Provider() ? encodeURI(srcPath) : srcPath;
        List<S3Object> objects = listS3Objects(s3Client, bucket, srcPath);

        profiler.objectsList();
        for (S3Object object : objects) {
            guardian.checkWriteAccess(bucket, destPath.getRelativePath());
            profiler.objectCopy();
            String srcKey = object.key();
            String destKey = destPath.getChildPath(IoUtil.getFileNameFromPath(srcKey));
            CopyObjectRequest copyReq = CopyObjectRequest.builder().sourceBucket(bucket).sourceKey(srcKey)
                    .destinationBucket(bucket).destinationKey(config.getPrefix() + destKey).build();
            try {
                s3Client.copyObject(copyReq);
            } catch (NoSuchKeyException ex) {
                if (config.isStorageListEventuallyConsistent()) {
                    LOGGER.warn("ignoring 404 on copy of {} since list is configured as eventually consistent", srcKey);
                } else {
                    throw ex;
                }
            }
        }
    }

    @Override
    public void deleteObjects(String bucket, Collection<String> paths) throws HyracksDataException {
        if (paths.isEmpty()) {
            return;
        }

        List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
        Iterator<String> pathIter = paths.iterator();
        ObjectIdentifier.Builder builder = ObjectIdentifier.builder();
        while (pathIter.hasNext()) {
            objectIdentifiers.clear();
            for (int i = 0; pathIter.hasNext() && i < DELETE_BATCH_SIZE; i++) {
                String path = pathIter.next();
                guardian.checkWriteAccess(bucket, path);
                objectIdentifiers.add(builder.key(config.getPrefix() + path).build());
            }

            Delete delete = Delete.builder().objects(objectIdentifiers).build();
            DeleteObjectsRequest deleteReq = DeleteObjectsRequest.builder().bucket(bucket).delete(delete).build();
            DeleteObjectsResponse deleteObjectsResponse = s3Client.deleteObjects(deleteReq);
            if (deleteObjectsResponse.hasErrors()) {
                List<S3Error> deleteErrors = deleteObjectsResponse.errors();
                for (S3Error s3Error : deleteErrors) {
                    LOGGER.warn("Failed to delete object: {}, code: {}, message: {}", s3Error.key(), s3Error.code(),
                            s3Error.message());
                }
                throw new RuntimeDataException(ErrorCode.CLOUD_IO_FAILURE, "DELETE",
                        !deleteErrors.isEmpty() ? deleteErrors.get(0).key() : "", paths.toString());
            }
            profiler.objectDelete();
        }
    }

    @Override
    public long getObjectSize(String bucket, String path) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        try {
            return s3Client
                    .headObject(HeadObjectRequest.builder().bucket(bucket).key(config.getPrefix() + path).build())
                    .contentLength();
        } catch (NoSuchKeyException ex) {
            return 0;
        } catch (Exception ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public boolean exists(String bucket, String path) throws HyracksDataException {
        guardian.checkReadAccess(bucket, path);
        profiler.objectGet();
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(config.getPrefix() + path).build());
            return true;
        } catch (NoSuchKeyException ex) {
            return false;
        } catch (Exception ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public boolean isEmptyPrefix(String bucket, String path) throws HyracksDataException {
        profiler.objectsList();
        return S3ClientUtils.isEmptyPrefix(s3Client, bucket, config.getPrefix() + path);
    }

    @Override
    public IParallelDownloader createParallelDownloader(String bucket, IOManager ioManager) {
        return new S3ParallelDownloader(bucket, ioManager, config, profiler);
    }

    @Override
    public JsonNode listAsJson(ObjectMapper objectMapper, String bucket) {
        List<S3Object> objects =
                ensureListConsistent(listS3Objects(s3Client, bucket, config.getPrefix()), bucket, S3Object::key);
        ArrayNode objectsInfo = objectMapper.createArrayNode();

        objects.sort((x, y) -> String.CASE_INSENSITIVE_ORDER.compare(x.key(), y.key()));
        for (S3Object object : objects) {
            ObjectNode objectInfo = objectsInfo.addObject();
            objectInfo.put("path", object.key().substring(config.getPrefix().length()));
            objectInfo.put("size", object.size());
        }
        return objectsInfo;
    }

    @Override
    public void close() {
        s3Client.close();
    }

    @Override
    public Predicate<Exception> getObjectNotFoundExceptionPredicate() {
        return ex -> ex instanceof NoSuchKeyException;
    }

    /**
     * FOR TESTING ONLY
     */
    public ICloudBufferedWriter createBufferedWriter(String bucket, String path) {
        return new S3BufferedWriter(s3Client, profiler, guardian, bucket, config.getPrefix() + path);
    }

    private static S3Client buildClient(S3ClientConfig config) {
        S3ClientBuilder builder = S3Client.builder();
        builder.credentialsProvider(config.createCredentialsProvider());
        builder.region(Region.of(config.getRegion()));
        builder.forcePathStyle(config.isForcePathStyle());

        AttributeMap.Builder customHttpConfigBuilder = AttributeMap.builder();
        if (config.getRequestsMaxHttpConnections() > 0) {
            customHttpConfigBuilder.put(SdkHttpConfigurationOption.MAX_CONNECTIONS,
                    config.getRequestsMaxHttpConnections());
        }
        if (config.getRequestsMaxPendingHttpConnections() > 0) {
            customHttpConfigBuilder.put(SdkHttpConfigurationOption.MAX_PENDING_CONNECTION_ACQUIRES,
                    config.getRequestsMaxPendingHttpConnections());
        }
        if (config.getRequestsHttpConnectionAcquireTimeout() > 0) {
            customHttpConfigBuilder.put(SdkHttpConfigurationOption.CONNECTION_ACQUIRE_TIMEOUT,
                    Duration.ofSeconds(config.getRequestsHttpConnectionAcquireTimeout()));
        }
        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }
        if (config.isDisableSslVerify()) {
            customHttpConfigBuilder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true);
        }
        SdkHttpClient httpClient = ApacheHttpClient.builder().buildWithDefaults(customHttpConfigBuilder.build());
        builder.httpClient(httpClient);
        return builder.build();
    }

    private Set<CloudFile> filterAndGet(List<S3Object> contents, FilenameFilter filter) {
        Set<CloudFile> files = new HashSet<>();
        for (S3Object s3Object : contents) {
            String path = config.isLocalS3Provider() ? S3ClientUtils.decodeURI(s3Object.key()) : s3Object.key();
            if (filter.accept(null, IoUtil.getFileNameFromPath(path))) {
                path = path.substring(config.getPrefix().length());
                files.add(CloudFile.of(path, s3Object.size()));
            }
        }
        return files;
    }

    private <T, C extends Collection<T>> C ensureListConsistent(C cloudFiles, String bucket,
            Function<T, String> pathExtractor) {
        if (!config.isStorageListEventuallyConsistent()) {
            return cloudFiles;
        }
        Iterator<T> iterator = cloudFiles.iterator();
        while (iterator.hasNext()) {
            String path = pathExtractor.apply(iterator.next());
            try {
                if (!exists(bucket, path)) {
                    LOGGER.warn("Removing non-existent file from list result: {}", path);
                    iterator.remove();
                }
            } catch (HyracksDataException e) {
                LOGGER.warn("Ignoring exception on exists check on {}", path, e);
            }
        }
        return cloudFiles;
    }
}
