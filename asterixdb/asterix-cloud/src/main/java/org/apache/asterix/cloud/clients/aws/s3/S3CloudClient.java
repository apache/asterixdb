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

import static org.apache.asterix.cloud.clients.aws.s3.S3Utils.encodeURI;
import static org.apache.asterix.cloud.clients.aws.s3.S3Utils.listS3Objects;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.profiler.CountRequestProfiler;
import org.apache.asterix.cloud.clients.profiler.IRequestProfiler;
import org.apache.asterix.cloud.clients.profiler.NoOpRequestProfiler;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.DirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;

public class S3CloudClient implements ICloudClient {

    private static final Logger LOGGER = LogManager.getLogger();
    // TODO(htowaileb): Temporary variables, can we get this from the used instance?
    private static final double MAX_HOST_BANDWIDTH = 10.0; // in Gbps
    // The maximum number of file that can be deleted (AWS restriction)
    private static final int DELETE_BATCH_SIZE = 1000;

    private final S3ClientConfig config;
    private final S3Client s3Client;
    private final IRequestProfiler profiler;
    private S3TransferManager s3TransferManager;

    public S3CloudClient(S3ClientConfig config) {
        this.config = config;
        s3Client = buildClient();
        long profilerInterval = config.getProfilerLogInterval();
        if (profilerInterval > 0) {
            profiler = new CountRequestProfiler(profilerInterval);
        } else {
            profiler = NoOpRequestProfiler.INSTANCE;
        }

    }

    @Override
    public ICloudBufferedWriter createBufferedWriter(String bucket, String path) {
        return new S3BufferedWriter(s3Client, profiler, bucket, path);
    }

    @Override
    public Set<String> listObjects(String bucket, String path, FilenameFilter filter) {
        profiler.objectsList();
        path = config.isEncodeKeys() ? encodeURI(path) : path;
        return filterAndGet(listS3Objects(s3Client, bucket, path), filter);
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
        profiler.objectGet();
        long readTo = offset + buffer.remaining();
        GetObjectRequest rangeGetObjectRequest =
                GetObjectRequest.builder().range("bytes=" + offset + "-" + readTo).bucket(bucket).key(path).build();

        int totalRead = 0;
        int read = 0;

        // TODO(htowaileb): add retry logic here
        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(rangeGetObjectRequest)) {
            while (buffer.remaining() > 0) {
                read = response.read(buffer.array(), buffer.position(), buffer.remaining());
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
        profiler.objectGet();
        GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucket).key(path).build();

        try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(getReq)) {
            return stream.readAllBytes();
        } catch (NoSuchKeyException e) {
            return null;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public InputStream getObjectStream(String bucket, String path) {
        profiler.objectGet();
        GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucket).key(path).build();
        try {
            return s3Client.getObject(getReq);
        } catch (NoSuchKeyException e) {
            // This should not happen at least from the only caller of this method
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void write(String bucket, String path, byte[] data) {
        profiler.objectWrite();
        PutObjectRequest putReq = PutObjectRequest.builder().bucket(bucket).key(path).build();

        // TODO(htowaileb): add retry logic here
        s3Client.putObject(putReq, RequestBody.fromBytes(data));
    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) {
        srcPath = config.isEncodeKeys() ? encodeURI(srcPath) : srcPath;
        List<S3Object> objects = listS3Objects(s3Client, bucket, srcPath);

        profiler.objectsList();
        for (S3Object object : objects) {
            profiler.objectCopy();
            String srcKey = object.key();
            String destKey = destPath.getChildPath(IoUtil.getFileNameFromPath(srcKey));
            CopyObjectRequest copyReq = CopyObjectRequest.builder().sourceBucket(bucket).sourceKey(srcKey)
                    .destinationBucket(bucket).destinationKey(destKey).build();
            s3Client.copyObject(copyReq);
        }
    }

    @Override
    public void deleteObjects(String bucket, Collection<String> paths) {
        if (paths.isEmpty()) {
            return;
        }

        List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
        Iterator<String> pathIter = paths.iterator();
        ObjectIdentifier.Builder builder = ObjectIdentifier.builder();
        while (pathIter.hasNext()) {
            objectIdentifiers.clear();
            for (int i = 0; pathIter.hasNext() && i < DELETE_BATCH_SIZE; i++) {
                objectIdentifiers.add(builder.key(pathIter.next()).build());
            }

            Delete delete = Delete.builder().objects(objectIdentifiers).build();
            DeleteObjectsRequest deleteReq = DeleteObjectsRequest.builder().bucket(bucket).delete(delete).build();
            s3Client.deleteObjects(deleteReq);
            profiler.objectDelete();
        }
    }

    @Override
    public long getObjectSize(String bucket, String path) throws HyracksDataException {
        profiler.objectGet();
        try {
            return s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(path).build()).contentLength();
        } catch (NoSuchKeyException ex) {
            return 0;
        } catch (Exception ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public boolean exists(String bucket, String path) throws HyracksDataException {
        profiler.objectGet();
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(path).build());
            return true;
        } catch (NoSuchKeyException ex) {
            return false;
        } catch (Exception ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public void syncFiles(String bucket, Map<String, String> cloudToLocalStoragePaths) throws HyracksDataException {
        LOGGER.info("Syncing cloud storage to local storage started");

        S3TransferManager s3TransferManager = getS3TransferManager();

        List<CompletableFuture<CompletedDirectoryDownload>> downloads = new ArrayList<>();
        cloudToLocalStoragePaths.forEach((cloudStoragePath, localStoragePath) -> {
            DownloadDirectoryRequest.Builder builder = DownloadDirectoryRequest.builder();
            builder.bucket(bucket);
            builder.destination(Paths.get(localStoragePath));
            builder.listObjectsV2RequestTransformer(l -> l.prefix(cloudStoragePath));

            LOGGER.info("TransferManager started downloading from cloud \"{}\" to local storage \"{}\"",
                    cloudStoragePath, localStoragePath);
            DirectoryDownload directoryDownload = s3TransferManager.downloadDirectory(builder.build());
            downloads.add(directoryDownload.completionFuture());
        });

        try {
            for (CompletableFuture<CompletedDirectoryDownload> download : downloads) {
                // multipart download
                profiler.objectMultipartDownload();
                download.join();
                CompletedDirectoryDownload completedDirectoryDownload = download.get();

                // if we have failed downloads with transfer manager, try to download them with GetObject
                if (!completedDirectoryDownload.failedTransfers().isEmpty()) {
                    LOGGER.warn("TransferManager failed to download file(s), will retry to download each separately");
                    completedDirectoryDownload.failedTransfers().forEach(LOGGER::warn);

                    Map<String, String> failedFiles = new HashMap<>();
                    completedDirectoryDownload.failedTransfers().forEach(failed -> {
                        String cloudStoragePath = failed.request().getObjectRequest().key();
                        String localStoragePath = failed.request().destination().toAbsolutePath().toString();
                        failedFiles.put(cloudStoragePath, localStoragePath);
                    });
                    downloadFiles(bucket, failedFiles);
                }
                LOGGER.info("TransferManager finished downloading {} to local storage", completedDirectoryDownload);
            }
        } catch (ExecutionException | InterruptedException e) {
            throw HyracksDataException.create(e);
        }
        LOGGER.info("Syncing cloud storage to local storage successful");
    }

    @Override
    public JsonNode listAsJson(ObjectMapper objectMapper, String bucket) {
        List<S3Object> objects = listS3Objects(s3Client, bucket, "/");
        ArrayNode objectsInfo = objectMapper.createArrayNode();

        objects.sort((x, y) -> String.CASE_INSENSITIVE_ORDER.compare(x.key(), y.key()));
        for (S3Object object : objects) {
            ObjectNode objectInfo = objectsInfo.addObject();
            objectInfo.put("path", object.key());
            objectInfo.put("size", object.size());
        }
        return objectsInfo;
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }

        if (s3TransferManager != null) {
            s3TransferManager.close();
        }
    }

    private S3Client buildClient() {
        S3ClientBuilder builder = S3Client.builder();
        builder.credentialsProvider(config.createCredentialsProvider());
        builder.region(Region.of(config.getRegion()));
        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            URI uri;
            try {
                uri = new URI(config.getEndpoint());
            } catch (URISyntaxException ex) {
                throw new IllegalArgumentException(ex);
            }
            builder.endpointOverride(uri);
        }
        return builder.build();
    }

    private Set<String> filterAndGet(List<S3Object> contents, FilenameFilter filter) {
        Set<String> files = new HashSet<>();
        for (S3Object s3Object : contents) {
            String path = config.isEncodeKeys() ? S3Utils.decodeURI(s3Object.key()) : s3Object.key();
            if (filter.accept(null, IoUtil.getFileNameFromPath(path))) {
                files.add(path);
            }
        }
        return files;
    }

    private void downloadFiles(String bucket, Map<String, String> cloudToLocalStoragePaths)
            throws HyracksDataException {
        byte[] buffer = new byte[8 * 1024];
        for (Map.Entry<String, String> entry : cloudToLocalStoragePaths.entrySet()) {
            String cloudStoragePath = entry.getKey();
            String localStoragePath = entry.getValue();

            LOGGER.info("GetObject started downloading from cloud \"{}\" to local storage \"{}\"", cloudStoragePath,
                    localStoragePath);

            // TODO(htowaileb): add retry logic here
            try {
                File localFile = new File(localStoragePath);
                FileUtils.createParentDirectories(localFile);
                if (!localFile.createNewFile()) {
                    // do nothing for now, a restart has the files when trying to flush, for testing
                    //throw new IllegalStateException("Couldn't create local file");
                }

                try (InputStream inputStream = getObjectStream(bucket, cloudStoragePath);
                        FileOutputStream outputStream = new FileOutputStream(localFile)) {
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                }
            } catch (IOException ex) {
                throw HyracksDataException.create(ex);
            }
            LOGGER.info("GetObject successful downloading from cloud \"{}\" to local storage \"{}\"", cloudStoragePath,
                    localStoragePath);
        }
    }

    private S3TransferManager getS3TransferManager() {
        if (s3TransferManager != null) {
            return s3TransferManager;
        }

        S3CrtAsyncClientBuilder builder = S3AsyncClient.crtBuilder();
        builder.credentialsProvider(config.createCredentialsProvider());
        builder.region(Region.of(config.getRegion()));
        builder.targetThroughputInGbps(MAX_HOST_BANDWIDTH);
        builder.minimumPartSizeInBytes((long) 8 * 1024 * 1024);

        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }

        S3AsyncClient client = builder.build();
        s3TransferManager = S3TransferManager.builder().s3Client(client).build();
        return s3TransferManager;
    }
}
