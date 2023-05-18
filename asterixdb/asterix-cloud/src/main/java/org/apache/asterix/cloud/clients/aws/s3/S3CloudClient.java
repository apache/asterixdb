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

import static org.apache.asterix.cloud.clients.aws.s3.S3Utils.listS3Objects;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.aws.s3.credentials.IS3Credentials;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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

    private final IS3Credentials credentials;
    private final S3Client s3Client;
    private S3TransferManager s3TransferManager;

    // TODO(htowaileb): Temporary variables, can we get this from the used instance?
    private static final double MAX_HOST_BANDWIDTH = 10.0; // in Gbps

    public S3CloudClient(IS3Credentials credentials) throws HyracksDataException {
        this.credentials = credentials;
        s3Client = buildClient();
    }

    private S3Client buildClient() throws HyracksDataException {
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider
                .create(AwsBasicCredentials.create(credentials.getAccessKeyId(), credentials.getSecretAccessKey()));
        S3ClientBuilder builder = S3Client.builder();
        builder.credentialsProvider(credentialsProvider);
        builder.region(Region.of(credentials.getRegion()));

        if (credentials.getEndpoint() != null && !credentials.getEndpoint().isEmpty()) {
            try {
                URI uri = new URI(credentials.getEndpoint());
                builder.endpointOverride(uri);
            } catch (Exception ex) {
                throw HyracksDataException.create(ex);
            }
        }
        return builder.build();
    }

    @Override
    public ICloudBufferedWriter createBufferedWriter(String bucket, String path) {
        return new S3BufferedWriter(s3Client, bucket, path);
    }

    @Override
    public Set<String> listObjects(String bucket, String path, FilenameFilter filter) {
        return filterAndGet(listS3Objects(s3Client, bucket, path), filter);
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
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
        GetObjectRequest getReq = GetObjectRequest.builder().bucket(bucket).key(path).build();
        try {
            ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(getReq);
            return stream.readAllBytes();
        } catch (NoSuchKeyException e) {
            return null;
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public InputStream getObjectStream(String bucket, String path) {
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
        PutObjectRequest putReq = PutObjectRequest.builder().bucket(bucket).key(path).build();

        // TODO(htowaileb): add retry logic here
        s3Client.putObject(putReq, RequestBody.fromBytes(data));
    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) {
        List<S3Object> objects = listS3Objects(s3Client, bucket, srcPath);
        for (S3Object object : objects) {
            String srcKey = object.key();
            String destKey = destPath.getChildPath(IoUtil.getFileNameFromPath(srcKey));
            CopyObjectRequest copyReq = CopyObjectRequest.builder().sourceBucket(bucket).sourceKey(srcKey)
                    .destinationBucket(bucket).destinationKey(destKey).build();
            s3Client.copyObject(copyReq);
        }
    }

    @Override
    public void deleteObject(String bucket, String path) {
        Set<String> fileList = listObjects(bucket, path, IoUtil.NO_OP_FILTER);
        if (fileList.isEmpty()) {
            return;
        }

        List<ObjectIdentifier> objectIdentifiers = new ArrayList<>();
        for (String file : fileList) {
            objectIdentifiers.add(ObjectIdentifier.builder().key(file).build());
        }
        Delete delete = Delete.builder().objects(objectIdentifiers).build();
        DeleteObjectsRequest deleteReq = DeleteObjectsRequest.builder().bucket(bucket).delete(delete).build();
        s3Client.deleteObjects(deleteReq);
    }

    @Override
    public long getObjectSize(String bucket, String path) throws HyracksDataException {
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
        try {
            s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(path).build());
            return true;
        } catch (NoSuchKeyException ex) {
            return false;
        } catch (Exception ex) {
            throw HyracksDataException.create(ex);
        }
    }

    private Set<String> filterAndGet(List<S3Object> contents, FilenameFilter filter) {
        Set<String> files = new HashSet<>();
        for (S3Object s3Object : contents) {
            String path = s3Object.key();
            if (filter.accept(null, IoUtil.getFileNameFromPath(path))) {
                files.add(path);
            }
        }
        return files;
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
        builder.credentialsProvider(StaticCredentialsProvider
                .create(AwsBasicCredentials.create(credentials.getAccessKeyId(), credentials.getSecretAccessKey())));
        builder.region(Region.of(credentials.getRegion()));
        builder.targetThroughputInGbps(MAX_HOST_BANDWIDTH);
        builder.minimumPartSizeInBytes((long) 8 * 1024 * 1024);

        if (credentials.getEndpoint() != null && !credentials.getEndpoint().isEmpty()) {
            builder.endpointOverride(URI.create(credentials.getEndpoint()));
        }

        S3AsyncClient client = builder.build();
        s3TransferManager = S3TransferManager.builder().s3Client(client).build();
        return s3TransferManager;
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
}
