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

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.util.annotations.ThreadSafe;

import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FailedFileDownload;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.utils.AttributeMap;

@ThreadSafe
class S3ParallelDownloader implements IParallelDownloader {
    private final String bucket;
    private final IOManager ioManager;
    private final S3AsyncClient s3AsyncClient;
    private final S3TransferManager transferManager;
    private final S3ClientConfig config;
    private final IRequestProfilerLimiter profiler;

    S3ParallelDownloader(String bucket, IOManager ioManager, S3ClientConfig config, IRequestProfilerLimiter profiler) {
        this.bucket = bucket;
        this.ioManager = ioManager;
        this.config = config;
        this.profiler = profiler;
        s3AsyncClient = createAsyncClient(config);
        transferManager = createS3TransferManager(s3AsyncClient);
    }

    @Override
    public void downloadFiles(Collection<FileReference> toDownload) throws HyracksDataException {
        try {
            List<CompletableFuture<CompletedFileDownload>> downloads = startDownloadingFiles(toDownload);
            waitForFileDownloads(downloads);
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Collection<FileReference> downloadDirectories(Collection<FileReference> toDownload)
            throws HyracksDataException {
        Set<FileReference> failedFiles;
        List<CompletableFuture<CompletedDirectoryDownload>> downloads = startDownloadingDirectories(toDownload);
        try {
            failedFiles = waitForDirectoryDownloads(downloads);
        } catch (ExecutionException | InterruptedException e) {
            throw HyracksDataException.create(e);
        }

        return failedFiles;
    }

    @Override
    public void close() {
        transferManager.close();
        s3AsyncClient.close();
    }

    private List<CompletableFuture<CompletedFileDownload>> startDownloadingFiles(Collection<FileReference> toDownload)
            throws IOException {
        List<CompletableFuture<CompletedFileDownload>> downloads = new ArrayList<>();
        for (FileReference fileReference : toDownload) {
            // multipart download
            profiler.objectGet();

            // Create parent directories
            FileUtils.createParentDirectories(fileReference.getFile());

            // GetObjectRequest
            GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder();
            requestBuilder.bucket(bucket);
            requestBuilder.key(config.getPrefix() + fileReference.getRelativePath());

            // Download object
            DownloadFileRequest.Builder builder = DownloadFileRequest.builder();
            builder.getObjectRequest(requestBuilder.build());
            builder.destination(fileReference.getFile());

            FileDownload fileDownload = transferManager.downloadFile(builder.build());
            downloads.add(fileDownload.completionFuture());
        }
        return downloads;
    }

    private void waitForFileDownloads(List<CompletableFuture<CompletedFileDownload>> downloads)
            throws ExecutionException, InterruptedException {

        for (CompletableFuture<CompletedFileDownload> download : downloads) {
            download.get();
        }
    }

    private List<CompletableFuture<CompletedDirectoryDownload>> startDownloadingDirectories(
            Collection<FileReference> toDownload) {
        List<CompletableFuture<CompletedDirectoryDownload>> downloads = new ArrayList<>();
        for (FileReference fileReference : toDownload) {
            DownloadDirectoryRequest.Builder builder = DownloadDirectoryRequest.builder();
            builder.bucket(bucket);
            builder.destination(fileReference.getFile().toPath());
            builder.listObjectsV2RequestTransformer(
                    l -> l.prefix(config.getPrefix() + fileReference.getRelativePath()));
            DirectoryDownload directoryDownload = transferManager.downloadDirectory(builder.build());
            downloads.add(directoryDownload.completionFuture());
        }
        return downloads;
    }

    private Set<FileReference> waitForDirectoryDownloads(List<CompletableFuture<CompletedDirectoryDownload>> downloads)
            throws ExecutionException, InterruptedException, HyracksDataException {
        Set<FileReference> failedFiles = Collections.emptySet();
        for (CompletableFuture<CompletedDirectoryDownload> download : downloads) {
            // multipart download
            profiler.objectMultipartDownload();
            download.join();
            CompletedDirectoryDownload completedDirectoryDownload = download.get();

            // if we have failed downloads with transfer manager, try to download them with GetObject
            if (!completedDirectoryDownload.failedTransfers().isEmpty()) {
                failedFiles = failedFiles.isEmpty() ? new HashSet<>() : failedFiles;
                for (FailedFileDownload failedFileDownload : completedDirectoryDownload.failedTransfers()) {
                    FileReference failedFile = ioManager.resolve(failedFileDownload.request().getObjectRequest().key());
                    failedFiles.add(failedFile);
                }
            }
        }
        return failedFiles;
    }

    private static S3AsyncClient createAsyncClient(S3ClientConfig config) {
        // CRT client is not supported by all local S3 providers, but provides a better performance with AWS S3
        if (!config.isLocalS3Provider()) {
            return createS3CrtAsyncClient(config);
        }
        return createS3AsyncClient(config);
    }

    private static S3AsyncClient createS3AsyncClient(S3ClientConfig config) {
        S3AsyncClientBuilder builder = S3AsyncClient.builder();
        builder.credentialsProvider(config.createCredentialsProvider());
        builder.region(Region.of(config.getRegion()));
        builder.forcePathStyle(config.isForcePathStyle());
        AttributeMap.Builder customHttpConfigBuilder = AttributeMap.builder();
        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }
        if (config.isDisableSslVerify()) {
            customHttpConfigBuilder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true);
        }
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
        SdkAsyncHttpClient nettyHttpClient =
                NettyNioAsyncHttpClient.builder().buildWithDefaults(customHttpConfigBuilder.build());
        builder.httpClient(nettyHttpClient);
        return builder.build();
    }

    private static S3AsyncClient createS3CrtAsyncClient(S3ClientConfig config) {
        S3CrtAsyncClientBuilder builder = S3AsyncClient.crtBuilder();
        builder.credentialsProvider(config.createCredentialsProvider());
        builder.region(Region.of(config.getRegion()));
        builder.forcePathStyle(config.isForcePathStyle());
        if (config.getEndpoint() != null && !config.getEndpoint().isEmpty()) {
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }
        return builder.build();
    }

    private S3TransferManager createS3TransferManager(S3AsyncClient s3AsyncClient) {
        return S3TransferManager.builder().s3Client(s3AsyncClient).build();
    }
}
