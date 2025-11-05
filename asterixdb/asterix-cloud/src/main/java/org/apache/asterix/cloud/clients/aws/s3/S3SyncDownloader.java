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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

@ThreadSafe
public class S3SyncDownloader implements IParallelDownloader {
    private static final Logger LOGGER = LogManager.getLogger();

    private final String bucket;
    private final IOManager ioManager;
    private final S3Client s3Client;
    private final S3ClientConfig config;
    private final IRequestProfilerLimiter profiler;
    private final ExecutorService executorService;

    S3SyncDownloader(String bucket, IOManager ioManager, S3ClientConfig config, IRequestProfilerLimiter profiler) {
        this.bucket = bucket;
        this.ioManager = ioManager;
        this.config = config;
        this.profiler = profiler;
        this.s3Client = (S3Client) S3CloudClient.buildClient(config).getConsumingClient();
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void downloadFiles(Collection<FileReference> toDownload) throws HyracksDataException {
        try {
            downloadFilesAndWait(toDownload);
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void downloadFilesAndWait(Collection<FileReference> toDownload)
            throws IOException, ExecutionException, InterruptedException {
        List<Future<?>> downloads = new ArrayList<>();
        int maxPending = config.getRequestsMaxPendingHttpConnections();
        for (FileReference fileReference : toDownload) {
            profiler.objectGet();
            FileUtils.createParentDirectories(fileReference.getFile());
            Future<?> future = executorService.submit(() -> {
                try {
                    downloadFile(fileReference);
                } catch (HyracksDataException e) {
                    throw new RuntimeException(e);
                }
            });
            downloads.add(future);

            if (maxPending > 0 && downloads.size() >= maxPending) {
                waitForFileDownloads(downloads);
                downloads.clear();
            }
        }
        if (!downloads.isEmpty()) {
            waitForFileDownloads(downloads);
        }
    }

    private void waitForFileDownloads(List<Future<?>> downloads) throws ExecutionException, InterruptedException {
        for (Future<?> download : downloads) {
            download.get();
        }
    }

    private void downloadFile(FileReference fileReference) throws HyracksDataException {
        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket)
                .key(config.getPrefix() + fileReference.getRelativePath()).build();

        Path targetPath = fileReference.getFile().toPath();
        try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);
                OutputStream outputStream = Files.newOutputStream(targetPath, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            response.transferTo(outputStream);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Collection<FileReference> downloadDirectories(Collection<FileReference> toDownload)
            throws HyracksDataException {
        Set<FileReference> failedFiles;
        try {
            failedFiles = downloadDirectoriesAndWait(toDownload);
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw HyracksDataException.create(e);
        }
        return failedFiles;
    }

    private Set<FileReference> downloadDirectoriesAndWait(Collection<FileReference> toDownload)
            throws IOException, ExecutionException, InterruptedException {
        Set<FileReference> failedFiles = ConcurrentHashMap.newKeySet();
        List<Future<?>> downloads = new ArrayList<>();

        int maxPending = config.getRequestsMaxPendingHttpConnections();
        List<S3Object> downloadObjects = new ArrayList<>();
        for (FileReference fileReference : toDownload) {
            profiler.objectMultipartDownload();
            String prefix = config.getPrefix() + fileReference.getRelativePath();
            List<S3Object> objects = S3ClientUtils.listS3Objects(s3Client, bucket, prefix);
            downloadObjects.addAll(objects);
        }

        for (S3Object s3Object : downloadObjects) {
            String key = createDiskSubPath(s3Object.key());
            FileReference targetFile = ioManager.resolve(key);

            FileUtils.createParentDirectories(targetFile.getFile());

            Future<Void> future = executorService.submit(() -> {
                try {
                    profiler.objectGet();
                    downloadFile(targetFile);
                } catch (IOException e) {
                    // Record failed file
                    failedFiles.add(targetFile);
                    LOGGER.debug("Failed to download file using sync client: file {} having s3Key: {}", targetFile,
                            s3Object.key(), e);
                }
                return null;
            });
            downloads.add(future);

            if (maxPending > 0 && downloads.size() >= maxPending) {
                waitForDirectoryFileDownloads(downloads);
                downloads.clear();
            }
        }

        if (!downloads.isEmpty()) {
            waitForDirectoryFileDownloads(downloads);
        }

        return failedFiles;
    }

    private void waitForDirectoryFileDownloads(List<Future<?>> downloads)
            throws ExecutionException, InterruptedException {
        for (Future<?> download : downloads) {
            download.get();
        }
    }

    private String createDiskSubPath(String objectName) {
        if (!objectName.startsWith(STORAGE_SUB_DIR)) {
            objectName = objectName.substring(objectName.indexOf(STORAGE_SUB_DIR));
        }
        return objectName;
    }

    @Override
    public void close() throws HyracksDataException {
        s3Client.close();
    }
}
