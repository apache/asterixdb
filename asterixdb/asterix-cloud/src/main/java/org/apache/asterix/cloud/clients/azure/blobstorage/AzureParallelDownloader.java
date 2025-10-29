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

package org.apache.asterix.cloud.clients.azure.blobstorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.nc.io.IOManager;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class AzureParallelDownloader implements IParallelDownloader {
    public static final String STORAGE_SUB_DIR = "storage";
    private final IOManager ioManager;
    private final BlobContainerAsyncClient blobContainerAsyncClient;
    private final IRequestProfilerLimiter profiler;
    private final AzBlobStorageClientConfig config;

    public AzureParallelDownloader(IOManager ioManager, BlobContainerAsyncClient blobContainerAsyncClient,
            IRequestProfilerLimiter profiler, AzBlobStorageClientConfig config) {
        this.ioManager = ioManager;
        this.blobContainerAsyncClient = blobContainerAsyncClient;
        this.profiler = profiler;
        this.config = config;
    }

    @Override
    public void downloadFiles(Collection<FileReference> toDownload) throws HyracksDataException {
        try {
            downloadFilesAndWait(toDownload);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void downloadFilesAndWait(Collection<FileReference> toDownload) throws IOException {
        List<Mono<Void>> downloads = new ArrayList<>();
        int maxConcurrent = config.getRequestsMaxPendingHttpConnections();

        for (FileReference fileReference : toDownload) {
            profiler.objectGet();

            Path absPath = Path.of(fileReference.getAbsolutePath());
            Path parentPath = absPath.getParent();
            createDirectories(parentPath);

            BlobAsyncClient blobAsyncClient =
                    blobContainerAsyncClient.getBlobAsyncClient(config.getPrefix() + fileReference.getRelativePath());

            Mono<Void> downloadTask = blobAsyncClient.downloadToFile(absPath.toString()).then();
            downloads.add(downloadTask);

            if (maxConcurrent > 0 && downloads.size() >= maxConcurrent) {
                waitForFileDownloads(downloads);
                downloads.clear();
            }
        }

        if (!downloads.isEmpty()) {
            waitForFileDownloads(downloads);
        }
    }

    private void waitForFileDownloads(List<Mono<Void>> downloads) throws HyracksDataException {
        runBlockingWithExceptionHandling(
                () -> Flux.fromIterable(downloads).flatMap(mono -> mono, downloads.size()).then().block());
    }

    @Override
    public Collection<FileReference> downloadDirectories(Collection<FileReference> directories)
            throws HyracksDataException {

        Set<FileReference> failedFiles = new HashSet<>();
        List<Mono<Void>> directoryDownloads = new ArrayList<>();

        for (FileReference directory : directories) {
            Mono<Void> directoryTask = downloadDirectoryAsync(directory, failedFiles).onErrorResume(e -> Mono.empty()); // Continue even if a directory fails
            directoryDownloads.add(directoryTask);
        }

        runBlockingWithExceptionHandling(() -> Flux.fromIterable(directoryDownloads)
                .flatMap(mono -> mono, config.getRequestsMaxPendingHttpConnections()).then().block());

        return failedFiles;
    }

    private Mono<Void> downloadDirectoryAsync(FileReference directory, Set<FileReference> failedFiles) {
        return getBlobItems(directory).flatMap(blobItem -> {
            profiler.objectGet();
            return downloadBlobAsync(blobItem, failedFiles);
        }, config.getRequestsMaxPendingHttpConnections()).then().doOnError(error -> failedFiles.add(directory)); // Record directory failure
    }

    private Mono<Void> downloadBlobAsync(BlobItem blobItem, Set<FileReference> failedFiles) {
        try {
            // Resolve destination path
            FileReference diskDestFile = ioManager.resolve(createDiskSubPath(blobItem.getName()));
            Path absDiskBlobPath = getDiskDestPath(diskDestFile);
            Path parentDiskPath = absDiskBlobPath.getParent();

            createDirectories(parentDiskPath);

            BlobAsyncClient blobAsyncClient = blobContainerAsyncClient.getBlobAsyncClient(blobItem.getName());

            return blobAsyncClient.downloadToFile(absDiskBlobPath.toString()).doOnError(error -> {
                FileReference failedFile = ioManager.resolve(blobItem.getName());
                failedFiles.add(failedFile);
            }).then();
        } catch (Exception e) {
            failedFiles.add(ioManager.resolve(blobItem.getName()));
            return Mono.error(HyracksDataException.create(e));
        }
    }

    private String createDiskSubPath(String blobName) {
        int idx = blobName.indexOf(STORAGE_SUB_DIR);
        if (idx >= 0) {
            return blobName.substring(idx);
        }
        return blobName;
    }

    private void createDirectories(Path parentPath) throws HyracksDataException {
        if (Files.notExists(parentPath)) {
            try {
                Files.createDirectories(parentPath);
            } catch (IOException ex) {
                throw HyracksDataException.create(ex);
            }
        }
    }

    private Path getDiskDestPath(FileReference destFile) throws HyracksDataException {
        try {
            return Path.of(destFile.getAbsolutePath());
        } catch (InvalidPathException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    private Flux<BlobItem> getBlobItems(FileReference directoryToDownload) {
        ListBlobsOptions listBlobsOptions =
                new ListBlobsOptions().setPrefix(config.getPrefix() + directoryToDownload.getRelativePath());
        return blobContainerAsyncClient.listBlobs(listBlobsOptions);
    }

    @Override
    public void close() {
        // Closing Azure Blob Clients is not required as the underlying netty connection pool
        // handles the same for the apps.
        // Ref: https://github.com/Azure/azure-sdk-for-java/issues/17903
        // Hence this implementation is a no op.
    }

    private static void runBlockingWithExceptionHandling(Runnable runnable) throws HyracksDataException {
        try {
            runnable.run();
        } catch (Exception e) {
            if (ExceptionUtils.causedByInterrupt(e)) {
                Thread.currentThread().interrupt();
            }
            throw HyracksDataException.create(e);
        }
    }
}