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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

public class AzureParallelDownloader implements IParallelDownloader {
    public static final String STORAGE_SUB_DIR = "storage";
    private final IOManager ioManager;
    private final BlobContainerClient blobContainerClient;
    private final IRequestProfilerLimiter profiler;
    private final AzBlobStorageClientConfig config;
    private static final Logger LOGGER = LogManager.getLogger();

    public AzureParallelDownloader(IOManager ioManager, BlobContainerClient blobContainerClient,
            IRequestProfilerLimiter profiler, AzBlobStorageClientConfig config) {
        this.ioManager = ioManager;
        this.blobContainerClient = blobContainerClient;
        this.profiler = profiler;
        this.config = config;
    }

    @Override
    public void downloadFiles(Collection<FileReference> toDownload) throws HyracksDataException {
        for (FileReference fileReference : toDownload) {
            BlobClient blobClient =
                    blobContainerClient.getBlobClient(config.getPrefix() + fileReference.getRelativePath());
            Path absPath = Path.of(fileReference.getAbsolutePath());
            Path parentPath = absPath.getParent();
            OutputStream fileOutputStream = null;
            try {
                createDirectories(parentPath);
                fileOutputStream = Files.newOutputStream(absPath);
                blobClient.downloadStream(fileOutputStream);
                fileOutputStream.close();
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            } finally {
                closeOutputStream(fileOutputStream);
            }
        }
    }

    private static void closeOutputStream(OutputStream fileOutputStream) throws HyracksDataException {
        if (fileOutputStream != null) {
            try {
                fileOutputStream.close();
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    @Override
    public Collection<FileReference> downloadDirectories(Collection<FileReference> directories)
            throws HyracksDataException {
        Set<FileReference> failedFiles = new HashSet<>();
        for (FileReference directory : directories) {
            PagedIterable<BlobItem> blobsInDir = getBlobItems(directory);
            for (BlobItem blobItem : blobsInDir) {
                profiler.objectGet();
                download(blobItem, failedFiles);
            }
        }
        return failedFiles;
    }

    private void download(BlobItem blobItem, Set<FileReference> failedFiles) throws HyracksDataException {
        BlobClient blobClient = blobContainerClient.getBlobClient(blobItem.getName());
        FileReference diskDestFile = ioManager.resolve(createDiskSubPath(blobItem.getName()));
        Path absDiskBlobPath = getDiskDestPath(diskDestFile);
        Path parentDiskPath = absDiskBlobPath.getParent();
        createDirectories(parentDiskPath);
        FileOutputStream outputStreamToDest = getOutputStreamToDest(diskDestFile);
        try {
            blobClient.downloadStream(outputStreamToDest);
        } catch (Exception e) {
            FileReference failedFile = ioManager.resolve(blobItem.getName());
            failedFiles.add(failedFile);
        }
    }

    private String createDiskSubPath(String blobName) {
        if (!blobName.startsWith(STORAGE_SUB_DIR)) {
            blobName = blobName.substring(blobName.indexOf(STORAGE_SUB_DIR));
        }
        return blobName;
    }

    private FileOutputStream getOutputStreamToDest(FileReference destFile) throws HyracksDataException {
        try {
            return new FileOutputStream(destFile.getAbsolutePath());
        } catch (FileNotFoundException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    private void createDirectories(Path parentPath) throws HyracksDataException {
        if (Files.notExists(parentPath))
            try {
                Files.createDirectories(parentPath);
            } catch (IOException ex) {
                throw HyracksDataException.create(ex);
            }
    }

    private Path getDiskDestPath(FileReference destFile) throws HyracksDataException {
        try {
            return Path.of(destFile.getAbsolutePath());
        } catch (InvalidPathException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    private PagedIterable<BlobItem> getBlobItems(FileReference directoryToDownload) {
        ListBlobsOptions listBlobsOptions =
                new ListBlobsOptions().setPrefix(config.getPrefix() + directoryToDownload.getRelativePath());
        return blobContainerClient.listBlobs(listBlobsOptions, null);
    }

    @Override
    public void close() {
        // Closing Azure Blob Clients is not required as the underlying netty connection pool
        // handles the same for the apps.
        // Ref: https://github.com/Azure/azure-sdk-for-java/issues/17903
        // Hence this implementation is a no op.
    }
}
