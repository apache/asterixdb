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
package org.apache.asterix.cloud.lazy.accessor;

import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.cloud.CloudFileHandle;
import org.apache.asterix.cloud.WriteBufferProvider;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.util.CloudFileUtil;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ReplaceableCloudAccessor will be used when some (or all) of the files in the cloud storage are not cached locally.
 * It will be replaced by {@link LocalAccessor} once everything is cached
 */
public class ReplaceableCloudAccessor extends AbstractLazyAccessor {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Set<Integer> partitions;
    private final AtomicInteger numberOfUncachedFiles;
    private final WriteBufferProvider writeBufferProvider;
    private final ILazyAccessorReplacer replacer;

    public ReplaceableCloudAccessor(ICloudClient cloudClient, String bucket, IOManager localIoManager,
            Set<Integer> partitions, int numberOfUncachedFiles, WriteBufferProvider writeBufferProvider,
            ILazyAccessorReplacer replacer) {
        super(cloudClient, bucket, localIoManager);
        this.partitions = partitions;
        this.numberOfUncachedFiles = new AtomicInteger(numberOfUncachedFiles);
        this.writeBufferProvider = writeBufferProvider;
        this.replacer = replacer;
    }

    @Override
    public boolean isLocalAccessor() {
        return false;
    }

    @Override
    public void doOnOpen(CloudFileHandle fileHandle, IIOManager.FileReadWriteMode rwMode,
            IIOManager.FileSyncMode syncMode) throws HyracksDataException {
        FileReference fileRef = fileHandle.getFileReference();
        if (!localIoManager.exists(fileRef) && cloudClient.exists(bucket, fileRef.getRelativePath())) {
            // File doesn't exist locally, download it.
            ByteBuffer writeBuffer = writeBufferProvider.getBuffer();
            try {
                // TODO download for all partitions at once
                LOGGER.info("Downloading {} from S3..", fileRef.getRelativePath());
                CloudFileUtil.downloadFile(localIoManager, cloudClient, bucket, fileHandle, rwMode, syncMode,
                        writeBuffer);
                localIoManager.close(fileHandle);
                LOGGER.info("Finished downloading {} from S3..", fileRef.getRelativePath());
            } finally {
                writeBufferProvider.recycle(writeBuffer);
            }
            // TODO decrement by the number of downloaded files in all partitions (once the above TODO is fixed)
            decrementNumberOfUncachedFiles();
        }
    }

    @Override
    public Set<FileReference> doList(FileReference dir, FilenameFilter filter) throws HyracksDataException {
        Set<String> cloudFiles = cloudClient.listObjects(bucket, dir.getRelativePath(), filter);
        if (cloudFiles.isEmpty()) {
            return Collections.emptySet();
        }

        // First get the set of local files
        Set<FileReference> localFiles = localIoManager.list(dir, filter);

        // Reconcile local files and cloud files
        for (FileReference file : localFiles) {
            String path = file.getRelativePath();
            if (!cloudFiles.contains(path)) {
                throw new IllegalStateException("Local file is not clean");
            } else {
                // No need to re-add it in the following loop
                cloudFiles.remove(path);
            }
        }

        // Add the remaining files that are not stored locally in their designated partitions (if any)
        for (String cloudFile : cloudFiles) {
            FileReference localFile = localIoManager.resolve(cloudFile);
            if (isInNodePartition(cloudFile) && dir.getDeviceHandle().equals(localFile.getDeviceHandle())) {
                localFiles.add(localFile);
            }
        }
        return localFiles;
    }

    @Override
    public boolean doExists(FileReference fileRef) throws HyracksDataException {
        return localIoManager.exists(fileRef) || cloudClient.exists(bucket, fileRef.getRelativePath());
    }

    @Override
    public long doGetSize(FileReference fileReference) throws HyracksDataException {
        if (localIoManager.exists(fileReference)) {
            return localIoManager.getSize(fileReference);
        }
        return cloudClient.getObjectSize(bucket, fileReference.getRelativePath());
    }

    @Override
    public byte[] doReadAllBytes(FileReference fileRef) throws HyracksDataException {
        if (!localIoManager.exists(fileRef) && isInNodePartition(fileRef.getRelativePath())) {
            byte[] bytes = cloudClient.readAllBytes(bucket, fileRef.getRelativePath());
            if (bytes != null && !partitions.isEmpty()) {
                // Download the missing file for subsequent reads
                localIoManager.overwrite(fileRef, bytes);
                decrementNumberOfUncachedFiles();
            }
            return bytes;
        }
        return localIoManager.readAllBytes(fileRef);
    }

    @Override
    public void doDelete(FileReference fileReference) throws HyracksDataException {
        // Never delete the storage dir in cloud storage
        int numberOfCloudDeletes = doCloudDelete(fileReference);
        // check local
        if (numberOfCloudDeletes > 0 && localIoManager.exists(fileReference)) {
            int numberOfLocalDeletes = fileReference.getFile().isFile() ? 1 : localIoManager.list(fileReference).size();
            // Decrement by number of cloud deletes that have no counterparts locally
            decrementNumberOfUncachedFiles(numberOfCloudDeletes - numberOfLocalDeletes);
        }

        // Finally, delete locally
        localIoManager.delete(fileReference);
    }

    @Override
    public void doOverwrite(FileReference fileReference, byte[] bytes) throws HyracksDataException {
        boolean existsLocally = localIoManager.exists(fileReference);
        cloudClient.write(bucket, fileReference.getRelativePath(), bytes);
        localIoManager.overwrite(fileReference, bytes);

        if (!existsLocally) {
            decrementNumberOfUncachedFiles();
        }
    }

    protected void decrementNumberOfUncachedFiles() {
        replaceAccessor(numberOfUncachedFiles.decrementAndGet());
    }

    protected void decrementNumberOfUncachedFiles(int count) {
        if (count > 0) {
            replaceAccessor(numberOfUncachedFiles.addAndGet(-count));
        }
    }

    private boolean isInNodePartition(String path) {
        return partitions.contains(StoragePathUtil.getPartitionNumFromRelativePath(path));
    }

    void replaceAccessor(int remainingUncached) {
        if (remainingUncached > 0) {
            // Some files still not cached yet
            return;
        }

        if (remainingUncached < 0) {
            // This should not happen, log in case that happen
            LOGGER.warn("Some files were downloaded multiple times. Reported remaining uncached files = {}",
                    remainingUncached);
        }
        replacer.replace();
    }
}
