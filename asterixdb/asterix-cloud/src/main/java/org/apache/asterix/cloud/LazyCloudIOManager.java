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
package org.apache.asterix.cloud;

import static org.apache.asterix.common.utils.StorageConstants.PARTITION_DIR_PREFIX;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.cloud.util.CloudFileUtil;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CloudIOManager with lazy caching
 * - Overrides some of {@link IOManager} functions
 */
class LazyCloudIOManager extends AbstractCloudIOManager {
    private static final Logger LOGGER = LogManager.getLogger();

    public LazyCloudIOManager(IOManager ioManager, CloudProperties cloudProperties) throws HyracksDataException {
        super(ioManager, cloudProperties);
    }

    /*
     * ******************************************************************
     * AbstractCloudIOManager functions
     * ******************************************************************
     */

    @Override
    protected void downloadPartitions() {
        // NoOp
    }

    @Override
    protected void onOpen(CloudFileHandle fileHandle, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException {
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
        }
    }

    /*
     * ******************************************************************
     * IIOManager functions
     * ******************************************************************
     */
    @Override
    public Set<FileReference> list(FileReference dir, FilenameFilter filter) throws HyracksDataException {
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
            FileReference localFile = resolve(cloudFile);
            if (isInNodePartition(cloudFile) && dir.getDeviceHandle().equals(localFile.getDeviceHandle())) {
                localFiles.add(localFile);
            }
        }
        return new HashSet<>(localFiles);
    }

    @Override
    public long getSize(FileReference fileReference) throws HyracksDataException {
        if (localIoManager.exists(fileReference)) {
            return localIoManager.getSize(fileReference);
        }
        return cloudClient.getObjectSize(bucket, fileReference.getRelativePath());
    }

    @Override
    public byte[] readAllBytes(FileReference fileRef) throws HyracksDataException {
        if (!localIoManager.exists(fileRef) && isInNodePartition(fileRef.getRelativePath())) {
            byte[] bytes = cloudClient.readAllBytes(bucket, fileRef.getRelativePath());
            if (bytes != null && !partitions.isEmpty()) {
                localIoManager.overwrite(fileRef, bytes);
            }
            return bytes;
        }
        return localIoManager.readAllBytes(fileRef);
    }

    private boolean isInNodePartition(String path) {
        int start = path.indexOf(PARTITION_DIR_PREFIX) + PARTITION_DIR_PREFIX.length();
        int length = path.indexOf(File.separatorChar, start);
        return partitions.contains(Integer.parseInt(path.substring(start, length)));
    }
}
