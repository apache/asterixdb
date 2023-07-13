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

import static org.apache.asterix.common.utils.StorageConstants.STORAGE_ROOT_DIR_NAME;

import java.io.FilenameFilter;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.cloud.lazy.accessor.ILazyAccessor;
import org.apache.asterix.cloud.lazy.accessor.ILazyAccessorReplacer;
import org.apache.asterix.cloud.lazy.accessor.InitialCloudAccessor;
import org.apache.asterix.cloud.lazy.accessor.LocalAccessor;
import org.apache.asterix.cloud.lazy.accessor.ReplaceableCloudAccessor;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CloudIOManager with lazy caching
 * - Overrides some of {@link IOManager} functions
 * Note: once everything is cached, this will eventually be similar to {@link EagerCloudIOManager}
 */
final class LazyCloudIOManager extends AbstractCloudIOManager {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ILazyAccessorReplacer replacer;
    private ILazyAccessor accessor;

    public LazyCloudIOManager(IOManager ioManager, CloudProperties cloudProperties) throws HyracksDataException {
        super(ioManager, cloudProperties);
        accessor = new InitialCloudAccessor(cloudClient, bucket, localIoManager, writeBufferProvider);
        replacer = () -> {
            synchronized (this) {
                if (!accessor.isLocalAccessor()) {
                    LOGGER.warn("Replacing cloud-accessor to local-accessor");
                    accessor = new LocalAccessor(cloudClient, bucket, localIoManager);
                }
            }
        };
    }

    /*
     * ******************************************************************
     * AbstractCloudIOManager functions
     * ******************************************************************
     */

    @Override
    protected void downloadPartitions() throws HyracksDataException {
        // Get the files in all relevant partitions from the cloud
        Set<String> cloudFiles = cloudClient.listObjects(bucket, STORAGE_ROOT_DIR_NAME, IoUtil.NO_OP_FILTER).stream()
                .filter(f -> partitions.contains(StoragePathUtil.getPartitionNumFromRelativePath(f)))
                .collect(Collectors.toSet());

        // Get all files stored locally
        Set<String> localFiles = new HashSet<>();
        for (IODeviceHandle deviceHandle : getIODevices()) {
            FileReference storageRoot = deviceHandle.createFileRef(STORAGE_ROOT_DIR_NAME);
            Set<FileReference> deviceFiles = localIoManager.list(storageRoot, IoUtil.NO_OP_FILTER);
            for (FileReference fileReference : deviceFiles) {
                localFiles.add(fileReference.getRelativePath());
            }
        }

        // Keep uncached files list (i.e., files exists in cloud only)
        cloudFiles.removeAll(localFiles);
        int remainingUncachedFiles = cloudFiles.size();
        if (remainingUncachedFiles > 0) {
            // Local cache misses some files, cloud-based accessor is needed for read operations
            accessor = new ReplaceableCloudAccessor(cloudClient, bucket, localIoManager, partitions,
                    remainingUncachedFiles, writeBufferProvider, replacer);
        } else {
            // Everything is cached, no need to invoke cloud-based accessor for read operations
            accessor = new LocalAccessor(cloudClient, bucket, localIoManager);
        }
        LOGGER.info("The number of uncached files: {}", remainingUncachedFiles);
    }

    @Override
    protected void onOpen(CloudFileHandle fileHandle, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException {
        accessor.doOnOpen(fileHandle, rwMode, syncMode);
    }

    /*
     * ******************************************************************
     * IIOManager functions
     * ******************************************************************
     */
    @Override
    public Set<FileReference> list(FileReference dir, FilenameFilter filter) throws HyracksDataException {
        return accessor.doList(dir, filter);
    }

    @Override
    public boolean exists(FileReference fileRef) throws HyracksDataException {
        return accessor.doExists(fileRef);
    }

    @Override
    public long getSize(FileReference fileReference) throws HyracksDataException {
        return accessor.doGetSize(fileReference);
    }

    @Override
    public byte[] readAllBytes(FileReference fileRef) throws HyracksDataException {
        return accessor.doReadAllBytes(fileRef);
    }

    @Override
    public void delete(FileReference fileRef) throws HyracksDataException {
        accessor.doDelete(fileRef);
    }

    @Override
    public void overwrite(FileReference fileRef, byte[] bytes) throws HyracksDataException {
        accessor.doOverwrite(fileRef, bytes);
    }
}
