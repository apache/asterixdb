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

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * CloudIOManager with eager caching
 * {@link org.apache.hyracks.api.io.IIOManager} methods are implemented either in
 * - {@link IOManager}
 * OR
 * - {@link AbstractCloudIOManager}
 */
final class EagerCloudIOManager extends AbstractCloudIOManager {
    private static final Logger LOGGER = LogManager.getLogger();

    public EagerCloudIOManager(IOManager ioManager, CloudProperties cloudProperties,
            INamespacePathResolver nsPathResolver) throws HyracksDataException {
        super(ioManager, cloudProperties, nsPathResolver);
    }

    /*
     * ******************************************************************
     * AbstractCloudIOManager functions
     * ******************************************************************
     */

    @Override
    protected void downloadPartitions(boolean metadataNode, int metadataPartition) throws HyracksDataException {
        IParallelDownloader downloader = cloudClient.createParallelDownloader(bucket, localIoManager);
        LOGGER.info("Downloading all files located in {}", partitionPaths);
        downloader.downloadDirectories(partitionPaths);
        LOGGER.info("Finished downloading {}", partitionPaths);
    }

    @Override
    protected void onOpen(CloudFileHandle fileHandle, FileReadWriteMode rwMode, FileSyncMode syncMode) {
        // NoOp
    }

    @Override
    public boolean exists(FileReference fileRef) throws HyracksDataException {
        return localIoManager.exists(fileRef);
    }

    @Override
    public void delete(FileReference fileRef) throws HyracksDataException {
        // Never delete the storage dir in cloud storage
        if (!STORAGE_ROOT_DIR_NAME.equals(IoUtil.getFileNameFromPath(fileRef.getAbsolutePath()))) {
            File localFile = fileRef.getFile();
            // if file reference exists,and it is a file, then list is not required
            Set<String> paths =
                    localFile.exists() && localFile.isFile() ? Collections.singleton(fileRef.getRelativePath())
                            : list(fileRef).stream().map(FileReference::getRelativePath).collect(Collectors.toSet());
            cloudClient.deleteObjects(bucket, paths);
        }
        localIoManager.delete(fileRef);
    }

    @Override
    public void overwrite(FileReference fileRef, byte[] bytes) throws HyracksDataException {
        // Write here will overwrite the older object if exists
        cloudClient.write(bucket, fileRef.getRelativePath(), bytes);
        localIoManager.overwrite(fileRef, bytes);
    }
}
