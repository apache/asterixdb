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
import java.util.Collections;
import java.util.Set;

import org.apache.asterix.cloud.CloudFileHandle;
import org.apache.asterix.cloud.bulk.IBulkOperationCallBack;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.lazy.IParallelCacher;
import org.apache.asterix.common.utils.StorageConstants;
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
    private final ILazyAccessorReplacer replacer;
    private final IParallelCacher cacher;
    private final IBulkOperationCallBack deleteCallBack;

    public ReplaceableCloudAccessor(ICloudClient cloudClient, String bucket, IOManager localIoManager,
            Set<Integer> partitions, ILazyAccessorReplacer replacer, IParallelCacher cacher) {
        super(cloudClient, bucket, localIoManager);
        this.partitions = partitions;
        this.replacer = replacer;
        this.cacher = cacher;
        deleteCallBack = deletedFiles -> {
            if (cacher.remove(deletedFiles)) {
                replace();
            }
        };
    }

    @Override
    public boolean isLocalAccessor() {
        return false;
    }

    @Override
    public IBulkOperationCallBack getBulkOperationCallBack() {
        return deleteCallBack;
    }

    @Override
    public void doOnOpen(CloudFileHandle fileHandle, IIOManager.FileReadWriteMode rwMode,
            IIOManager.FileSyncMode syncMode) throws HyracksDataException {
        FileReference fileRef = fileHandle.getFileReference();
        if (!localIoManager.exists(fileRef) && cloudClient.exists(bucket, fileRef.getRelativePath())) {
            if (cacher.downloadData(fileRef)) {
                replace();
            }
        }
    }

    @Override
    public Set<FileReference> doList(FileReference dir, FilenameFilter filter) throws HyracksDataException {
        if (isTxnDir(dir)) {
            return cloudBackedList(dir, filter);
        }
        Set<FileReference> localList = localIoManager.list(dir, filter);
        Set<FileReference> uncachedFiles = cacher.getUncachedFiles(dir, filter);
        localList.addAll(uncachedFiles);
        return localList;
    }

    private static boolean isTxnDir(FileReference dir) {
        return dir.getRelativePath().startsWith(StorageConstants.METADATA_TXN_NOWAL_DIR_NAME)
                || dir.getName().equals(StorageConstants.GLOBAL_TXN_DIR_NAME);
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
            if (cacher.downloadMetadata(fileRef)) {
                replace();
            }
        }
        return localIoManager.readAllBytes(fileRef);
    }

    @Override
    public void doDelete(FileReference fileReference) throws HyracksDataException {
        // Never delete the storage dir in cloud storage
        Set<FileReference> deletedFiles = doCloudDelete(fileReference);
        if (cacher.remove(deletedFiles)) {
            replace();
        }
        // Finally, delete locally
        localIoManager.delete(fileReference);
    }

    @Override
    public void doOverwrite(FileReference fileReference, byte[] bytes) throws HyracksDataException {
        boolean existsLocally = localIoManager.exists(fileReference);
        cloudClient.write(bucket, fileReference.getRelativePath(), bytes);
        localIoManager.overwrite(fileReference, bytes);
        if (!existsLocally && cacher.remove(fileReference)) {
            replace();
        }
    }

    private Set<FileReference> cloudBackedList(FileReference dir, FilenameFilter filter) throws HyracksDataException {
        LOGGER.debug("CLOUD LIST: {}", dir);
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
                throw new IllegalStateException("Local file is not clean. Offending path: " + path);
            } else {
                // No need to re-add it in the following loop
                cloudFiles.remove(path);
            }
        }

        // Add the remaining files that are not stored locally in their designated partitions (if any)
        for (String cloudFile : cloudFiles) {
            FileReference localFile = localIoManager.resolve(cloudFile);
            if (isInNodePartition(cloudFile) && StoragePathUtil.hasSameStorageRoot(dir, localFile)) {
                localFiles.add(localFile);
            }
        }
        return localFiles;
    }

    private boolean isInNodePartition(String path) {
        return partitions.contains(StoragePathUtil.getPartitionNumFromRelativePath(path));
    }

    private void replace() {
        cacher.close();
        replacer.replace();
    }
}
