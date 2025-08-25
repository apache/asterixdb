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
package org.apache.asterix.cloud.util;

import java.io.FilenameFilter;
import java.util.Iterator;
import java.util.Set;

import org.apache.asterix.cloud.clients.CloudFile;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudFileUtil {
    private static final Logger LOGGER = LogManager.getLogger();

    // TODO Should we consider bloomfilter and LAF files as metadata files so that they are downloaded on bootstrap?
    public static final FilenameFilter METADATA_FILTER =
            ((dir, name) -> name.startsWith(StorageConstants.INDEX_NON_DATA_FILES_PREFIX)
                    || name.endsWith(AbstractLSMIndexFileManager.LAF_SUFFIX)
                    || name.endsWith(AbstractLSMIndexFileManager.BLOOM_FILTER_SUFFIX));
    public static final FilenameFilter DATA_FILTER = ((dir, name) -> !METADATA_FILTER.accept(dir, name));

    private CloudFileUtil() {
    }

    public static void cleanDirectoryFiles(IOManager ioManager, Set<CloudFile> cloudFiles, FileReference partitionPath)
            throws HyracksDataException {
        // First get the set of local files
        Set<FileReference> localFiles = ioManager.list(partitionPath);
        Iterator<FileReference> localFilesIter = localFiles.iterator();
        LOGGER.info("Cleaning partition {}.", partitionPath.getRelativePath());

        // Reconcile local files and cloud files
        while (localFilesIter.hasNext()) {
            FileReference file = localFilesIter.next();
            if (file.getFile().isDirectory()) {
                continue;
            }

            CloudFile path = CloudFile.of(file.getRelativePath(), ioManager.getSize(file));
            if (!cloudFiles.contains(path)) {
                /*
                 * Delete local files that do not exist in cloud storage (the ground truth for valid files), or files
                 * that has not been downloaded completely.
                 */
                logDeleteFile(file, path);
                localFilesIter.remove();
                ioManager.delete(file);
            } else {
                // No need to re-add it in the following loop
                cloudFiles.remove(path);
            }
        }

        // Add the remaining files that are not stored locally (if any)
        for (CloudFile cloudFile : cloudFiles) {
            String cloudFilePath = cloudFile.getPath();
            if (!cloudFilePath.contains(partitionPath.getRelativePath())) {
                continue;
            }
            localFiles.add(new FileReference(partitionPath.getDeviceHandle(),
                    cloudFilePath.substring(cloudFilePath.indexOf(partitionPath.getRelativePath()))));
        }
    }

    private static void logDeleteFile(FileReference fileReference, CloudFile path) {
        LOGGER.info(
                "Deleting {} from the local cache as {} either doesn't exist in the cloud or it wasn't downloaded completely",
                fileReference, path);
    }
}
