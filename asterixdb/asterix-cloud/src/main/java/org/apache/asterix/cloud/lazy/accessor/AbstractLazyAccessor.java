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

import static org.apache.asterix.common.utils.StorageConstants.STORAGE_ROOT_DIR_NAME;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.IOManager;

abstract class AbstractLazyAccessor implements ILazyAccessor {
    protected final ICloudClient cloudClient;
    protected final String bucket;
    protected final IOManager localIoManager;

    AbstractLazyAccessor(ICloudClient cloudClient, String bucket, IOManager localIoManager) {
        this.cloudClient = cloudClient;
        this.bucket = bucket;
        this.localIoManager = localIoManager;
    }

    Set<FileReference> doCloudDelete(FileReference fileReference) throws HyracksDataException {
        Set<FileReference> deletedFiles = Collections.emptySet();
        if (!STORAGE_ROOT_DIR_NAME.equals(IoUtil.getFileNameFromPath(fileReference.getAbsolutePath()))) {
            File localFile = fileReference.getFile();

            Set<String> paths;
            if (localFile.exists() && localFile.isFile()) {
                // If file reference exists, and it is a file, then list is not required
                paths = Collections.singleton(fileReference.getRelativePath());
            } else {
                // List and delete
                deletedFiles = doList(fileReference, IoUtil.NO_OP_FILTER);
                paths = deletedFiles.stream().map(FileReference::getRelativePath).collect(Collectors.toSet());
            }

            cloudClient.deleteObjects(bucket, paths);
        }
        return deletedFiles;
    }
}
