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
import java.util.Set;

import org.apache.asterix.cloud.CloudFileHandle;
import org.apache.asterix.cloud.bulk.IBulkOperationCallBack;
import org.apache.asterix.cloud.bulk.NoOpDeleteBulkCallBack;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;

/**
 * LocalAccessor would be used once everything in the cloud storage is cached locally
 */
public class LocalAccessor extends AbstractLazyAccessor {

    public LocalAccessor(ICloudClient cloudClient, String bucket, IOManager localIoManager) {
        super(cloudClient, bucket, localIoManager);
    }

    @Override
    public boolean isLocalAccessor() {
        return true;
    }

    @Override
    public IBulkOperationCallBack getBulkOperationCallBack() {
        return NoOpDeleteBulkCallBack.INSTANCE;
    }

    @Override
    public void doOnOpen(CloudFileHandle fileHandle) throws HyracksDataException {
        // NoOp
    }

    @Override
    public Set<FileReference> doList(FileReference dir, FilenameFilter filter) throws HyracksDataException {
        return localIoManager.list(dir, filter);
    }

    @Override
    public boolean doExists(FileReference fileRef) throws HyracksDataException {
        return localIoManager.exists(fileRef);
    }

    @Override
    public long doGetSize(FileReference fileReference) throws HyracksDataException {
        return localIoManager.getSize(fileReference);
    }

    @Override
    public byte[] doReadAllBytes(FileReference fileReference) throws HyracksDataException {
        return localIoManager.readAllBytes(fileReference);
    }

    @Override
    public void doDelete(FileReference fileReference) throws HyracksDataException {
        doCloudDelete(fileReference);
        localIoManager.delete(fileReference);
    }

    @Override
    public void doOverwrite(FileReference fileReference, byte[] bytes) throws HyracksDataException {
        cloudClient.write(bucket, fileReference.getRelativePath(), bytes);
        localIoManager.overwrite(fileReference, bytes);
    }
}
