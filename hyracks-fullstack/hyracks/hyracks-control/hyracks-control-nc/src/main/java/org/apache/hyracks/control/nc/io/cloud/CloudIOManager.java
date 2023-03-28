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
package org.apache.hyracks.control.nc.io.cloud;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.io.cloud.clients.CloudClientProvider;
import org.apache.hyracks.control.nc.io.cloud.clients.ICloudClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudIOManager extends IOManager {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ICloudClient cloudClient;
    private final WriteBufferProvider writeBufferProvider;
    private final String bucket;

    // TODO(htowaileb): temporary, will need to be read from somewhere
    public static final String STORAGE_ROOT_DIR_NAME = "storage";

    public CloudIOManager(IIOManager ioManager, int queueSize, int ioParallelism) throws HyracksDataException {
        super((IOManager) ioManager, queueSize, ioParallelism);

        // TODO(htowaileb): temporary, this needs to be provided somehow
        try {
            List<String> lines = FileUtils.readLines(new File("/etc/s3"), "UTF-8");
            bucket = lines.get(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        cloudClient = CloudClientProvider.getClient(CloudClientProvider.ClientType.S3);
        int numOfThreads = ioManager.getIODevices().size() * ioParallelism;
        writeBufferProvider = new WriteBufferProvider(numOfThreads);
    }

    public String getBucket() {
        return bucket;
    }

    @Override
    public long doSyncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray) throws HyracksDataException {
        long writtenBytes = super.doSyncWrite(fHandle, offset, dataArray);
        CloudResettableInputStream inputStream = ((CloudFileHandle) fHandle).getInputStream();
        try {
            inputStream.write(dataArray[0], dataArray[1]);
        } catch (HyracksDataException e) {
            inputStream.abort();
            throw e;
        }

        return writtenBytes;
    }

    @Override
    public int doSyncWrite(IFileHandle fHandle, long offset, ByteBuffer dataArray) throws HyracksDataException {
        int writtenBytes = super.doSyncWrite(fHandle, offset, dataArray);
        CloudResettableInputStream cloudInputStream = ((CloudFileHandle) fHandle).getInputStream();
        try {
            cloudInputStream.write(dataArray);
        } catch (HyracksDataException e) {
            cloudInputStream.abort();
            throw e;
        }

        return writtenBytes;
    }

    @Override
    public IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException {
        CloudFileHandle fHandle = new CloudFileHandle(cloudClient, bucket, fileRef, writeBufferProvider);
        if (!super.exists(fileRef) && cloudClient.exists(bucket, fileRef.getRelativePath())) {
            ByteBuffer writeBuffer = writeBufferProvider.getBuffer();
            try {
                // TODO: We need a proper caching mechanism
                LOGGER.info("Downloading {} from cloud storage..", fileRef.getRelativePath());
                LocalCacheUtil.download(cloudClient, this, fHandle, rwMode, syncMode, writeBuffer);
                super.close(fHandle);
                LOGGER.info("Finished downloading {} from cloud storage..", fileRef.getRelativePath());
            } finally {
                writeBufferProvider.recycle(writeBuffer);
            }
        }

        try {
            fHandle.open(rwMode, syncMode);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return fHandle;
    }

    @Override
    public void delete(FileReference fileRef) throws HyracksDataException {
        if (!STORAGE_ROOT_DIR_NAME.equals(IoUtil.getFileNameFromPath(fileRef.getAbsolutePath()))) {
            // Never delete the storage dir in cloud storage
            cloudClient.deleteObject(bucket, fileRef.getRelativePath());
        }
        super.delete(fileRef);
    }

    @Override
    public void close(IFileHandle fHandle) throws HyracksDataException {
        try {
            ((CloudFileHandle) fHandle).getInputStream().close();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        super.close(fHandle);
    }

    @Override
    public Set<FileReference> list(FileReference dir, FilenameFilter filter) throws HyracksDataException {
        Set<String> cloudFiles = cloudClient.listObjects(bucket, dir.getRelativePath(), filter);
        if (cloudFiles.isEmpty()) {
            return Collections.emptySet();
        }

        // First get the set of local files
        Set<FileReference> localFiles = super.list(dir, filter);
        Iterator<FileReference> localFilesIter = localFiles.iterator();

        // Reconcile local files and cloud files
        while (localFilesIter.hasNext()) {
            FileReference file = localFilesIter.next();
            if (file.getFile().isDirectory()) {
                continue;
            }

            String path = file.getRelativePath();
            if (!cloudFiles.contains(path)) {
                // Delete local files that do not exist in cloud storage (the ground truth for valid files)
                localFilesIter.remove();
                super.delete(file);
            } else {
                // No need to re-add it in the following loop
                cloudFiles.remove(path);
            }
        }

        // Add the remaining files that are not stored locally (if any)
        for (String cloudFile : cloudFiles) {
            localFiles.add(dir.getChild(IoUtil.getFileNameFromPath(cloudFile)));
        }
        return new HashSet<>(localFiles);
    }

    @Override
    public void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException {
        HyracksDataException savedEx = null;
        if (metadata) {
            // only finish writing if metadata == true to prevent write limiter from finishing the stream and
            // completing the upload.
            CloudResettableInputStream stream = ((CloudFileHandle) fileHandle).getInputStream();
            try {
                stream.finish();
            } catch (HyracksDataException e) {
                savedEx = e;
            }

            if (savedEx != null) {
                try {
                    stream.abort();
                } catch (HyracksDataException e) {
                    savedEx.addSuppressed(e);
                }
                throw savedEx;
            }
        }
        // Sync only after finalizing the upload to cloud storage
        super.sync(fileHandle, metadata);
    }

    @Override
    public long getSize(IFileHandle fileHandle) {
        if (fileHandle.getFileReference().getFile().exists()) {
            // This should always provide the correct size despite what is buffered in local disk
            return super.getSize(fileHandle);
        }
        return cloudClient.getObjectSize(bucket, fileHandle.getFileReference().getRelativePath());
    }

    @Override
    public void overwrite(FileReference fileRef, byte[] bytes) throws ClosedByInterruptException, HyracksDataException {
        super.overwrite(fileRef, bytes);
        // Write here will overwrite the older object if exists
        cloudClient.write(bucket, fileRef.getRelativePath(), bytes);
    }

    // TODO utilize locally stored files for reading
    @Override
    public int doSyncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        if (fHandle.getFileReference().getFile().exists()) {
            return super.doSyncRead(fHandle, offset, data);
        }
        return cloudClient.read(bucket, fHandle.getFileReference().getRelativePath(), offset, data);
    }

    // TODO: We need to download this too
    @Override
    public byte[] readAllBytes(FileReference fileRef) throws HyracksDataException {
        if (fileRef.getFile().exists()) {
            return super.readAllBytes(fileRef);
        }
        return cloudClient.readAllBytes(bucket, fileRef.getRelativePath());
    }

    @Override
    public void deleteDirectory(FileReference fileRef) throws HyracksDataException {
        super.deleteDirectory(fileRef);
        if (!STORAGE_ROOT_DIR_NAME.equals(IoUtil.getFileNameFromPath(fileRef.getAbsolutePath()))) {
            // Never delete the storage dir in cloud storage
            cloudClient.deleteObject(bucket, fileRef.getRelativePath());
        }
    }

    @Override
    public Collection<FileReference> getMatchingFiles(FileReference root, FilenameFilter filter) {
        Set<String> paths = cloudClient.listObjects(bucket, root.getRelativePath(), filter);
        List<FileReference> fileReferences = new ArrayList<>();
        for (String path : paths) {
            fileReferences.add(new FileReference(root.getDeviceHandle(), path));
        }
        return fileReferences;
    }

    @Override
    public boolean exists(FileReference fileRef) {
        // Check if the file exists locally first as newly created files (i.e., they are empty) are not stored in cloud storage
        return fileRef.getFile().exists() || cloudClient.exists(bucket, fileRef.getRelativePath());
    }

    @Override
    public void create(FileReference fileRef) throws HyracksDataException {
        // We need to delete the local file on create as the cloud storage didn't complete the upload
        // In other words, both cloud files and the local files are not in sync
        super.delete(fileRef);
        super.create(fileRef);
    }

    @Override
    public void copyDirectory(FileReference srcFileRef, FileReference destFileRef) throws HyracksDataException {
        cloudClient.copy(bucket, srcFileRef.getRelativePath(), destFileRef);
        super.copyDirectory(srcFileRef, destFileRef);
    }

    protected long writeLocally(IFileHandle fHandle, long offset, ByteBuffer buffer) throws HyracksDataException {
        return super.doSyncWrite(fHandle, offset, buffer);
    }

    protected void syncLocally(IFileHandle fileHandle) throws HyracksDataException {
        super.sync(fileHandle, true);
    }
}
