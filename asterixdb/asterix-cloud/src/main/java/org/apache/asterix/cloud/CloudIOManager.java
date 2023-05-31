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
import static org.apache.asterix.common.utils.StorageConstants.STORAGE_ROOT_DIR_NAME;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.cloud.clients.CloudClientProvider;
import org.apache.asterix.cloud.clients.CloudClientProvider.ClientType;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.ICloudClientCredentialsProvider.CredentialsType;
import org.apache.asterix.cloud.storage.CloudStorageConfigurationProvider;
import org.apache.asterix.cloud.storage.ICloudStorageConfiguration;
import org.apache.asterix.cloud.storage.ICloudStorageConfiguration.ConfigurationType;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileDeviceResolver;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.control.nc.io.FileHandle;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudIOManager extends IOManager {
    private static final Logger LOGGER = LogManager.getLogger();
    private final ICloudClient cloudClient;
    private final WriteBufferProvider writeBufferProvider;
    private final String bucket;
    private IOManager localIoManager;

    private CloudIOManager(List<IODeviceHandle> devices, IFileDeviceResolver deviceComputer, int ioParallelism,
            int queueSize) throws HyracksDataException {
        super(devices, deviceComputer, ioParallelism, queueSize);
        ICloudStorageConfiguration cloudStorageConfiguration =
                CloudStorageConfigurationProvider.INSTANCE.getConfiguration(ConfigurationType.FILE);
        this.bucket = cloudStorageConfiguration.getContainer();
        cloudClient = CloudClientProvider.getClient(ClientType.S3, CredentialsType.FILE);
        int numOfThreads = getIODevices().size() * getIoParallelism();
        writeBufferProvider = new WriteBufferProvider(numOfThreads);
    }

    public CloudIOManager(IOManager ioManager) throws HyracksDataException {
        this(ioManager.getIoDevices(), ioManager.getDeviceComputer(), ioManager.getIoParallelism(),
                ioManager.getQueueSize());
        this.localIoManager = ioManager;
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
        CloudResettableInputStream inputStream = ((CloudFileHandle) fHandle).getInputStream();
        try {
            inputStream.write(dataArray);
        } catch (HyracksDataException e) {
            inputStream.abort();
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
                LOGGER.info("Downloading {} from S3..", fileRef.getRelativePath());
                downloadFile(fHandle, rwMode, syncMode, writeBuffer);
                super.close(fHandle);
                LOGGER.info("Finished downloading {} from S3..", fileRef.getRelativePath());
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

    // TODO This method should not do any syncing. It simply should list the files
    @Override
    public Set<FileReference> list(FileReference dir, FilenameFilter filter) throws HyracksDataException {
        Set<String> cloudFiles = cloudClient.listObjects(bucket, dir.getRelativePath(), filter);
        if (cloudFiles.isEmpty()) {
            // TODO(htowaileb): Can we end up in a state where local has files but cloud does not?
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
            localFiles.add(new FileReference(dir.getDeviceHandle(),
                    cloudFile.substring(cloudFile.indexOf(dir.getRelativePath()))));
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
    public long getSize(IFileHandle fileHandle) throws HyracksDataException {
        if (!fileHandle.getFileReference().getFile().exists()) {
            return cloudClient.getObjectSize(bucket, fileHandle.getFileReference().getRelativePath());
        }
        return super.getSize(fileHandle);
    }

    @Override
    public long getSize(FileReference fileReference) throws HyracksDataException {
        if (!fileReference.getFile().exists()) {
            return cloudClient.getObjectSize(bucket, fileReference.getRelativePath());
        }
        return super.getSize(fileReference);
    }

    @Override
    public void overwrite(FileReference fileRef, byte[] bytes) throws ClosedByInterruptException, HyracksDataException {
        super.overwrite(fileRef, bytes);
        // Write here will overwrite the older object if exists
        cloudClient.write(bucket, fileRef.getRelativePath(), bytes);
    }

    @Override
    public int doSyncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        return super.doSyncRead(fHandle, offset, data);
    }

    @Override
    public byte[] readAllBytes(FileReference fileRef) throws HyracksDataException {
        if (!fileRef.getFile().exists()) {
            IFileHandle open = open(fileRef, FileReadWriteMode.READ_WRITE, FileSyncMode.METADATA_SYNC_DATA_SYNC);
            fileRef = open.getFileReference();
        }
        return super.readAllBytes(fileRef);
    }

    @Override
    public void deleteDirectory(FileReference fileRef) throws HyracksDataException {
        // TODO(htowaileb): Should we delete the cloud first?
        super.deleteDirectory(fileRef);
        if (!STORAGE_ROOT_DIR_NAME.equals(IoUtil.getFileNameFromPath(fileRef.getAbsolutePath()))) {
            // Never delete the storage dir in cloud storage
            cloudClient.deleteObject(bucket, fileRef.getRelativePath());
        }
    }

    @Override
    public boolean exists(FileReference fileRef) throws HyracksDataException {
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

    @Override
    public void syncFiles(Set<Integer> activePartitions) throws HyracksDataException {
        Map<String, String> cloudToLocalStoragePaths = new HashMap<>();
        for (Integer partition : activePartitions) {
            String partitionToFind = PARTITION_DIR_PREFIX + partition + "/";
            IODeviceHandle deviceHandle = getDeviceComputer().resolve(partitionToFind, getIODevices());

            String cloudStoragePath = STORAGE_ROOT_DIR_NAME + "/" + partitionToFind;
            String localStoragePath = deviceHandle.getMount().getAbsolutePath() + "/" + cloudStoragePath;
            cloudToLocalStoragePaths.put(cloudStoragePath, localStoragePath);
        }
        LOGGER.info("Resolved paths to io devices: {}", cloudToLocalStoragePaths);
        cloudClient.syncFiles(bucket, cloudToLocalStoragePaths);
    }

    // TODO(htowaileb): the localIoManager is closed by the node controller service as well, check if we need this
    @Override
    public void close() throws IOException {
        cloudClient.close();
        super.close();
        localIoManager.close();
    }

    private void downloadFile(FileHandle fileHandle, FileReadWriteMode rwMode, FileSyncMode syncMode,
            ByteBuffer writeBuffer) throws HyracksDataException {
        FileReference fileRef = fileHandle.getFileReference();
        File file = fileRef.getFile();

        try (InputStream inputStream = cloudClient.getObjectStream(bucket, fileRef.getRelativePath())) {
            FileUtils.createParentDirectories(file);
            if (!file.createNewFile()) {
                throw new IllegalStateException("Couldn't create local file");
            }

            fileHandle.open(rwMode, syncMode);
            writeToFile(fileHandle, inputStream, writeBuffer);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void writeToFile(IFileHandle fileHandle, InputStream inStream, ByteBuffer writeBuffer)
            throws HyracksDataException {
        writeBuffer.clear();
        try {
            int position = 0;
            long offset = 0;
            int read;
            while ((read = inStream.read(writeBuffer.array(), position, writeBuffer.remaining())) >= 0) {
                position += read;
                writeBuffer.position(position);
                if (writeBuffer.remaining() == 0) {
                    offset += writeBufferToFile(fileHandle, writeBuffer, offset);
                    position = 0;
                }
            }

            if (writeBuffer.position() > 0) {
                writeBufferToFile(fileHandle, writeBuffer, offset);
                syncLocally(fileHandle);
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private long writeBufferToFile(IFileHandle fileHandle, ByteBuffer writeBuffer, long offset)
            throws HyracksDataException {
        writeBuffer.flip();
        long written = writeLocally(fileHandle, offset, writeBuffer);
        writeBuffer.clear();
        return written;
    }
}
