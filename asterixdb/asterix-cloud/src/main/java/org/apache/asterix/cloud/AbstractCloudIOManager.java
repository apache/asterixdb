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

import static org.apache.asterix.common.utils.StorageConstants.METADATA_PARTITION;
import static org.apache.asterix.common.utils.StorageConstants.PARTITION_DIR_PREFIX;
import static org.apache.asterix.common.utils.StorageConstants.STORAGE_ROOT_DIR_NAME;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.cloud.bulk.DeleteBulkCloudOperation;
import org.apache.asterix.cloud.bulk.NoOpDeleteBulkCallBack;
import org.apache.asterix.cloud.clients.CloudClientProvider;
import org.apache.asterix.cloud.clients.CloudFile;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.asterix.cloud.util.CloudFileUtil;
import org.apache.asterix.common.api.INamespacePathResolver;
import org.apache.asterix.common.cloud.IPartitionBootstrapper;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOBulkOperation;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractCloudIOManager extends IOManager implements IPartitionBootstrapper, ICloudIOManager {
    private static final Logger LOGGER = LogManager.getLogger();
    protected final ICloudClient cloudClient;
    protected final ICloudGuardian guardian;
    protected final IWriteBufferProvider writeBufferProvider;
    protected final String bucket;
    protected final Set<Integer> partitions;
    protected final List<FileReference> partitionPaths;
    protected final IOManager localIoManager;
    protected final INamespacePathResolver nsPathResolver;

    public AbstractCloudIOManager(IOManager ioManager, CloudProperties cloudProperties,
            INamespacePathResolver nsPathResolver, ICloudGuardian guardian) throws HyracksDataException {
        super(ioManager.getIODevices(), ioManager.getDeviceComputer(), ioManager.getIOParallelism(),
                ioManager.getQueueSize());
        this.nsPathResolver = nsPathResolver;
        this.bucket = cloudProperties.getStorageBucket();
        cloudClient = CloudClientProvider.getClient(cloudProperties, guardian);
        this.guardian = guardian;
        int numOfThreads = getIODevices().size() * getIOParallelism();
        writeBufferProvider = new WriteBufferProvider(numOfThreads, cloudClient.getWriteBufferSize());
        partitions = new HashSet<>();
        partitionPaths = new ArrayList<>();
        this.localIoManager = ioManager;
    }

    /*
     * ******************************************************************
     * IPartitionBootstrapper functions
     * ******************************************************************
     */

    @Override
    public IRecoveryManager.SystemState getSystemStateOnMissingCheckpoint() {
        Set<CloudFile> existingMetadataFiles = getCloudMetadataPartitionFiles();
        CloudFile bootstrapMarkerPath = CloudFile.of(StoragePathUtil.getBootstrapMarkerRelativePath(nsPathResolver));
        if (existingMetadataFiles.isEmpty() || existingMetadataFiles.contains(bootstrapMarkerPath)) {
            LOGGER.info("First time to initialize this cluster: systemState = PERMANENT_DATA_LOSS");
            return IRecoveryManager.SystemState.PERMANENT_DATA_LOSS;
        } else {
            LOGGER.info("Resuming a previous initialized cluster: systemState = HEALTHY");
            return IRecoveryManager.SystemState.HEALTHY;
        }
    }

    @Override
    public final void bootstrap(Set<Integer> activePartitions, List<FileReference> currentOnDiskPartitions,
            boolean metadataNode, int metadataPartition, boolean cleanup, boolean ensureCompleteBootstrap)
            throws HyracksDataException {
        partitions.clear();
        partitions.addAll(activePartitions);
        if (metadataNode) {
            partitions.add(metadataPartition);
            if (ensureCompleteBootstrap) {
                ensureCompleteMetadataBootstrap();
            }
        }

        partitionPaths.clear();
        for (Integer partition : activePartitions) {
            String partitionDir = PARTITION_DIR_PREFIX + partition;
            partitionPaths.add(resolve(STORAGE_ROOT_DIR_NAME + File.separator + partitionDir));
        }

        LOGGER.warn("Initializing cloud manager with ({}) storage partitions: {}", partitions.size(), partitions);

        if (cleanup) {
            deleteUnkeptPartitionDirs(currentOnDiskPartitions);
            cleanupLocalFiles();
        }

        // Has different implementations depending on the caching policy
        downloadPartitions(metadataNode, metadataPartition);
    }

    private void deleteUnkeptPartitionDirs(List<FileReference> currentOnDiskPartitions) throws HyracksDataException {
        for (FileReference partitionDir : currentOnDiskPartitions) {
            int partitionNum = StoragePathUtil.getPartitionNumFromRelativePath(partitionDir.getRelativePath());
            if (!partitions.contains(partitionNum)) {
                LOGGER.warn("Deleting storage partition {} as it does not belong to the current storage partitions {}",
                        partitionNum, partitions);
                localIoManager.delete(partitionDir);
            }
        }
    }

    private void cleanupLocalFiles() throws HyracksDataException {
        Set<CloudFile> cloudFiles = cloudClient.listObjects(bucket, STORAGE_ROOT_DIR_NAME, IoUtil.NO_OP_FILTER);
        if (cloudFiles.isEmpty()) {
            LOGGER.warn("No files in the cloud. Deleting all local files in partitions {}...", partitions);
            for (FileReference partitionPath : partitionPaths) {
                if (localIoManager.exists(partitionPath)) {
                    // Clean local dir from all files
                    localIoManager.cleanDirectory(partitionPath);
                }
            }
        } else {
            LOGGER.info("Cleaning node partitions...");
            for (FileReference partitionPath : partitionPaths) {
                CloudFileUtil.cleanDirectoryFiles(localIoManager, cloudFiles, partitionPath);
            }
        }
    }

    protected abstract void downloadPartitions(boolean metadataNode, int metadataPartition) throws HyracksDataException;

    /*
     * ******************************************************************
     * ICloudIOManager functions
     * ******************************************************************
     */

    @Override
    public final void cloudRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        cloudClient.read(bucket, fHandle.getFileReference().getRelativePath(), offset, data);
    }

    @Override
    public final InputStream cloudRead(IFileHandle fHandle, long offset, long length) {
        return cloudClient.getObjectStream(bucket, fHandle.getFileReference().getRelativePath(), offset, length);
    }

    @Override
    public final int localWriter(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        return localIoManager.doSyncWrite(fHandle, offset, data);
    }

    @Override
    public final int cloudWrite(IFileHandle fHandle, ByteBuffer data) throws HyracksDataException {
        ICloudWriter cloudWriter = ((CloudFileHandle) fHandle).getCloudWriter();
        int writtenBytes;
        try {
            writtenBytes = cloudWriter.write(data);
        } catch (HyracksDataException e) {
            cloudWriter.abort();
            throw e;
        }
        return writtenBytes;
    }

    @Override
    public final long cloudWrite(IFileHandle fHandle, ByteBuffer[] data) throws HyracksDataException {
        ICloudWriter cloudWriter = ((CloudFileHandle) fHandle).getCloudWriter();
        int writtenBytes;
        try {
            writtenBytes = cloudWriter.write(data[0], data[1]);
        } catch (HyracksDataException e) {
            cloudWriter.abort();
            throw e;
        }
        return writtenBytes;
    }

    /*
     * ******************************************************************
     * IIOManager functions
     * ******************************************************************
     */

    @Override
    public final IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException {
        ICloudWriter cloudWriter = cloudClient.createWriter(bucket, fileRef.getRelativePath(), writeBufferProvider);
        CloudFileHandle fHandle = new CloudFileHandle(fileRef, cloudWriter);
        onOpen(fHandle);
        try {
            fHandle.open(rwMode, syncMode);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return fHandle;
    }

    /**
     * Action required to do when opening a file
     *
     * @param fileHandle file to open
     */
    protected abstract void onOpen(CloudFileHandle fileHandle) throws HyracksDataException;

    @Override
    public final long doSyncWrite(IFileHandle fHandle, long offset, ByteBuffer[] dataArray)
            throws HyracksDataException {
        long writtenBytes = localIoManager.doSyncWrite(fHandle, offset, dataArray);
        dataArray[0].flip();
        dataArray[1].flip();
        cloudWrite(fHandle, dataArray);
        return writtenBytes;
    }

    @Override
    public final int doSyncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException {
        int writtenBytes = localIoManager.doSyncWrite(fHandle, offset, data);
        data.flip();
        cloudWrite(fHandle, data);
        return writtenBytes;
    }

    @Override
    public IIOBulkOperation createDeleteBulkOperation() {
        return new DeleteBulkCloudOperation(localIoManager, bucket, cloudClient, NoOpDeleteBulkCallBack.INSTANCE);
    }

    @Override
    public final void close(IFileHandle fHandle) throws HyracksDataException {
        try {
            CloudFileHandle cloudFileHandle = (CloudFileHandle) fHandle;
            cloudFileHandle.close();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public final void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException {
        HyracksDataException savedEx = null;
        if (metadata) {
            // only finish writing if metadata == true to prevent write limiter from finishing the stream and
            // completing the upload.
            ICloudWriter cloudWriter = ((CloudFileHandle) fileHandle).getCloudWriter();
            try {
                cloudWriter.finish();
            } catch (HyracksDataException e) {
                savedEx = e;
            }

            if (savedEx != null) {
                try {
                    cloudWriter.abort();
                } catch (HyracksDataException e) {
                    savedEx.addSuppressed(e);
                }
                throw savedEx;
            }
        }
        // Sync only after finalizing the upload to cloud storage
        localIoManager.sync(fileHandle, metadata);
    }

    @Override
    public final void create(FileReference fileRef) throws HyracksDataException {
        // We need to delete the local file on create as the cloud storage didn't complete the upload
        // In other words, both cloud files and the local files are not in sync
        localIoManager.delete(fileRef);
        localIoManager.create(fileRef);
    }

    @Override
    public final void copyDirectory(FileReference srcFileRef, FileReference destFileRef) throws HyracksDataException {
        cloudClient.copy(bucket, srcFileRef.getRelativePath(), destFileRef);
        localIoManager.copyDirectory(srcFileRef, destFileRef);
    }

    // TODO(htowaileb): the localIoManager is closed by the node controller service as well, check if we need this
    @Override
    public final void close() throws IOException {
        cloudClient.close();
        super.close();
        localIoManager.close();
    }

    /**
     * Returns a list of all stored objects (sorted ASC by path) in the cloud and their sizes
     *
     * @param objectMapper to create the result {@link JsonNode}
     * @return {@link JsonNode} with stored objects' information
     */
    public final JsonNode listAsJson(ObjectMapper objectMapper) {
        return cloudClient.listAsJson(objectMapper, bucket);
    }

    /**
     * Writes the bytes to the specified key in the bucket
     *
     * @param key   the key where the bytes will be written
     * @param bytes the bytes to write
     */
    public final void put(String key, byte[] bytes) {
        cloudClient.write(bucket, key, bytes);
    }

    public ICloudClient getCloudClient() {
        return cloudClient;
    }

    private Set<CloudFile> getCloudMetadataPartitionFiles() {
        String metadataNamespacePath = StoragePathUtil.getNamespacePath(nsPathResolver,
                MetadataConstants.METADATA_NAMESPACE, METADATA_PARTITION);
        return cloudClient.listObjects(bucket, metadataNamespacePath, IoUtil.NO_OP_FILTER);
    }

    private void ensureCompleteMetadataBootstrap() throws HyracksDataException {
        Set<CloudFile> metadataPartitionFiles = getCloudMetadataPartitionFiles();
        CloudFile marker = CloudFile.of(StoragePathUtil.getBootstrapMarkerRelativePath(nsPathResolver));
        boolean foundBootstrapMarker = metadataPartitionFiles.contains(marker);
        // if the bootstrap file exists, we failed to bootstrap --> delete all partial files in metadata partition
        if (foundBootstrapMarker) {
            LOGGER.info(
                    "detected failed bootstrap attempted, deleting all existing files in the metadata partition: {}",
                    metadataPartitionFiles);
            IIOBulkOperation deleteBulkOperation = createDeleteBulkOperation();
            for (CloudFile file : metadataPartitionFiles) {
                deleteBulkOperation.add(resolve(file.getPath()));
            }
            performBulkOperation(deleteBulkOperation);
        }
    }
}
