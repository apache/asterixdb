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
package org.apache.asterix.replication.messaging;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.asterix.replication.management.NetworkingUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A task to replicate a file from a master replica
 */
public class ReplicateFileTask implements IReplicaTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private final String file;
    private final long size;
    private final boolean indexMetadata;

    public ReplicateFileTask(String file, long size, boolean indexMetadata) {
        this.file = file;
        this.size = size;
        this.indexMetadata = indexMetadata;
    }

    @Override
    public void perform(INcApplicationContext appCtx, IReplicationWorker worker) {
        try {
            final IIOManager ioManager = appCtx.getIoManager();
            // resolve path
            final FileReference localPath = ioManager.resolve(file);
            final Path resourceDir = Files.createDirectories(localPath.getFile().getParentFile().toPath());
            // create mask
            final Path maskPath = Paths.get(resourceDir.toString(),
                    StorageConstants.MASK_FILE_PREFIX + localPath.getFile().getName());
            Files.createFile(maskPath);

            // receive actual file
            final Path filePath = Paths.get(resourceDir.toString(), localPath.getFile().getName());
            Files.createFile(filePath);
            try (RandomAccessFile fileOutputStream = new RandomAccessFile(filePath.toFile(), "rw");
                    FileChannel fileChannel = fileOutputStream.getChannel()) {
                fileOutputStream.setLength(size);
                NetworkingUtil.downloadFile(fileChannel, worker.getChannel());
                fileChannel.force(true);
            }
            if (indexMetadata) {
                initIndexCheckpoint(appCtx);
            }
            //delete mask
            Files.delete(maskPath);
            LOGGER.info(() -> "Replicated file: " + localPath);
            ReplicationProtocol.sendAck(worker.getChannel(), worker.getReusableBuffer());
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    private void initIndexCheckpoint(INcApplicationContext appCtx) throws HyracksDataException {
        final ResourceReference indexRef = ResourceReference.of(file);
        final IIndexCheckpointManagerProvider checkpointManagerProvider = appCtx.getIndexCheckpointManagerProvider();
        final IIndexCheckpointManager indexCheckpointManager = checkpointManagerProvider.get(indexRef);
        final long currentLSN = appCtx.getTransactionSubsystem().getLogManager().getAppendLSN();
        indexCheckpointManager.delete();
        indexCheckpointManager.init(Long.MIN_VALUE, currentLSN,
                LSMComponentId.EMPTY_INDEX_LAST_COMPONENT_ID.getMaxId());
        LOGGER.info(() -> "Checkpoint index: " + indexRef);
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.REPLICATE_RESOURCE_FILE;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(file);
            dos.writeLong(size);
            dos.writeBoolean(indexMetadata);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static ReplicateFileTask create(DataInput input) throws IOException {
        final String s = input.readUTF();
        final long i = input.readLong();
        final boolean isMetadata = input.readBoolean();
        return new ReplicateFileTask(s, i, isMetadata);
    }
}
