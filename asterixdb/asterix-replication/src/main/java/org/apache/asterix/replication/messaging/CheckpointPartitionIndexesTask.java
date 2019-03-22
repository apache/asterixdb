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
import java.nio.file.Path;
import java.util.Collection;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.hyracks.storage.common.LocalResource;

/**
 * A task to initialize the checkpoints for all indexes in a partition with the replica's current LSN
 */
public class CheckpointPartitionIndexesTask implements IReplicaTask {

    private final int partition;
    private final long maxComponentId;

    public CheckpointPartitionIndexesTask(int partition, long maxComponentId) {
        this.partition = partition;
        this.maxComponentId = maxComponentId;
    }

    @Override
    public void perform(INcApplicationContext appCtx, IReplicationWorker worker) throws HyracksDataException {
        final IIndexCheckpointManagerProvider indexCheckpointManagerProvider =
                appCtx.getIndexCheckpointManagerProvider();
        PersistentLocalResourceRepository resRepo =
                (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        final IIOManager ioManager = appCtx.getIoManager();
        final Collection<LocalResource> partitionResources = resRepo.getPartitionResources(partition).values();
        final long currentLSN = appCtx.getTransactionSubsystem().getLogManager().getAppendLSN();
        for (LocalResource ls : partitionResources) {
            DatasetResourceReference ref = DatasetResourceReference.of(ls);
            final IIndexCheckpointManager indexCheckpointManager = indexCheckpointManagerProvider.get(ref);
            indexCheckpointManager.delete();
            // Get most recent sequence of existing files to avoid deletion
            Path indexPath = StoragePathUtil.getIndexPath(ioManager, ref);
            String[] files = indexPath.toFile().list(AbstractLSMIndexFileManager.COMPONENT_FILES_FILTER);
            if (files == null) {
                throw HyracksDataException
                        .create(new IOException(indexPath + " is not a directory or an IO Error occurred"));
            }
            long maxComponentSequence = Long.MIN_VALUE;
            for (String file : files) {
                maxComponentSequence =
                        Math.max(maxComponentSequence, IndexComponentFileReference.of(file).getSequenceEnd());
            }
            indexCheckpointManager.init(maxComponentSequence, currentLSN, maxComponentId);
        }
        ReplicationProtocol.sendAck(worker.getChannel(), worker.getReusableBuffer());
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.CHECKPOINT_PARTITION;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(partition);
            dos.writeLong(maxComponentId);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static CheckpointPartitionIndexesTask create(DataInput input) throws HyracksDataException {
        try {
            int partition = input.readInt();
            long maxComponentId = input.readLong();
            return new CheckpointPartitionIndexesTask(partition, maxComponentId);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
