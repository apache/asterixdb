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

import static org.apache.asterix.common.utils.StorageConstants.METADATA_PARTITION;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.api.IReplicationWorker;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A task to get the list of the files in a partition on a replica
 */
public class PartitionResourcesListTask implements IReplicaTask {

    private static final Logger LOGGER = LogManager.getLogger();
    private final int partition;

    public PartitionResourcesListTask(int partition) {
        this.partition = partition;
    }

    @Override
    public void perform(INcApplicationContext appCtx, IReplicationWorker worker) throws HyracksDataException {
        LOGGER.debug("processing {}", this);
        if (METADATA_PARTITION == partition && appCtx.getReplicaManager().getPartitions().contains(partition)) {
            LOGGER.warn("received request to get metadata files from non-master {}", worker.getRemoteAddress());
            throw new IllegalStateException();
        }
        final PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        localResourceRepository.cleanup(partition);
        LOGGER.debug("cleaned up partition {}", partition);
        final IReplicationStrategy replicationStrategy = appCtx.getReplicationManager().getReplicationStrategy();
        // .metadata file -> resource id
        Map<String, Long> partitionReplicatedResources =
                localResourceRepository.getPartitionReplicatedResources(partition, replicationStrategy);
        LOGGER.debug("got partition {} resources", partition);
        // all data files in partitions + .metadata files
        final List<String> partitionFiles =
                localResourceRepository.getPartitionReplicatedFiles(partition, replicationStrategy).stream()
                        .map(StoragePathUtil::getFileRelativePath).collect(Collectors.toList());
        LOGGER.debug("got partition {} files ({})", partition, partitionFiles.size());
        final PartitionResourcesListResponse response = new PartitionResourcesListResponse(partition,
                partitionReplicatedResources, partitionFiles, appCtx.getReplicaManager().isPartitionOrigin(partition));
        ReplicationProtocol.sendTo(worker.getChannel(), response, worker.getReusableBuffer());
        LOGGER.debug("sent partition {} files list to requester", partition);
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.PARTITION_RESOURCES_REQUEST;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(partition);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public String toString() {
        return "PartitionResourcesListTask{" + "partition=" + partition + '}';
    }

    public static PartitionResourcesListTask create(DataInput input) throws HyracksDataException {
        try {
            int partition = input.readInt();
            return new PartitionResourcesListTask(partition);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
