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
package org.apache.asterix.replication.sync;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.replication.messaging.PartitionResourcesListResponse;
import org.apache.asterix.replication.messaging.PartitionResourcesListTask;
import org.apache.asterix.replication.messaging.ReplicationProtocol;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.network.ISocketChannel;

/**
 * Ensures that the files between master and a replica are synchronized
 */
public class ReplicaFilesSynchronizer {

    private final PartitionReplica replica;
    private final INcApplicationContext appCtx;

    public ReplicaFilesSynchronizer(INcApplicationContext appCtx, PartitionReplica replica) {
        this.appCtx = appCtx;
        this.replica = replica;
    }

    public void sync() throws IOException {
        final int partition = replica.getIdentifier().getPartition();
        final Set<String> replicaFiles = getReplicaFiles(partition);
        final PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        final IReplicationStrategy replicationStrategy = appCtx.getReplicationManager().getReplicationStrategy();
        final Set<String> masterFiles =
                localResourceRepository.getPartitionReplicatedFiles(partition, replicationStrategy).stream()
                        .map(StoragePathUtil::getFileRelativePath).collect(Collectors.toSet());
        // find files on master and not on replica
        final List<String> replicaMissingFiles =
                masterFiles.stream().filter(file -> !replicaFiles.contains(file)).collect(Collectors.toList());
        replicateMissingFiles(replicaMissingFiles);
        // find files on replica and not on master
        final List<String> replicaInvalidFiles =
                replicaFiles.stream().filter(file -> !masterFiles.contains(file)).collect(Collectors.toList());
        deleteInvalidFiles(replicaInvalidFiles);
    }

    private Set<String> getReplicaFiles(int partition) throws IOException {
        final PartitionResourcesListTask replicaFilesRequest = new PartitionResourcesListTask(partition);
        final ISocketChannel channel = replica.getChannel();
        final ByteBuffer reusableBuffer = replica.getReusableBuffer();
        ReplicationProtocol.sendTo(replica, replicaFilesRequest);
        final PartitionResourcesListResponse response =
                (PartitionResourcesListResponse) ReplicationProtocol.read(channel, reusableBuffer);
        return new HashSet<>(response.getResources());
    }

    private void replicateMissingFiles(List<String> files) {
        final FileSynchronizer sync = new FileSynchronizer(appCtx, replica);
        // sort files to ensure index metadata files starting with "." are replicated first
        files.sort(String::compareTo);
        files.forEach(sync::replicate);
    }

    private void deleteInvalidFiles(List<String> files) {
        final FileSynchronizer sync = new FileSynchronizer(appCtx, replica);
        files.forEach(sync::delete);
    }
}
