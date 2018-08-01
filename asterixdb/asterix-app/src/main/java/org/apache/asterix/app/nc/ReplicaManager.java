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
package org.apache.asterix.app.nc;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.replication.IPartitionReplica;
import org.apache.asterix.common.storage.IReplicaManager;
import org.apache.asterix.common.storage.ReplicaIdentifier;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.replication.api.PartitionReplica;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ThreadSafe
public class ReplicaManager implements IReplicaManager {
    private static final Logger LOGGER = LogManager.getLogger();

    private final INcApplicationContext appCtx;
    /**
     * the partitions to which the current node is master
     */
    private final Set<Integer> partitions = new HashSet<>();
    /**
     * current replicas
     */
    private final Map<ReplicaIdentifier, PartitionReplica> replicas = new HashMap<>();
    private final Object replicaSyncLock = new Object();

    public ReplicaManager(INcApplicationContext appCtx, Set<Integer> partitions) {
        this.appCtx = appCtx;
        this.partitions.addAll(partitions);
    }

    @Override
    public synchronized void addReplica(ReplicaIdentifier id) {
        final NodeControllerService controllerService =
                (NodeControllerService) appCtx.getServiceContext().getControllerService();
        final NodeStatus nodeStatus = controllerService.getNodeStatus();
        if (nodeStatus != NodeStatus.ACTIVE) {
            LOGGER.warn("Ignoring request to add replica. Node is not ACTIVE yet. Current status: {}", nodeStatus);
            return;
        }
        if (!partitions.contains(id.getPartition())) {
            throw new IllegalStateException(
                    "This node is not the current master of partition(" + id.getPartition() + ")");
        }
        if (isSelf(id)) {
            LOGGER.info("ignoring request to add replica to ourselves");
            return;
        }
        replicas.computeIfAbsent(id, k -> new PartitionReplica(k, appCtx));
        replicas.get(id).sync();
    }

    @Override
    public synchronized void removeReplica(ReplicaIdentifier id) {
        if (!replicas.containsKey(id)) {
            throw new IllegalStateException("replica with id(" + id + ") does not exist");
        }
        PartitionReplica replica = replicas.remove(id);
        appCtx.getReplicationManager().unregister(replica);

    }

    @Override
    public synchronized List<IPartitionReplica> getReplicas(int partition) {
        return replicas.entrySet().stream().filter(e -> e.getKey().getPartition() == partition).map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public synchronized Set<Integer> getPartitions() {
        return Collections.unmodifiableSet(partitions);
    }

    @Override
    public synchronized void promote(int partition) throws HyracksDataException {
        if (partitions.contains(partition)) {
            return;
        }
        final PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        localResourceRepository.cleanup(partition);
        final IRecoveryManager recoveryManager = appCtx.getTransactionSubsystem().getRecoveryManager();
        recoveryManager.replayReplicaPartitionLogs(Stream.of(partition).collect(Collectors.toSet()), true);
        partitions.add(partition);
    }

    @Override
    public synchronized void release(int partition) throws HyracksDataException {
        if (!partitions.contains(partition)) {
            return;
        }
        final IDatasetLifecycleManager datasetLifecycleManager = appCtx.getDatasetLifecycleManager();
        datasetLifecycleManager.flushDataset(appCtx.getReplicationManager().getReplicationStrategy());
        closePartitionResources(partition);
        final List<IPartitionReplica> partitionReplicas = getReplicas(partition);
        for (IPartitionReplica replica : partitionReplicas) {
            appCtx.getReplicationManager().unregister(replica);
        }
        partitions.remove(partition);
    }

    @Override
    public Object getReplicaSyncLock() {
        return replicaSyncLock;
    }

    private void closePartitionResources(int partition) throws HyracksDataException {
        final PersistentLocalResourceRepository resourceRepository =
                (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        final Map<Long, LocalResource> partitionResources = resourceRepository.getPartitionResources(partition);
        final IDatasetLifecycleManager datasetLifecycleManager = appCtx.getDatasetLifecycleManager();
        for (LocalResource resource : partitionResources.values()) {
            datasetLifecycleManager.close(resource.getPath());
        }
    }

    private boolean isSelf(ReplicaIdentifier id) {
        IApplicationConfig appConfig = appCtx.getServiceContext().getAppConfig();
        String host = appConfig.getString(NCConfig.Option.REPLICATION_LISTEN_ADDRESS);
        int port = appConfig.getInt(NCConfig.Option.REPLICATION_LISTEN_PORT);

        final InetSocketAddress replicaAddress = new InetSocketAddress(host, port);
        return id.equals(ReplicaIdentifier.of(id.getPartition(), replicaAddress));
    }
}
