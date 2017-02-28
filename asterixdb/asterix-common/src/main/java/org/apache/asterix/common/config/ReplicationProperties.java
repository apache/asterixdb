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
package org.apache.asterix.common.config;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;

public class ReplicationProperties extends AbstractProperties {

    private static final int REPLICATION_DATAPORT_DEFAULT = 2000;

    private static final String REPLICATION_TIMEOUT_KEY = "replication.timeout";
    private static final int REPLICATION_TIME_OUT_DEFAULT = 15;

    private static final String REPLICATION_MAX_REMOTE_RECOVERY_ATTEMPTS_KEY =
            "replication.max.remote.recovery.attempts";
    private static final int MAX_REMOTE_RECOVERY_ATTEMPTS = 5;

    private static final String NODE_IP_ADDRESS_DEFAULT = "127.0.0.1";

    private static final String REPLICATION_LOG_BATCH_SIZE_KEY = "replication.log.batchsize";
    private static final int REPLICATION_LOG_BATCH_SIZE_DEFAULT = StorageUtil.getSizeInBytes(4, StorageUnit.KILOBYTE);

    private static final String REPLICATION_LOG_BUFFER_NUM_PAGES_KEY = "replication.log.buffer.numpages";
    private static final int REPLICATION_LOG_BUFFER_NUM_PAGES_DEFAULT = 8;

    private static final String REPLICATION_LOG_BUFFER_PAGE_SIZE_KEY = "replication.log.buffer.pagesize";
    private static final int REPLICATION_LOG_BUFFER_PAGE_SIZE_DEFAULT = StorageUtil.getSizeInBytes(128,
            StorageUnit.KILOBYTE);

    private final Cluster cluster;
    private final IReplicationStrategy repStrategy;

    public ReplicationProperties(PropertiesAccessor accessor) throws HyracksDataException {
        super(accessor);
        this.cluster = ClusterProperties.INSTANCE.getCluster();
        this.repStrategy = ClusterProperties.INSTANCE.getReplicationStrategy();
    }

    public String getReplicaIPAddress(String nodeId) {
        Node node = ClusterProperties.INSTANCE.getNodeById(nodeId);
        return node != null ? node.getClusterIp() : NODE_IP_ADDRESS_DEFAULT;
    }

    public int getDataReplicationPort(String nodeId) {
        Node node = ClusterProperties.INSTANCE.getNodeById(nodeId);
        if (node != null) {
            return node.getReplicationPort() != null ? node.getReplicationPort().intValue()
                    : cluster.getHighAvailability().getDataReplication().getReplicationPort().intValue();
        }
        return REPLICATION_DATAPORT_DEFAULT;
    }

    public Replica getReplicaById(String nodeId) {
        Node node = ClusterProperties.INSTANCE.getNodeById(nodeId);
        if (node != null) {
            return new Replica(node);
        }
        return null;
    }

    public Set<String> getRemoteReplicasIds(String nodeId) {
        return repStrategy.getRemoteReplicas(nodeId).stream().map(Replica::getId).collect(Collectors.toSet());
    }

    public Set<String> getRemotePrimaryReplicasIds(String nodeId) {
        return repStrategy.getRemotePrimaryReplicas(nodeId).stream().map(Replica::getId).collect(Collectors.toSet());
    }

    public Set<String> getNodeReplicasIds(String nodeId) {
        Set<String> remoteReplicasIds = getRemoteReplicasIds(nodeId);
        // This includes the node itself
        remoteReplicasIds.add(nodeId);
        return remoteReplicasIds;
    }

    @PropertyKey(REPLICATION_TIMEOUT_KEY)
    public int getReplicationTimeOut() {
        if (cluster != null) {
            return cluster.getHighAvailability().getDataReplication().getReplicationTimeOut().intValue();
        }
        return REPLICATION_TIME_OUT_DEFAULT;
    }

    @PropertyKey(REPLICATION_MAX_REMOTE_RECOVERY_ATTEMPTS_KEY)
    public int getMaxRemoteRecoveryAttempts() {
        return MAX_REMOTE_RECOVERY_ATTEMPTS;
    }

    @PropertyKey(REPLICATION_LOG_BUFFER_PAGE_SIZE_KEY)
    public int getLogBufferPageSize() {
        return accessor.getProperty(REPLICATION_LOG_BUFFER_PAGE_SIZE_KEY, REPLICATION_LOG_BUFFER_PAGE_SIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    @PropertyKey(REPLICATION_LOG_BUFFER_NUM_PAGES_KEY)
    public int getLogBufferNumOfPages() {
        return accessor.getProperty(REPLICATION_LOG_BUFFER_NUM_PAGES_KEY, REPLICATION_LOG_BUFFER_NUM_PAGES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(REPLICATION_LOG_BATCH_SIZE_KEY)
    public int getLogBatchSize() {
        return accessor.getProperty(REPLICATION_LOG_BATCH_SIZE_KEY, REPLICATION_LOG_BATCH_SIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    public boolean isParticipant(String nodeId) {
        return repStrategy.isParticipant(nodeId);
    }

    public IReplicationStrategy getReplicationStrategy() {
        return repStrategy;
    }
}