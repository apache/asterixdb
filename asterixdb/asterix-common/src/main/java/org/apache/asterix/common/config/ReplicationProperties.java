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

import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER;
import static org.apache.hyracks.control.common.config.OptionTypes.INTEGER_BYTE_UNIT;

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;

public class ReplicationProperties extends AbstractProperties {

    public enum Option implements IOption {
        REPLICATION_MAX_REMOTE_RECOVERY_ATTEMPTS(INTEGER, 5,
                "The maximum number of times to attempt to recover from a replica on failure before giving up"),
        REPLICATION_LOG_BUFFER_PAGESIZE(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(128,
                StorageUnit.KILOBYTE), "The size in bytes of each log buffer page"),
        REPLICATION_LOG_BUFFER_NUMPAGES(INTEGER, 8, "The number of log buffer pages"),
        REPLICATION_LOG_BATCHSIZE(INTEGER_BYTE_UNIT, StorageUtil.getIntSizeInBytes(4, StorageUnit.KILOBYTE),
                "The size in bytes to replicate in each batch"),
        REPLICATION_TIMEOUT(INTEGER, REPLICATION_TIME_OUT_DEFAULT,
                "The time in seconds to timeout when trying to contact a replica, before assuming it is dead"),
        ;

        private final IOptionType type;
        private final Object defaultValue;
        private final String description;

        Option(IOptionType type, Object defaultValue, String description) {
            this.type = type;
            this.defaultValue = defaultValue;
            this.description = description;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public IOptionType type() {
            return type;
        }

        @Override
        public Object defaultValue() {
            return defaultValue;
        }

        @Override
        public Object get(IApplicationConfig config) {
            switch (this) {
                case REPLICATION_TIMEOUT:
                    final Cluster cluster = ClusterProperties.INSTANCE.getCluster();
                    if (cluster != null
                            && cluster.getHighAvailability() != null
                            && cluster.getHighAvailability().getDataReplication() != null
                            && cluster.getHighAvailability().getDataReplication().getReplicationTimeOut() != null) {
                        return cluster.getHighAvailability().getDataReplication().getReplicationTimeOut().intValue();
                    }
                    return REPLICATION_TIME_OUT_DEFAULT;
                default:
                    return config.getStatic(this);
            }
        }
    }

    private static final int REPLICATION_DATAPORT_DEFAULT = 2000;

    private static final int REPLICATION_TIME_OUT_DEFAULT = 15;

    private static final String NODE_IP_ADDRESS_DEFAULT = "127.0.0.1";

    private final IReplicationStrategy repStrategy;

    public ReplicationProperties(PropertiesAccessor accessor) throws HyracksDataException {
        super(accessor);
        this.repStrategy = ClusterProperties.INSTANCE.getReplicationStrategy();
    }

    public int getMaxRemoteRecoveryAttempts() {
        return accessor.getInt(Option.REPLICATION_MAX_REMOTE_RECOVERY_ATTEMPTS);
    }

    public int getLogBufferPageSize() {
        return accessor.getInt(Option.REPLICATION_LOG_BUFFER_PAGESIZE);
    }

    public int getLogBufferNumOfPages() {
        return accessor.getInt(Option.REPLICATION_LOG_BUFFER_NUMPAGES);
    }

    public int getLogBatchSize() {
        return accessor.getInt(Option.REPLICATION_LOG_BATCHSIZE);
    }

    public String getReplicaIPAddress(String nodeId) {
        Node node = ClusterProperties.INSTANCE.getNodeById(nodeId);
        return node != null ? node.getClusterIp() : NODE_IP_ADDRESS_DEFAULT;
    }

    public int getDataReplicationPort(String nodeId) {
        final Cluster cluster = ClusterProperties.INSTANCE.getCluster();
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

    public int getReplicationTimeOut() {
        return accessor.getInt(Option.REPLICATION_TIMEOUT);
    }

    public boolean isParticipant(String nodeId) {
        return repStrategy.isParticipant(nodeId);
    }

    public IReplicationStrategy getReplicationStrategy() {
        return repStrategy;
    }
}