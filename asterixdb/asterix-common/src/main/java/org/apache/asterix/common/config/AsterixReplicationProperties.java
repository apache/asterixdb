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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.hyracks.util.StorageUtil;
import org.apache.hyracks.util.StorageUtil.StorageUnit;

public class AsterixReplicationProperties extends AbstractAsterixProperties {

    private static final Logger LOGGER = Logger.getLogger(AsterixReplicationProperties.class.getName());

    private static final int REPLICATION_DATAPORT_DEFAULT = 2000;
    private static final int REPLICATION_FACTOR_DEFAULT = 1;
    private static final int REPLICATION_TIME_OUT_DEFAULT = 15;
    private static final int MAX_REMOTE_RECOVERY_ATTEMPTS = 5;
    private static final String NODE_IP_ADDRESS_DEFAULT = "127.0.0.1";
    private final String NODE_NAME_PREFIX;
    private final Cluster cluster;

    private static final String REPLICATION_LOG_BATCH_SIZE_KEY = "replication.log.batchsize";
    private static final int REPLICATION_LOG_BATCH_SIZE_DEFAULT = StorageUtil.getSizeInBytes(4, StorageUnit.KILOBYTE);

    private static final String REPLICATION_LOG_BUFFER_NUM_PAGES_KEY = "replication.log.buffer.numpages";
    private static final int REPLICATION_LOG_BUFFER_NUM_PAGES_DEFAULT = 8;

    private static final String REPLICATION_LOG_BUFFER_PAGE_SIZE_KEY = "replication.log.buffer.pagesize";
    private static final int REPLICATION_LOG_BUFFER_PAGE_SIZE_DEFAULT = StorageUtil.getSizeInBytes(128,
            StorageUnit.KILOBYTE);

    public AsterixReplicationProperties(AsterixPropertiesAccessor accessor, Cluster cluster) {
        super(accessor);
        this.cluster = cluster;

        if (cluster != null) {
            NODE_NAME_PREFIX = cluster.getInstanceName() + "_";
        } else {
            NODE_NAME_PREFIX = "";
        }
    }

    public boolean isReplicationEnabled() {
        if (cluster != null && cluster.getDataReplication() != null) {
            if (getReplicationFactor() == 1) {
                return false;
            }

            return cluster.getDataReplication().isEnabled();

        } else {
            return false;
        }
    }

    public String getReplicaIPAddress(String nodeId) {
        if (cluster != null) {

            for (int i = 0; i < cluster.getNode().size(); i++) {
                Node node = cluster.getNode().get(i);
                if (getRealCluserNodeID(node.getId()).equals(nodeId)) {
                    return node.getClusterIp();
                }
            }
        }
        return NODE_IP_ADDRESS_DEFAULT;
    }

    public int getDataReplicationPort(String nodeId) {
        if (cluster != null && cluster.getDataReplication() != null) {
            for (int i = 0; i < cluster.getNode().size(); i++) {
                Node node = cluster.getNode().get(i);
                if (getRealCluserNodeID(node.getId()).equals(nodeId)) {
                    return node.getReplicationPort() != null ? node.getReplicationPort().intValue()
                            : cluster.getDataReplication().getReplicationPort().intValue();
                }
            }
        }
        return REPLICATION_DATAPORT_DEFAULT;
    }

    public Set<Replica> getRemoteReplicas(String nodeId) {
        Set<Replica> remoteReplicas = new HashSet<>();;

        int numberOfRemoteReplicas = getReplicationFactor() - 1;
        //Using chained-declustering
        if (cluster != null) {
            int nodeIndex = -1;
            //find the node index in the cluster config
            for (int i = 0; i < cluster.getNode().size(); i++) {
                Node node = cluster.getNode().get(i);
                if (getRealCluserNodeID(node.getId()).equals(nodeId)) {
                    nodeIndex = i;
                    break;
                }
            }

            if (nodeIndex == -1) {
                LOGGER.log(Level.WARNING,
                        "Could not find node " + getRealCluserNodeID(nodeId) + " in cluster configurations");
                return null;
            }

            //find nodes to the right of this node
            for (int i = nodeIndex + 1; i < cluster.getNode().size(); i++) {
                remoteReplicas.add(getReplicaByNodeIndex(i));
                if (remoteReplicas.size() == numberOfRemoteReplicas) {
                    break;
                }
            }

            //if not all remote replicas have been found, start from the beginning
            if (remoteReplicas.size() != numberOfRemoteReplicas) {
                for (int i = 0; i < cluster.getNode().size(); i++) {
                    remoteReplicas.add(getReplicaByNodeIndex(i));
                    if (remoteReplicas.size() == numberOfRemoteReplicas) {
                        break;
                    }
                }
            }
        }
        return remoteReplicas;
    }

    private Replica getReplicaByNodeIndex(int nodeIndex) {
        Node node = cluster.getNode().get(nodeIndex);
        Node replicaNode = new Node();
        replicaNode.setId(getRealCluserNodeID(node.getId()));
        replicaNode.setClusterIp(node.getClusterIp());
        return new Replica(replicaNode);
    }

    public Replica getReplicaById(String nodeId) {
        int nodeIndex = -1;
        if (cluster != null) {
            for (int i = 0; i < cluster.getNode().size(); i++) {
                Node node = cluster.getNode().get(i);

                if (getRealCluserNodeID(node.getId()).equals(nodeId)) {
                    nodeIndex = i;
                    break;
                }
            }
        }

        if (nodeIndex < 0) {
            return null;
        }

        return getReplicaByNodeIndex(nodeIndex);
    }

    public Set<String> getRemoteReplicasIds(String nodeId) {
        Set<String> remoteReplicasIds = new HashSet<>();
        Set<Replica> remoteReplicas = getRemoteReplicas(nodeId);

        for (Replica replica : remoteReplicas) {
            remoteReplicasIds.add(replica.getId());
        }

        return remoteReplicasIds;
    }

    public String getRealCluserNodeID(String nodeId) {
        return NODE_NAME_PREFIX + nodeId;
    }

    public Set<String> getNodeReplicasIds(String nodeId) {
        Set<String> replicaIds = new HashSet<>();
        replicaIds.add(nodeId);
        replicaIds.addAll(getRemoteReplicasIds(nodeId));
        return replicaIds;
    }

    public int getReplicationFactor() {
        if (cluster != null) {
            if (cluster.getDataReplication() == null || cluster.getDataReplication().getReplicationFactor() == null) {
                return REPLICATION_FACTOR_DEFAULT;
            }
            return cluster.getDataReplication().getReplicationFactor().intValue();
        }
        return REPLICATION_FACTOR_DEFAULT;
    }

    public int getReplicationTimeOut() {
        if (cluster != null) {
            return cluster.getDataReplication().getReplicationTimeOut().intValue();
        }
        return REPLICATION_TIME_OUT_DEFAULT;
    }

    /**
     * @param nodeId
     * @return The set of nodes which replicate to this node, including the node itself
     */
    public Set<String> getNodeReplicationClients(String nodeId) {
        Set<String> clientReplicas = new HashSet<>();
        clientReplicas.add(nodeId);

        int clientsCount = getReplicationFactor();

        //Using chained-declustering backwards
        if (cluster != null) {
            int nodeIndex = -1;
            //find the node index in the cluster config
            for (int i = 0; i < cluster.getNode().size(); i++) {
                Node node = cluster.getNode().get(i);
                if (getRealCluserNodeID(node.getId()).equals(nodeId)) {
                    nodeIndex = i;
                    break;
                }
            }

            //find nodes to the left of this node
            for (int i = nodeIndex - 1; i >= 0; i--) {
                clientReplicas.add(getReplicaByNodeIndex(i).getId());
                if (clientReplicas.size() == clientsCount) {
                    break;
                }
            }

            //if not all client replicas have been found, start from the end
            if (clientReplicas.size() != clientsCount) {
                for (int i = cluster.getNode().size() - 1; i >= 0; i--) {
                    clientReplicas.add(getReplicaByNodeIndex(i).getId());
                    if (clientReplicas.size() == clientsCount) {
                        break;
                    }
                }
            }
        }
        return clientReplicas;
    }

    public int getMaxRemoteRecoveryAttempts() {
        return MAX_REMOTE_RECOVERY_ATTEMPTS;
    }

    public int getLogBufferPageSize() {
        return accessor.getProperty(REPLICATION_LOG_BUFFER_PAGE_SIZE_KEY, REPLICATION_LOG_BUFFER_PAGE_SIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    public int getLogBufferNumOfPages() {
        return accessor.getProperty(REPLICATION_LOG_BUFFER_NUM_PAGES_KEY, REPLICATION_LOG_BUFFER_NUM_PAGES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getLogBatchSize() {
        return accessor.getProperty(REPLICATION_LOG_BATCH_SIZE_KEY, REPLICATION_LOG_BATCH_SIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }
}
