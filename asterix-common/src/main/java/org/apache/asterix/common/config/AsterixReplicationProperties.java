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

public class AsterixReplicationProperties extends AbstractAsterixProperties {

    private static final Logger LOGGER = Logger.getLogger(AsterixReplicationProperties.class.getName());

    private static int REPLICATION_DATAPORT_DEFAULT = 2000;
    private static int REPLICATION_FACTOR_DEFAULT = 1;
    private static int REPLICATION_TIME_OUT_DEFAULT = 15;

    private static final String NODE_IP_ADDRESS_DEFAULT = "127.0.0.1";
    private static final String REPLICATION_STORE_DEFAULT = "asterix-replication";
    private final String NODE_NAME_PREFIX;
    private final Cluster cluster;

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
        if (cluster != null) {
            return cluster.getDataReplication().getReplicationPort().intValue();
        }

        return REPLICATION_DATAPORT_DEFAULT;
    }

    public Set<Replica> getRemoteReplicas(String nodeId) {
        Set<Replica> remoteReplicas = new HashSet<Replica>();;

        int numberOfRemoteReplicas = getReplicationFactor() - 1;

        //Using chained-declustering
        if (cluster != null) {
            int nodeIndex = -1;
            for (int i = 0; i < cluster.getNode().size(); i++) {
                Node node = cluster.getNode().get(i);
                if (getRealCluserNodeID(node.getId()).equals(nodeId)) {
                    nodeIndex = i;
                    break;
                }
            }

            if (nodeIndex == -1) {
                LOGGER.log(Level.WARNING, "Could not find node " + getRealCluserNodeID(nodeId)
                        + " in cluster configurations");
                return null;
            }

            for (int i = nodeIndex + 1; i < cluster.getNode().size(); i++) {
                remoteReplicas.add(getReplicaByNodeIndex(i));

                if (remoteReplicas.size() == numberOfRemoteReplicas) {
                    break;
                }
            }

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
        Set<String> remoteReplicasIds = new HashSet<String>();
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
        Set<String> replicaIds = new HashSet<String>();
        replicaIds.add(nodeId);
        replicaIds.addAll(getRemoteReplicasIds(nodeId));
        return replicaIds;
    }

    public String getReplicationStore() {
        if (cluster != null) {
            return cluster.getDataReplication().getReplicationStore();
        }
        return REPLICATION_STORE_DEFAULT;
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

}