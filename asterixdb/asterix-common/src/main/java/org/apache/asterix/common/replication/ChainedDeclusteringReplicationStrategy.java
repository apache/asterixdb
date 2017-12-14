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
package org.apache.asterix.common.replication;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.NCConfig;

public class ChainedDeclusteringReplicationStrategy implements IReplicationStrategy {

    private static final Logger LOGGER = Logger.getLogger(ChainedDeclusteringReplicationStrategy.class.getName());
    private int replicationFactor;
    private ReplicationProperties repProp;
    private ConfigManager configManager;

    @Override
    public boolean isMatch(int datasetId) {
        return true;
    }

    @Override
    public Set<Replica> getRemoteReplicas(String nodeId) {
        Set<Replica> remoteReplicas = new HashSet<>();
        int numberOfRemoteReplicas = replicationFactor - 1;
        int nodeIndex = repProp.getNodeIds().indexOf(nodeId);

        if (nodeIndex == -1) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not find node " + nodeId + " in cluster configurations");
            }
            return Collections.emptySet();
        }

        //find nodes to the right of this node
        while (remoteReplicas.size() != numberOfRemoteReplicas) {
            String replica = repProp.getNodeIds().get(++nodeIndex % repProp.getNodeIds().size());
            remoteReplicas.add(new Replica(replica,
                    configManager.getNodeEffectiveConfig(replica).getString(NCConfig.Option.REPLICATION_LISTEN_ADDRESS),
                    configManager.getNodeEffectiveConfig(replica).getInt(NCConfig.Option.REPLICATION_LISTEN_PORT)));
        }

        return remoteReplicas;
    }

    @Override
    public Set<Replica> getRemoteReplicasAndSelf(String nodeId) {
        Set<Replica> replicas = getRemoteReplicas(nodeId);
        replicas.add(new Replica(nodeId,
                configManager.getNodeEffectiveConfig(nodeId).getString(NCConfig.Option.REPLICATION_LISTEN_ADDRESS),
                configManager.getNodeEffectiveConfig(nodeId).getInt(NCConfig.Option.REPLICATION_LISTEN_PORT)));
        return replicas;

    }

    @Override
    public Set<Replica> getRemotePrimaryReplicas(String nodeId) {
        Set<Replica> clientReplicas = new HashSet<>();
        final int remotePrimaryReplicasCount = replicationFactor - 1;
        int nodeIndex = repProp.getNodeIds().indexOf(nodeId);

        //find nodes to the left of this node
        while (clientReplicas.size() != remotePrimaryReplicasCount) {
            String replica = repProp.getNodeIds().get(Math.abs(--nodeIndex % repProp.getNodeIds().size()));
            clientReplicas.add(new Replica(replica,
                    configManager.getNodeEffectiveConfig(replica).getString(NCConfig.Option.REPLICATION_LISTEN_ADDRESS),
                    configManager.getNodeEffectiveConfig(replica).getInt(NCConfig.Option.REPLICATION_LISTEN_PORT)));
        }

        return clientReplicas;
    }

    @Override
    public ChainedDeclusteringReplicationStrategy from(ReplicationProperties repProp, IConfigManager configManager)
            throws HyracksDataException {
        ChainedDeclusteringReplicationStrategy cd = new ChainedDeclusteringReplicationStrategy();
        cd.repProp = repProp;
        cd.replicationFactor = repProp.getReplicationFactor();
        cd.configManager = (ConfigManager) configManager;
        return cd;
    }
}