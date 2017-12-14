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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.NCConfig;

public class MetadataOnlyReplicationStrategy implements IReplicationStrategy {

    private String metadataPrimaryReplicaId;
    private Replica metadataPrimaryReplica;
    private Set<Replica> metadataNodeReplicas;
    MetadataProperties metadataProperties;

    @Override
    public boolean isMatch(int datasetId) {
        return datasetId < MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID && datasetId >= 0;
    }

    @Override
    public Set<Replica> getRemoteReplicas(String nodeId) {
        if (nodeId.equals(metadataPrimaryReplicaId)) {
            return metadataNodeReplicas;
        }
        return Collections.emptySet();
    }

    @Override
    public Set<Replica> getRemoteReplicasAndSelf(String nodeId) {

        if (nodeId.equals(metadataPrimaryReplicaId)) {
            Set<Replica> replicasAndSelf = new HashSet<>();
            replicasAndSelf.addAll(metadataNodeReplicas);
            replicasAndSelf.add(metadataPrimaryReplica);
            return replicasAndSelf;
        }
        return Collections.emptySet();
    }

    @Override
    public Set<Replica> getRemotePrimaryReplicas(String nodeId) {
        if (metadataNodeReplicas.stream().map(Replica::getId).filter(replicaId -> replicaId.equals(nodeId))
                .count() != 0) {
            return new HashSet<>(Arrays.asList(metadataPrimaryReplica));
        }
        return Collections.emptySet();
    }

    @Override
    public MetadataOnlyReplicationStrategy from(ReplicationProperties p, IConfigManager configManager)
            throws HyracksDataException {
        MetadataOnlyReplicationStrategy st = new MetadataOnlyReplicationStrategy();
        st.metadataProperties = p.getMetadataProperties();
        st.metadataPrimaryReplicaId = st.metadataProperties.getMetadataNodeName();
        st.metadataPrimaryReplica = new Replica(st.metadataPrimaryReplicaId,
                ((ConfigManager) configManager).getNodeEffectiveConfig(st.metadataPrimaryReplicaId)
                        .getString(NCConfig.Option.REPLICATION_LISTEN_ADDRESS),
                ((ConfigManager) configManager).getNodeEffectiveConfig(st.metadataPrimaryReplicaId)
                        .getInt(NCConfig.Option.REPLICATION_LISTEN_PORT));
        final Set<Replica> replicas = new HashSet<>();
        Set<String> candidateSet = new HashSet<>();
        candidateSet.addAll(((ConfigManager) (configManager)).getNodeNames());
        candidateSet.remove(st.metadataPrimaryReplicaId);
        String[] candidateAry = new String[candidateSet.size()];
        candidateSet.toArray(candidateAry);
        for (int i = 0; i < candidateAry.length && i < p.getReplicationFactor(); i++) {
            replicas.add(new Replica(candidateAry[i],
                    ((ConfigManager) configManager).getNodeEffectiveConfig(candidateAry[i])
                            .getString(NCConfig.Option.REPLICATION_LISTEN_ADDRESS),
                    ((ConfigManager) configManager).getNodeEffectiveConfig(candidateAry[i])
                            .getInt(NCConfig.Option.REPLICATION_LISTEN_PORT)));
        }
        st.metadataNodeReplicas = replicas;
        return st;
    }

    @Override
    public boolean isParticipant(String nodeId) {
        return true;
    }
}
