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

import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class MetadataOnlyReplicationStrategy implements IReplicationStrategy {

    private String metadataNodeId;
    private Replica metadataPrimaryReplica;
    private Set<Replica> metadataNodeReplicas;

    @Override
    public boolean isMatch(int datasetId) {
        return datasetId < MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID && datasetId >= 0;
    }

    @Override
    public Set<Replica> getRemoteReplicas(String nodeId) {
        if (nodeId.equals(metadataNodeId)) {
            return metadataNodeReplicas;
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
    public MetadataOnlyReplicationStrategy from(Cluster cluster) throws HyracksDataException {
        if (cluster.getMetadataNode() == null) {
            throw new RuntimeDataException(ErrorCode.INVALID_CONFIGURATION, "Metadata node must be specified.");
        }

        Node metadataNode = ClusterProperties.INSTANCE.getNodeById(cluster.getMetadataNode());
        if (metadataNode == null) {
            throw new IllegalStateException("Invalid metadata node specified");
        }

        if (cluster.getHighAvailability().getFaultTolerance().getReplica() == null
                || cluster.getHighAvailability().getFaultTolerance().getReplica().getNodeId() == null
                || cluster.getHighAvailability().getFaultTolerance().getReplica().getNodeId().isEmpty()) {
            throw new RuntimeDataException(ErrorCode.INVALID_CONFIGURATION,
                    "One or more replicas must be specified for metadata node.");
        }

        final Set<Replica> replicas = new HashSet<>();
        for (String nodeId : cluster.getHighAvailability().getFaultTolerance().getReplica().getNodeId()) {
            Node node = ClusterProperties.INSTANCE.getNodeById(nodeId);
            if (node == null) {
                throw new RuntimeDataException(ErrorCode.INVALID_CONFIGURATION, "Invalid replica specified: " + nodeId);
            }
            replicas.add(new Replica(node));
        }
        MetadataOnlyReplicationStrategy st = new MetadataOnlyReplicationStrategy();
        st.metadataNodeId = cluster.getMetadataNode();
        st.metadataPrimaryReplica = new Replica(metadataNode);
        st.metadataNodeReplicas = replicas;
        return st;
    }
}
