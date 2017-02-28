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

import java.io.InputStream;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.ReplicationStrategyFactory;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.event.schema.cluster.Replica;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ClusterProperties {

    public static final ClusterProperties INSTANCE = new ClusterProperties();
    private static final String CLUSTER_CONFIGURATION_FILE = "cluster.xml";
    private static final String DEFAULT_STORAGE_DIR_NAME = "storage";
    private String nodeNamePrefix = StringUtils.EMPTY;
    private Cluster cluster;

    private ClusterProperties() {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(CLUSTER_CONFIGURATION_FILE);
        if (is != null) {
            try {
                JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
                Unmarshaller unmarshaller = ctx.createUnmarshaller();
                cluster = (Cluster) unmarshaller.unmarshal(is);
                nodeNamePrefix = cluster.getInstanceName() + "_";
                updateNodeIdToFullName();
            } catch (JAXBException e) {
                throw new IllegalStateException("Failed to read configuration file " + CLUSTER_CONFIGURATION_FILE, e);
            }
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public String getStorageDirectoryName() {
        if (cluster != null) {
            return cluster.getStore();
        }
        // virtual cluster without cluster config file
        return DEFAULT_STORAGE_DIR_NAME;
    }

    public Node getNodeById(String nodeId) {
        Optional<Node> matchingNode = cluster.getNode().stream().filter(node -> node.getId().equals(nodeId)).findAny();
        return matchingNode.isPresent() ? matchingNode.get() : null;
    }

    public int getNodeIndex(String nodeId) {
        for (int i = 0; i < cluster.getNode().size(); i++) {
            Node node = cluster.getNode().get(i);
            if (node.getId().equals(nodeId)) {
                return i;
            }
        }
        return -1;
    }

    public IReplicationStrategy getReplicationStrategy() throws HyracksDataException {
        return ReplicationStrategyFactory.create(cluster);
    }

    private String getNodeFullName(String nodeId) {
        if (nodeId.startsWith(nodeNamePrefix)) {
            return nodeId;
        }
        return nodeNamePrefix + nodeId;
    }

    private void updateNodeIdToFullName() {
        cluster.getNode().forEach(node -> node.setId(getNodeFullName(node.getId())));
        if (cluster.getMetadataNode() != null) {
            cluster.setMetadataNode(getNodeFullName(cluster.getMetadataNode()));
        }
        if (cluster.getHighAvailability() != null && cluster.getHighAvailability().getFaultTolerance() != null
                && cluster.getHighAvailability().getFaultTolerance().getReplica() != null) {
            Replica replicas = cluster.getHighAvailability().getFaultTolerance().getReplica();
            replicas.setNodeId(replicas.getNodeId().stream().map(this::getNodeFullName).collect(Collectors.toList()));
        }
    }
}