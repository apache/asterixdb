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
package org.apache.asterix.om.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;

/**
 * A holder class for properties related to the Asterix cluster.
 */

public class AsterixClusterProperties {

    private static final Logger LOGGER = Logger.getLogger(AsterixClusterProperties.class.getName());

    public static final AsterixClusterProperties INSTANCE = new AsterixClusterProperties();
    public static final String CLUSTER_CONFIGURATION_FILE = "cluster.xml";

    private static final String IO_DEVICES = "iodevices";
    private static final String DEFAULT_STORAGE_DIR_NAME = "storage";
    private Map<String, Map<String, String>> ncConfiguration = new HashMap<String, Map<String, String>>();

    private final Cluster cluster;

    private AlgebricksAbsolutePartitionConstraint clusterPartitionConstraint;

    private boolean globalRecoveryCompleted = false;

    private Map<String, ClusterPartition[]> node2PartitionsMap = null;
    private SortedMap<Integer, ClusterPartition> clusterPartitions = null;

    private AsterixClusterProperties() {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(CLUSTER_CONFIGURATION_FILE);
        if (is != null) {
            try {
                JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
                Unmarshaller unmarshaller = ctx.createUnmarshaller();
                cluster = (Cluster) unmarshaller.unmarshal(is);
            } catch (JAXBException e) {
                throw new IllegalStateException("Failed to read configuration file " + CLUSTER_CONFIGURATION_FILE);
            }
        } else {
            cluster = null;
        }
        //if this is the CC process
        if (AsterixAppContextInfo.getInstance() != null) {
            if (AsterixAppContextInfo.getInstance().getCCApplicationContext() != null) {
                node2PartitionsMap = AsterixAppContextInfo.getInstance().getMetadataProperties().getNodePartitions();
                clusterPartitions = AsterixAppContextInfo.getInstance().getMetadataProperties().getClusterPartitions();
            }
        }
    }

    private ClusterState state = ClusterState.UNUSABLE;

    public synchronized void removeNCConfiguration(String nodeId) {
        updateNodePartitions(nodeId, false);
        ncConfiguration.remove(nodeId);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" Removing configuration parameters for node id " + nodeId);
        }
        //TODO implement fault tolerance as follows:
        //1. collect the partitions of the failed NC
        //2. For each partition, request a remote replica to take over. 
        //3. wait until each remote replica completes the recovery for the lost partitions
        //4. update the cluster state
    }

    public synchronized void addNCConfiguration(String nodeId, Map<String, String> configuration) {
        ncConfiguration.put(nodeId, configuration);
        updateNodePartitions(nodeId, true);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" Registering configuration parameters for node id " + nodeId);
        }
    }

    private synchronized void updateNodePartitions(String nodeId, boolean added) {
        ClusterPartition[] nodePartitions = node2PartitionsMap.get(nodeId);
        //if this isn't a storage node, it will not have cluster partitions
        if (nodePartitions != null) {
            for (ClusterPartition p : nodePartitions) {
                //set the active node for this node's partitions
                p.setActive(added);
                if (added) {
                    p.setActiveNodeId(nodeId);
                } else {
                    p.setActiveNodeId(null);
                }
            }
            resetClusterPartitionConstraint();
            updateClusterState();
        }
    }

    private synchronized void updateClusterState() {
        for (ClusterPartition p : clusterPartitions.values()) {
            if (!p.isActive()) {
                state = ClusterState.UNUSABLE;
                LOGGER.info("Cluster is in UNSABLE state");
                return;
            }
        }
        //if all storage partitions are active, then the cluster is active
        state = ClusterState.ACTIVE;
        LOGGER.info("Cluster is now ACTIVE");
    }

    /**
     * Returns the number of IO devices configured for a Node Controller
     *
     * @param nodeId
     *            unique identifier of the Node Controller
     * @return number of IO devices. -1 if the node id is not valid. A node id
     *         is not valid if it does not correspond to the set of registered
     *         Node Controllers.
     */
    public int getNumberOfIODevices(String nodeId) {
        String[] ioDevs = getIODevices(nodeId);
        return ioDevs == null ? -1 : ioDevs.length;
    }

    /**
     * Returns the IO devices configured for a Node Controller
     *
     * @param nodeId
     *            unique identifier of the Node Controller
     * @return a list of IO devices. null if node id is not valid. A node id is not valid
     *         if it does not correspond to the set of registered Node Controllers.
     */
    public synchronized String[] getIODevices(String nodeId) {
        Map<String, String> ncConfig = ncConfiguration.get(nodeId);
        if (ncConfig == null) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Configuration parameters for nodeId " + nodeId
                        + " not found. The node has not joined yet or has left.");
            }
            return null;
        }
        return ncConfig.get(IO_DEVICES).split(",");
    }

    public ClusterState getState() {
        return state;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public synchronized Node getAvailableSubstitutionNode() {
        List<Node> subNodes = cluster.getSubstituteNodes() == null ? null : cluster.getSubstituteNodes().getNode();
        return subNodes == null || subNodes.isEmpty() ? null : subNodes.get(0);
    }

    public synchronized Set<String> getParticipantNodes() {
        Set<String> participantNodes = new HashSet<String>();
        for (String pNode : ncConfiguration.keySet()) {
            participantNodes.add(pNode);
        }
        return participantNodes;
    }

    public synchronized AlgebricksPartitionConstraint getClusterLocations() {
        if (clusterPartitionConstraint == null) {
            resetClusterPartitionConstraint();
        }
        return clusterPartitionConstraint;
    }

    private synchronized void resetClusterPartitionConstraint() {
        ArrayList<String> clusterActiveLocations = new ArrayList<>();
        for (ClusterPartition p : clusterPartitions.values()) {
            if (p.isActive()) {
                clusterActiveLocations.add(p.getActiveNodeId());
            }
        }
        clusterPartitionConstraint = new AlgebricksAbsolutePartitionConstraint(
                clusterActiveLocations.toArray(new String[] {}));
    }

    public boolean isGlobalRecoveryCompleted() {
        return globalRecoveryCompleted;
    }

    public void setGlobalRecoveryCompleted(boolean globalRecoveryCompleted) {
        this.globalRecoveryCompleted = globalRecoveryCompleted;
    }

    public static boolean isClusterActive() {
        if (AsterixClusterProperties.INSTANCE.getCluster() == null) {
            //this is a virtual cluster
            return true;
        }
        return AsterixClusterProperties.INSTANCE.getState() == ClusterState.ACTIVE;
    }

    public static int getNumberOfNodes() {
        return AsterixAppContextInfo.getInstance().getMetadataProperties().getNodeNames().size();
    }

    public synchronized ClusterPartition[] getNodePartitions(String nodeId) {
        return node2PartitionsMap.get(nodeId);
    }

    public synchronized int getNodePartitionsCount(String node) {
        if (node2PartitionsMap.containsKey(node)) {
            return node2PartitionsMap.get(node).length;
        }
        return 0;
    }

    public synchronized ClusterPartition[] getClusterPartitons() {
        ArrayList<ClusterPartition> partitons = new ArrayList<>();
        for (ClusterPartition cluster : clusterPartitions.values()) {
            partitons.add(cluster);
        }
        return partitons.toArray(new ClusterPartition[] {});
    }

    public String getStorageDirectoryName() {
        if (cluster != null) {
            return cluster.getStore();
        }
        //virtual cluster without cluster config file
        return DEFAULT_STORAGE_DIR_NAME;
    }
}
