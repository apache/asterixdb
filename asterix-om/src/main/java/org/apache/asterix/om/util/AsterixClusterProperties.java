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
import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.messaging.TakeoverMetadataNodeRequestMessage;
import org.apache.asterix.common.messaging.TakeoverMetadataNodeResponseMessage;
import org.apache.asterix.common.messaging.TakeoverPartitionsRequestMessage;
import org.apache.asterix.common.messaging.TakeoverPartitionsResponseMessage;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;

/**
 * A holder class for properties related to the Asterix cluster.
 */

public class AsterixClusterProperties {
    /**
     * TODO: currently after instance restarts we require all nodes to join again, otherwise the cluster wont be ACTIVE.
     * we may overcome this by storing the cluster state before the instance shutdown and using it on startup to identify
     * the nodes that are expected the join.
     */

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
    private Map<Long, TakeoverPartitionsRequestMessage> pendingTakeoverRequests = null;

    private long takeoverRequestId = 0;
    private String currentMetadataNode = null;
    private boolean isMetadataNodeActive = false;
    private boolean autoFailover = false;

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
        // if this is the CC process
        if (AsterixAppContextInfo.getInstance() != null) {
            if (AsterixAppContextInfo.getInstance().getCCApplicationContext() != null) {
                node2PartitionsMap = AsterixAppContextInfo.getInstance().getMetadataProperties().getNodePartitions();
                clusterPartitions = AsterixAppContextInfo.getInstance().getMetadataProperties().getClusterPartitions();
                currentMetadataNode = AsterixAppContextInfo.getInstance().getMetadataProperties().getMetadataNodeName();
                if (isAutoFailoverEnabled()) {
                    autoFailover = cluster.getDataReplication().isAutoFailover();
                }
                if (autoFailover) {
                    pendingTakeoverRequests = new HashMap<>();
                }
            }
        }
    }

    private ClusterState state = ClusterState.UNUSABLE;

    public synchronized void removeNCConfiguration(String nodeId) {
        updateNodePartitions(nodeId, false);
        ncConfiguration.remove(nodeId);
        if (nodeId.equals(currentMetadataNode)) {
            isMetadataNodeActive = false;
            LOGGER.info("Metadata node is now inactive");
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Removing configuration parameters for node id " + nodeId);
        }
        if (autoFailover) {
            requestPartitionsTakeover(nodeId);
        }
    }

    public synchronized void addNCConfiguration(String nodeId, Map<String, String> configuration) {
        ncConfiguration.put(nodeId, configuration);
        if (nodeId.equals(currentMetadataNode)) {
            isMetadataNodeActive = true;
            LOGGER.info("Metadata node is now active");
        }
        updateNodePartitions(nodeId, true);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" Registering configuration parameters for node id " + nodeId);
        }
    }

    private synchronized void updateNodePartitions(String nodeId, boolean added) {
        ClusterPartition[] nodePartitions = node2PartitionsMap.get(nodeId);
        // if this isn't a storage node, it will not have cluster partitions
        if (nodePartitions != null) {
            for (ClusterPartition p : nodePartitions) {
                // set the active node for this node's partitions
                p.setActive(added);
                if (added) {
                    p.setActiveNodeId(nodeId);
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
        //if all storage partitions are active as well as the metadata node, then the cluster is active
        if (isMetadataNodeActive) {
            state = ClusterState.ACTIVE;
            LOGGER.info("Cluster is now ACTIVE");
            //start global recovery
            AsterixAppContextInfo.getInstance().getGlobalRecoveryManager().startGlobalRecovery();
        } else {
            requestMetadataNodeTakeover();
        }
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
            // this is a virtual cluster
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
        // virtual cluster without cluster config file
        return DEFAULT_STORAGE_DIR_NAME;
    }

    private synchronized void requestPartitionsTakeover(String failedNodeId) {
        //replica -> list of partitions to takeover
        Map<String, List<Integer>> partitionRecoveryPlan = new HashMap<>();
        AsterixReplicationProperties replicationProperties = AsterixAppContextInfo.getInstance()
                .getReplicationProperties();

        //collect the partitions of the failed NC
        List<ClusterPartition> lostPartitions = getNodeAssignedPartitions(failedNodeId);
        for (ClusterPartition partition : lostPartitions) {
            //find replicas for this partitions
            Set<String> partitionReplicas = replicationProperties.getNodeReplicasIds(partition.getNodeId());
            //find a replica that is still active
            for (String replica : partitionReplicas) {
                //TODO (mhubail) currently this assigns the partition to the first found active replica.
                //It needs to be modified to consider load balancing.
                if (ncConfiguration.containsKey(replica)) {
                    if (!partitionRecoveryPlan.containsKey(replica)) {
                        List<Integer> replicaPartitions = new ArrayList<>();
                        replicaPartitions.add(partition.getPartitionId());
                        partitionRecoveryPlan.put(replica, replicaPartitions);
                    } else {
                        partitionRecoveryPlan.get(replica).add(partition.getPartitionId());
                    }
                }
            }
        }

        ICCMessageBroker messageBroker = (ICCMessageBroker) AsterixAppContextInfo.getInstance()
                .getCCApplicationContext().getMessageBroker();
        //For each replica, send a request to takeover the assigned partitions
        for (String replica : partitionRecoveryPlan.keySet()) {
            Integer[] partitionsToTakeover = partitionRecoveryPlan.get(replica).toArray(new Integer[] {});
            long requestId = takeoverRequestId++;
            TakeoverPartitionsRequestMessage takeoverRequest = new TakeoverPartitionsRequestMessage(requestId, replica,
                    failedNodeId, partitionsToTakeover);
            pendingTakeoverRequests.put(requestId, takeoverRequest);
            try {
                messageBroker.sendApplicationMessageToNC(takeoverRequest, replica);
            } catch (Exception e) {
                /**
                 * if we fail to send the request, it means the NC we tried to send the request to
                 * has failed. When the failure notification arrives, we will send any pending request
                 * that belongs to the failed NC to a different active replica.
                 */
                LOGGER.warning("Failed to send takeover request: " + takeoverRequest);
                e.printStackTrace();
            }
        }
    }

    private synchronized List<ClusterPartition> getNodeAssignedPartitions(String nodeId) {
        List<ClusterPartition> nodePartitions = new ArrayList<>();
        for (ClusterPartition partition : clusterPartitions.values()) {
            if (partition.getActiveNodeId().equals(nodeId)) {
                nodePartitions.add(partition);
            }
        }
        /**
         * if there is any pending takeover request that this node was supposed to handle,
         * it needs to be sent to a different replica
         */
        List<Long> failedTakeoverRequests = new ArrayList<>();
        for (TakeoverPartitionsRequestMessage request : pendingTakeoverRequests.values()) {
            if (request.getNodeId().equals(nodeId)) {
                for (Integer partitionId : request.getPartitions()) {
                    nodePartitions.add(clusterPartitions.get(partitionId));
                }
                failedTakeoverRequests.add(request.getId());
            }
        }

        //remove failed requests
        for (Long requestId : failedTakeoverRequests) {
            pendingTakeoverRequests.remove(requestId);
        }

        return nodePartitions;
    }

    private synchronized void requestMetadataNodeTakeover() {
        //need a new node to takeover metadata node
        ClusterPartition metadataPartiton = AsterixAppContextInfo.getInstance().getMetadataProperties()
                .getMetadataPartition();
        //request the metadataPartition node to register itself as the metadata node
        TakeoverMetadataNodeRequestMessage takeoverRequest = new TakeoverMetadataNodeRequestMessage();
        ICCMessageBroker messageBroker = (ICCMessageBroker) AsterixAppContextInfo.getInstance()
                .getCCApplicationContext().getMessageBroker();
        try {
            messageBroker.sendApplicationMessageToNC(takeoverRequest, metadataPartiton.getActiveNodeId());
        } catch (Exception e) {
            /**
             * if we fail to send the request, it means the NC we tried to send the request to
             * has failed. When the failure notification arrives, a new NC will be assigned to
             * the metadata partition and a new metadata node takeover request will be sent to it.
             */
            LOGGER.warning("Failed to send metadata node takeover request to: " + metadataPartiton.getActiveNodeId());
            e.printStackTrace();
        }
    }

    public synchronized void processPartitionTakeoverResponse(TakeoverPartitionsResponseMessage reponse) {
        for (Integer partitonId : reponse.getPartitions()) {
            ClusterPartition partition = clusterPartitions.get(partitonId);
            partition.setActive(true);
            partition.setActiveNodeId(reponse.getNodeId());
        }
        pendingTakeoverRequests.remove(reponse.getRequestId());
        resetClusterPartitionConstraint();
        updateClusterState();
    }

    public synchronized void processMetadataNodeTakeoverResponse(TakeoverMetadataNodeResponseMessage reponse) {
        currentMetadataNode = reponse.getNodeId();
        isMetadataNodeActive = true;
        LOGGER.info("Current metadata node: " + currentMetadataNode);
        updateClusterState();
    }

    public synchronized String getCurrentMetadataNode() {
        return currentMetadataNode;
    }

    public boolean isAutoFailoverEnabled() {
        if (cluster != null && cluster.getDataReplication() != null && cluster.getDataReplication().isEnabled()) {
            return cluster.getDataReplication().isAutoFailover();
        }
        return false;
    }
}