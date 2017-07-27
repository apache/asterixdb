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
package org.apache.asterix.runtime.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.ClusterProperties;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.control.common.controllers.NCConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A holder class for properties related to the Asterix cluster.
 */

public class ClusterStateManager implements IClusterStateManager {
    /*
     * TODO: currently after instance restarts we require all nodes to join again,
     * otherwise the cluster wont be ACTIVE. we may overcome this by storing the cluster state before the instance
     * shutdown and using it on startup to identify the nodes that are expected the join.
     */

    private static final Logger LOGGER = Logger.getLogger(ClusterStateManager.class.getName());
    public static final ClusterStateManager INSTANCE = new ClusterStateManager();
    private final Map<String, Map<IOption, Object>> activeNcConfiguration = new HashMap<>();
    private Set<String> pendingRemoval = new HashSet<>();
    private final Cluster cluster;
    private ClusterState state = ClusterState.UNUSABLE;
    private AlgebricksAbsolutePartitionConstraint clusterPartitionConstraint;
    private Map<String, ClusterPartition[]> node2PartitionsMap;
    private SortedMap<Integer, ClusterPartition> clusterPartitions;
    private String currentMetadataNode = null;
    private boolean metadataNodeActive = false;
    private Set<String> failedNodes = new HashSet<>();
    private IFaultToleranceStrategy ftStrategy;
    private CcApplicationContext appCtx;

    private ClusterStateManager() {
        cluster = ClusterProperties.INSTANCE.getCluster();
    }

    public void setCcAppCtx(CcApplicationContext appCtx) {
        this.appCtx = appCtx;
        node2PartitionsMap = appCtx.getMetadataProperties().getNodePartitions();
        clusterPartitions = appCtx.getMetadataProperties().getClusterPartitions();
        currentMetadataNode = appCtx.getMetadataProperties().getMetadataNodeName();
        ftStrategy = appCtx.getFaultToleranceStrategy();
        ftStrategy.bindTo(this);
    }

    public synchronized void removeNCConfiguration(String nodeId) throws HyracksException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Removing configuration parameters for node id " + nodeId);
        }
        failedNodes.add(nodeId);
        ftStrategy.notifyNodeFailure(nodeId);
        pendingRemoval.remove(nodeId);
    }

    public synchronized void addNCConfiguration(String nodeId, Map<IOption, Object> configuration)
            throws HyracksException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registering configuration parameters for node id " + nodeId);
        }
        activeNcConfiguration.put(nodeId, configuration);
        failedNodes.remove(nodeId);
        ftStrategy.notifyNodeJoin(nodeId);
    }

    @Override
    public synchronized void setState(ClusterState state) {
        LOGGER.info("updating cluster state from " + this.state + " to " + state.name());
        this.state = state;
        appCtx.getGlobalRecoveryManager().notifyStateChange(state);
        LOGGER.info("Cluster State is now " + state.name());
        // Notify any waiting threads for the cluster state to change.
        notifyAll();
    }

    @Override
    public void updateMetadataNode(String nodeId, boolean active) {
        currentMetadataNode = nodeId;
        metadataNodeActive = active;
        if (active) {
            LOGGER.info(String.format("Metadata node %s is now active", currentMetadataNode));
        }
    }

    @Override
    public synchronized void updateNodePartitions(String nodeId, boolean active) throws HyracksDataException {
        ClusterPartition[] nodePartitions = node2PartitionsMap.get(nodeId);
        // if this isn't a storage node, it will not have cluster partitions
        if (nodePartitions != null) {
            for (ClusterPartition p : nodePartitions) {
                updateClusterPartition(p.getPartitionId(), nodeId, active);
            }
        }
    }

    @Override
    public synchronized void updateClusterPartition(Integer partitionNum, String activeNode, boolean active) {
        ClusterPartition clusterPartition = clusterPartitions.get(partitionNum);
        if (clusterPartition != null) {
            // set the active node for this node's partitions
            clusterPartition.setActive(active);
            if (active) {
                clusterPartition.setActiveNodeId(activeNode);
            }
        }
    }

    @Override
    public synchronized void refreshState() throws HyracksDataException {
        resetClusterPartitionConstraint();
        for (ClusterPartition p : clusterPartitions.values()) {
            if (!p.isActive()) {
                setState(ClusterState.UNUSABLE);
                return;
            }
        }

        // if all storage partitions are active as well as the metadata node, then the cluster is active
        if (metadataNodeActive) {
            if (state != ClusterState.ACTIVE) {
                setState(ClusterState.PENDING);
            }
            appCtx.getMetadataBootstrap().init();
            setState(ClusterState.ACTIVE);
            notifyAll();
            // start global recovery
            appCtx.getGlobalRecoveryManager().startGlobalRecovery(appCtx);
        } else {
            setState(ClusterState.PENDING);
        }
    }

    @Override
    public synchronized void waitForState(ClusterState waitForState) throws HyracksDataException, InterruptedException {
        while (state != waitForState) {
            wait();
        }
    }

    @Override
    public synchronized boolean waitForState(ClusterState waitForState, long timeout, TimeUnit unit)
            throws HyracksDataException, InterruptedException {
        final long startMillis = System.currentTimeMillis();
        final long endMillis = startMillis + unit.toMillis(timeout);
        while (state != waitForState) {
            long millisToSleep = endMillis - System.currentTimeMillis();
            if (millisToSleep > 0) {
                wait(millisToSleep);
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the IO devices configured for a Node Controller
     *
     * @param nodeId
     *            unique identifier of the Node Controller
     * @return a list of IO devices.
     */
    public synchronized String[] getIODevices(String nodeId) {
        Map<IOption, Object> ncConfig = activeNcConfiguration.get(nodeId);
        if (ncConfig == null) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Configuration parameters for nodeId " + nodeId
                        + " not found. The node has not joined yet or has left.");
            }
            return new String[0];
        }
        return (String[]) ncConfig.get(NCConfig.Option.IODEVICES);
    }

    @Override
    public ClusterState getState() {
        return state;
    }

    public synchronized Node getAvailableSubstitutionNode() {
        List<Node> subNodes = cluster.getSubstituteNodes() == null ? null : cluster.getSubstituteNodes().getNode();
        return subNodes == null || subNodes.isEmpty() ? null : subNodes.get(0);
    }

    public synchronized Set<String> getParticipantNodes() {
        Set<String> participantNodes = new HashSet<>();
        for (String pNode : activeNcConfiguration.keySet()) {
            participantNodes.add(pNode);
        }
        return participantNodes;
    }

    public synchronized Set<String> getParticipantNodes(boolean excludePendingRemoval) {
        Set<String> participantNodes = getParticipantNodes();
        if (excludePendingRemoval) {
            participantNodes.removeAll(pendingRemoval);
        }
        return participantNodes;
    }

    public synchronized AlgebricksAbsolutePartitionConstraint getClusterLocations() {
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
        clusterPartitionConstraint =
                new AlgebricksAbsolutePartitionConstraint(clusterActiveLocations.toArray(new String[] {}));
    }

    public boolean isClusterActive() {
        if (cluster == null) {
            // this is a virtual cluster
            return true;
        }
        return state == ClusterState.ACTIVE;
    }

    public int getNumberOfNodes() {
        return appCtx.getMetadataProperties().getNodeNames().size();
    }

    @Override
    public synchronized ClusterPartition[] getNodePartitions(String nodeId) {
        return node2PartitionsMap.get(nodeId);
    }

    public synchronized int getNodePartitionsCount(String node) {
        if (node2PartitionsMap.containsKey(node)) {
            return node2PartitionsMap.get(node).length;
        }
        return 0;
    }

    @Override
    public synchronized ClusterPartition[] getClusterPartitons() {
        ArrayList<ClusterPartition> partitons = new ArrayList<>();
        for (ClusterPartition partition : clusterPartitions.values()) {
            partitons.add(partition);
        }
        return partitons.toArray(new ClusterPartition[] {});
    }

    public synchronized boolean isMetadataNodeActive() {
        return metadataNodeActive;
    }

    public synchronized ObjectNode getClusterStateDescription() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode stateDescription = om.createObjectNode();
        stateDescription.put("state", state.name());
        stateDescription.put("metadata_node", currentMetadataNode);
        ArrayNode ncs = om.createArrayNode();
        stateDescription.set("ncs", ncs);
        for (String node : new TreeSet<>(node2PartitionsMap.keySet())) {
            ObjectNode nodeJSON = om.createObjectNode();
            nodeJSON.put("node_id", node);
            boolean allActive = true;
            boolean anyActive = false;
            Set<Map<String, Object>> partitions = new HashSet<>();
            if (node2PartitionsMap.containsKey(node)) {
                for (ClusterPartition part : node2PartitionsMap.get(node)) {
                    HashMap<String, Object> partition = new HashMap<>();
                    partition.put("partition_id", "partition_" + part.getPartitionId());
                    partition.put("active", part.isActive());
                    partitions.add(partition);
                    allActive = allActive && part.isActive();
                    if (allActive) {
                        anyActive = true;
                    }
                }
            }
            nodeJSON.put("state", failedNodes.contains(node) ? "FAILED"
                    : allActive && anyActive ? "ACTIVE" : anyActive ? "PARTIALLY_ACTIVE" : "INACTIVE");
            nodeJSON.putPOJO("partitions", partitions);
            ncs.add(nodeJSON);
        }
        return stateDescription;
    }

    public synchronized ObjectNode getClusterStateSummary() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode stateDescription = om.createObjectNode();
        stateDescription.put("state", state.name());
        stateDescription.putPOJO("metadata_node", currentMetadataNode);
        stateDescription.putPOJO("partitions", clusterPartitions);
        return stateDescription;
    }

    @Override
    public Map<String, Map<IOption, Object>> getActiveNcConfiguration() {
        return Collections.unmodifiableMap(activeNcConfiguration);
    }

    @Override
    public String getCurrentMetadataNodeId() {
        return currentMetadataNode;
    }

    @Override
    public synchronized void registerNodePartitions(String nodeId, ClusterPartition[] nodePartitions)
            throws AsterixException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registering node partitions for node " + nodeId + ": " + Arrays.toString(nodePartitions));
        }
        // We want to make sure there are no conflicts; make two passes for simplicity...
        for (ClusterPartition nodePartition : nodePartitions) {
            if (clusterPartitions.containsKey(nodePartition.getPartitionId())) {
                throw AsterixException.create(ErrorCode.DUPLICATE_PARTITION_ID, nodePartition.getPartitionId(), nodeId,
                        clusterPartitions.get(nodePartition.getPartitionId()).getNodeId());
            }
        }
        for (ClusterPartition nodePartition : nodePartitions) {
            clusterPartitions.put(nodePartition.getPartitionId(), nodePartition);
        }
        node2PartitionsMap.put(nodeId, nodePartitions);
    }

    @Override
    public synchronized void deregisterNodePartitions(String nodeId) {
        ClusterPartition[] nodePartitions = node2PartitionsMap.remove(nodeId);
        if (nodePartitions == null) {
            LOGGER.info("deregisterNodePartitions unknown node " + nodeId + " (already removed?)");
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("deregisterNodePartitions for node " + nodeId + ": " + Arrays.toString(nodePartitions));
            }
            for (ClusterPartition nodePartition : nodePartitions) {
                clusterPartitions.remove(nodePartition.getPartitionId());
            }
        }
    }

    public synchronized void removePending(String nodeId) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registering intention to remove node id " + nodeId);
        }
        if (!activeNcConfiguration.containsKey(nodeId)) {
            LOGGER.warning("Cannot register unknown node " + nodeId + " for pending removal");
        }
        pendingRemoval.add(nodeId);
    }

    public synchronized boolean cancelRemovePending(String nodeId) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Deregistering intention to remove node id " + nodeId);
        }
        if (!pendingRemoval.remove(nodeId)) {
            LOGGER.warning("Cannot deregister intention to remove node id " + nodeId + " that was not registered");
            return false;
        } else {
            return true;
        }
    }

    public synchronized Set<String> getNodesPendingRemoval() {
        return new HashSet<>(pendingRemoval);
    }

}
