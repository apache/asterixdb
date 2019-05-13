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
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.replication.INcLifecycleCoordinator;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.asterix.common.utils.NcLocalCounters;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.application.ConfigManagerApplicationConfig;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A holder class for properties related to the Asterix cluster.
 */

public class ClusterStateManager implements IClusterStateManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<String, Map<IOption, Object>> ncConfigMap = new HashMap<>();
    private Set<String> pendingRemoval = new HashSet<>();
    private ClusterState state = ClusterState.UNUSABLE;
    private AlgebricksAbsolutePartitionConstraint clusterPartitionConstraint;
    private Map<String, ClusterPartition[]> node2PartitionsMap;
    private SortedMap<Integer, ClusterPartition> clusterPartitions;
    private String currentMetadataNode = null;
    private boolean metadataNodeActive = false;
    private Set<String> failedNodes = new HashSet<>();
    private Set<String> participantNodes = new HashSet<>();
    private INcLifecycleCoordinator lifecycleCoordinator;
    private ICcApplicationContext appCtx;
    private ClusterPartition metadataPartition;
    private boolean rebalanceRequired;

    @Override
    public void setCcAppCtx(ICcApplicationContext appCtx) {
        this.appCtx = appCtx;
        node2PartitionsMap = appCtx.getMetadataProperties().getNodePartitions();
        clusterPartitions = appCtx.getMetadataProperties().getClusterPartitions();
        currentMetadataNode = appCtx.getMetadataProperties().getMetadataNodeName();
        metadataPartition = node2PartitionsMap.get(currentMetadataNode)[0];
        lifecycleCoordinator = appCtx.getNcLifecycleCoordinator();
        lifecycleCoordinator.bindTo(this);
    }

    @Override
    public synchronized void notifyNodeFailure(String nodeId) throws HyracksException {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Removing configuration parameters for node id " + nodeId);
        }
        failedNodes.add(nodeId);
        ncConfigMap.remove(nodeId);
        pendingRemoval.remove(nodeId);
        lifecycleCoordinator.notifyNodeFailure(nodeId);
    }

    @Override
    public synchronized void notifyNodeJoin(String nodeId, Map<IOption, Object> configuration) throws HyracksException {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Registering configuration parameters for node id " + nodeId);
        }
        failedNodes.remove(nodeId);
        ncConfigMap.put(nodeId, configuration);
        updateNodeConfig(nodeId, configuration);
        lifecycleCoordinator.notifyNodeJoin(nodeId);
    }

    @Override
    public synchronized void setState(ClusterState state) {
        if (this.state == state) {
            LOGGER.info("ignoring update to same cluster state of " + this.state);
            return;
        }
        LOGGER.info("updating cluster state from " + this.state + " to " + state.name());
        this.state = state;
        appCtx.getGlobalRecoveryManager().notifyStateChange(state);
        LOGGER.info("Cluster State is now " + state.name());
        // Notify any waiting threads for the cluster state to change.
        notifyAll();
    }

    @Override
    public synchronized void updateMetadataNode(String nodeId, boolean active) {
        currentMetadataNode = nodeId;
        metadataNodeActive = active;
        if (active) {
            metadataPartition.setActiveNodeId(currentMetadataNode);
            LOGGER.info("Metadata node {} is now active", currentMetadataNode);
        }
        notifyAll();
    }

    @Override
    public synchronized void updateNodeState(String nodeId, boolean active, NcLocalCounters localCounters) {
        if (active) {
            updateClusterCounters(nodeId, localCounters);
            participantNodes.add(nodeId);
        } else {
            participantNodes.remove(nodeId);
        }
        ClusterPartition[] nodePartitions = node2PartitionsMap.get(nodeId);
        // if this isn't a storage node, it will not have cluster partitions
        if (nodePartitions != null) {
            for (ClusterPartition p : nodePartitions) {
                updateClusterPartition(p.getPartitionId(), nodeId, active);
            }
        }
    }

    @Override
    public synchronized void updateClusterPartition(int partitionNum, String activeNode, boolean active) {
        ClusterPartition clusterPartition = clusterPartitions.get(partitionNum);
        if (clusterPartition != null) {
            // set the active node for this node's partitions
            clusterPartition.setActive(active);
            if (active) {
                clusterPartition.setActiveNodeId(activeNode);
                clusterPartition.setPendingActivation(false);
            }
            notifyAll();
        }
    }

    @Override
    public synchronized void refreshState() throws HyracksDataException {
        if (state == ClusterState.SHUTTING_DOWN) {
            LOGGER.info("Not refreshing final state {}", state);
            return;
        }
        resetClusterPartitionConstraint();
        // if the cluster has no registered partitions or all partitions are pending activation -> UNUSABLE
        if (clusterPartitions.isEmpty()
                || clusterPartitions.values().stream().allMatch(ClusterPartition::isPendingActivation)) {
            LOGGER.info("Cluster does not have any registered partitions");
            setState(ClusterState.UNUSABLE);
            return;
        }

        // exclude partitions that are pending activation
        if (clusterPartitions.values().stream().anyMatch(p -> !p.isActive() && !p.isPendingActivation())) {
            setState(ClusterState.UNUSABLE);
            return;
        }
        // the metadata bootstrap & global recovery must be complete before the cluster can be active
        if (!metadataNodeActive) {
            setState(ClusterState.PENDING);
            return;
        }
        if (state != ClusterState.ACTIVE && state != ClusterState.RECOVERING) {
            setState(ClusterState.PENDING);
        }
        appCtx.getMetadataBootstrap().init();

        if (!appCtx.getGlobalRecoveryManager().isRecoveryCompleted()) {
            // start global recovery
            setState(ClusterState.RECOVERING);
            appCtx.getGlobalRecoveryManager().startGlobalRecovery(appCtx);
            return;
        }
        if (rebalanceRequired) {
            setState(ClusterState.REBALANCE_REQUIRED);
            return;
        }
        // finally- life is good, set the state to ACTIVE
        setState(ClusterState.ACTIVE);
    }

    @Override
    public synchronized void waitForState(ClusterState waitForState) throws InterruptedException {
        while (state != waitForState) {
            wait();
        }
    }

    @Override
    public boolean waitForState(ClusterState waitForState, long timeout, TimeUnit unit) throws InterruptedException {
        return waitForState(waitForState::equals, timeout, unit) != null;
    }

    @Override
    public synchronized ClusterState waitForState(Predicate<ClusterState> predicate, long timeout, TimeUnit unit)
            throws InterruptedException {
        final long startMillis = System.currentTimeMillis();
        final long endMillis = startMillis + unit.toMillis(timeout);
        while (!predicate.test(state)) {
            long millisToSleep = endMillis - System.currentTimeMillis();
            if (millisToSleep > 0) {
                wait(millisToSleep);
            } else {
                return null;
            }
        }
        return state;
    }

    @Override
    public synchronized String[] getIODevices(String nodeId) {
        Map<IOption, Object> ncConfig = ncConfigMap.get(nodeId);
        if (ncConfig == null) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Configuration parameters for nodeId " + nodeId
                        + " not found. The node has not joined yet or has left.");
            }
            return new String[0];
        }
        return (String[]) ncConfig.get(NCConfig.Option.IODEVICES);
    }

    @Override
    public synchronized ClusterState getState() {
        return state;
    }

    @Override
    public synchronized Set<String> getParticipantNodes() {
        return new HashSet<>(participantNodes);
    }

    @Override
    public synchronized Set<String> getFailedNodes() {
        return new HashSet<>(failedNodes);
    }

    @Override
    public synchronized Set<String> getNodes() {
        Set<String> nodes = new HashSet<>(participantNodes);
        nodes.addAll(failedNodes);
        return nodes;
    }

    @Override
    public synchronized Set<String> getParticipantNodes(boolean excludePendingRemoval) {
        final Set<String> participantNodesCopy = getParticipantNodes();
        if (excludePendingRemoval) {
            participantNodesCopy.removeAll(pendingRemoval);
        }
        return participantNodesCopy;
    }

    @Override
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
        clusterActiveLocations.removeAll(pendingRemoval);
        clusterPartitionConstraint =
                new AlgebricksAbsolutePartitionConstraint(clusterActiveLocations.toArray(new String[] {}));
    }

    @Override
    public synchronized boolean isClusterActive() {
        return state == ClusterState.ACTIVE;
    }

    @Override
    public synchronized int getNumberOfNodes() {
        return participantNodes.size();
    }

    @Override
    public synchronized ClusterPartition[] getNodePartitions(String nodeId) {
        return node2PartitionsMap.get(nodeId);
    }

    @Override
    public synchronized int getNodePartitionsCount(String node) {
        if (node2PartitionsMap.containsKey(node)) {
            return node2PartitionsMap.get(node).length;
        }
        return 0;
    }

    @Override
    public synchronized ClusterPartition[] getClusterPartitons() {
        return clusterPartitions.values().toArray(new ClusterPartition[] {});
    }

    @Override
    public synchronized boolean isMetadataNodeActive() {
        return metadataNodeActive;
    }

    @Override
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

    @Override
    public synchronized ObjectNode getClusterStateSummary() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode stateDescription = om.createObjectNode();
        stateDescription.put("state", state.name());
        stateDescription.putPOJO("metadata_node", currentMetadataNode);
        stateDescription.putPOJO("partitions", clusterPartitions);
        return stateDescription;
    }

    @Override
    public Map<String, Map<IOption, Object>> getNcConfiguration() {
        return Collections.unmodifiableMap(ncConfigMap);
    }

    @Override
    public String getCurrentMetadataNodeId() {
        return currentMetadataNode;
    }

    @Override
    public synchronized void registerNodePartitions(String nodeId, ClusterPartition[] nodePartitions)
            throws AlgebricksException {
        if (LOGGER.isInfoEnabled()) {
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
            nodePartition.setPendingActivation(true);
            clusterPartitions.put(nodePartition.getPartitionId(), nodePartition);
        }
        node2PartitionsMap.put(nodeId, nodePartitions);
    }

    @Override
    public synchronized void deregisterNodePartitions(String nodeId) throws HyracksDataException {
        ClusterPartition[] nodePartitions = node2PartitionsMap.remove(nodeId);
        if (nodePartitions == null) {
            LOGGER.info("deregisterNodePartitions unknown node " + nodeId + " (already removed?)");
        } else {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("deregisterNodePartitions for node " + nodeId + ": " + Arrays.toString(nodePartitions));
            }
            for (ClusterPartition nodePartition : nodePartitions) {
                clusterPartitions.remove(nodePartition.getPartitionId());
            }
            participantNodes.remove(nodeId);
        }
    }

    @Override
    public synchronized void removePending(String nodeId) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Registering intention to remove node id " + nodeId);
        }
        if (participantNodes.contains(nodeId)) {
            pendingRemoval.add(nodeId);
        } else {
            LOGGER.warn("Cannot register unknown node " + nodeId + " for pending removal");
        }
    }

    @Override
    public synchronized boolean cancelRemovePending(String nodeId) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Deregistering intention to remove node id " + nodeId);
        }
        if (!pendingRemoval.remove(nodeId)) {
            LOGGER.warn("Cannot deregister intention to remove node id " + nodeId + " that was not registered");
            return false;
        } else {
            return true;
        }
    }

    @Override
    public Map<String, Map<IOption, Object>> getActiveNcConfiguration() {
        return ncConfigMap;
    }

    public synchronized Set<String> getNodesPendingRemoval() {
        return new HashSet<>(pendingRemoval);
    }

    @Override
    public synchronized void setMetadataPartitionId(ClusterPartition partition) {
        metadataPartition = partition;
    }

    @Override
    public synchronized ClusterPartition getMetadataPartition() {
        return metadataPartition;
    }

    @Override
    public synchronized void setRebalanceRequired(boolean rebalanceRequired) throws HyracksDataException {
        this.rebalanceRequired = rebalanceRequired;
        refreshState();
    }

    private void updateClusterCounters(String nodeId, NcLocalCounters localCounters) {
        final IResourceIdManager resourceIdManager = appCtx.getResourceIdManager();
        resourceIdManager.report(nodeId, localCounters.getMaxResourceId());
        appCtx.getTxnIdFactory().ensureMinimumId(localCounters.getMaxTxnId());
        ((ClusterControllerService) appCtx.getServiceContext().getControllerService()).getJobIdFactory()
                .setMaxJobId(localCounters.getMaxJobId());
    }

    private void updateNodeConfig(String nodeId, Map<IOption, Object> configuration) {
        ConfigManager configManager =
                ((ConfigManagerApplicationConfig) appCtx.getServiceContext().getAppConfig()).getConfigManager();
        configuration.forEach((key, value) -> {
            if (key.section() == Section.NC) {
                configManager.set(nodeId, key, value);
            }
        });
    }

}
