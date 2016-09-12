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
package org.apache.asterix.runtime.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.event.schema.cluster.Cluster;
import org.apache.asterix.event.schema.cluster.Node;
import org.apache.asterix.runtime.message.CompleteFailbackRequestMessage;
import org.apache.asterix.runtime.message.CompleteFailbackResponseMessage;
import org.apache.asterix.runtime.message.NodeFailbackPlan;
import org.apache.asterix.runtime.message.NodeFailbackPlan.FailbackPlanState;
import org.apache.asterix.runtime.message.PreparePartitionsFailbackRequestMessage;
import org.apache.asterix.runtime.message.PreparePartitionsFailbackResponseMessage;
import org.apache.asterix.runtime.message.ReplicaEventMessage;
import org.apache.asterix.runtime.message.TakeoverMetadataNodeRequestMessage;
import org.apache.asterix.runtime.message.TakeoverMetadataNodeResponseMessage;
import org.apache.asterix.runtime.message.TakeoverPartitionsRequestMessage;
import org.apache.asterix.runtime.message.TakeoverPartitionsResponseMessage;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.IClusterLifecycleListener.ClusterEventType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A holder class for properties related to the Asterix cluster.
 */

public class AsterixClusterProperties {
    /*
     * TODO: currently after instance restarts we require all nodes to join again,
     * otherwise the cluster wont be ACTIVE. we may overcome this by storing the cluster state before the instance
     * shutdown and using it on startup to identify the nodes that are expected the join.
     */

    private static final Logger LOGGER = Logger.getLogger(AsterixClusterProperties.class.getName());
    public static final AsterixClusterProperties INSTANCE = new AsterixClusterProperties();
    public static final String CLUSTER_CONFIGURATION_FILE = "cluster.xml";

    private static final String CLUSTER_NET_IP_ADDRESS_KEY = "cluster-net-ip-address";
    private static final String IO_DEVICES = "iodevices";
    private static final String DEFAULT_STORAGE_DIR_NAME = "storage";
    private Map<String, Map<String, String>> activeNcConfiguration = new HashMap<>();

    private final Cluster cluster;
    private ClusterState state = ClusterState.UNUSABLE;

    private AlgebricksAbsolutePartitionConstraint clusterPartitionConstraint;

    private boolean globalRecoveryCompleted = false;

    private Map<String, ClusterPartition[]> node2PartitionsMap = null;
    private SortedMap<Integer, ClusterPartition> clusterPartitions = null;
    private Map<Long, TakeoverPartitionsRequestMessage> pendingTakeoverRequests = null;

    private long clusterRequestId = 0;
    private String currentMetadataNode = null;
    private boolean metadataNodeActive = false;
    private boolean autoFailover = false;
    private boolean replicationEnabled = false;
    private Set<String> failedNodes = new HashSet<>();
    private LinkedList<NodeFailbackPlan> pendingProcessingFailbackPlans;
    private Map<Long, NodeFailbackPlan> planId2FailbackPlanMap;

    private AsterixClusterProperties() {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(CLUSTER_CONFIGURATION_FILE);
        if (is != null) {
            try {
                JAXBContext ctx = JAXBContext.newInstance(Cluster.class);
                Unmarshaller unmarshaller = ctx.createUnmarshaller();
                cluster = (Cluster) unmarshaller.unmarshal(is);
            } catch (JAXBException e) {
                throw new IllegalStateException("Failed to read configuration file " + CLUSTER_CONFIGURATION_FILE, e);
            }
        } else {
            cluster = null;
        }
        // if this is the CC process
        if (AsterixAppContextInfo.INSTANCE.initialized()
                && AsterixAppContextInfo.INSTANCE.getCCApplicationContext() != null) {
            node2PartitionsMap = AsterixAppContextInfo.INSTANCE.getMetadataProperties().getNodePartitions();
            clusterPartitions = AsterixAppContextInfo.INSTANCE.getMetadataProperties().getClusterPartitions();
            currentMetadataNode = AsterixAppContextInfo.INSTANCE.getMetadataProperties().getMetadataNodeName();
            replicationEnabled = isReplicationEnabled();
            autoFailover = isAutoFailoverEnabled();
            if (autoFailover) {
                pendingTakeoverRequests = new HashMap<>();
                pendingProcessingFailbackPlans = new LinkedList<>();
                planId2FailbackPlanMap = new HashMap<>();
            }
        }
    }

    public synchronized void removeNCConfiguration(String nodeId) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Removing configuration parameters for node id " + nodeId);
        }
        activeNcConfiguration.remove(nodeId);

        //if this node was waiting for failback and failed before it completed
        if (failedNodes.contains(nodeId)) {
            if (autoFailover) {
                notifyFailbackPlansNodeFailure(nodeId);
                revertFailedFailbackPlanEffects();
            }
        } else {
            //an active node failed
            failedNodes.add(nodeId);
            if (nodeId.equals(currentMetadataNode)) {
                metadataNodeActive = false;
                LOGGER.info("Metadata node is now inactive");
            }
            updateNodePartitions(nodeId, false);
            if (replicationEnabled) {
                notifyImpactedReplicas(nodeId, ClusterEventType.NODE_FAILURE);
                if (autoFailover) {
                    notifyFailbackPlansNodeFailure(nodeId);
                    requestPartitionsTakeover(nodeId);
                }
            }
        }
    }

    public synchronized void addNCConfiguration(String nodeId, Map<String, String> configuration) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registering configuration parameters for node id " + nodeId);
        }
        activeNcConfiguration.put(nodeId, configuration);

        //a node trying to come back after failure
        if (failedNodes.contains(nodeId)) {
            if (autoFailover) {
                prepareFailbackPlan(nodeId);
                return;
            } else {
                //a node completed local or remote recovery and rejoined
                failedNodes.remove(nodeId);
                if (replicationEnabled) {
                    //notify other replica to reconnect to this node
                    notifyImpactedReplicas(nodeId, ClusterEventType.NODE_JOIN);
                }
            }
        }

        if (nodeId.equals(currentMetadataNode)) {
            metadataNodeActive = true;
            LOGGER.info("Metadata node is now active");
        }
        updateNodePartitions(nodeId, true);
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
                LOGGER.info("Cluster is in UNUSABLE state");
                return;
            }
        }
        //if all storage partitions are active as well as the metadata node, then the cluster is active
        if (metadataNodeActive) {
            state = ClusterState.ACTIVE;
            LOGGER.info("Cluster is now ACTIVE");
            //start global recovery
            AsterixAppContextInfo.INSTANCE.getGlobalRecoveryManager().startGlobalRecovery();
            if (autoFailover && !pendingProcessingFailbackPlans.isEmpty()) {
                processPendingFailbackPlans();
            }
        } else {
            requestMetadataNodeTakeover();
        }
    }

    /**
     * Returns the IO devices configured for a Node Controller
     *
     * @param nodeId
     *            unique identifier of the Node Controller
     * @return a list of IO devices.
     */
    public synchronized String[] getIODevices(String nodeId) {
        Map<String, String> ncConfig = activeNcConfiguration.get(nodeId);
        if (ncConfig == null) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Configuration parameters for nodeId " + nodeId
                        + " not found. The node has not joined yet or has left.");
            }
            return new String[0];
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
        Set<String> participantNodes = new HashSet<>();
        for (String pNode : activeNcConfiguration.keySet()) {
            participantNodes.add(pNode);
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
        clusterPartitionConstraint = new AlgebricksAbsolutePartitionConstraint(
                clusterActiveLocations.toArray(new String[] {}));
    }

    public boolean isGlobalRecoveryCompleted() {
        return globalRecoveryCompleted;
    }

    public void setGlobalRecoveryCompleted(boolean globalRecoveryCompleted) {
        this.globalRecoveryCompleted = globalRecoveryCompleted;
    }

    public boolean isClusterActive() {
        if (cluster == null) {
            // this is a virtual cluster
            return true;
        }
        return state == ClusterState.ACTIVE;
    }

    public static int getNumberOfNodes() {
        return AsterixAppContextInfo.INSTANCE.getMetadataProperties().getNodeNames().size();
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
        for (ClusterPartition partition : clusterPartitions.values()) {
            partitons.add(partition);
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
        AsterixReplicationProperties replicationProperties = AsterixAppContextInfo.INSTANCE
                .getReplicationProperties();

        //collect the partitions of the failed NC
        List<ClusterPartition> lostPartitions = getNodeAssignedPartitions(failedNodeId);
        if (!lostPartitions.isEmpty()) {
            for (ClusterPartition partition : lostPartitions) {
                //find replicas for this partitions
                Set<String> partitionReplicas = replicationProperties.getNodeReplicasIds(partition.getNodeId());
                //find a replica that is still active
                for (String replica : partitionReplicas) {
                    //TODO (mhubail) currently this assigns the partition to the first found active replica.
                    //It needs to be modified to consider load balancing.
                    addActiveReplica(replica, partition, partitionRecoveryPlan);
                    // bug? will always break on first loop execution
                    break;
                }
            }

            if (partitionRecoveryPlan.size() == 0) {
                //no active replicas were found for the failed node
                LOGGER.severe("Could not find active replicas for the partitions " + lostPartitions);
                return;
            } else {
                LOGGER.info("Partitions to recover: " + lostPartitions);
            }
            ICCMessageBroker messageBroker = (ICCMessageBroker) AsterixAppContextInfo.INSTANCE
                    .getCCApplicationContext().getMessageBroker();
            //For each replica, send a request to takeover the assigned partitions
            for (Entry<String, List<Integer>> entry : partitionRecoveryPlan.entrySet()) {
                String replica = entry.getKey();
                Integer[] partitionsToTakeover = entry.getValue().toArray(new Integer[entry.getValue().size()]);
                long requestId = clusterRequestId++;
                TakeoverPartitionsRequestMessage takeoverRequest = new TakeoverPartitionsRequestMessage(requestId,
                        replica, partitionsToTakeover);
                pendingTakeoverRequests.put(requestId, takeoverRequest);
                try {
                    messageBroker.sendApplicationMessageToNC(takeoverRequest, replica);
                } catch (Exception e) {
                    /**
                     * if we fail to send the request, it means the NC we tried to send the request to
                     * has failed. When the failure notification arrives, we will send any pending request
                     * that belongs to the failed NC to a different active replica.
                     */
                    LOGGER.log(Level.WARNING, "Failed to send takeover request: " + takeoverRequest, e);
                }
            }
        }
    }

    private void addActiveReplica(String replica, ClusterPartition partition,
            Map<String, List<Integer>> partitionRecoveryPlan) {
        if (activeNcConfiguration.containsKey(replica) && !failedNodes.contains(replica)) {
            if (!partitionRecoveryPlan.containsKey(replica)) {
                List<Integer> replicaPartitions = new ArrayList<>();
                replicaPartitions.add(partition.getPartitionId());
                partitionRecoveryPlan.put(replica, replicaPartitions);
            } else {
                partitionRecoveryPlan.get(replica).add(partition.getPartitionId());
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
                failedTakeoverRequests.add(request.getRequestId());
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
        ClusterPartition metadataPartiton = AsterixAppContextInfo.INSTANCE.getMetadataProperties()
                .getMetadataPartition();
        //request the metadataPartition node to register itself as the metadata node
        TakeoverMetadataNodeRequestMessage takeoverRequest = new TakeoverMetadataNodeRequestMessage();
        ICCMessageBroker messageBroker = (ICCMessageBroker) AsterixAppContextInfo.INSTANCE
                .getCCApplicationContext().getMessageBroker();
        try {
            messageBroker.sendApplicationMessageToNC(takeoverRequest, metadataPartiton.getActiveNodeId());
        } catch (Exception e) {
            /**
             * if we fail to send the request, it means the NC we tried to send the request to
             * has failed. When the failure notification arrives, a new NC will be assigned to
             * the metadata partition and a new metadata node takeover request will be sent to it.
             */
            LOGGER.log(Level.WARNING,
                    "Failed to send metadata node takeover request to: " + metadataPartiton.getActiveNodeId(), e);
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
        metadataNodeActive = true;
        LOGGER.info("Current metadata node: " + currentMetadataNode);
        updateClusterState();
    }

    private synchronized void prepareFailbackPlan(String failingBackNodeId) {
        NodeFailbackPlan plan = NodeFailbackPlan.createPlan(failingBackNodeId);
        pendingProcessingFailbackPlans.add(plan);
        planId2FailbackPlanMap.put(plan.getPlanId(), plan);

        //get all partitions this node requires to resync
        AsterixReplicationProperties replicationProperties = AsterixAppContextInfo.INSTANCE
                .getReplicationProperties();
        Set<String> nodeReplicas = replicationProperties.getNodeReplicationClients(failingBackNodeId);
        for (String replicaId : nodeReplicas) {
            ClusterPartition[] nodePartitions = node2PartitionsMap.get(replicaId);
            for (ClusterPartition partition : nodePartitions) {
                plan.addParticipant(partition.getActiveNodeId());
                /**
                 * if the partition original node is the returning node,
                 * add it to the list of the partitions which will be failed back
                 */
                if (partition.getNodeId().equals(failingBackNodeId)) {
                    plan.addPartitionToFailback(partition.getPartitionId(), partition.getActiveNodeId());
                }
            }
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Prepared Failback plan: " + plan.toString());
        }

        processPendingFailbackPlans();
    }

    private synchronized void processPendingFailbackPlans() {
        /**
         * if the cluster state is not ACTIVE, then failbacks should not be processed
         * since some partitions are not active
         */
        if (state == ClusterState.ACTIVE) {
            while (!pendingProcessingFailbackPlans.isEmpty()) {
                //take the first pending failback plan
                NodeFailbackPlan plan = pendingProcessingFailbackPlans.pop();
                /**
                 * A plan at this stage will be in one of two states:
                 * 1. PREPARING -> the participants were selected but we haven't sent any request.
                 * 2. PENDING_ROLLBACK -> a participant failed before we send any requests
                 */
                if (plan.getState() == FailbackPlanState.PREPARING) {
                    //set the partitions that will be failed back as inactive
                    String failbackNode = plan.getNodeId();
                    for (Integer partitionId : plan.getPartitionsToFailback()) {
                        ClusterPartition clusterPartition = clusterPartitions.get(partitionId);
                        clusterPartition.setActive(false);
                        //partition expected to be returned to the failing back node
                        clusterPartition.setActiveNodeId(failbackNode);
                    }

                    /**
                     * if the returning node is the original metadata node,
                     * then metadata node will change after the failback completes
                     */
                    String originalMetadataNode = AsterixAppContextInfo.INSTANCE.getMetadataProperties()
                            .getMetadataNodeName();
                    if (originalMetadataNode.equals(failbackNode)) {
                        plan.setNodeToReleaseMetadataManager(currentMetadataNode);
                        currentMetadataNode = "";
                        metadataNodeActive = false;
                    }

                    //force new jobs to wait
                    state = ClusterState.REBALANCING;
                    ICCMessageBroker messageBroker = (ICCMessageBroker) AsterixAppContextInfo.INSTANCE
                            .getCCApplicationContext().getMessageBroker();
                    handleFailbackRequests(plan, messageBroker);
                    /**
                     * wait until the current plan is completed before processing the next plan.
                     * when the current one completes or is reverted, the cluster state will be
                     * ACTIVE again, and the next failback plan (if any) will be processed.
                     */
                    break;
                } else if (plan.getState() == FailbackPlanState.PENDING_ROLLBACK) {
                    //this plan failed before sending any requests -> nothing to rollback
                    planId2FailbackPlanMap.remove(plan.getPlanId());
                }
            }
        }
    }

    private void handleFailbackRequests(NodeFailbackPlan plan, ICCMessageBroker messageBroker) {
        //send requests to other nodes to complete on-going jobs and prepare partitions for failback
        for (PreparePartitionsFailbackRequestMessage request : plan.getPlanFailbackRequests()) {
            try {
                messageBroker.sendApplicationMessageToNC(request, request.getNodeID());
                plan.addPendingRequest(request);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to send failback request to: " + request.getNodeID(), e);
                plan.notifyNodeFailure(request.getNodeID());
                revertFailedFailbackPlanEffects();
                break;
            }
        }
    }

    public synchronized void processPreparePartitionsFailbackResponse(PreparePartitionsFailbackResponseMessage msg) {
        NodeFailbackPlan plan = planId2FailbackPlanMap.get(msg.getPlanId());
        plan.markRequestCompleted(msg.getRequestId());
        /**
         * A plan at this stage will be in one of three states:
         * 1. PENDING_PARTICIPANT_REPONSE -> one or more responses are still expected (wait).
         * 2. PENDING_COMPLETION -> all responses received (time to send completion request).
         * 3. PENDING_ROLLBACK -> the plan failed and we just received the final pending response (revert).
         */
        if (plan.getState() == FailbackPlanState.PENDING_COMPLETION) {
            CompleteFailbackRequestMessage request = plan.getCompleteFailbackRequestMessage();

            //send complete resync and takeover partitions to the failing back node
            ICCMessageBroker messageBroker = (ICCMessageBroker) AsterixAppContextInfo.INSTANCE
                    .getCCApplicationContext().getMessageBroker();
            try {
                messageBroker.sendApplicationMessageToNC(request, request.getNodeId());
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to send complete failback request to: " + request.getNodeId(), e);
                notifyFailbackPlansNodeFailure(request.getNodeId());
                revertFailedFailbackPlanEffects();
            }
        } else if (plan.getState() == FailbackPlanState.PENDING_ROLLBACK) {
            revertFailedFailbackPlanEffects();
        }
    }

    public synchronized void processCompleteFailbackResponse(CompleteFailbackResponseMessage reponse) {
        /**
         * the failback plan completed successfully:
         * Remove all references to it.
         * Remove the the failing back node from the failed nodes list.
         * Notify its replicas to reconnect to it.
         * Set the failing back node partitions as active.
         */
        NodeFailbackPlan plan = planId2FailbackPlanMap.remove(reponse.getPlanId());
        String nodeId = plan.getNodeId();
        failedNodes.remove(nodeId);
        //notify impacted replicas they can reconnect to this node
        notifyImpactedReplicas(nodeId, ClusterEventType.NODE_JOIN);
        updateNodePartitions(nodeId, true);
    }

    private synchronized void notifyImpactedReplicas(String nodeId, ClusterEventType event) {
        AsterixReplicationProperties replicationProperties = AsterixAppContextInfo.INSTANCE
                .getReplicationProperties();
        Set<String> remoteReplicas = replicationProperties.getRemoteReplicasIds(nodeId);
        String nodeIdAddress = "";
        //in case the node joined with a new IP address, we need to send it to the other replicas
        if (event == ClusterEventType.NODE_JOIN) {
            nodeIdAddress = activeNcConfiguration.get(nodeId).get(CLUSTER_NET_IP_ADDRESS_KEY);
        }

        ReplicaEventMessage msg = new ReplicaEventMessage(nodeId, nodeIdAddress, event);
        ICCMessageBroker messageBroker = (ICCMessageBroker) AsterixAppContextInfo.INSTANCE
                .getCCApplicationContext().getMessageBroker();
        for (String replica : remoteReplicas) {
            //if the remote replica is alive, send the event
            if (activeNcConfiguration.containsKey(replica)) {
                try {
                    messageBroker.sendApplicationMessageToNC(msg, replica);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed sending an application message to an NC", e);
                }
            }
        }
    }

    private synchronized void revertFailedFailbackPlanEffects() {
        Iterator<NodeFailbackPlan> iterator = planId2FailbackPlanMap.values().iterator();
        while (iterator.hasNext()) {
            NodeFailbackPlan plan = iterator.next();
            if (plan.getState() == FailbackPlanState.PENDING_ROLLBACK) {
                //TODO if the failing back node is still active, notify it to construct a new plan for it
                iterator.remove();

                //reassign the partitions that were supposed to be failed back to an active replica
                requestPartitionsTakeover(plan.getNodeId());
            }
        }
    }

    private synchronized void notifyFailbackPlansNodeFailure(String nodeId) {
        Iterator<NodeFailbackPlan> iterator = planId2FailbackPlanMap.values().iterator();
        while (iterator.hasNext()) {
            NodeFailbackPlan plan = iterator.next();
            plan.notifyNodeFailure(nodeId);
        }
    }

    public synchronized boolean isMetadataNodeActive() {
        return metadataNodeActive;
    }

    public boolean isReplicationEnabled() {
        if (cluster != null && cluster.getDataReplication() != null) {
            return cluster.getDataReplication().isEnabled();
        }
        return false;
    }

    public boolean isAutoFailoverEnabled() {
        return isReplicationEnabled() && cluster.getDataReplication().isAutoFailover();
    }

    public synchronized JSONObject getClusterStateDescription() throws JSONException {
        JSONObject stateDescription = new JSONObject();
        stateDescription.put("state", state.name());
        stateDescription.put("metadata_node", currentMetadataNode);
        for (Map.Entry<String, ClusterPartition[]> entry : node2PartitionsMap.entrySet()) {
            JSONObject nodeJSON = new JSONObject();
            nodeJSON.put("node_id", entry.getKey());
            List<String> partitions = new ArrayList<>();
            for (ClusterPartition part : entry.getValue()) {
                partitions.add("partition_" + part.getPartitionId());
            }
            nodeJSON.put("partitions", partitions);
            stateDescription.accumulate("ncs", nodeJSON);
        }
        return stateDescription;
    }
}
