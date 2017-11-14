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
package org.apache.asterix.app.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.nc.task.BindMetadataNodeTask;
import org.apache.asterix.app.nc.task.CheckpointTask;
import org.apache.asterix.app.nc.task.ExternalLibrarySetupTask;
import org.apache.asterix.app.nc.task.MetadataBootstrapTask;
import org.apache.asterix.app.nc.task.ReportLocalCountersTask;
import org.apache.asterix.app.nc.task.StartFailbackTask;
import org.apache.asterix.app.nc.task.StartLifecycleComponentsTask;
import org.apache.asterix.app.nc.task.StartReplicationServiceTask;
import org.apache.asterix.app.replication.NodeFailbackPlan.FailbackPlanState;
import org.apache.asterix.app.replication.message.CompleteFailbackRequestMessage;
import org.apache.asterix.app.replication.message.CompleteFailbackResponseMessage;
import org.apache.asterix.app.replication.message.NCLifecycleTaskReportMessage;
import org.apache.asterix.app.replication.message.PreparePartitionsFailbackRequestMessage;
import org.apache.asterix.app.replication.message.PreparePartitionsFailbackResponseMessage;
import org.apache.asterix.app.replication.message.RegistrationTasksRequestMessage;
import org.apache.asterix.app.replication.message.RegistrationTasksResponseMessage;
import org.apache.asterix.app.replication.message.TakeoverMetadataNodeRequestMessage;
import org.apache.asterix.app.replication.message.TakeoverMetadataNodeResponseMessage;
import org.apache.asterix.app.replication.message.TakeoverPartitionsRequestMessage;
import org.apache.asterix.app.replication.message.TakeoverPartitionsResponseMessage;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.util.FaultToleranceUtil;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.application.IClusterLifecycleListener.ClusterEventType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AutoFaultToleranceStrategy implements IFaultToleranceStrategy {

    private static final Logger LOGGER = Logger.getLogger(AutoFaultToleranceStrategy.class.getName());
    private long clusterRequestId = 0;

    private Set<String> failedNodes = new HashSet<>();
    private LinkedList<NodeFailbackPlan> pendingProcessingFailbackPlans = new LinkedList<>();
    private Map<Long, NodeFailbackPlan> planId2FailbackPlanMap = new HashMap<>();
    private Map<Long, TakeoverPartitionsRequestMessage> pendingTakeoverRequests = new HashMap<>();;
    private String currentMetadataNode;
    private boolean metadataNodeActive = false;
    private IClusterStateManager clusterManager;
    private ICCMessageBroker messageBroker;
    private IReplicationStrategy replicationStrategy;
    private ICCServiceContext serviceCtx;
    private Set<String> pendingStartupCompletionNodes = new HashSet<>();

    @Override
    public void notifyNodeJoin(String nodeId) throws HyracksDataException {
        pendingStartupCompletionNodes.add(nodeId);
    }

    @Override
    public void notifyNodeFailure(String nodeId) throws HyracksDataException {
        //if this node was waiting for failback and failed before it completed
        if (failedNodes.contains(nodeId)) {
            notifyFailbackPlansNodeFailure(nodeId);
            revertFailedFailbackPlanEffects();
            return;
        }
        //an active node failed
        failedNodes.add(nodeId);
        clusterManager.updateNodePartitions(nodeId, false);
        if (nodeId.equals(currentMetadataNode)) {
            metadataNodeActive = false;
            clusterManager.updateMetadataNode(nodeId, metadataNodeActive);
        }
        validateClusterState();
        FaultToleranceUtil.notifyImpactedReplicas(nodeId, ClusterEventType.NODE_FAILURE, clusterManager, messageBroker,
                replicationStrategy);
        notifyFailbackPlansNodeFailure(nodeId);
        requestPartitionsTakeover(nodeId);
    }

    private synchronized void notifyFailbackPlansNodeFailure(String nodeId) {
        for (NodeFailbackPlan plan : planId2FailbackPlanMap.values()) {
            plan.notifyNodeFailure(nodeId);
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

    private synchronized void requestPartitionsTakeover(String failedNodeId) {
        //replica -> list of partitions to takeover
        Map<String, List<Integer>> partitionRecoveryPlan = new HashMap<>();
        ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
        ReplicationProperties replicationProperties = appCtx.getReplicationProperties();
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
                    if (addActiveReplica(replica, partition, partitionRecoveryPlan)) {
                        break;
                    }
                }
            }

            if (partitionRecoveryPlan.size() == 0) {
                //no active replicas were found for the failed node
                LOGGER.severe("Could not find active replicas for the partitions " + lostPartitions);
                return;
            } else {
                LOGGER.info("Partitions to recover: " + lostPartitions);
            }
            //For each replica, send a request to takeover the assigned partitions
            partitionRecoveryPlan.forEach((replica, value) -> {
                Integer[] partitionsToTakeover = value.toArray(new Integer[value.size()]);
                long requestId = clusterRequestId++;
                TakeoverPartitionsRequestMessage takeoverRequest = new TakeoverPartitionsRequestMessage(requestId,
                        replica, partitionsToTakeover);
                pendingTakeoverRequests.put(requestId, takeoverRequest);
                try {
                    messageBroker.sendApplicationMessageToNC(takeoverRequest, replica);
                } catch (Exception e) {
                    /*
                     * if we fail to send the request, it means the NC we tried to send the request to
                     * has failed. When the failure notification arrives, we will send any pending request
                     * that belongs to the failed NC to a different active replica.
                     */
                    LOGGER.log(Level.WARNING, "Failed to send takeover request: " + takeoverRequest, e);
                }
            });
        }
    }

    private boolean addActiveReplica(String replica, ClusterPartition partition,
            Map<String, List<Integer>> partitionRecoveryPlan) {
        final Set<String> participantNodes = clusterManager.getParticipantNodes();
        if (participantNodes.contains(replica) && !failedNodes.contains(replica)) {
            if (!partitionRecoveryPlan.containsKey(replica)) {
                List<Integer> replicaPartitions = new ArrayList<>();
                replicaPartitions.add(partition.getPartitionId());
                partitionRecoveryPlan.put(replica, replicaPartitions);
            } else {
                partitionRecoveryPlan.get(replica).add(partition.getPartitionId());
            }
            return true;
        }
        return false;
    }

    private synchronized void prepareFailbackPlan(String failingBackNodeId) {
        NodeFailbackPlan plan = NodeFailbackPlan.createPlan(failingBackNodeId);
        pendingProcessingFailbackPlans.add(plan);
        planId2FailbackPlanMap.put(plan.getPlanId(), plan);

        //get all partitions this node requires to resync
        ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
        ReplicationProperties replicationProperties = appCtx.getReplicationProperties();
        Set<String> nodeReplicas = replicationProperties.getNodeReplicasIds(failingBackNodeId);
        clusterManager.getClusterPartitons();
        for (String replicaId : nodeReplicas) {
            ClusterPartition[] nodePartitions = clusterManager.getNodePartitions(replicaId);
            for (ClusterPartition partition : nodePartitions) {
                plan.addParticipant(partition.getActiveNodeId());
                /*
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
        ClusterState state = clusterManager.getState();
        /*
         * if the cluster state is not ACTIVE, then failbacks should not be processed
         * since some partitions are not active
         */
        if (state == ClusterState.ACTIVE) {
            while (!pendingProcessingFailbackPlans.isEmpty()) {
                //take the first pending failback plan
                NodeFailbackPlan plan = pendingProcessingFailbackPlans.pop();
                /*
                 * A plan at this stage will be in one of two states:
                 * 1. PREPARING -> the participants were selected but we haven't sent any request.
                 * 2. PENDING_ROLLBACK -> a participant failed before we send any requests
                 */
                if (plan.getState() == FailbackPlanState.PREPARING) {
                    //set the partitions that will be failed back as inactive
                    String failbackNode = plan.getNodeId();
                    for (Integer partitionId : plan.getPartitionsToFailback()) {
                        //partition expected to be returned to the failing back node
                        clusterManager.updateClusterPartition(partitionId, failbackNode, false);
                    }

                    /*
                     * if the returning node is the original metadata node,
                     * then metadata node will change after the failback completes
                     */
                    ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
                    String originalMetadataNode = appCtx.getMetadataProperties().getMetadataNodeName();
                    if (originalMetadataNode.equals(failbackNode)) {
                        plan.setNodeToReleaseMetadataManager(currentMetadataNode);
                        currentMetadataNode = "";
                        metadataNodeActive = false;
                        clusterManager.updateMetadataNode(currentMetadataNode, metadataNodeActive);
                    }

                    //force new jobs to wait
                    clusterManager.setState(ClusterState.REBALANCING);
                    handleFailbackRequests(plan, messageBroker);
                    /*
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

    public synchronized List<ClusterPartition> getNodeAssignedPartitions(String nodeId) {
        List<ClusterPartition> nodePartitions = new ArrayList<>();
        ClusterPartition[] clusterPartitons = clusterManager.getClusterPartitons();
        Map<Integer, ClusterPartition> clusterPartitionsMap = new HashMap<>();
        for (ClusterPartition partition : clusterPartitons) {
            clusterPartitionsMap.put(partition.getPartitionId(), partition);
        }
        for (ClusterPartition partition : clusterPartitons) {
            if (nodeId.equals(partition.getActiveNodeId())) {
                nodePartitions.add(partition);
            }
        }
        /*
         * if there is any pending takeover request this node was supposed to handle,
         * it needs to be sent to a different replica
         */
        List<Long> failedTakeoverRequests = new ArrayList<>();
        for (TakeoverPartitionsRequestMessage request : pendingTakeoverRequests.values()) {
            if (request.getNodeId().equals(nodeId)) {
                for (Integer partitionId : request.getPartitions()) {
                    nodePartitions.add(clusterPartitionsMap.get(partitionId));
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

    public synchronized void process(TakeoverPartitionsResponseMessage response) throws HyracksDataException {
        for (Integer partitonId : response.getPartitions()) {
            clusterManager.updateClusterPartition(partitonId, response.getNodeId(), true);
        }
        pendingTakeoverRequests.remove(response.getRequestId());
        validateClusterState();
    }

    public synchronized void process(TakeoverMetadataNodeResponseMessage response) throws HyracksDataException {
        currentMetadataNode = response.getNodeId();
        metadataNodeActive = true;
        clusterManager.updateMetadataNode(currentMetadataNode, metadataNodeActive);
        validateClusterState();
    }

    private void validateClusterState() throws HyracksDataException {
        clusterManager.refreshState();
        ClusterState newState = clusterManager.getState();
        // PENDING: all partitions are active but metadata node is not
        if (newState == ClusterState.PENDING) {
            requestMetadataNodeTakeover();
        } else if (newState == ClusterState.ACTIVE) {
            processPendingFailbackPlans();
        }
    }

    public synchronized void process(PreparePartitionsFailbackResponseMessage msg) {
        NodeFailbackPlan plan = planId2FailbackPlanMap.get(msg.getPlanId());
        plan.markRequestCompleted(msg.getRequestId());
        /*
         * A plan at this stage will be in one of three states:
         * 1. PENDING_PARTICIPANT_REPONSE -> one or more responses are still expected (wait).
         * 2. PENDING_COMPLETION -> all responses received (time to send completion request).
         * 3. PENDING_ROLLBACK -> the plan failed and we just received the final pending response (revert).
         */
        if (plan.getState() == FailbackPlanState.PENDING_COMPLETION) {
            CompleteFailbackRequestMessage request = plan.getCompleteFailbackRequestMessage();

            //send complete resync and takeover partitions to the failing back node
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

    public synchronized void process(CompleteFailbackResponseMessage response) throws HyracksDataException {
        /*
         * the failback plan completed successfully:
         * Remove all references to it.
         * Remove the the failing back node from the failed nodes list.
         * Notify its replicas to reconnect to it.
         * Set the failing back node partitions as active.
         */
        NodeFailbackPlan plan = planId2FailbackPlanMap.remove(response.getPlanId());
        String nodeId = plan.getNodeId();
        failedNodes.remove(nodeId);
        //notify impacted replicas they can reconnect to this node
        FaultToleranceUtil.notifyImpactedReplicas(nodeId, ClusterEventType.NODE_JOIN, clusterManager, messageBroker,
                replicationStrategy);
        clusterManager.updateNodePartitions(nodeId, true);
        validateClusterState();
    }

    private synchronized void requestMetadataNodeTakeover() {
        //need a new node to takeover metadata node
        ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
        ClusterPartition metadataPartiton = appCtx.getMetadataProperties().getMetadataPartition();
        //request the metadataPartition node to register itself as the metadata node
        TakeoverMetadataNodeRequestMessage takeoverRequest = new TakeoverMetadataNodeRequestMessage();
        try {
            messageBroker.sendApplicationMessageToNC(takeoverRequest, metadataPartiton.getActiveNodeId());
            // Since the metadata node will be changed, we need to rebind the proxy object
            MetadataManager.INSTANCE.rebindMetadataNode();
        } catch (Exception e) {
            /*
             * if we fail to send the request, it means the NC we tried to send the request to
             * has failed. When the failure notification arrives, a new NC will be assigned to
             * the metadata partition and a new metadata node takeover request will be sent to it.
             */
            LOGGER.log(Level.WARNING,
                    "Failed to send metadata node takeover request to: " + metadataPartiton.getActiveNodeId(), e);
        }
    }

    @Override
    public IFaultToleranceStrategy from(ICCServiceContext serviceCtx, IReplicationStrategy replicationStrategy) {
        AutoFaultToleranceStrategy ft = new AutoFaultToleranceStrategy();
        ft.messageBroker = (ICCMessageBroker) serviceCtx.getMessageBroker();
        ft.replicationStrategy = replicationStrategy;
        ft.serviceCtx = serviceCtx;
        return ft;
    }

    @Override
    public synchronized void process(INCLifecycleMessage message) throws HyracksDataException {
        switch (message.getType()) {
            case REGISTRATION_TASKS_REQUEST:
                process((RegistrationTasksRequestMessage) message);
                break;
            case REGISTRATION_TASKS_RESULT:
                process((NCLifecycleTaskReportMessage) message);
                break;
            case TAKEOVER_PARTITION_RESPONSE:
                process((TakeoverPartitionsResponseMessage) message);
                break;
            case TAKEOVER_METADATA_NODE_RESPONSE:
                process((TakeoverMetadataNodeResponseMessage) message);
                break;
            case PREPARE_FAILBACK_RESPONSE:
                process((PreparePartitionsFailbackResponseMessage) message);
                break;
            case COMPLETE_FAILBACK_RESPONSE:
                process((CompleteFailbackResponseMessage) message);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.UNSUPPORTED_MESSAGE_TYPE, message.getType().name());
        }
    }

    private synchronized void process(NCLifecycleTaskReportMessage msg) throws HyracksDataException {
        final String nodeId = msg.getNodeId();
        pendingStartupCompletionNodes.remove(nodeId);
        if (msg.isSuccess()) {
            if (failedNodes.contains(nodeId)) {
                prepareFailbackPlan(nodeId);
                return;
            }
            // If this node failed and recovered, notify impacted replicas to reconnect to it
            if (replicationStrategy.isParticipant(nodeId) && failedNodes.remove(nodeId)) {
                FaultToleranceUtil.notifyImpactedReplicas(nodeId, ClusterEventType.NODE_JOIN, clusterManager,
                        messageBroker, replicationStrategy);
            }
            clusterManager.updateNodePartitions(nodeId, true);
            if (msg.getNodeId().equals(currentMetadataNode)) {
                clusterManager.updateMetadataNode(currentMetadataNode, true);
            }
            clusterManager.refreshState();
        } else {
            LOGGER.log(Level.SEVERE, msg.getNodeId() + " failed to complete startup. ", msg.getException());
        }
    }

    @Override
    public void bindTo(IClusterStateManager clusterManager) {
        this.clusterManager = clusterManager;
        currentMetadataNode = clusterManager.getCurrentMetadataNodeId();
    }

    private synchronized void process(RegistrationTasksRequestMessage msg) throws HyracksDataException {
        final String nodeId = msg.getNodeId();
        final SystemState state = msg.getState();
        List<INCLifecycleTask> tasks;
        if (state == SystemState.BOOTSTRAPPING || state == SystemState.HEALTHY) {
            tasks = buildStartupSequence(nodeId);
        } else {
            // failed node returned. Need to start failback process
            tasks = buildFailbackStartupSequence();
        }
        RegistrationTasksResponseMessage response = new RegistrationTasksResponseMessage(nodeId, tasks);
        try {
            messageBroker.sendApplicationMessageToNC(response, msg.getNodeId());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private List<INCLifecycleTask> buildFailbackStartupSequence() {
        final List<INCLifecycleTask> tasks = new ArrayList<>();
        tasks.add(new StartFailbackTask());
        tasks.add(new ReportLocalCountersTask());
        tasks.add(new StartLifecycleComponentsTask());
        return tasks;
    }

    private List<INCLifecycleTask> buildStartupSequence(String nodeId) {
        final List<INCLifecycleTask> tasks = new ArrayList<>();
        tasks.add(new StartReplicationServiceTask());
        final boolean isMetadataNode = nodeId.equals(currentMetadataNode);
        if (isMetadataNode) {
            tasks.add(new MetadataBootstrapTask());
        }
        tasks.add(new ExternalLibrarySetupTask(isMetadataNode));
        tasks.add(new ReportLocalCountersTask());
        tasks.add(new CheckpointTask());
        tasks.add(new StartLifecycleComponentsTask());
        if (isMetadataNode) {
            tasks.add(new BindMetadataNodeTask(true));
        }
        return tasks;
    }
}