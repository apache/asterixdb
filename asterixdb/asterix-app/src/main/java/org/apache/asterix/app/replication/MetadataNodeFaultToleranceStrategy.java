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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.app.nc.task.BindMetadataNodeTask;
import org.apache.asterix.app.nc.task.CheckpointTask;
import org.apache.asterix.app.nc.task.ExternalLibrarySetupTask;
import org.apache.asterix.app.nc.task.LocalRecoveryTask;
import org.apache.asterix.app.nc.task.MetadataBootstrapTask;
import org.apache.asterix.app.nc.task.RemoteRecoveryTask;
import org.apache.asterix.app.nc.task.ReportLocalCountersTask;
import org.apache.asterix.app.nc.task.StartLifecycleComponentsTask;
import org.apache.asterix.app.nc.task.StartReplicationServiceTask;
import org.apache.asterix.app.replication.message.MetadataNodeRequestMessage;
import org.apache.asterix.app.replication.message.MetadataNodeResponseMessage;
import org.apache.asterix.app.replication.message.NCLifecycleTaskReportMessage;
import org.apache.asterix.app.replication.message.RegistrationTasksRequestMessage;
import org.apache.asterix.app.replication.message.ReplayPartitionLogsRequestMessage;
import org.apache.asterix.app.replication.message.ReplayPartitionLogsResponseMessage;
import org.apache.asterix.app.replication.message.RegistrationTasksResponseMessage;
import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.replication.Replica;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.util.FaultToleranceUtil;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.application.IClusterLifecycleListener.ClusterEventType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetadataNodeFaultToleranceStrategy implements IFaultToleranceStrategy {

    private static final Logger LOGGER = LogManager.getLogger();
    private IClusterStateManager clusterManager;
    private String metadataNodeId;
    private IReplicationStrategy replicationStrategy;
    private ICCMessageBroker messageBroker;
    private ICCServiceContext serviceCtx;
    private final Set<String> hotStandbyMetadataReplica = new HashSet<>();
    private final Set<String> failedNodes = new HashSet<>();
    private Set<String> pendingStartupCompletionNodes = new HashSet<>();

    @Override
    public synchronized void notifyNodeJoin(String nodeId) throws HyracksDataException {
        pendingStartupCompletionNodes.add(nodeId);
    }

    @Override
    public synchronized void notifyNodeFailure(String nodeId) throws HyracksDataException {
        failedNodes.add(nodeId);
        hotStandbyMetadataReplica.remove(nodeId);
        clusterManager.updateNodePartitions(nodeId, false);
        if (nodeId.equals(metadataNodeId)) {
            clusterManager.updateMetadataNode(metadataNodeId, false);
        }
        clusterManager.refreshState();
        if (replicationStrategy.isParticipant(nodeId)) {
            // Notify impacted replica
            FaultToleranceUtil.notifyImpactedReplicas(nodeId, ClusterEventType.NODE_FAILURE, clusterManager,
                    messageBroker, replicationStrategy);
        }
        // If the failed node is the metadata node, ask its replicas to replay any committed jobs
        if (nodeId.equals(metadataNodeId)) {
            ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
            int metadataPartitionId = appCtx.getMetadataProperties().getMetadataPartition().getPartitionId();
            Set<Integer> metadataPartition = new HashSet<>(Arrays.asList(metadataPartitionId));
            Set<Replica> activeRemoteReplicas = replicationStrategy.getRemoteReplicas(metadataNodeId).stream()
                    .filter(replica -> !failedNodes.contains(replica.getId())).collect(Collectors.toSet());
            //TODO Do election to identity the node with latest state
            for (Replica replica : activeRemoteReplicas) {
                ReplayPartitionLogsRequestMessage msg = new ReplayPartitionLogsRequestMessage(metadataPartition);
                try {
                    messageBroker.sendApplicationMessageToNC(msg, replica.getId());
                } catch (Exception e) {
                    LOGGER.log(Level.WARN, "Failed sending an application message to an NC", e);
                    continue;
                }
            }
        }
    }

    @Override
    public IFaultToleranceStrategy from(ICCServiceContext serviceCtx, IReplicationStrategy replicationStrategy) {
        MetadataNodeFaultToleranceStrategy ft = new MetadataNodeFaultToleranceStrategy();
        ft.replicationStrategy = replicationStrategy;
        ft.messageBroker = (ICCMessageBroker) serviceCtx.getMessageBroker();
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
            case REPLAY_LOGS_RESPONSE:
                process((ReplayPartitionLogsResponseMessage) message);
                break;
            case METADATA_NODE_RESPONSE:
                process((MetadataNodeResponseMessage) message);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.UNSUPPORTED_MESSAGE_TYPE, message.getType().name());
        }
    }

    @Override
    public synchronized void bindTo(IClusterStateManager clusterManager) {
        this.clusterManager = clusterManager;
        this.metadataNodeId = clusterManager.getCurrentMetadataNodeId();
    }

    @Override
    public void notifyMetadataNodeChange(String node) throws HyracksDataException {
        if (metadataNodeId.equals(node)) {
            return;
        }
        // if current metadata node is active, we need to unbind its metadata proxy object
        if (clusterManager.isMetadataNodeActive()) {
            MetadataNodeRequestMessage msg = new MetadataNodeRequestMessage(false);
            try {
                messageBroker.sendApplicationMessageToNC(msg, metadataNodeId);
                // when the current node responses, we will bind to the new one
                metadataNodeId = node;
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        } else {
            requestMetadataNodeTakeover(node);
        }
    }

    private synchronized void process(ReplayPartitionLogsResponseMessage msg) {
        hotStandbyMetadataReplica.add(msg.getNodeId());
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Hot Standby Metadata Replicas: " + hotStandbyMetadataReplica);
        }
    }

    private synchronized void process(RegistrationTasksRequestMessage msg) throws HyracksDataException {
        final String nodeId = msg.getNodeId();
        final SystemState state = msg.getState();
        final boolean isParticipant = replicationStrategy.isParticipant(nodeId);
        List<INCLifecycleTask> tasks;
        if (!isParticipant) {
            tasks = buildNonParticipantStartupSequence(nodeId, state);
        } else {
            tasks = buildParticipantStartupSequence(nodeId, state);
        }
        RegistrationTasksResponseMessage response = new RegistrationTasksResponseMessage(nodeId, tasks);
        try {
            messageBroker.sendApplicationMessageToNC(response, msg.getNodeId());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private synchronized void process(NCLifecycleTaskReportMessage msg) throws HyracksDataException {
        final String nodeId = msg.getNodeId();
        pendingStartupCompletionNodes.remove(nodeId);
        if (msg.isSuccess()) {
            // If this node failed and recovered, notify impacted replicas to reconnect to it
            if (replicationStrategy.isParticipant(nodeId) && failedNodes.remove(nodeId)) {
                FaultToleranceUtil.notifyImpactedReplicas(nodeId, ClusterEventType.NODE_JOIN, clusterManager,
                        messageBroker, replicationStrategy);
            }
            clusterManager.updateNodePartitions(msg.getNodeId(), true);
            if (msg.getNodeId().equals(metadataNodeId)) {
                clusterManager.updateMetadataNode(metadataNodeId, true);
                // When metadata node is active, it is the only hot standby replica
                hotStandbyMetadataReplica.clear();
                hotStandbyMetadataReplica.add(metadataNodeId);
            }
            clusterManager.refreshState();
        } else {
            LOGGER.log(Level.ERROR, msg.getNodeId() + " failed to complete startup. ", msg.getException());
        }
    }

    private List<INCLifecycleTask> buildNonParticipantStartupSequence(String nodeId, SystemState state) {
        final List<INCLifecycleTask> tasks = new ArrayList<>();
        if (state == SystemState.CORRUPTED) {
            //need to perform local recovery for node partitions
            LocalRecoveryTask rt = new LocalRecoveryTask(Arrays.asList(clusterManager.getNodePartitions(nodeId))
                    .stream().map(ClusterPartition::getPartitionId).collect(Collectors.toSet()));
            tasks.add(rt);
        }
        tasks.add(new ExternalLibrarySetupTask(false));
        tasks.add(new ReportLocalCountersTask());
        tasks.add(new CheckpointTask());
        tasks.add(new StartLifecycleComponentsTask());
        return tasks;
    }

    private List<INCLifecycleTask> buildParticipantStartupSequence(String nodeId, SystemState state) {
        final List<INCLifecycleTask> tasks = new ArrayList<>();
        switch (state) {
            case PERMANENT_DATA_LOSS:
                if (failedNodes.isEmpty()) { //bootstrap
                    break;
                }
                // If the metadata node (or replica) failed and lost its data
                // => Metadata Remote Recovery from standby replica
                tasks.add(getMetadataPartitionRecoveryPlan());
                // Checkpoint after remote recovery to move node to HEALTHY state
                tasks.add(new CheckpointTask());
                break;
            case CORRUPTED:
                // If the metadata node (or replica) failed and started again without losing data => Local Recovery
                LocalRecoveryTask rt = new LocalRecoveryTask(Arrays.asList(clusterManager.getNodePartitions(nodeId))
                        .stream().map(ClusterPartition::getPartitionId).collect(Collectors.toSet()));
                tasks.add(rt);
                break;
            case BOOTSTRAPPING:
            case HEALTHY:
            case RECOVERING:
                break;
            default:
                break;
        }
        tasks.add(new StartReplicationServiceTask());
        final boolean isMetadataNode = nodeId.equals(metadataNodeId);
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

    private RemoteRecoveryTask getMetadataPartitionRecoveryPlan() {
        if (hotStandbyMetadataReplica.isEmpty()) {
            throw new IllegalStateException("No metadata replicas to recover from");
        }
        // Construct recovery plan: Node => Set of partitions to recover from it
        Map<String, Set<Integer>> recoveryPlan = new HashMap<>();
        // Recover metadata partition from any metadata hot standby replica
        ICcApplicationContext appCtx = (ICcApplicationContext) serviceCtx.getApplicationContext();
        int metadataPartitionId = appCtx.getMetadataProperties().getMetadataPartition().getPartitionId();
        Set<Integer> metadataPartition = new HashSet<>(Arrays.asList(metadataPartitionId));
        recoveryPlan.put(hotStandbyMetadataReplica.iterator().next(), metadataPartition);
        return new RemoteRecoveryTask(recoveryPlan);
    }

    private void process(MetadataNodeResponseMessage response) throws HyracksDataException {
        clusterManager.updateMetadataNode(response.getNodeId(), response.isExported());
        if (!response.isExported()) {
            requestMetadataNodeTakeover(metadataNodeId);
        }
    }

    private void requestMetadataNodeTakeover(String node) throws HyracksDataException {
        MetadataNodeRequestMessage msg = new MetadataNodeRequestMessage(true);
        try {
            messageBroker.sendApplicationMessageToNC(msg, node);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}