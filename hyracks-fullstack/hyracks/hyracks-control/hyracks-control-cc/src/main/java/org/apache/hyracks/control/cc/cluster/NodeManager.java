/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.control.cc.cluster;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.control.IGatekeeper;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.util.annotations.Idempotent;
import org.apache.hyracks.util.annotations.NotThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@NotThreadSafe
public class NodeManager implements INodeManager {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;
    private final CCConfig ccConfig;
    private final IResourceManager resourceManager;
    private final Map<String, NodeControllerState> nodeRegistry;
    private final Map<InetAddress, Set<String>> ipAddressNodeNameMap;
    private final int nodeCoresMultiplier;
    private final IGatekeeper gatekeeper;

    public NodeManager(ClusterControllerService ccs, CCConfig ccConfig, IResourceManager resourceManager,
            IGatekeeper gatekeeper) {
        this.ccs = ccs;
        this.ccConfig = ccConfig;
        this.resourceManager = resourceManager;
        this.nodeRegistry = new LinkedHashMap<>();
        this.ipAddressNodeNameMap = new HashMap<>();
        this.nodeCoresMultiplier = ccConfig.getCoresMultiplier();
        this.gatekeeper = gatekeeper;
    }

    @Override
    public Map<InetAddress, Set<String>> getIpAddressNodeNameMap() {
        return Collections.unmodifiableMap(ipAddressNodeNameMap);
    }

    @Override
    public Collection<String> getAllNodeIds() {
        return Collections.unmodifiableSet(nodeRegistry.keySet());
    }

    @Override
    public Collection<NodeControllerState> getAllNodeControllerStates() {
        return Collections.unmodifiableCollection(nodeRegistry.values());
    }

    @Override
    public NodeControllerState getNodeControllerState(String nodeId) {
        return nodeRegistry.get(nodeId);
    }

    @Override
    public synchronized void addNode(String nodeId, NodeControllerState ncState) throws HyracksException {
        LOGGER.warn("+addNode: " + nodeId);
        if (nodeId == null || ncState == null) {
            throw HyracksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
        }
        if (!gatekeeper.isAuthorized(nodeId)) {
            throw HyracksException.create(ErrorCode.NO_SUCH_NODE, nodeId);
        }
        // Updates the node registry.
        if (nodeRegistry.containsKey(nodeId)) {
            LOGGER.warn("Node '" + nodeId + "' is already registered; failing the node then re-registering.");
            failNode(nodeId);
        }
        try {
            ncState.getNodeController().abortJobs(ccs.getCcId());
        } catch (IPCException e) {
            throw HyracksDataException.create(e);
        }
        LOGGER.info("adding node to registry");
        nodeRegistry.put(nodeId, ncState);
        // Updates the IP address to node names map.
        try {
            InetAddress ipAddress = getIpAddress(ncState);
            Set<String> nodes = ipAddressNodeNameMap.computeIfAbsent(ipAddress, k -> new HashSet<>());
            nodes.add(nodeId);
        } catch (HyracksException e) {
            // If anything fails, we ignore the node.
            nodeRegistry.remove(nodeId);
            throw e;
        }
        LOGGER.info("updating cluster capacity");
        resourceManager.update(nodeId, getAdjustedNodeCapacity(ncState.getCapacity()));
    }

    @Override
    @Idempotent
    public synchronized void removeNode(String nodeId) throws HyracksException {
        NodeControllerState ncState = nodeRegistry.remove(nodeId);
        if (ncState == null) {
            LOGGER.warn("request to remove unknown node {}; ignoring", nodeId);
        } else {
            removeNodeFromIpAddressMap(nodeId, ncState);
        }
        // Updates the cluster capacity (idempotent)
        resourceManager.update(nodeId, new NodeCapacity(0L, 0));
    }

    @Override
    public Map<String, NodeControllerInfo> getNodeControllerInfoMap() {
        Map<String, NodeControllerInfo> result = new LinkedHashMap<>();
        nodeRegistry.forEach(
                (key, ncState) -> result.put(key, new NodeControllerInfo(key, NodeStatus.ACTIVE, ncState.getDataPort(),
                        ncState.getResultPort(), ncState.getMessagingPort(), ncState.getCapacity().getCores())));
        return result;
    }

    @Override
    public synchronized Pair<Collection<String>, Collection<JobId>> removeDeadNodes() throws HyracksException {
        Set<String> deadNodes = new HashSet<>();
        Set<JobId> affectedJobIds = new HashSet<>();
        Iterator<Map.Entry<String, NodeControllerState>> nodeIterator = nodeRegistry.entrySet().iterator();
        long deadNodeNanosThreshold =
                TimeUnit.MILLISECONDS.toNanos(ccConfig.getHeartbeatMaxMisses() * ccConfig.getHeartbeatPeriodMillis());
        while (nodeIterator.hasNext()) {
            Map.Entry<String, NodeControllerState> entry = nodeIterator.next();
            String nodeId = entry.getKey();
            NodeControllerState state = entry.getValue();
            final long nanosSinceLastHeartbeat = state.nanosSinceLastHeartbeat();
            if (nanosSinceLastHeartbeat >= deadNodeNanosThreshold) {
                ensureNodeFailure(nodeId, state);
                deadNodes.add(nodeId);
                affectedJobIds.addAll(state.getActiveJobIds());
                nodeIterator.remove();
                removeNodeFromIpAddressMap(nodeId, state);
                resourceManager.update(nodeId, new NodeCapacity(0L, 0));
                LOGGER.info("{} considered dead. Last heartbeat received {}ms ago. Max miss period: {}ms", nodeId,
                        TimeUnit.NANOSECONDS.toMillis(nanosSinceLastHeartbeat),
                        TimeUnit.NANOSECONDS.toMillis(deadNodeNanosThreshold));
            }
        }
        return Pair.of(deadNodes, affectedJobIds);
    }

    public synchronized void failNode(String nodeId) throws HyracksException {
        NodeControllerState state = nodeRegistry.get(nodeId);
        Set<JobId> affectedJobIds = state.getActiveJobIds();
        // Removes the node from node map.
        nodeRegistry.remove(nodeId);
        // Removes the node from IP map.
        removeNodeFromIpAddressMap(nodeId, state);
        // Updates the cluster capacity.
        resourceManager.update(nodeId, new NodeCapacity(0L, 0));
        LOGGER.info(nodeId + " considered dead");
        IJobManager jobManager = ccs.getJobManager();
        Set<String> collection = Collections.singleton(nodeId);
        for (JobId jobId : affectedJobIds) {
            JobRun run = jobManager.get(jobId);
            if (run != null) {
                run.getExecutor().notifyNodeFailures(collection);
            }
        }
        ccs.getContext().notifyNodeFailure(collection);
    }

    @Override
    public void apply(NodeFunction nodeFunction) {
        nodeRegistry.forEach(nodeFunction::apply);
    }

    private void removeNodeFromIpAddressMap(String nodeId, NodeControllerState ncState) {
        InetAddress ipAddress;
        try {
            ipAddress = getIpAddress(ncState);
        } catch (Exception e) {
            LOGGER.warn("failed to get ip address of node {}; attempting to find it on existing nodes lists", nodeId,
                    e);
            ipAddress = findNodeIpById(nodeId);
        }
        if (ipAddress == null) {
            LOGGER.warn("failed to get ip address of node {}", nodeId);
            return;
        }
        Set<String> nodes = ipAddressNodeNameMap.get(ipAddress);
        if (nodes != null) {
            nodes.remove(nodeId);
            if (nodes.isEmpty()) {
                // Removes the ip if no corresponding node exists.
                ipAddressNodeNameMap.remove(ipAddress);
            }
        }
    }

    private InetAddress getIpAddress(NodeControllerState ncState) throws HyracksException {
        String ipAddress = (String) ncState.getConfig().get(NCConfig.Option.DATA_PUBLIC_ADDRESS.toSerializable());
        try {
            return InetAddress.getByName(ipAddress);
        } catch (UnknownHostException e) {
            throw HyracksException.create(ErrorCode.INVALID_NETWORK_ADDRESS, e, e.getMessage());
        }
    }

    private NodeCapacity getAdjustedNodeCapacity(NodeCapacity nodeCapacity) {
        return new NodeCapacity(nodeCapacity.getMemoryByteSize(), nodeCapacity.getCores() * nodeCoresMultiplier);
    }

    private void ensureNodeFailure(String nodeId, NodeControllerState state) {
        ccs.getExecutor().submit(() -> {
            try {
                LOGGER.info("Requesting node {} to shutdown to ensure failure", nodeId);
                state.getNodeController().shutdown(false);
                LOGGER.warn("Request to shutdown failed node {} succeeded. false positive heartbeat miss indication",
                        nodeId);
            } catch (Exception ex) {
                LOGGER.debug(() -> "Ignoring failure on ensuring node " + nodeId + " has failed", ex);
            }
        });
    }

    private InetAddress findNodeIpById(String nodeId) {
        for (Map.Entry<InetAddress, Set<String>> ipToNodesEntry : ipAddressNodeNameMap.entrySet()) {
            if (ipToNodesEntry.getValue().contains(nodeId)) {
                return ipToNodesEntry.getKey();
            }
        }
        return null;
    }
}
