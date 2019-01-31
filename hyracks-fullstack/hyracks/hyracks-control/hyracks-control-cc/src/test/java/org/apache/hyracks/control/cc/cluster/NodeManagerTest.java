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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.Executors;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.resource.IReadOnlyClusterCapacity;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.hyracks.control.cc.scheduler.ResourceManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class NodeManagerTest {

    private static final long NODE_MEMORY_SIZE = 1024L;
    private static final int NODE_CORES = 8;
    private static final String NODE1 = "node1";
    private static final String NODE2 = "node2";

    @Test
    public void testNormal() throws HyracksException, IPCException {
        IResourceManager resourceManager = new ResourceManager();
        final CCConfig ccConfig = makeCCConfig();
        final int coresMultiplier = 1;
        ccConfig.setCoresMultiplier(coresMultiplier);
        INodeManager nodeManager = new NodeManager(mockCcs(), ccConfig, resourceManager, nodeId -> true);
        NodeControllerState ncState1 = mockNodeControllerState(NODE1, false);
        NodeControllerState ncState2 = mockNodeControllerState(NODE2, false);

        // Verifies states after adding nodes.
        nodeManager.addNode(NODE1, ncState1);
        nodeManager.addNode(NODE2, ncState2);
        Assert.assertTrue(nodeManager.getIpAddressNodeNameMap().size() == 1);
        Assert.assertTrue(nodeManager.getAllNodeIds().size() == 2);
        Assert.assertTrue(nodeManager.getAllNodeControllerStates().size() == 2);
        Assert.assertTrue(nodeManager.getNodeControllerState(NODE1) == ncState1);
        Assert.assertTrue(nodeManager.getNodeControllerState(NODE2) == ncState2);
        Assert.assertTrue(resourceManager.getCurrentCapacity().getAggregatedMemoryByteSize() == NODE_MEMORY_SIZE * 2);
        Assert.assertTrue(resourceManager.getCurrentCapacity().getAggregatedCores() == NODE_CORES * 2);
        Assert.assertTrue(resourceManager.getMaximumCapacity().getAggregatedMemoryByteSize() == NODE_MEMORY_SIZE * 2);
        Assert.assertTrue(resourceManager.getMaximumCapacity().getAggregatedCores() == NODE_CORES * 2);

        // Verifies states after removing dead nodes.
        nodeManager.removeDeadNodes();
        verifyEmptyCluster(resourceManager, nodeManager);
    }

    @Test
    public void testAdjustedNodeCapacity() throws HyracksException, IPCException {
        IResourceManager resourceManager = new ResourceManager();
        final CCConfig ccConfig = makeCCConfig();
        final int coresMultiplier = 3;
        ccConfig.setCoresMultiplier(coresMultiplier);
        INodeManager nodeManager = new NodeManager(mockCcs(), ccConfig, resourceManager, nodeId -> true);
        NodeControllerState ncState1 = mockNodeControllerState(NODE1, false);
        NodeControllerState ncState2 = mockNodeControllerState(NODE2, false);

        // verify state after adding two nodes
        nodeManager.addNode(NODE1, ncState1);
        nodeManager.addNode(NODE2, ncState2);
        int activeNodes = 2;
        // verify adjusted cores
        Assert.assertEquals(NODE_CORES * activeNodes * coresMultiplier,
                resourceManager.getCurrentCapacity().getAggregatedCores());
        // verify unadjusted memory size
        Assert.assertEquals(NODE_MEMORY_SIZE * activeNodes,
                resourceManager.getCurrentCapacity().getAggregatedMemoryByteSize());
        // verify state after removing a node.
        nodeManager.removeNode(NODE1);
        activeNodes = 1;
        Assert.assertEquals(NODE_CORES * activeNodes * coresMultiplier,
                resourceManager.getCurrentCapacity().getAggregatedCores());
        Assert.assertEquals(NODE_MEMORY_SIZE * activeNodes,
                resourceManager.getCurrentCapacity().getAggregatedMemoryByteSize());
        // verify state after removing last node
        nodeManager.removeNode(NODE2);
        verifyEmptyCluster(resourceManager, nodeManager);
    }

    @Test
    public void testException() throws HyracksException, IPCException {
        IResourceManager resourceManager = new ResourceManager();
        INodeManager nodeManager = new NodeManager(mockCcs(), makeCCConfig(), resourceManager, nodeId -> true);
        NodeControllerState ncState1 = mockNodeControllerState(NODE1, true);

        boolean invalidNetworkAddress = false;
        // Verifies states after a failure during adding nodes.
        try {
            nodeManager.addNode(NODE1, ncState1);
        } catch (HyracksException e) {
            invalidNetworkAddress = e.getErrorCode() == ErrorCode.INVALID_NETWORK_ADDRESS;
        }
        Assert.assertTrue(invalidNetworkAddress);

        // Verifies that the cluster is empty.
        verifyEmptyCluster(resourceManager, nodeManager);
    }

    private ClusterControllerService mockCcs() throws IPCException {
        ClusterControllerService ccs = Mockito.mock(ClusterControllerService.class);
        IPCSystem ipcSystem = Mockito.mock(IPCSystem.class);
        IIPCHandle ipcHandle = Mockito.mock(IIPCHandle.class);
        Mockito.when(ccs.getClusterIPC()).thenReturn(ipcSystem);
        Mockito.when(ipcSystem.getHandle(Mockito.any(), Mockito.anyInt())).thenReturn(ipcHandle);
        Mockito.when(ccs.getExecutor()).thenReturn(Executors.newCachedThreadPool());
        return ccs;
    }

    @Test
    public void testNullNode() throws HyracksException {
        IResourceManager resourceManager = new ResourceManager();
        INodeManager nodeManager = new NodeManager(null, makeCCConfig(), resourceManager, nodeId -> true);

        boolean invalidParameter = false;
        // Verifies states after a failure during adding nodes.
        try {
            nodeManager.addNode(null, null);
        } catch (HyracksException e) {
            invalidParameter = e.getErrorCode() == ErrorCode.INVALID_INPUT_PARAMETER;
        }
        Assert.assertTrue(invalidParameter);

        // Verifies that the cluster is empty.
        verifyEmptyCluster(resourceManager, nodeManager);
    }

    private CCConfig makeCCConfig() {
        CCConfig ccConfig = new CCConfig();
        ccConfig.setHeartbeatMaxMisses(0);
        return ccConfig;
    }

    private NodeControllerState mockNodeControllerState(String nodeId, boolean invalidIpAddr) {
        NodeControllerState ncState = mock(NodeControllerState.class);
        NodeControllerRemoteProxy ncProxy = Mockito.mock(NodeControllerRemoteProxy.class);
        String ipAddr = invalidIpAddr ? "255.255.255:255" : "127.0.0.2";
        NetworkAddress dataAddr = new NetworkAddress(ipAddr, 1001);
        NetworkAddress resultAddr = new NetworkAddress(ipAddr, 1002);
        NetworkAddress msgAddr = new NetworkAddress(ipAddr, 1003);
        when(ncState.getCapacity()).thenReturn(new NodeCapacity(NODE_MEMORY_SIZE, NODE_CORES));
        when(ncState.getDataPort()).thenReturn(dataAddr);
        when(ncState.getResultPort()).thenReturn(resultAddr);
        when(ncState.getMessagingPort()).thenReturn(msgAddr);
        when(ncState.getConfig())
                .thenReturn(Collections.singletonMap(NCConfig.Option.DATA_PUBLIC_ADDRESS.toSerializable(), ipAddr));
        Mockito.when(ncState.getNodeController()).thenReturn(ncProxy);
        return ncState;
    }

    private void verifyEmptyCluster(IResourceManager resourceManager, INodeManager nodeManager) {
        Assert.assertTrue(nodeManager.getIpAddressNodeNameMap().isEmpty());
        Assert.assertTrue(nodeManager.getAllNodeIds().isEmpty());
        Assert.assertTrue(nodeManager.getAllNodeControllerStates().isEmpty());
        Assert.assertTrue(nodeManager.getNodeControllerState(NODE1) == null);
        Assert.assertTrue(nodeManager.getNodeControllerState(NODE2) == null);

        IReadOnlyClusterCapacity currentCapacity = resourceManager.getCurrentCapacity();
        IReadOnlyClusterCapacity maximumCapacity = resourceManager.getMaximumCapacity();
        Assert.assertTrue(currentCapacity.getAggregatedMemoryByteSize() == 0L);
        Assert.assertTrue(currentCapacity.getAggregatedCores() == 0);
        Assert.assertTrue(maximumCapacity.getAggregatedMemoryByteSize() == 0L);
        Assert.assertTrue(maximumCapacity.getAggregatedCores() == 0);
        verifyNodeNotExistInCapacity(currentCapacity, NODE1);
        verifyNodeNotExistInCapacity(currentCapacity, NODE2);
        verifyNodeNotExistInCapacity(maximumCapacity, NODE1);
        verifyNodeNotExistInCapacity(maximumCapacity, NODE1);
    }

    private void verifyNodeNotExistInCapacity(IReadOnlyClusterCapacity capacity, String nodeId) {
        boolean nodeNotExist = false;
        try {
            capacity.getMemoryByteSize(nodeId);
        } catch (HyracksException e) {
            nodeNotExist = e.getErrorCode() == ErrorCode.NO_SUCH_NODE;
        }
        Assert.assertTrue(nodeNotExist);
        nodeNotExist = false;
        try {
            capacity.getCores(nodeId);
        } catch (HyracksException e) {
            nodeNotExist = e.getErrorCode() == ErrorCode.NO_SUCH_NODE;
        }
        Assert.assertTrue(nodeNotExist);
    }

    @Test
    public void testUnauthorized() throws HyracksException, IPCException {
        IResourceManager resourceManager = new ResourceManager();
        final CCConfig ccConfig = makeCCConfig();
        INodeManager nodeManager = new NodeManager(mockCcs(), ccConfig, resourceManager, NODE1::equals);
        NodeControllerState ncState1 = mockNodeControllerState(NODE1, false);
        NodeControllerState ncState2 = mockNodeControllerState(NODE2, false);

        nodeManager.addNode(NODE1, ncState1);
        boolean nodeNotExist = false;
        try {
            nodeManager.addNode(NODE2, ncState2);
        } catch (HyracksException e) {
            nodeNotExist = e.getErrorCode() == ErrorCode.NO_SUCH_NODE;
        }
        Assert.assertTrue(nodeNotExist);
        Assert.assertTrue(nodeManager.getIpAddressNodeNameMap().size() == 1);
        Assert.assertTrue(nodeManager.getAllNodeIds().size() == 1);
        Assert.assertTrue(nodeManager.getAllNodeControllerStates().size() == 1);
        Assert.assertTrue(nodeManager.getNodeControllerState(NODE1) == ncState1);
    }

}
